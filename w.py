import time, threading, json
from collections import defaultdict, deque
import websocket
import yaml
import os
from binance.client import Client
from dotenv import load_dotenv
from advanced_crypto_screener2 import analyze_market, apply_ruleA

# تحميل مفاتيح Binance من .env
load_dotenv()
api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")
client = Client(api_key, api_secret, testnet=False)

# تحميل إعدادات config.yaml
def load_config(config_path="config.yaml"):
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

CONFIG = load_config("config.yaml")
WHITELIST = [s.upper() for s in CONFIG.get("whitelist", [])]
BASE_QUOTE = "USDT"
INTERVAL = "1h"
KLINE_LIMIT = 300
MIN_QUOTE_VOLUME = CONFIG.get("min_trade_usd", 1000.0)
SCAN_PAUSE_SEC = CONFIG.get("scan_interval_min", 15) * 60  # ربع ساعة (15 دقيقة)
POSITION_SIZE = 500.0  # المبلغ لكل صفقة
LIQ_WINDOW_SEC = 12 * 3600  # 12 ساعة
NET_LIQ_THRESHOLD = 20000

SYMBOLS = WHITELIST
THRESHOLD = 1000
data = defaultdict(lambda: {"b": 0.0, "s": 0.0, "dq": deque()})
lock = threading.Lock()
entered_symbols = {}  # {'SYMBOL': اخر توقيت دخول للعملة}

def get_lot_size(symbol):
    info = client.get_symbol_info(symbol)
    for f in info['filters']:
        if f['filterType'] == 'LOT_SIZE':
            minQty = float(f['minQty'])
            stepSize = float(f['stepSize'])
            return minQty, stepSize
    return 0.0, 1.0

def adjust_quantity(qty, step):
    precision = abs(str(step)[::-1].find('.'))
    return float(format((qty // step) * step, f'.{precision}f'))

def cleanup(sym):
    now = time.time()
    d = data[sym]
    dq = d["dq"]
    while dq and now - dq[0][0] > LIQ_WINDOW_SEC:
        _, val, side = dq.popleft()
        if side == "B":
            d["b"] -= val
        else:
            d["s"] -= val

def on_message(ws, msg):
    m = json.loads(msg)
    if "stream" not in m:
        return
    t = m["data"]
    sym = t["s"].upper()
    if sym not in SYMBOLS:
        return
    p = float(t["p"])
    q = float(t["q"])
    val = p * q
    side = "B" if not t["m"] else "S"
    if val < THRESHOLD:
        return
    with lock:
        cleanup(sym)
        d = data[sym]
        if side == "B":
            d["b"] += val
        else:
            d["s"] += val
        d["dq"].append((time.time(), val, side))

def ws_loop():
    streams = "/".join(f"{s.lower()}@aggTrade" for s in SYMBOLS)
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    ws = websocket.WebSocketApp(url, on_message=on_message)
    ws.run_forever()

def get_net_liq(sym):
    with lock:
        d = data.get(sym, {})
        return d.get("b", 0) - d.get("s", 0)

def print_entry_exit(symbol, entry, stop, target):
    print(f"🔹 عملة: {symbol}")
    print(f"🔸 نقطة الدخول: {entry:.6f}")
    print(f"🔸 وقف الخسارة: {stop:.6f}")
    print(f"🔸 أخذ الربح:   {target:.6f}")
    print(f"🔸 مقدار الحركة للوقف/الهدف: 3%")

def place_stop_loss(symbol, qty, stop_price):
    # Limit price يجب أن يكون أقل قليلاً من stop_price للتنفيذ المضمون
    limit_price = stop_price * 0.9995
    try:
        order = client.create_order(
            symbol=symbol,
            side='SELL',
            type='STOP_LOSS_LIMIT',
            quantity=qty,
            price=f"{limit_price:.6f}",
            stopPrice=f"{stop_price:.6f}",
            timeInForce='GTC'
        )
        print(f"✅ وضع أمر وقف خسارة على {stop_price:.6f} ({symbol})")
        return order
    except Exception as e:
        print(f"❌ فشل وضع أمر وقف الخسارة: {e}")

def place_take_profit(symbol, qty, target_price):
    try:
        order = client.create_order(
            symbol=symbol,
            side='SELL',
            type='LIMIT',
            quantity=qty,
            price=f"{target_price:.6f}",
            timeInForce='GTC'
        )
        print(f"✅ وضع أمر أخذ ربح على {target_price:.6f} ({symbol})")
        return order
    except Exception as e:
        print(f"❌ فشل وضع أمر أخذ الربح: {e}")

def execute_order(symbol, entry_price):
    min_qty, step_size = get_lot_size(symbol)
    raw_qty = POSITION_SIZE / entry_price
    qty = adjust_quantity(raw_qty, step_size)
    if qty < min_qty:
        print(f"❌ الكمية ({qty}) أقل من الحد الأدنى {min_qty} للشراء في {symbol}. الصفقة مهملة!")
        return
    print(f"🔺 تنفيذ صفقة شراء على {symbol} (${POSITION_SIZE}) بكمية {qty}")
    try:
        order = client.create_order(
            symbol=symbol,
            side="BUY",
            type="MARKET",
            quantity=qty
        )
        print("✅ تم تنفيذ الشراء بنجاح!")

        # حساب الهدف والوقف حسب مواصفاتك (+3% و -3%)
        stop_loss = entry_price * 0.97
        take_profit = entry_price * 1.03
        print_entry_exit(symbol, entry_price, stop_loss, take_profit)

        # ضع أوامر البيع مباشرة بعد الشراء
        place_stop_loss(symbol, qty, stop_loss)
        place_take_profit(symbol, qty, take_profit)
    except Exception as e:
        print(f"❌ خطأ في تنفيذ صفقة الشراء: {e}")

def cleanup_entered_symbols():
    """تنظيف العملات المدخولة قبل 12 ساعة."""
    now = time.time()
    to_del = [sym for sym, t in entered_symbols.items() if now - t > LIQ_WINDOW_SEC]
    for sym in to_del:
        del entered_symbols[sym]

def scanner_loop():
    while True:
        print('\n⏳ سكان عملات (كل ربع ساعة)...')
        df_scan = analyze_market(
            base_quote=BASE_QUOTE,
            interval=INTERVAL,
            kline_limit=KLINE_LIMIT,
            min_quote_volume=MIN_QUOTE_VOLUME,
            max_symbols=len(WHITELIST),
            top_n=None,
            mode="fast"
        )
        cleanup_entered_symbols()  # تنظيف العملات القديمة من الدخولات السابقة

        if df_scan.empty:
            print("❌ سكان فارغ!")
            time.sleep(SCAN_PAUSE_SEC)
            continue

        df_signal = df_scan[df_scan['symbol'].isin(WHITELIST)]
        df_signal = df_signal[df_signal.apply(apply_ruleA, axis=1)]

        if df_signal.empty:
            print("⚠️ لا عملة اجتازت الرول 1.")
            time.sleep(SCAN_PAUSE_SEC)
            continue

        print(f"🚦 عملات اجتازت RuleA: {[str(s) for s in df_signal['symbol']]}")

        entries_this_run = 0

        for idx, row in df_signal.iterrows():
            symbol = row['symbol']
            net_liq = get_net_liq(symbol)
            if net_liq > NET_LIQ_THRESHOLD:
                last_entry = entered_symbols.get(symbol)
                if last_entry is not None and time.time() - last_entry < LIQ_WINDOW_SEC:
                    print(f"🚫 تم الدخول على {symbol} خلال آخر 12 ساعة.")
                    continue
                entry_price = float(row['price'])
                execute_order(symbol, entry_price)
                entered_symbols[symbol] = time.time()
                entries_this_run += 1
            else:
                print(f"🚫 net_liq غير كافي ({net_liq:,.0f}) للرمز {symbol}")

        if entries_this_run == 0:
            print("⚠️ لم تدخل أي عملة جديدة في هذه الدورة.")

        print(f"🕒 سكان جديد بعد {SCAN_PAUSE_SEC//60} دقيقة...")
        time.sleep(SCAN_PAUSE_SEC)

if __name__ == "__main__":
    t = threading.Thread(target=ws_loop, daemon=True)
    t.start()
    scanner_loop()

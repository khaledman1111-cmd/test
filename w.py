import time
import threading
import json
from collections import defaultdict, deque
import websocket
import yaml
import os
from binance.client import Client
from dotenv import load_dotenv
from advanced_crypto_screener2 import analyze_market, apply_ruleA

# تحميل المفاتيح من .env
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
SCAN_PAUSE_SEC = CONFIG.get("scan_interval_min", 15) * 60  # كل ربع ساعة

POSITION_SIZE = CONFIG.get("position_size_usd", 500.0)  # مبلغ الصفقة من الكونفج
STOP_LOSS_PCT = CONFIG.get("sl_pct", 0.05)   # نسبة وقف الخسارة (5%) من الكونفج

LIQ_WINDOW_SEC = 12 * 3600  # 12 ساعة
NET_LIQ_THRESHOLD = CONFIG.get("min_net_usd", 10000)

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

def print_entry_exit(symbol, entry, stop):
    print(f"🔹 عملة: {symbol}")
    print(f"🔸 نقطة الدخول: {entry:.6f}")
    print(f"🔸 وقف الخسارة: {stop:.6f}")
    print(f"🔸 مقدار الحركة للوقف: {STOP_LOSS_PCT*100:.1f}%")

def place_stop_limit_order(symbol, qty, stop_price, limit_price, precision_price):
    try:
        order = client.create_order(
            symbol=symbol,
            side='SELL',
            type='STOP_LOSS_LIMIT',
            quantity=qty,
            price=f"{limit_price:.{precision_price}f}",
            stopPrice=f"{stop_price:.{precision_price}f}",
            timeInForce='GTC'
        )
        print(f"✅ أمر ستوب-ليميت على {symbol} تم بنجاح")
        return order
    except Exception as e:
        print(f"⚠️ فشل أمر ستوب-ليميت: {e}")
        with open("failed_sell_orders.log", "a") as f:
            f.write(f"{time.ctime()} - STOP LIMIT FAIL - {symbol}: qty={qty}, error={e}\n")

def execute_order(symbol, entry_price):
    min_qty, step_size = get_lot_size(symbol)
    raw_qty = POSITION_SIZE / entry_price
    qty = adjust_quantity(raw_qty, step_size)
    if qty < min_qty:
        print(f"❌ الكمية ({qty}) أقل من الحد الأدنى {min_qty} للشراء في {symbol}. الصفقة مهملة!")
        return
    print(f"🔺 شراء على {symbol} (${POSITION_SIZE}) بكمية {qty}")
    try:
        buy_order = client.create_order(
            symbol=symbol,
            side="BUY",
            type="MARKET",
            quantity=qty
        )
        print("✅ شراء العملة بنجاح!")

        # انتظر لترحيل الرصيد
        time.sleep(2)
        base_asset = client.get_symbol_info(symbol)['baseAsset']
        account_balance = client.get_asset_balance(asset=base_asset)
        real_qty = float(account_balance['free'])
        real_qty = (real_qty // step_size) * step_size
        real_qty = float(format(real_qty, f".{abs(str(step_size)[::-1].find('.'))}f"))

        print(f"➡️ الرصيد الفعلي بعد الشراء: {real_qty}, الحد الأدنى للتداول: {min_qty}")

        if real_qty < min_qty:
            print(f"⚠️ الرصيد ({real_qty}) أقل من الحد الأدنى ({min_qty}) ولن يتم وضع أمر بيع.")
            with open("failed_sell_orders.log", "a") as f:
                f.write(f"{time.ctime()} - SELL SKIPPED - {symbol}: qty={real_qty}, min_qty={min_qty}\n")
            return

        # وقف الخسارة عند 5%
        stop_loss = entry_price * (1-STOP_LOSS_PCT)
        stop_limit_price = stop_loss * 0.9995

        info = client.get_symbol_info(symbol)
        price_tick = [float(f['tickSize']) for f in info['filters'] if f['filterType']=='PRICE_FILTER'][0]
        precision_price = abs(str(price_tick)[::-1].find('.'))

        stop_loss = float(format(stop_loss, f".{precision_price}f"))
        stop_limit_price = float(format(stop_limit_price, f".{precision_price}f"))

        if stop_loss <= stop_limit_price:
            stop_limit_price = stop_loss - price_tick
            stop_limit_price = float(format(stop_limit_price, f".{precision_price}f"))
            if stop_limit_price <= 0:
                print(f"⚠️ stop_limit_price صغير جداً في {symbol}. لن يتم وضع أمر بيع.")
                with open("failed_sell_orders.log", "a") as f:
                    f.write(f"{time.ctime()} - STOPLIMIT_LOW - {symbol}: stop_limit_price={stop_limit_price}\n")
                return

        print_entry_exit(symbol, entry_price, stop_loss)

        # أمر ستوب لوز فقط
        try:
            place_stop_limit_order(symbol, real_qty, stop_loss, stop_limit_price, precision_price)
        except Exception as e:
            print(f"⚠️ لم يتم وضع أمر البيع! {e}. السكربت سيستمر...")
            with open("failed_sell_orders.log", "a") as f:
                f.write(f"{time.ctime()} - STOPLIMIT SELL ERROR - {symbol}: qty={real_qty}, error={e}\n")
    except Exception as e:
        print(f"⚠️ خطأ بالشراء أو أثناء التنفيذ {e}. السكربت سيكمل العمل بشكل طبيعي.")
        with open("failed_sell_orders.log", "a") as f:
            f.write(f"{time.ctime()} - BUY ERROR - {symbol}: error={e}\n")

def cleanup_entered_symbols():
    now = time.time()
    to_del = [sym for sym, t in entered_symbols.items() if now - t > LIQ_WINDOW_SEC]
    for sym in to_del:
        del entered_symbols[sym]

def scanner_loop():
    while True:
        print('\n⏳ سكان عملات (كل ربع ساعة)...')
        try:
            df_scan = analyze_market(
                base_quote=BASE_QUOTE,
                interval=INTERVAL,
                kline_limit=KLINE_LIMIT,
                min_quote_volume=MIN_QUOTE_VOLUME,
                max_symbols=len(WHITELIST),
                top_n=None,
                mode="fast"
            )
        except Exception as e:
            print(f"⚠️ تحليل السوق فشل: {e}. ننتظر الدورة التالية.")
            time.sleep(SCAN_PAUSE_SEC)
            continue

        cleanup_entered_symbols()

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

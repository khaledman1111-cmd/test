
import time, threading, json
from collections import defaultdict, deque
import websocket
import yaml
from advanced_crypto_screener2 import analyze_market, apply_ruleA

from dotenv import load_dotenv
import os
from binance.client import Client

# ==== تحميل مفاتيح Binance من .env ====
load_dotenv()
api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")
client = Client(api_key, api_secret, testnet=False)

# ==== تحميل الإعدادات ====
def load_config(config_path="config.yaml"):
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

CONFIG = load_config("config.yaml")
WHITELIST = [s.upper() for s in CONFIG.get("whitelist", [])]
BASE_QUOTE = "USDT"
INTERVAL = "1h"
KLINE_LIMIT = 300
MIN_QUOTE_VOLUME = CONFIG.get("min_trade_usd", 1000.0)
SCAN_PAUSE_SEC = CONFIG.get("scan_interval_min", 30) * 60
POSITION_SIZE = CONFIG.get("position_size_usd", 50.0)
LIQ_WINDOW_SEC = 12 * 3600
NET_LIQ_THRESHOLD = 10000

SYMBOLS = WHITELIST
THRESHOLD = 1000
data = defaultdict(lambda: {"b":0.0,"s":0.0,"dq":deque()})
lock = threading.Lock()

# ضبط الكمية وفق قواعد LOT_SIZE لكل عملة
def get_lot_size(symbol):
    info = client.get_symbol_info(symbol)
    for f in info['filters']:
        if f['filterType'] == 'LOT_SIZE':
            minQty = float(f['minQty'])
            stepSize = float(f['stepSize'])
            return minQty, stepSize
    return 0.0, 1.0

def adjust_quantity(qty, step):
    # يجعل الكمية مضاعفات stepSize (مع تقريب صحيح جداً)
    precision = abs(str(step)[::-1].find('.'))
    return float(format((qty // step) * step, f'.{precision}f'))

def cleanup(sym):
    now = time.time()
    d = data[sym]; dq = d["dq"]
    while dq and now - dq[0][0] > LIQ_WINDOW_SEC:
        _, val, side = dq.popleft()
        if side == "B": d["b"] -= val
        else:           d["s"] -= val

def on_message(ws, msg):
    m = json.loads(msg)
    if "stream" not in m: return
    t = m["data"]
    sym = t["s"].upper()
    if sym not in SYMBOLS: return
    p = float(t["p"]); q = float(t["q"])
    val = p*q; side = "B" if not t["m"] else "S"
    if val < THRESHOLD: return
    with lock:
        cleanup(sym)
        d = data[sym]
        if side == "B": d["b"] += val
        else:           d["s"] += val
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

def print_entry_exit(row):
    price = row['price']
    stop_loss = row['stop_loss']
    target_1 = row['target_1']
    rr_t1 = row['rr_t1']
    print(f"🔹 نقطة الدخول: {price:.6f}")
    print(f"🔸 وقف الخسارة: {stop_loss:.6f}")
    print(f"🔸 أخذ الربح:   {target_1:.6f}")
    print(f"🔸 نسبة RR:     {rr_t1}")

def execute_order(symbol, price, position_size_usd):
    min_qty, step_size = get_lot_size(symbol)
    raw_qty = position_size_usd / price
    qty = adjust_quantity(raw_qty, step_size)
    if qty < min_qty:
        print(f"❌ الكمية ({qty}) أقل من الحد الأدنى المسموح {min_qty} لـ {symbol}. لن يتم تنفيذ الصفقة!")
        return
    print(f"🔺 تنفيذ صفقة شراء حقيقة على {symbol} (${position_size_usd}) بكمية {qty}")
    try:
        order = client.create_order(
            symbol=symbol,
            side="BUY",
            type="MARKET",
            quantity=qty
        )
        print("✅ تم تنفيذ الصفقة!", order)
    except Exception as e:
        print(f"❌ خطأ في تنفيذ الصفقة: {e}")

def scanner_loop():
    while True:
        print('\n⏳ سكان عملات (كل نص ساعة)...')
        df_scan = analyze_market(
            base_quote=BASE_QUOTE,
            interval=INTERVAL,
            kline_limit=KLINE_LIMIT,
            min_quote_volume=MIN_QUOTE_VOLUME,
            max_symbols=len(WHITELIST),
            top_n=None,
            mode="fast"
        )
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

        for idx, row in df_signal.iterrows():
            symbol = row['symbol']
            net_liq = get_net_liq(symbol)
            print(f"- {symbol} | net_liq = {net_liq:,.0f}")
            if net_liq > NET_LIQ_THRESHOLD:
                print(f"✅ دخول حقيقي على {symbol} .. net_liq قوي!")
                print_entry_exit(row)
                print(f"🔺 حجم الصفقة: {POSITION_SIZE}$")
                execute_order(symbol, row['price'], POSITION_SIZE)
                break
            else:
                print(f"🚫 net_liq غير كافي ({net_liq:,.0f}) للعملة {symbol}")

        print(f"🕒 سكان جديد بعد {SCAN_PAUSE_SEC//60} دقيقة...")
        time.sleep(SCAN_PAUSE_SEC)

if __name__ == "__main__":
    t = threading.Thread(target=ws_loop, daemon=True)
    t.start()
    scanner_loop()

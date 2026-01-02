from binance.client import Client
from dotenv import load_dotenv
import os

load_dotenv()

api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")

if not api_key or not api_secret:
    raise ValueError("API_KEY or API_SECRET missing! تأكد من ملف .env في نفس مكان السكربت.")

client = Client(api_key, api_secret, testnet=False)

symbol = "PHAUSDT"
usd_amount = 10.0  # قيمة الشراء بالدولار

# جلب معلومات السوق وضبط الكمية حسب LOT_SIZE
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

# جلب سعر السوق الحالي
price = float(client.get_symbol_ticker(symbol=symbol)["price"])

min_qty, step_size = get_lot_size(symbol)
raw_qty = usd_amount / price
qty = adjust_quantity(raw_qty, step_size)

if qty < min_qty:
    print(f"❌ الكمية المطلوبة ({qty}) أقل من الحد الأدنى {min_qty} للشراء في {symbol}")
else:
    print(f"🔺 تنفيذ شراء لسعر {symbol} بسعر السوق، بكمية {qty} ({usd_amount}$)")
    try:
        order = client.create_order(
            symbol=symbol,
            side="BUY",
            type="MARKET",
            quantity=qty
        )
        print("✅ تم تنفيذ الصفقة بنجاح!")
        print(order)
    except Exception as e:
        print(f"❌ حصل خطأ أثناء التنفيذ: {e}")

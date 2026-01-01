
import time
import yaml
from advanced_crypto_screener2 import analyze_market, apply_ruleA, analyze_order_book

# ==== تحميل إعداداتك من config.yaml ====
def load_config(config_path="config.yaml"):
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

CONFIG = load_config("config.yaml")

BASE_QUOTE = "USDT"
INTERVAL = "1h"
KLINE_LIMIT = 300
MIN_QUOTE_VOLUME = CONFIG.get("min_trade_usd", 1000.0)
SCAN_PAUSE_SEC = CONFIG.get("scan_interval_min", 30) * 60
ORDER_BOOK_PRESSURE_THRESHOLD = 8.0  # شرط ضغط السيولة المناسب، عدّل لو أردت
ORDER_BOOK_SIGNAL_ALLOWED = ['buy', 'strong_buy']

WHITELIST = [s.upper() for s in CONFIG.get("whitelist", [])] if CONFIG.get("whitelist") else None

def main():
    while True:
        print('\n⏳ بدء سكان السوق...')
        # 1. سحب العملات ودراستها من السوق
        df_scan = analyze_market(
            base_quote=BASE_QUOTE,
            interval=INTERVAL,
            kline_limit=KLINE_LIMIT,
            min_quote_volume=MIN_QUOTE_VOLUME,
            max_symbols=len(WHITELIST) if WHITELIST else 500,
            top_n=None,
            mode="fast"
        )
        
        if df_scan.empty:
            print("❌ لا يوجد نتائج سكان! سيتم الانتظار للسكان التالي...")
            time.sleep(SCAN_PAUSE_SEC)
            continue

        # 2. فلترة العملات فقط عبر الوايت ليست ثم RuleA
        df_signal = df_scan[df_scan['symbol'].isin(WHITELIST)] if WHITELIST else df_scan
        df_signal = df_signal[df_signal.apply(apply_ruleA, axis=1)]

        if df_signal.empty:
            print("⚠️ لم يتم اجتياز أي عملة RuleA بعد الوايت ليست.")
            time.sleep(SCAN_PAUSE_SEC)
            continue

        print(f"🚦 عملات اجتازت RuleA: {[str(s) for s in df_signal['symbol']]}")

        # 3. فحص السيولة اللحظي (order book)
        entry_candidates = []
        for _idx, row in df_signal.iterrows():
            symbol = row['symbol']
            ob = analyze_order_book(symbol)
            pressure = ob.get('pressure', 0)
            signal = ob.get('signal', '')
            print(f"- {symbol} | ضغط سيولة: {pressure:.2f}% | إشارة: {signal}")
            if abs(pressure) >= ORDER_BOOK_PRESSURE_THRESHOLD and signal in ORDER_BOOK_SIGNAL_ALLOWED:
                entry_candidates.append(symbol)
                print(f"  ✅ {symbol} يحقق RuleA والسيولة")

        # 4. تنفيذ الصفقة (هنا تضيف منطقك للشراء أو الإشارة)
        if entry_candidates:
            print(f"\n🚀 العملات المؤهلة للتنفيذ الآن: {entry_candidates}")
            # مثال: execute_order(entry_candidates[0])
        else:
            print("⚠️ لا يوجد عملة اجتازت شرط السيولة النهائي.")

        print(f"🕒 انتهاء سكان، سيتم تكرار العملية بعد {SCAN_PAUSE_SEC//60} دقيقة...")
        time.sleep(SCAN_PAUSE_SEC)

if __name__ == "__main__":
    main()

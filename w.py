#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Advanced Educational Crypto Screener v3.1 - الاحترافي
===============================================================================
هذا السكربت لأغراض تعليمية فقط. لا يمثل نصيحة استثمارية. استخدمه على مسؤوليتك الخاصة.
"""

import time
from typing import List, Dict, Any, Optional
import random
import requests
import pandas as pd
import numpy as np
from collections import defaultdict

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 220)
pd.set_option("display.max_colwidth", None)

# --- [1] قوائم العملات المحظورة ----------------------
STABLECOINS = [
    "USDT", "USDC", "BUSD", "TUSD", "USDP", "DAI", "USDD", "USDN",
    "GUSD", "HUSD", "SUSD", "LUSD", "FRAX", "USDJ", "USDK",
    "PAX", "PAXG", "EURS", "EURT", "XAUT", "USTC", "FDUSD", "EURI"
]
GAMBLING_COINS = [
    "WINK", "BET", "BFG", "DEGEN", "FUN", "RLB", "SX", "LAZIO", "BTTC", "BNB", "BTC", "ETC", "ETH", "BCH", "ZEC", "ZEN", "APT", "DOGE", "LTC","XRP","ADA"
]
MEME_COINS = [
    "SHIB", "DOGE", "FLOKI", "PEPE", "BONK"
]
SCAM_SUSPICIOUS = [
    "SQUID", "SAFEMOON", "PORN", "XXX", "WEED", "POT",
    "CANNABIS", "DRUG", "COCAINE"
]
LEVERAGED_TOKENS = [
    "UP", "DOWN", "BULL", "BEAR", "ETHUP", "ETHDOWN",
    "BTCUP", "BTCDOWN", "BNBUP", "BNBDOWN"
]
SPORTS_FAN_TOKENS = [
    "ACM", "ASR", "ATM", "BAR", "CITY", "JUV", "PSG", "CHZ"
]
GAMING_SUSPICIOUS = [
    "AXS", "SLP", "ALICE", "TLM"
]
PONZI_MLM = [
    "HEX", "PULSE", "BITCONNECT", "ONECOIN"
]
PRIVACY_SUSPICIOUS = [
    "XMR", "FIRO"
]
WRAPPED_TOKENS = [
    "WBTC", "WETH", "WBNB"
]
FORK_COINS = [
    "BSV", "BTG", "ETHW", "COCOS"
]
BAD_REPUTATION = [
    "0X0", "0XGEN", "1000CHEEMS", "1CAT", "1MBABYDOGE", "2DAI", "47", "67", "AA", "AAPLX", "ABOND", "ACE", "ACQ", "ACS", "ACX", "AERO", "AEVO", "AFD", "AFK", "AGENTS", "AGLA", "AGX", "AI", "AIBB", "AIC", "AIDOGE", "AIKEK", "AIMBOT", "AIPAD", "AIRTOK", "AIS", "AIT", "AKROPOLIS", "AL", "ALCHEMIX", "ALLO", "AMI", "AMPL", "AMU", "ANC", "ANI", "ANIME", "ANOME", "ANON", "ANTS", "AOG", "AP", "APEX", "APR", "APX", "AQT", "ARBS", "ARIX", "ARKI", "ASC", "ASSETS", "AST", "ASTER", "ASTRA", "ATN", "ATOR", "AUCTION", "AUDIO", "AUTOLAYER", "AVL", "AVNT", "AXM", "BABY", "BABYDOGE", "BADGER", "BAKE", "BAL", "BAN", "BANANA", "BANANAS31", "BANK", "BARD", "BAS", "BASEX", "BB", "BCE", "BCP", "BDIN", "BEAT", "BEER", "BEER2", "BEFI", "BERA", "BERRY", "BETA", "BETH", "BFUSD", "BGB", "BGSOL", "BIFI", "BIRD", "BISO", "BIT", "BLAST", "BLENDR", "BLIND", "BLM", "BLOCK", "BLUM", "BMX", "BNC", "BNSOL", "BNT", "BNX", "BOAM", "BOB", "BOME", "BOND", "BONE", "BONK", "BORING", "BOT", "BPX", "BR", "BRETT", "BROCCOLI", "BROCCOLI714", "BROCK", "BSW", "BSX", "BTG", "BTR", "BTS", "BTX", "BUBB", "BUILD", "BULLA", "BURGER", "C98", "CAKE", "CANTO", "CAS", "CAT", "CC", "CDL", "CDT", "CEEK", "CEL", "CET", "CETUS", "CFG", "CGN", "CGV", "CGX", "CHAT", "CHEEL", "CHESS", "CHILLGUY", "CLFI", "CLH", "CLND", "CLOUD", "CMC20", "CNV", "CO", "COCORO", "CODEX", "COM", "COMP", "CONV", "COPYCAT", "COQ", "CORL", "COW", "CPOOL", "CR7", "CREA", "CREAM", "CREDI", "CRO", "CRP", "CRV", "CSIX", "CSKY", "CSTAR", "CSWAP", "CTC", "CULT", "CVP", "CVX", "CYBA", "CYPR", "CZ4", "DADDY", "DAI", "DAM", "DAO", "DAPP", "DAPPX", "DCI", "DEEP", "DEEPSEEK", "DEFI", "DEGO", "DERI", "DEVVE", "DF", "DFC", "DFE", "DIONE", "DIVI", "DJI6930", "DLC", "DODO", "DOGCOIN", "DOGS", "DOLO", "DONKEY", "DOP", "DORKY", "DOSE", "DRESS", "DRIFT", "DRV", "DSLA", "DSP", "DUEL", "DUELNOW", "DYDX", "DYM", "DYP", "EAVE", "ECHO", "EDEN", "EGP", "EGP1", "EIGEN", "ELECTRON", "ELX", "EMPIRE", "ENA", "EPS", "ERG", "ESE", "ETHFI", "ETS", "EUL", "EV", "EVAA", "EYWA", "FAKEAI", "FALCON", "FARM", "FARTCOIN", "FEAR", "FEG", "FF", "FHT", "FINANCE", "FINC", "FIRE", "FIS", "FLEX", "FLIP", "FLM", "FLOKI", "FLX", "FLZ", "FOLKS", "FOMO", "FOR", "FORM", "FORTH", "FOX", "FRAG", "FRAX", "FRBK", "FRONT", "FTON", "FTT", "FUN", "FUSE", "FXF", "FXS", "GAME", "GARI", "GAU", "GEAR", "GEMS", "GEN", "GFI", "GHIBLI", "GHST", "GIGA", "GIGGLE", "GIZA", "GMX", "GNO", "GNS", "GOAT", "GOATED", "GODL", "GODS", "GOLC", "GORILLA", "GP", "GPT", "GPU", "GRIFT", "GT", "GUA", "H1", "HABIBI", "HAEDAL", "HANA", "HANDY", "HARD", "HBD", "HDRO", "HEGIC", "HEMI", "HENAI", "HERA", "HFT", "HIFI", "HIPPO", "HMSTR", "HODL", "HOME", "HOTKEY", "HQ", "HTS", "HUMA", "HUND", "HUSBY", "HUSTLE", "HYPE", "IDEX", "IDOL", "ILV", "IMGNAI", "IN", "INFOFI", "INFRA", "INJ", "INN", "INTR", "INV", "IR", "IXS", "IZZY", "JAGER", "JITO", "JITOSOL", "JLP", "JOE", "JST", "JUP", "KARATE", "KATA", "KAVA", "KEKE", "KEKIUS", "KERNEL", "KICKS", "KILO", "KING", "KINT", "KISHU", "KMNO", "KNC", "KOMA", "KP3R", "KTC", "KWAI", "LANLAN", "LAUNCHCOIN", "LAYER", "LBP", "LBR", "LDO", "LEASH", "LEE", "LEO", "LEVER", "LIF3", "LIGHT", "LIKE", "LILA", "LINA", "LISTA", "LIZA", "LMWR", "LNQ", "LOGX", "LON", "LOOT", "LOUD", "LQTY", "LSD", "LSHARE", "LTD", "LUCE", "LUMA", "LUMIA", "LUNC", "LUSH", "LYK", "LYM", "LYNX", "M87", "MAGMA", "MAHA", "MAMO", "MANYU", "MARSH", "MBD", "MBG", "MBL", "MBOX", "MCDULL", "MDX", "MEDXT", "MEFA", "MEFAI", "MELD", "MEME", "MEMEFI", "MEMES", "MESH", "MET", "METFI", "METO", "MEV", "MEW", "MFT", "MILK", "MINDFAK", "MIR", "MITO", "MKR", "ML", "MLN", "MMAI", "MMT", "MNT", "MOCK", "MODE", "MOG", "MONKY", "MOODENG", "MOONEDGE", "MOOV", "MOR", "MOROS", "MORPHO", "MOTHER", "MOVEZ", "MOZ", "MRLN", "MRX", "MUBARAK", "MUBI", "MURATIAI", "MUSIC", "MX", "MYRO", "MYSTERY", "MYX", "NABOX", "NAYM", "NEIRO", "NEO", "NEOS", "NETWORK", "NEURA", "NEURAL", "NEXO", "NGL", "NIGELLA", "NIZA", "NMR", "NODEX", "NOM", "NORD", "NOT", "NOTAI", "NRWA", "NTRN", "O4DX", "OAX", "OGN", "OGPU", "OKB", "ONDO", "ONT", "OOE", "OOKI", "OPM",
    "OPUL", "ORACLE", "ORANGE", "ORCA", "ORDER", "ORDS", "ORN", "OSMO", "P00FAI", "PADD", "PAI", "PAID", "PALAI", "PALM", "PAW", "PAWS", "PAXE", "PBR", "PBUX", "PBX", "PELL", "PENDLE", "PEOPLE", "PEPEAI", "PERP", "PICA", "PING", "PINGPONG", "PIPPIN", "PLANCK", "PLANET", "PLS", "PNDR", "PNL", "PNUT", "POLK", "POLS", "POLX", "POLYDOGE", "PONKE", "POR", "PORTALS", "PRCL", "PRIMAL", "PRO", "PROJECT", "PROS", "PROTOCOL", "PSTAKE", "PTB", "PUFFER", "PULSE", "PUMP", "PX", "PYR", "PYTHIA", "QI", "QSHX", "QUICK", "QUILL", "QUQ", "RACA", "RATS", "RAVE", "RAY", "RBN", "RDAC", "RDEX", "RDNT", "REAL", "RECALL", "RED", "REN", "REP", "RESOLV", "RETARD", "REZ", "RFC", "RHEA", "RING", "RIVER", "RLS", "ROOBEE", "RPL", "RTX", "RUBYCOIN", "RUNE", "RVF", "RWA", "SAI", "SAKAI", "SAL", "SALT", "SAMO", "SARA", "SAROS", "SATS", "SCA", "SCALE", "SCLP", "SCPT", "SD", "SDAO", "SDEX", "SEN", "SEND", "SENTIS", "SFUND", "SHADOW", "SHIB", "SIGMA", "SIS", "SKY", "SLAY", "SLERF", "SLIM", "SMILE", "SNEK", "SNL", "SNS", "SNX", "SOCIAL", "SOLAMA", "SOLO", "SOLV", "SOLX", "SOS", "SPA", "SPECTRE", "SPELL", "SPHERE", "SPK", "SPX", "SRM", "SSWP", "STABUL", "STARTUP", "STB", "STBL", "STELLA", "STETH", "STG", "STO", "STORM", "STRD", "STRK", "STRUMP", "STUPID", "SUIA", "SUIP", "SUN", "SUNDOGD", "SUP", "SUPER", "SUPR", "SURE", "SUSHI", "SWAP", "SWCH", "SWELL", "SYN", "SYNTH", "SYRUP", "T1", "TAB", "TAKER", "TAOBOT", "TATSU", "TBC", "TCOM", "TENET", "TERMINUS", "THE", "THOR", "THQ", "THR", "TIDAL", "TITN", "TKO", "TKT", "TMX", "TOKEN", "TOR", "TOSHI", "TOWN", "TRADOOR", "TRAVA", "TRC", "TREE", "TRESTLE", "TRIBE", "TRID", "TROLL", "TROY", "TRU", "TRUMP", "TRUST", "TRUU", "TST", "TUNA", "TURBO", "TURBOS", "TURTLE", "TYPE", "UAI", "UB", "UE", "UFT", "ULX", "UNFI", "UNI", "UNICORN", "UNO", "UPTOP", "URO", "USBT", "USD0", "USD1", "USDD", "USDE", "USDS", "USDY", "USELESS", "USTC", "USUAL", "VEGA", "VELA", "VELAR", "VELO", "VENOM", "VES", "VGX", "VIA", "VIB", "VINE", "VISTA", "VLR", "VOLT", "VPAY", "VPS", "VRSW", "VRTX", "VRX", "VSG", "VUZZ", "VVS", "WAN", "WATER", "WBETH", "WBTC", "WEBAI", "WELL", "WEMIX", "WET", "WHITE", "WIF", "WILD", "WING", "WLAI", "WLFI", "WLTH", "WNXM", "WOJAK", "WOO", "WSM", "WWY", "XALPHA", "XCAD", "XCHNG", "XLAB", "XMON", "XMW", "XNL", "XODEX", "XPIN", "XPRT", "XPX", "XVS", "XYRO", "YAE", "YAI", "YB", "YBR", "YF-DAI", "YFI", "YND", "YU", "ZAM", "ZCN", "ZCX", "ZEAL", "ZEND", "ZENIQ", "ZEPH", "ZERC", "ZEREBRO", "ZERO", "ZEUS", "ZEX", "ZKML", "ZKX", "ZORA", "ZRO", "ZRX", "LUNA", "LUNC", "UST", "USTC", "FTT", "CEL", "LOKA", "MFT", "BAKE", "SKY", "HEMI",
    "BANANAS31", "USD1", "BFUSD", "POLY", "TVK", "COCOS", "DNT", "BLZ", "THE", "PORTAL", "DOLO",
    "HUMA", "ORCA", "LISTA", "EOS", "FRONT", "DAR", "MMT", "JST", "XUSD", "EUR", "ETHFI", "PLA",
    "RNDR", "CHESS", "BNX", "WAN", "HMSTR", "COW", "ERN", "MC", "ADADOWN", "ANT", "BTCUP", "BTCDOWN", "LEND", "ETHUP", "ETHDOWN", "ADAUP", "ADADOWN", "LINKUP", "LINKDOWN", "BNBUP", "BNBDOWN", "XTZUP", "XTZDOWN", "LTCUP", "LTCDOWN", "EOSUP", "EOSDOWN", "TRXUP", "TRXDOWN", "XRPUP", "XRPDOWN", "DOTUP", "DOTDOWN", "UNIUP", "UNIDOWN", "SXPUP", "SXPDOWN", "FILUP", "FILDOWN", "YFIUP", "YFIDOWN", "BCHUP", "BCHDOWN", "AAVEUP", "AAVEDOWN", "SUSD", "SUSHIUP", "SUSHIDOWN", "XLMUP", "XLMDOWN", "1INCHUP", "1INCHDOWN", "MKR", "SNT", "GAL", "TOMO", "HIFI", "VOXEL", "ORN", "MORPHO", "USDE", "REEF", "SYRUP", "twons", "EIGENU", "GAL", "BNSOL", "REN", "WLFI"
]

def get_full_blacklist() -> set:
    bl = set()
    for group in [STABLECOINS, GAMBLING_COINS, MEME_COINS, SCAM_SUSPICIOUS,
                  LEVERAGED_TOKENS, SPORTS_FAN_TOKENS, GAMING_SUSPICIOUS,
                  PONZI_MLM, PRIVACY_SUSPICIOUS, WRAPPED_TOKENS, FORK_COINS, BAD_REPUTATION]:
        bl.update(group)
    return bl

BLACKLIST = get_full_blacklist()

def is_blacklisted(symbol: str, base_quote: str = "USDT") -> bool:
    coin = symbol.replace(base_quote, "").upper()
    return coin in BLACKLIST

BINANCE_API = "https://api.binance.com"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Educational-Trading-Analyzer/3.0"})

def fetch_24hr_tickers() -> List[Dict[str, Any]]:
    url = f"{BINANCE_API}/api/v3/ticker/24hr"
    try:
        r = SESSION.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict): data = [data]
        return data
    except Exception:
        return []

def fetch_klines(symbol: str, interval: str = "1h", limit: int = 300) -> pd.DataFrame:
    url = f"{BINANCE_API}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    try:
        r = SESSION.get(url, params=params, timeout=15)
        r.raise_for_status()
        raw = r.json()
    except Exception:
        return pd.DataFrame()
    if not raw:
        return pd.DataFrame()
    cols = ["open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base", "taker_buy_quote", "ignore"]
    df = pd.DataFrame(raw, columns=cols)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df.dropna().reset_index(drop=True)

def fetch_klines_since(symbol: str, interval: str, start_ts_ms: int, limit: int = 600) -> pd.DataFrame:
    url = f"{BINANCE_API}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit, "startTime": start_ts_ms}
    try:
        r = SESSION.get(url, params=params, timeout=15)
        r.raise_for_status()
        raw = r.json()
    except Exception:
        return pd.DataFrame()
    if not raw:
        return pd.DataFrame()
    cols = ["open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base", "taker_buy_quote", "ignore"]
    df = pd.DataFrame(raw, columns=cols)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)
    return df.dropna().reset_index(drop=True)

def fetch_order_book(symbol: str, limit: int = 100) -> Dict[str, Any]:
    url = f"https://api.binance.com/api/v3/depth"
    params = {"symbol": symbol, "limit": limit}
    try:
        r = SESSION.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data if data else {"bids": [], "asks": []}
    except Exception:
        return {"bids": [], "asks": []}

def analyze_order_book(symbol: str) -> Dict[str, Any]:
    try:
        ob = fetch_order_book(symbol, limit=100)
        if not ob or not ob.get("bids") or not ob.get("asks"):
            return {"pressure": 0, "signal": "neutral", "bid_depth": 0, "ask_depth": 0}
        bids = ob["bids"]; asks = ob["asks"]
        def depth_value(levels): return sum(float(x[1]) * float(x[0]) for x in levels)
        b5 = depth_value(bids[:5]); a5 = depth_value(asks[:5])
        b10 = depth_value(bids[:10]); a10 = depth_value(asks[:10])
        b20 = depth_value(bids[:20]); a20 = depth_value(asks[:20])
        total20 = b20 + a20
        pressure = 0 if total20 == 0 else ((b20 - a20) / total20) * 100
        if pressure > 10: signal = "strong_buy"
        elif pressure > 3: signal = "buy"
        elif pressure < -10: signal = "strong_sell"
        elif pressure < -3: signal = "sell"
        else: signal = "neutral"
        best_bid = float(bids[0][0]) if bids else 0
        best_ask = float(asks[0][0]) if asks else 0
        spread = ((best_ask - best_bid) / best_bid * 100) if best_bid > 0 else 0
        return {
            "pressure": round(pressure, 2),
            "signal": signal,
            "bid_depth_5": round(b5, 2),
            "ask_depth_5": round(a5, 2),
            "bid_depth_10": round(b10, 2),
            "ask_depth_10": round(a10, 2),
            "bid_depth_20": round(b20, 2),
            "ask_depth_20": round(a20, 2),
            "spread_pct": round(spread, 4),
            "best_bid": best_bid,
            "best_ask": best_ask
        }
    except Exception as e:
        return {"pressure": 0, "signal": "neutral", "error": str(e)}

def filter_usdt_symbols(tickers: List[Dict[str, Any]], base_quote: str, min_quote_volume: float) -> List[Dict[str, Any]]:
    filtered = []
    for t in tickers:
        try:
            symbol = t.get("symbol", "")
            if not symbol.endswith(base_quote):
                continue
            if is_blacklisted(symbol, base_quote):
                continue
            quote_vol = float(t.get("quoteVolume", 0.0))
            if quote_vol < min_quote_volume:
                continue
            filtered.append({
                "symbol": symbol,
                "priceChangePercent": float(t.get("priceChangePercent", 0.0)),
                "quoteVolume": quote_vol,
                "lastPrice": float(t.get("lastPrice", 0.0)),
            })
        except Exception:
            continue
    filtered.sort(key=lambda x: x["quoteVolume"], reverse=True)
    return filtered

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / (loss.replace(0, np.nan))
    return (100 - (100 / (1 + rs))).fillna(50)

def macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    ema_fast = ema(series, fast); ema_slow = ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["high"]; low = df["low"]; close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(window=period).mean()

def bollinger_bands(series: pd.Series, period: int = 20, std_dev: float = 2.0):
    middle = series.rolling(window=period).mean()
    std = series.rolling(window=period).std()
    upper = middle + (std * std_dev)
    lower = middle - (std * std_dev)
    return upper, middle, lower

def stochastic_oscillator(df: pd.DataFrame, period: int = 14):
    low_min = df["low"].rolling(window=period).min()
    high_max = df["high"].rolling(window=period).max()
    k = 100 * ((df["close"] - low_min) / (high_max - low_min).replace(0, np.nan))
    d = k.rolling(window=3).mean()
    return k.fillna(50), d.fillna(50)

def obv(df: pd.DataFrame) -> pd.Series:
    return (np.sign(df["close"].diff()) * df["volume"]).fillna(0).cumsum()

def vwap(df: pd.DataFrame) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3
    return (typical * df["volume"]).cumsum() / df["volume"].cumsum()

def mfi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3
    money_flow = typical * df["volume"]
    positive = money_flow.where(typical > typical.shift(1), 0).rolling(period).sum()
    negative = money_flow.where(typical < typical.shift(1), 0).rolling(period).sum()
    return (100 - (100 / (1 + positive / negative.replace(0, np.nan)))).fillna(50)

def ichimoku_cloud(df: pd.DataFrame):
    h9 = df["high"].rolling(9).max(); l9 = df["low"].rolling(9).min()
    tenkan = (h9 + l9) / 2
    h26 = df["high"].rolling(26).max(); l26 = df["low"].rolling(26).min()
    kijun = (h26 + l26) / 2
    senkou_a = ((tenkan + kijun) / 2).shift(26)
    h52 = df["high"].rolling(52).max(); l52 = df["low"].rolling(52).min()
    senkou_b = ((h52 + l52) / 2).shift(26)
    return tenkan, kijun, senkou_a, senkou_b

def compute_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["high"]; low = df["low"]; close = df["close"]
    plus_dm = high.diff(); minus_dm = low.diff() * -1
    plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
    minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)
    tr1 = (high - low); tr2 = (high - close.shift(1)).abs(); tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    tr_rma = tr.ewm(alpha=1/period, adjust=False).mean()
    plus_dm_rma = plus_dm.ewm(alpha=1/period, adjust=False).mean()
    minus_dm_rma = minus_dm.ewm(alpha=1/period, adjust=False).mean()
    plus_di = 100 * (plus_dm_rma / tr_rma).replace([np.inf, -np.inf], np.nan)
    minus_di = 100 * (minus_dm_rma / tr_rma).replace([np.inf, -np.inf], np.nan)
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)).replace([np.inf, -np.inf], np.nan) * 100
    adx = dx.ewm(alpha=1/period, adjust=False).mean()
    return adx.fillna(0)

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["EMA50"] = ema(df["close"], 50)
    df["EMA200"] = ema(df["close"], 200)
    df["RSI14"] = rsi(df["close"], 14)
    macd_line, signal, hist = macd(df["close"])
    df["MACD"] = macd_line
    df["MACD_SIGNAL"] = signal
    df["MACD_HIST"] = hist
    df["ATR14"] = atr(df, 14)
    df["ATR_PCT"] = (df["ATR14"] / df["close"]).replace([np.inf, -np.inf], np.nan).fillna(0) * 100
    bb_u, bb_m, bb_l = bollinger_bands(df["close"])
    df["BB_UPPER"] = bb_u; df["BB_MIDDLE"] = bb_m; df["BB_LOWER"] = bb_l
    stoch_k, stoch_d = stochastic_oscillator(df)
    df["STOCH_K"] = stoch_k; df["STOCH_D"] = stoch_d
    df["OBV"] = obv(df); df["VWAP"] = vwap(df); df["MFI"] = mfi(df)
    tenkan, kijun, senkou_a, senkou_b = ichimoku_cloud(df)
    df["ICHIMOKU_TENKAN"] = tenkan; df["ICHIMOKU_KIJUN"] = kijun
    df["ICHIMOKU_SENKOU_A"] = senkou_a; df["ICHIMOKU_SENKOU_B"] = senkou_b
    df["VOL_MA20"] = df["volume"].rolling(20).mean()
    df["VOL_SURGE"] = (df["volume"] / df["VOL_MA20"]).replace([np.inf, -np.inf], np.nan).fillna(0)
    df["ADX14"] = compute_adx(df, 14)
    return df

def is_strong_downtrend(df, lookback=100, threshold=0.75):
    n = min(lookback, len(df))
    if n == 0: return False
    below = (df['close'].tail(n) < df['EMA200'].tail(n)).sum()
    return (below / n) >= threshold

def pass_multi_tf_trend(symbol, base_quote="USDT", trend_timeframe="1h", kline_limit=150, lb=80, th=0.7):
    df = fetch_klines(symbol, interval=trend_timeframe, limit=kline_limit)
    if df.empty or len(df) < 80: return False
    df = compute_indicators(df)
    return not is_strong_downtrend(df, lookback=lb, threshold=th)

def risk_assessment(row: pd.Series):
    risk_score = 0
    atr = row.get("atr_pct", 0); rsi_v = row.get("rsi", 50)
    if atr > 10: risk_score += 3
    elif atr > 7: risk_score += 2
    if rsi_v > 75 or rsi_v < 25: risk_score += 2
    if risk_score >= 4: return "🔴 عالي جداً", risk_score
    elif risk_score >= 2: return "🟠 عالي", risk_score
    else: return "🟢 منخفض", risk_score

def score_fast(df: pd.DataFrame) -> dict:
    try:
        row = df.iloc[-1]; score = 0.0; signals = []
        if row["EMA50"] > row["EMA200"]:
            score += 2.5; signals.append("✅ EMA50 > EMA200")
        rsi_val = float(row["RSI14"])
        if 45 <= rsi_val <= 65:
            score += 2.0; signals.append(f"✅ RSI صحي ({rsi_val:.1f})")
        elif rsi_val > 75:
            score -= 2.0; signals.append(f"🔴 RSI عالي ({rsi_val:.1f})")
        if float(row["MACD_HIST"]) > 0:
            score += 2.0; signals.append("✅ MACD إيجابي")
        vol_surge = float(row["VOL_SURGE"])
        if vol_surge >= 2.0:
            score += 2.7; signals.append(f"🚀 حجم قوي ({vol_surge:.1f}x)")
        elif vol_surge >= 1.2:
            score += 1.0; signals.append(f"📈 حجم فوق المتوسط ({vol_surge:.1f}x)")
        elif vol_surge < 0.6:
            score -= 0.5; signals.append(f"⚠️ حجم ضعيف ({vol_surge:.2f}x)")
        price_change = float(row.get("priceChangePercent", 0.0))
        if abs(price_change) > 10.0:
            score -= 3; signals.append(f"🔻 تحذير: التغير السعري كبير ({price_change:+.1f}%)")
        atr_pct = float(row["ATR_PCT"]); adx14 = float(row["ADX14"])
        if adx14 >= 25:
            score += 0.7; signals.append(f"📏 ADX قوي ({adx14:.1f})")
        elif adx14 < 10:
            score -= 0.7; signals.append(f"⚪ ADX ضعيف ({adx14:.1f})")
        elif adx14 < 15:
            score -= 0.3; signals.append(f"⚪ ADX متوسط ({adx14:.1f})")
        if atr_pct < 1.5:
            score -= 0.4; signals.append(f"⚠️ تقلب منخفض (ATR {atr_pct:.1f}%)")
        confirm_macd = df["MACD_HIST"].iloc[-2] > 0 and df["MACD_HIST"].iloc[-1] > 0
        confirm_rsi = (45 <= df["RSI14"].iloc[-2] <= 70) and (45 <= df["RSI14"].iloc[-1] <= 70)
        if confirm_macd and confirm_rsi:
            score += 0.5; signals.append("🔒 تأكيد إشارات لشمعتين")
        return {
            "score": round(score, 2), "signals": signals,
            "rsi_val": float(row["RSI14"]), "mfi": float(row["MFI"]),
            "macd_hist": float(row["MACD_HIST"]), "vol_surge": vol_surge,
            "atr_pct": atr_pct, "stoch_k": float(row["STOCH_K"]), "adx14": adx14
        }
    except Exception as ex:
        print(f"[score_fast] error: {ex}")
        return {"score": 0.0, "signals": [], "rsi_val": 50, "mfi": 50,
                "macd_hist": 0, "vol_surge": 1, "atr_pct": 1, "stoch_k": 50, "adx14": 20}

def analyze_smart_money(df: pd.DataFrame) -> dict:
    order_blocks = detect_order_blocks(df); fvgs = detect_fair_value_gaps(df)
    bullish_signals = len(order_blocks["bullish_ob"]) + len(fvgs["bullish_fvg"])
    bearish_signals = len(order_blocks["bearish_ob"]) + len(fvgs["bearish_fvg"])
    overall = "neutral"
    if bullish_signals > bearish_signals and bullish_signals > 0: overall = "bullish"
    elif bearish_signals > bullish_signals and bearish_signals > 0: overall = "bearish"
    return {
        "order_blocks": order_blocks, "fair_value_gaps": fvgs, "signal": overall,
        "bullish_strength": bullish_signals, "bearish_strength": bearish_signals
    }

def detect_fair_value_gaps(df: pd.DataFrame) -> dict:
    if len(df) < 3: return {"bullish_fvg": [], "bearish_fvg": []}
    bullish_fvgs = []; bearish_fvgs = []
    for i in range(1, len(df) - 1):
        c1 = df.iloc[i-1]; c2 = df.iloc[i]; c3 = df.iloc[i+1]
        if c3["low"] > c1["high"]:
            gap_size = c3["low"] - c1["high"]; gap_pct = (gap_size / max(c1["high"], 1e-9)) * 100
            if gap_pct > 0.1:
                bullish_fvgs.append({"index": i, "top": float(c3["low"]), "bottom": float(c1["high"]),
                                     "size_pct": float(gap_pct), "time": c2["open_time"], "filled": False})
        if c3["high"] < c1["low"]:
            gap_size = c1["low"] - c3["high"]; gap_pct = (gap_size / max(c1["low"], 1e-9)) * 100
            if gap_pct > 0.1:
                bearish_fvgs.append({"index": i, "top": float(c1["low"]), "bottom": float(c3["high"]),
                                     "size_pct": float(gap_pct), "time": c2["open_time"], "filled": False})
    for fvg in bullish_fvgs:
        later_lows = df["low"].iloc[fvg["index"]+1:]
        if (later_lows <= fvg["bottom"]).any(): fvg["filled"] = True
    for fvg in bearish_fvgs:
        later_highs = df["high"].iloc[fvg["index"]+1:]
        if (later_highs >= fvg["top"]).any(): fvg["filled"] = True
    unfilled_bullish = [f for f in bullish_fvgs if not f["filled"]]
    unfilled_bearish = [f for f in bearish_fvgs if not f["filled"]]
    return {
        "bullish_fvg": unfilled_bullish[-3:] if unfilled_bullish else [],
        "bearish_fvg": unfilled_bearish[-3:] if unfilled_bearish else [],
        "total_bullish": len(bullish_fvgs),
        "total_bearish": len(bearish_fvgs),
        "unfilled_bullish": len(unfilled_bullish),
        "unfilled_bearish": len(unfilled_bearish)
    }

def detect_liquidity_sweeps(df: pd.DataFrame, lookback: int = 20, threshold: float = 0.2) -> dict:
    if len(df) < lookback + 5: return {"sweep_detected": False, "type": None, "level": 0}
    sweeps = []
    for i in range(lookback, len(df) - 1):
        window = df.iloc[i-lookback:i]; current = df.iloc[i]; next_candle = df.iloc[i+1]
        window_low = window["low"].min()
        if current["low"] < window_low:
            bounce = (next_candle["close"] - current["low"]) / current["low"] * 100
            if bounce > threshold and next_candle["close"] > current["open"]:
                sweeps.append({"type": "bullish_sweep", "index": i, "level": float(current["low"]),
                               "bounce_pct": float(bounce), "time": current["open_time"]})
        window_high = window["high"].max()
        if current["high"] > window_high:
            drop = (current["high"] - next_candle["close"]) / current["high"] * 100
            if drop > threshold and next_candle["close"] < current["open"]:
                sweeps.append({"type": "bearish_sweep", "index": i, "level": float(current["high"]),
                               "drop_pct": float(drop), "time": current["open_time"]})
    last_sweep = sweeps[-1] if sweeps else None
    recent_sweeps = [s for s in sweeps if s["index"] > len(df) - 10]
    return {
        "sweep_detected": len(recent_sweeps) > 0,
        "last_sweep": last_sweep,
        "recent_sweeps": recent_sweeps,
        "total_sweeps": len(sweeps),
        "signal": last_sweep["type"] if last_sweep else "none"
    }

def calculate_volume_profile(df: pd.DataFrame, num_bins: int = 20) -> dict:
    if len(df) < 10:
        return {"poc": 0, "value_area_high": 0, "value_area_low": 0,
                "current_position": "unknown", "profile": []}
    price_min = df["low"].min(); price_max = df["high"].max()
    bins = np.linspace(price_min, price_max, num_bins)
    volume_at_price = defaultdict(float)
    for _, row in df.iterrows():
        price_range = (row["high"] + row["low"]) / 2
        bin_idx = np.digitize(price_range, bins) - 1
        bin_idx = max(0, min(bin_idx, len(bins) - 2))
        volume_at_price[bin_idx] += row["volume"]
    poc_price = df["close"].iloc[-1]
    if volume_at_price:
        poc_bin = max(volume_at_price.items(), key=lambda x: x[1])[0]
        poc_price = bins[poc_bin]
    total_volume = sum(volume_at_price.values()); target_volume = total_volume * 0.7
    sorted_bins = sorted(volume_at_price.items(), key=lambda x: x[1], reverse=True)
    cumulative_volume = 0; value_area_bins = []
    for bin_idx, vol in sorted_bins:
        value_area_bins.append(bin_idx); cumulative_volume += vol
        if cumulative_volume >= target_volume: break
    value_area_high = price_max if not value_area_bins else bins[max(value_area_bins)]
    value_area_low = price_min if not value_area_bins else bins[min(value_area_bins)]
    current_price = df["close"].iloc[-1]
    if current_price >= value_area_high: position = "above_value_area"
    elif current_price <= value_area_low: position = "below_value_area"
    else: position = "in_value_area"
    return {
        "poc": float(poc_price),
        "value_area_high": float(value_area_high),
        "value_area_low": float(value_area_low),
        "current_position": position,
        "profile": [(float(bins[i]), float(volume_at_price.get(i, 0))) for i in range(len(bins)-1)]
    }

def calculate_fibonacci_levels(df: pd.DataFrame, lookback: int = 50) -> dict:
    if len(df) < lookback: lookback = len(df)
    recent = df.tail(lookback)
    swing_high = recent["high"].max(); swing_low = recent["low"].min()
    diff = swing_high - swing_low
    fib_levels = {
        "0.0": swing_high, "0.236": swing_high - (diff * 0.236),
        "0.382": swing_high - (diff * 0.382), "0.5": swing_high - (diff * 0.5),
        "0.618": swing_high - (diff * 0.618), "0.786": swing_high - (diff * 0.786),
        "1.0": swing_low, "1.272": swing_low - (diff * 0.272),
        "1.618": swing_low - (diff * 0.618),
    }
    current_price = df["close"].iloc[-1]
    closest_level = min(fib_levels.items(), key=lambda x: abs(x[1] - current_price))
    trend = "bullish" if current_price > fib_levels["0.5"] else "bearish"
    levels_above = {k: v for k, v in fib_levels.items() if v > current_price}
    levels_below = {k: v for k, v in fib_levels.items() if v < current_price}
    next_resistance = min(levels_above.values()) if levels_above else swing_high
    next_support = max(levels_below.values()) if levels_below else swing_low
    return {
        "levels": {k: float(v) for k, v in fib_levels.items()},
        "closest_level": closest_level[0],
        "closest_price": float(closest_level[1]),
        "trend": trend,
        "next_resistance": float(next_resistance),
        "next_support": float(next_support),
        "swing_high": float(swing_high),
        "swing_low": float(swing_low)
    }

def score_deep(df: pd.DataFrame, symbol: str, df_btc_cache: Optional[pd.DataFrame] = None, interval: str = "1h") -> dict:
    try:
        base = score_fast(df); score = base["score"]; signals = list(base["signals"])
        vp = calculate_volume_profile(df); fib = calculate_fibonacci_levels(df)
        ob = analyze_order_book(symbol); ms = analyze_market_structure(df)
        ls = detect_liquidity_sweeps(df); smc = analyze_smart_money(df)
        btc_corr = 0.0; vs_btc = 0.0
        if df_btc_cache is not None and not df_btc_cache.empty:
            price = df["close"].tail(120); btc_price = df_btc_cache["close"].tail(120)
            if len(price) == len(btc_price):
                btc_corr = price.corr(btc_price)
                vs_btc = price.iloc[-1] / btc_price.iloc[-1] if btc_price.iloc[-1] != 0 else 0.0
        if btc_corr > 0.92:
            score -= 1.5; signals.append("⚠️ عقوبة ارتباط عالي بـ BTC")
        elif btc_corr > 0.85:
            score -= 0.8; signals.append("⚠️ ارتباط مرتفع بـ BTC")
        elif btc_corr < 0.3:
            score += 0.4; signals.append("🎯 تنويع: ارتباط منخفض")
        return {
            "score": round(score, 2), "signals": signals,
            "volume_profile": vp, "fibonacci": fib, "order_book": ob,
            "market_structure": ms, "liquidity_sweep": ls, "smart_money": smc,
            "btc_correlation": float(round(btc_corr, 3)), "vs_btc": float(round(vs_btc, 6)),
            "rsi_val": base["rsi_val"], "mfi": base["mfi"], "macd_hist": base["macd_hist"],
            "vol_surge": base["vol_surge"], "atr_pct": base["atr_pct"],
            "stoch_k": base["stoch_k"], "adx14": base["adx14"]
        }
    except Exception as ex:
        print(f"[score_deep] error: {ex} [symbol={symbol}]")
        return {"score": 0.0, "signals": [], "volume_profile": {"current_position": "unknown", "poc": 0},
                "fibonacci": {"closest_level": "0.5", "trend": "neutral"}, "order_book": {"pressure": 0, "signal": "neutral"},
                "market_structure": {"structure": "unknown", "trend": "neutral"},
                "liquidity_sweep": {"sweep_detected": False, "signal": "none"},
                "smart_money": {"signal": "neutral"}, "btc_correlation": 0.0, "vs_btc": 0.0,
                "rsi_val": 50, "mfi": 50, "macd_hist": 0, "vol_surge": 1, "atr_pct": 1, "stoch_k": 50, "adx14": 20}

def detect_order_blocks(df: pd.DataFrame, lookback: int = 60, min_size: float = 0.8) -> dict:
    if len(df) < lookback + 6: return {"bullish_ob": [], "bearish_ob": []}
    bullish_ob = []; bearish_ob = []
    for i in range(lookback, len(df) - 4):
        body_size = abs(df["close"].iloc[i] - df["open"].iloc[i])
        candle_range = df["high"].iloc[i] - df["low"].iloc[i]
        body_pct = body_size / candle_range if candle_range != 0 else 0
        if df["close"].iloc[i] < df["open"].iloc[i] and body_pct > min_size:
            if df["close"].iloc[i+4] > df["high"].iloc[i]:
                bullish_ob.append({"index": i, "price": float(df["low"].iloc[i]), "time": df["open_time"].iloc[i]})
        if df["close"].iloc[i] > df["open"].iloc[i] and body_pct > min_size:
            if df["close"].iloc[i+4] < df["low"].iloc[i]:
                bearish_ob.append({"index": i, "price": float(df["high"].iloc[i]), "time": df["open_time"].iloc[i]})
    return {
        "bullish_ob": bullish_ob[-3:] if bullish_ob else [],
        "bearish_ob": bearish_ob[-3:] if bearish_ob else [],
        "total_bullish": len(bullish_ob),
        "total_bearish": len(bearish_ob)
    }

def analyze_market_structure(df: pd.DataFrame, swing_period: int = 5) -> dict:
    if len(df) < swing_period * 3:
        return {"structure": "unknown", "trend": "neutral", "swing_highs": [], "swing_lows": []}
    swing_highs = []; swing_lows = []
    for i in range(swing_period, len(df) - swing_period):
        is_high = all(df["high"].iloc[i] > df["high"].iloc[i-j] for j in range(1, swing_period+1)) and \
                  all(df["high"].iloc[i] > df["high"].iloc[i+j] for j in range(1, swing_period+1))
        is_low = all(df["low"].iloc[i] < df["low"].iloc[i-j] for j in range(1, swing_period+1)) and \
                 all(df["low"].iloc[i] < df["low"].iloc[i+j] for j in range(1, swing_period+1))
        if is_high:
            swing_highs.append({"index": i, "price": float(df["high"].iloc[i]), "time": df["open_time"].iloc[i]})
        if is_low:
            swing_lows.append({"index": i, "price": float(df["low"].iloc[i]), "time": df["open_time"].iloc[i]})
    structure = "unknown"; trend = "neutral"
    if len(swing_highs) >= 2 and len(swing_lows) >= 2:
        h1, h2 = swing_highs[-2], swing_highs[-1]
        l1, l2 = swing_lows[-2], swing_lows[-1]
        if h2["price"] > h1["price"] and l2["price"] > l1["price"]:
            structure = "HH_HL"; trend = "bullish"
        elif h2["price"] < h1["price"] and l2["price"] < l1["price"]:
            structure = "LH_LL"; trend = "bearish"
        else:
            structure = "mixed"; trend = "ranging"
    return {
        "structure": structure, "trend": trend,
        "swing_highs": swing_highs[-5:] if swing_highs else [],
        "swing_lows": swing_lows[-5:] if swing_lows else [],
        "last_swing_high": swing_highs[-1] if swing_highs else None,
        "last_swing_low": swing_lows[-1] if swing_lows else None
    }
# أضِف في آخر الملف (أو في مكان مناسب)
def apply_ruleA(row: pd.Series) -> bool:
    # 1.2 <= vol_surge <= 4  &&  15 <= adx14 <= 32  &&  50 <= rsi <= 65
    return (
        row.get("vol_surge", 0) >= 1.2 and row.get("vol_surge", 0) <= 4 and
        row.get("adx14", 0) >= 15 and row.get("adx14", 0) <= 32 and
        row.get("rsi", 0) >= 50 and row.get("rsi", 0) <= 65
    )

RULES_INFO = {
    "RuleA": "1.2<=vol_surge<=4 & 15<=adx14<=32 & 50<=rsi<=65",
}
def analyze_market(
    base_quote: str = "USDT",
    interval: str = "1h",
    kline_limit: int = 300,
    min_quote_volume: float = 500000,
    max_symbols: int = 500,
    top_n: Optional[int] = 10,
    mode: str = "fast",
    max_advanced: int = 50,
    enable_mtf: bool = False,
    trend_timeframe: str = "1h"
) -> pd.DataFrame:
    tickers = fetch_24hr_tickers()
    if not tickers: return pd.DataFrame()
    candidates = filter_usdt_symbols(tickers, base_quote, min_quote_volume)
    if not candidates: return pd.DataFrame()
    symbols = [c["symbol"] for c in candidates[:max_symbols]]
    qv_map = {c["symbol"]: c["quoteVolume"] for c in candidates}
    rows_fast = []
    df_btc_cache = fetch_klines("BTCUSDT", interval=interval, limit=kline_limit)

    REQUIRED_COLS = [
        "symbol", "score", "signals_count", "rsi", "mfi", "macd_hist", "vol_surge", "atr_pct",
        "stoch_k", "adx14", "price", "stop_loss", "target_1", "rr_t1", "quote_volume_24h", "risk_level"
    ]

    for sym in symbols:
        try:
            df = fetch_klines(sym, interval=interval, limit=kline_limit)
            if df.empty or len(df) < 120: continue
            df = compute_indicators(df)
            if not pass_multi_tf_trend(sym, trend_timeframe=trend_timeframe):
                continue
            metrics_fast = score_fast(df)
            if not metrics_fast or "score" not in metrics_fast:
                print(f"⚠️ مشكلة في score_fast مع العملة: {sym}")
                continue
            last_close = float(df["close"].iloc[-1]); atr_val = float(df["ATR14"].iloc[-1])
            row_data = {"atr_pct": metrics_fast["atr_pct"], "rsi": metrics_fast["rsi_val"],
                        "vol_surge": metrics_fast["vol_surge"], "stoch_k": metrics_fast["stoch_k"]}
            risk_level, _ = risk_assessment(pd.Series(row_data))
            stop_loss = round(last_close - atr_val * 1.5, 6)
            target_1  = round(last_close + atr_val * 2, 6)
            rr_t1 = round((target_1 - last_close) / (last_close - stop_loss), 2) if stop_loss != last_close else 0
            row = {
                "symbol": sym, "score": metrics_fast["score"], "signals_count": len(metrics_fast["signals"]),
                "rsi": round(metrics_fast["rsi_val"], 2), "mfi": round(metrics_fast["mfi"], 2),
                "macd_hist": round(metrics_fast["macd_hist"], 6), "vol_surge": round(metrics_fast["vol_surge"], 2),
                "atr_pct": round(metrics_fast["atr_pct"], 2), "stoch_k": round(metrics_fast["stoch_k"], 2),
                "adx14": round(metrics_fast["adx14"], 2), "price": round(last_close, 8),
                "stop_loss": stop_loss, "target_1": target_1, "rr_t1": rr_t1,
                "quote_volume_24h": qv_map.get(sym, 0.0), "risk_level": risk_level,
            }
            for col in REQUIRED_COLS:
                if col not in row:
                    print(f"تحذير! صف العملة {sym} لا يحوي العمود {col}، سيتم إضافة قيمة افتراضية.")
                    if col == "symbol": row[col] = "غير محدد"
                    elif col == "risk_level": row[col] = "غير معروف"
                    else: row[col] = 0
            rows_fast.append(row)
        except Exception as ee:
            print(f"[analyze_market][fast] error: {ee} [symbol={sym}]")

    df_fast = pd.DataFrame(rows_fast)
    for col in REQUIRED_COLS:
        if col not in df_fast.columns:
            print(f"تحذير! DataFrame لا يحتوي عمود {col}، سيتم إضافة عمود افتراضي.")
            if col == "symbol": df_fast[col] = "غير محدد"
            elif col == "risk_level": df_fast[col] = "غير معروف"
            else: df_fast[col] = 0

    df_fast = df_fast[(df_fast["score"] >= 3.0) & (df_fast["vol_surge"] >= 0.4) & (df_fast["adx14"] >= 12)]
    df_fast = df_fast.sort_values(by=["score", "quote_volume_24h"], ascending=[False, False]).reset_index(drop=True)
    if mode != "deep": return df_fast.head(top_n) if top_n else df_fast

    deep_symbols = list(df_fast["symbol"].head(max_advanced))
    deep_rows = []
    for sym in deep_symbols:
        try:
            df = fetch_klines(sym, interval=interval, limit=kline_limit)
            if df.empty or len(df) < 120: continue
            df = compute_indicators(df)
            metrics_deep = score_deep(df, sym, df_btc_cache, interval)
            if not metrics_deep or "score" not in metrics_deep:
                print(f"⚠️ مشكلة في score_deep مع العملة: {sym}")
                continue
            last_close = float(df["close"].iloc[-1]); atr_val = float(df["ATR14"].iloc[-1])
            row_data = {"atr_pct": metrics_deep["atr_pct"], "rsi": metrics_deep["rsi_val"],
                        "vol_surge": metrics_deep["vol_surge"], "stoch_k": metrics_deep["stoch_k"]}
            risk_level, _ = risk_assessment(pd.Series(row_data))
            stop_loss = round(last_close - atr_val * 1.5, 6)
            target_1  = round(last_close + atr_val * 2, 6)
            rr_t1 = round((target_1 - last_close) / (last_close - stop_loss), 2) if stop_loss != last_close else 0
            row = {
                "symbol": sym, "score": metrics_deep["score"], "signals_count": len(metrics_deep["signals"]),
                "rsi": round(metrics_deep["rsi_val"], 2), "mfi": round(metrics_deep["mfi"], 2),
                "macd_hist": round(metrics_deep["macd_hist"], 6), "vol_surge": round(metrics_deep["vol_surge"], 2),
                "atr_pct": round(metrics_deep["atr_pct"], 2), "stoch_k": round(metrics_deep["stoch_k"], 2),
                "adx14": round(metrics_deep["adx14"], 2), "price": round(last_close, 8),
                "stop_loss": stop_loss, "target_1": target_1, "rr_t1": rr_t1,
                "quote_volume_24h": qv_map.get(sym, 0.0), "risk_level": risk_level,
                "vp_position": metrics_deep["volume_profile"].get("current_position", "unknown"),
                "vp_poc": metrics_deep["volume_profile"].get("poc", 0),
                "fib_level": metrics_deep["fibonacci"].get("closest_level", ""),
                "fib_trend": metrics_deep["fibonacci"].get("trend", ""),
                "fib_support": metrics_deep["fibonacci"].get("next_support", 0),
                "fib_resistance": metrics_deep["fibonacci"].get("next_resistance", 0),
                "ob_pressure": metrics_deep["order_book"].get("pressure", 0),
                "ob_signal": metrics_deep["order_book"].get("signal", "neutral"),
                "market_structure": metrics_deep["market_structure"].get("structure", "unknown"),
                "ms_trend": metrics_deep["market_structure"].get("trend", "neutral"),
                "liquidity_sweep": "✅" if metrics_deep.get("liquidity_sweep", {}).get("sweep_detected") else "❌",
                "ls_signal": metrics_deep.get("liquidity_sweep", {}).get("signal", "none"),
                "smc_signal": metrics_deep["smart_money"].get("signal", "neutral"),
                "smc_strength": metrics_deep["smart_money"].get("bullish_strength", 0) - metrics_deep["smart_money"].get("bearish_strength", 0),
                "btc_corr": metrics_deep.get("btc_correlation", 0.0),
                "vs_btc": metrics_deep.get("vs_btc", 0.0)
            }
            for col in REQUIRED_COLS:
                if col not in row:
                    print(f"تحذير! صف العملة (deep) {sym} لا يحوي العمود {col}، سيتم إضافة قيمة افتراضية.")
                    if col == "symbol": row[col] = "غير محدد"
                    elif col == "risk_level": row[col] = "غير معروف"
                    else: row[col] = 0
            deep_rows.append(row)
        except Exception as ee:
            print(f"[analyze_market][deep] error: {ee} [symbol={sym}]")

    df_deep = pd.DataFrame(deep_rows)
    for col in REQUIRED_COLS:
        if col not in df_deep.columns:
            print(f"تحذير! DataFrame (deep) لا يحتوي عمود {col}، سيتم إضافة عمود افتراضي.")
            if col == "symbol": df_deep[col] = "غير محدد"
            elif col == "risk_level": df_deep[col] = "غير معروف"
            else: df_deep[col] = 0

    df_deep = df_deep.sort_values(by=["score", "quote_volume_24h"], ascending=[False, False]).reset_index(drop=True)
    return df_deep.head(top_n) if top_n else df_deep

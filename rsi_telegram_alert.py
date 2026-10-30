#!/usr/bin/env python3
"""
Midas coinleri için "RSI dip dönüşü -> ilk fiyat tepesi" uyarısı -> Telegram
Zaman dilimleri: 1s, 2s, 4s, 1G   (sadece KAPANMIŞ mumlar kullanılır)

Kurgu:
  1) RSI(14) 30'un altından çıkıp 30'u yukarı keser  (dip dönüşü)
  2) Bundan sonraki ilk FİYAT tepesi (pivot high) oluşur:
     tepe mumunun en yükseği, solundaki PIVOT_LEFT mumun ve sağındaki PIVOT_RIGHT mumun
     en yükseğinden büyük olmalı. Sağ taraf onaylanınca bildirim gelir.
  3) RSI tepe oluşmadan tekrar 30'un altına inerse o kurgu iptal olur.

Kurulum:
  1) Telegram'da @BotFather -> /newbot -> TOKEN al, bota /start yaz
  2) Ortam değişkenleri (Windows):
       set TELEGRAM_TOKEN=123456:ABC...
       set TELEGRAM_CHAT_ID=1090406510
     (Mac/Linux: export TELEGRAM_TOKEN="..." gibi)
  3) python rsi_telegram_alert.py --test                   Telegram'a test mesajı
     python rsi_telegram_alert.py --backtest TIAUSDT 4h    Geçmiş sinyalleri listeler
     python rsi_telegram_alert.py --dry                    Taramayı yapar, Telegram'a göndermez
     python rsi_telegram_alert.py --loop                   Sürekli çalışır (5 dk'da bir)

Veri kaynağı: Binance herkese açık API (USDT pariteleri). Binance'te olmayan coinler atlanır.
Sadece Python 3.8+ standart kütüphanesi gerekir.
"""
import json, os, sys, time, urllib.request, urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

COINS = """
BTC ETH BNB XRP SOL TRX HYPE DOGE LINK ADA XLM RAIN BCH NEAR UNI LTC AVAX SUI HBAR TAO
SHIB CRO ENA XAUT ONDO OKB AAVE MNT DOT PUMP ASTER PAXG PEPE SKY WLD WLFI ICP ARB VVV
MORPHO POL LIT QNT KAS JUP ALGO PI RENDER JST ATOM CAKE FIL AERO VET INJ AKE APT ETHFI XDC
PENGU FLR TRUMP ZRO STX PYTH SEI NEXO FET CRV RAY VIRTUAL TIA PONS PENDLE SPX FF LDO BTT
KITE OP SUN BONK IMX XPL GRT ENS GNO AR STRK JTO CFX FLOKI SYRUP TWT WIF COMP EIGEN IOTA
THETA JASMY KAIA ZAMA AXS 2Z MET CASHCAT FARTCOIN XCN NEO TRAC MANA MINA SENT CHZ VSN DEXE
H APE 1INCH EGLD ZEN ZK SAND GRASS PLUME SUPER S GENIUS MARSCOIN DYDX QTUM BEAM GALA SKR
ORDI ORCA CHIP WAL HUMA TAG CFG RED KSM SNX SAFE COW ZETA KAITO O LPT IP VELO BIO W HOT
BERA PHA SUSHI SPK NMR BOME TURBO CKB ROSE IO AXL ASTR LINEA GPS ACH G QUBIC BLUR COAI
SOMI ENJ ALLO BRETT PNUT TRB TOSHI RAVE BARD MEGA ARK ANKR RON MOODENG NOT COTI MOG 0G
MASK CSPR ME MEW SKYAI BLEND AMP DOS UP MOVE EDU MEME SAHARA MERL ZORA NEIRO REZ LAYER
STEEM UMA CORE CATI VANA MANTA FOGO BEAT ACX ARKM CYS CETUS CTSI USUAL SKL HOME GMT CYBER
AUCTION METIS EDEN DEGEN DOGS AEVO API3 AIXBT GRVT SOLV ZKC BREV ROBO ACE TUT QANX ENSO
PRL AUDIO WCT INIT TNSR XAI SAPIEN GOAT NAORIS ANIME CP SIREN SPELL CAT NEWT ARPA MAGIC
KAT ORDER BASED LUMIA KERNEL SAGA RAD BMT OGN AEON DYM STORJ ARIA SXT BB SHELL RARE WET
HMSTR ACT BEL ATM OPN VINE PSG BAR ASR TREE PORTO LAZIO JUV CITY SCR ACM AFC PRCL ZEUS
ELIZAOS ARG POR ACA BAKE BELIEVE TROY TIG OM BNX OMNI MKR SLF EOS
""".split()

INTERVALS = ["1h", "2h", "4h", "1d"]
LABEL = {"1h": "1s", "2h": "2s", "4h": "4s", "1d": "1G"}
RSI_PERIOD = 14
LEVEL = 30.0
PIVOT_LEFT = 5     # tepenin solunda kaç mumdan yüksek olmalı
PIVOT_RIGHT = 2    # tepeden sonra kaç mum geçince onaylansın (küçültürsen erken, büyütürsen geç haber verir)
LOOP_SECONDS = 300
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rsi_state.json")
BASES = ["https://api.binance.com", "https://data-api.binance.vision"]
TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
TR = timezone(timedelta(hours=3))


def http_get(path, params=None, timeout=15):
    q = "?" + urllib.parse.urlencode(params) if params else ""
    last = None
    for base in BASES:
        try:
            req = urllib.request.Request(base + path + q, headers={"User-Agent": "rsi-alert/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            last = e
    raise last


def rsi_series(closes, period=RSI_PERIOD):
    """Wilder RSI. Dönen liste closes ile aynı uzunlukta (başta None)."""
    out = [None] * len(closes)
    if len(closes) <= period:
        return out
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0)
        losses += max(-d, 0)
    avg_g, avg_l = gains / period, losses / period

    def val(g, l):
        if l == 0:
            return 100.0 if g > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + g / l)

    out[period] = val(avg_g, avg_l)
    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        avg_g = (avg_g * (period - 1) + max(d, 0)) / period
        avg_l = (avg_l * (period - 1) + max(-d, 0)) / period
        out[i] = val(avg_g, avg_l)
    return out


def find_signals(highs, closes, left=PIVOT_LEFT, right=PIVOT_RIGHT, level=LEVEL):
    """Her RSI 30 yukarı kesişimi için ilk fiyat tepesini (pivot high) bulur.
    Döner: [{'cross': i, 'peak': p, 'confirm': q}, ...]  (indeksler mum sırasıdır)"""
    r = rsi_series(closes)
    n = len(r)
    crosses = [i for i in range(1, n)
               if r[i] is not None and r[i - 1] is not None and r[i - 1] < level <= r[i]]
    out = []
    for ci, c in enumerate(crosses):
        end = crosses[ci + 1] if ci + 1 < len(crosses) else n
        for p in range(max(c, left), end - right):
            if any(r[x] is not None and r[x] < level for x in range(c, p + right + 1)):
                break  # RSI tekrar 30 altına indi: kurgu iptal
            if highs[p] > max(highs[p - left:p]) and highs[p] >= max(highs[p + 1:p + right + 1]):
                out.append({"cross": c, "peak": p, "confirm": p + right})
                break
    return out


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(st):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(st, f)
    os.replace(tmp, STATE_FILE)


def available_pairs():
    info = http_get("/api/v3/exchangeInfo")
    ok = {s["symbol"] for s in info["symbols"] if s["status"] == "TRADING"}
    return [c + "USDT" for c in COINS if c + "USDT" in ok], [c for c in COINS if c + "USDT" not in ok]


def get_candles(pair, interval, limit=250):
    k = http_get("/api/v3/klines", {"symbol": pair, "interval": interval, "limit": limit})
    return k[:-1]  # son (henüz kapanmamış) mumu at


def check(pair, interval):
    try:
        k = get_candles(pair, interval)
    except Exception:
        return None
    if len(k) < RSI_PERIOD + PIVOT_LEFT + PIVOT_RIGHT + 3:
        return None
    highs = [float(x[2]) for x in k]
    closes = [float(x[4]) for x in k]
    n = len(k)
    for s in find_signals(highs, closes):
        if s["confirm"] == n - 1:  # tepe tam şimdi onaylandı
            r = rsi_series(closes)
            return {"pair": pair, "interval": interval, "peak_price": highs[s["peak"]],
                    "peak_rsi": r[s["peak"]], "now_price": closes[-1],
                    "bars": s["peak"] - s["cross"], "candle": k[s["peak"]][0]}
    return None


def fmt_price(p):
    return f"{p:,.2f}" if p >= 1 else f"{p:.8f}".rstrip("0")


def send_telegram(text):
    data = urllib.parse.urlencode({"chat_id": CHAT_ID, "text": text}).encode()
    req = urllib.request.Request(f"https://api.telegram.org/bot{TOKEN}/sendMessage", data=data)
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.read()


def notify(hits, dry):
    lines = [f"🔔 {h['pair'][:-4]} | {LABEL[h['interval']]} | ilk tepe ${fmt_price(h['peak_price'])} "
             f"(RSI {h['peak_rsi']:.0f}) | şimdi ${fmt_price(h['now_price'])}"
             for h in hits]
    chunk, size = [], 0
    for ln in lines + [None]:
        if ln is None or size + len(ln) > 3500:
            if chunk:
                msg = "RSI dip dönüşü sonrası ilk fiyat tepesi\n\n" + "\n".join(chunk)
                print(msg) if dry else send_telegram(msg)
            chunk, size = [], 0
        if ln:
            chunk.append(ln)
            size += len(ln) + 1


def backtest(pair, interval):
    k = http_get("/api/v3/klines", {"symbol": pair, "interval": interval, "limit": 1000})[:-1]
    highs = [float(x[2]) for x in k]
    closes = [float(x[4]) for x in k]
    r = rsi_series(closes)
    sig = find_signals(highs, closes)
    t = lambda i: datetime.fromtimestamp(k[i][0] / 1000, TR).strftime("%d.%m.%Y %H:%M")
    print(f"{pair} {interval} | {len(k)} mum | saatler Türkiye saati (mum açılış zamanı)")
    if not sig:
        print("Bu aralıkta sinyal yok.")
    for s in sig:
        print(f"RSI 30 yukarı kesti: {t(s['cross'])} | ilk tepe: {t(s['peak'])} "
              f"fiyat {fmt_price(highs[s['peak']])} RSI {r[s['peak']]:.0f} | bildirim: {t(s['confirm'])} mumu kapanınca")


def run_once(dry=False):
    pairs, missing = available_pairs()
    print(f"{time.strftime('%H:%M:%S')} taranan coin: {len(pairs)} | Binance'te olmayan: {len(missing)}")
    jobs = [(p, i) for p in pairs for i in INTERVALS]
    with ThreadPoolExecutor(max_workers=16) as ex:
        results = list(ex.map(lambda a: check(*a), jobs))
    state = load_state()
    new = []
    for h in results:
        if not h:
            continue
        key = f"{h['pair']}|{h['interval']}"
        if state.get(key) != h["candle"]:
            state[key] = h["candle"]
            new.append(h)
    if new:
        notify(new, dry)
        if not dry:
            save_state(state)
    print(f"yeni sinyal: {len(new)}")


def main():
    if "--test" in sys.argv:
        send_telegram("✅ RSI uyarı sistemi test mesajı")
        print("Test mesajı gönderildi.")
        return
    if "--backtest" in sys.argv:
        i = sys.argv.index("--backtest")
        backtest(sys.argv[i + 1].upper(), sys.argv[i + 2])
        return
    dry = "--dry" in sys.argv
    if not dry and (not TOKEN or not CHAT_ID):
        sys.exit("TELEGRAM_TOKEN ve TELEGRAM_CHAT_ID ortam değişkenlerini ayarla.")
    if "--loop" in sys.argv:
        while True:
            try:
                run_once(dry)
            except Exception as e:
                print("hata:", e)
            time.sleep(LOOP_SECONDS)
    else:
        run_once(dry)


if __name__ == "__main__":
    main()

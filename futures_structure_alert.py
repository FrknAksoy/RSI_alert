#!/usr/bin/env python3
"""Scan closed Binance USD-M perpetual candles and send structure matches to Telegram."""

from __future__ import annotations

import html
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
import urllib.error
import urllib.parse
import urllib.request
import ccxt
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any


BINANCE_FUTURES_URLS = (
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
)
TELEGRAM_URL = "https://api.telegram.org"
TIMEFRAMES = ("4h",)
KLINE_LIMIT = 1000
ATR_PERIOD = 14
ATR_MULTIPLIER = 1.0
ZIGZAG_LENGTH = 9
FIB_FACTOR = 0.33
MAX_ACTIVE_BU_OBS = 5
MAX_ACTIVE_BE_OBS = 5
MAX_ACTIVE_LIQUIDITY = 30
WORKERS = 8
REQUEST_INTERVAL = 0.15
STATE_FILE = Path(__file__).with_name("futures_state.json")
HTML_REPORT_FILE = Path(__file__).with_name("futures_report.html")
CONDITIONS = (
    ("bu_ob_inside", "BU-OB İÇİNDE"),
    ("bu_ob_confirm", "BU-OB TEYİT"),
    ("be_ob_inside", "BE-OB İÇİNDE"),
    ("choch", "CHoCH"),
    ("bos", "BOS"),
    ("ftr", "FTR BÖLGESİ"),
    ("reversal", "REVERSAL BÖLGESİ"),
)
_MEXC_THREAD_LOCAL = threading.local()


@dataclass(frozen=True)
class Candle:
    open_time: int
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class Level:
    price: float
    index: int


class RequestPacer:
    def __init__(self, min_interval: float) -> None:
        self.min_interval = min_interval
        self.lock = threading.Lock()
        self.next_request_at = 0.0

    def wait(self) -> None:
        with self.lock:
            now = time.monotonic()
            delay = max(0.0, self.next_request_at - now)
            self.next_request_at = max(now, self.next_request_at) + self.min_interval
        if delay:
            time.sleep(delay)


def http_json(
    base_url: str,
    path: str,
    params: dict[str, Any],
    pacer: RequestPacer | None = None,
) -> Any:
    query = urllib.parse.urlencode(params)
    url = f"{base_url}{path}?{query}" if query else f"{base_url}{path}"
    headers = {"User-Agent": "Futures-Structure-Alert/1.0"}

    for attempt in range(5):
        if pacer:
            pacer.wait()
        request = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            if error.code not in (418, 429, 500, 502, 503, 504) or attempt == 4:
                detail = error.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"HTTP {error.code}: {detail[:300]}") from error
            retry_after = error.headers.get("Retry-After")
            time.sleep(float(retry_after) if retry_after else min(2**attempt, 30))
        except (urllib.error.URLError, TimeoutError) as error:
            if attempt == 4:
                raise RuntimeError(f"Connection failed: {error}") from error
            time.sleep(min(2**attempt, 15))

    raise RuntimeError(f"Request failed: {path}")


def binance_perpetual_symbols(pacer: RequestPacer) -> list[str]:
    exchange_info = None
    last_error = ""
    for base_url in BINANCE_FUTURES_URLS:
        try:
            exchange_info = http_json(base_url, "/fapi/v1/exchangeInfo", {}, pacer)
            break
        except Exception as error:
            last_error = str(error)
    if exchange_info is None:
        raise RuntimeError(f"Binance Futures API unavailable: {last_error}")

    symbols = {
        item["symbol"]
        for item in exchange_info.get("symbols", [])
        if item.get("status") == "TRADING"
        and item.get("quoteAsset", item.get("quoteCoin")) == "USDT"
        and item.get("contractType") in ("PERPETUAL", "TRADIFI_PERPETUAL")
    }
    if not symbols:
        raise RuntimeError("Binance returned no active USDT-M perpetual contracts")
    return sorted(symbols)


def mexc_exchange(pacer: RequestPacer) -> Any:
    exchange = getattr(_MEXC_THREAD_LOCAL, "exchange", None)
    if exchange is None:
        exchange = ccxt.mexc({"enableRateLimit": True, "timeout": 20000})
        pacer.wait()
        exchange.load_markets()
        _MEXC_THREAD_LOCAL.exchange = exchange
    return exchange


def match_mexc_symbols(
    binance_symbols: list[str], markets: dict[str, dict[str, Any]]
) -> dict[str, str]:
    mexc_symbols_by_ticker: dict[str, list[str]] = {}
    for market in markets.values():
        if not (
            market.get("active") is True
            and market.get("contract") is True
            and market.get("swap") is True
            and market.get("linear") is True
            and market.get("quote") == "USDT"
        ):
            continue

        base_id = market.get("baseId")
        quote_id = market.get("quoteId")
        symbol = market.get("symbol")
        if not all(
            isinstance(value, str) and value
            for value in (base_id, quote_id, symbol)
        ):
            continue

        ticker = f"{base_id.upper()}{quote_id.upper()}"
        mexc_symbols_by_ticker.setdefault(ticker, []).append(symbol)

    return {
        ticker: mexc_symbols_by_ticker[ticker][0]
        for ticker in sorted(set(binance_symbols))
        if len(mexc_symbols_by_ticker.get(ticker, [])) == 1
    }


def mexc_symbol_map(
    binance_symbols: list[str], pacer: RequestPacer
) -> dict[str, str]:
    exchange = ccxt.mexc({"enableRateLimit": True, "timeout": 20000})
    pacer.wait()
    mexc_markets = exchange.load_markets()
    return match_mexc_symbols(binance_symbols, mexc_markets)


def available_symbols(
    pacer: RequestPacer,
) -> tuple[list[str], dict[str, str]]:
    binance_symbols = binance_perpetual_symbols(pacer)
    return binance_symbols, mexc_symbol_map(binance_symbols, pacer)


def closed_candles_from_rows(rows: list[list[Any]], timeframe: str) -> list[Candle]:
    if len(rows) < 3:
        raise RuntimeError(f"Not enough {timeframe} candles returned")
    rows.sort(key=lambda row: int(row[0]))
    return [
        Candle(
            open_time=int(row[0]),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
        )
        for row in rows[:-1]
    ]


def get_closed_candles(
    symbol: str,
    timeframe: str,
    pacer: RequestPacer,
    mexc_symbol: str | None = None,
) -> tuple[list[Candle], str]:
    binance_errors: list[str] = []
    for base_url in BINANCE_FUTURES_URLS:
        try:
            rows = http_json(
                base_url,
                "/fapi/v1/klines",
                {"symbol": symbol, "interval": timeframe, "limit": KLINE_LIMIT},
                pacer,
            )
            return closed_candles_from_rows(rows, timeframe), "Binance"
        except Exception as error:
            binance_errors.append(f"{base_url}: {error}")

    binance_error = "; ".join(binance_errors)
    if not mexc_symbol:
        raise RuntimeError(
            f"Binance candles unavailable for {symbol}: {binance_error}; "
            "no matching MEXC USDT perpetual market is available as fallback"
        )

    try:
        exchange = mexc_exchange(pacer)
        pacer.wait()
        mexc_rows = exchange.fetch_ohlcv(
            mexc_symbol, timeframe, limit=KLINE_LIMIT
        )
        return closed_candles_from_rows(mexc_rows, timeframe), "MEXC yedek"
    except Exception as error:
        raise RuntimeError(
            f"Binance candles unavailable for {symbol}: {binance_error}; "
            f"MEXC fallback failed for {mexc_symbol}: {error}"
        ) from error


def calculate_atr(candles: list[Candle], period: int) -> list[float | None]:
    true_ranges: list[float] = []
    for index, candle in enumerate(candles):
        previous_close = candles[index - 1].close if index else candle.close
        true_ranges.append(
            max(
                candle.high - candle.low,
                abs(candle.high - previous_close),
                abs(candle.low - previous_close),
            )
        )

    atr: list[float | None] = [None] * len(candles)
    if len(candles) < period:
        return atr

    current = sum(true_ranges[:period]) / period
    atr[period - 1] = current
    for index in range(period, len(candles)):
        current = (current * (period - 1) + true_ranges[index]) / period
        atr[index] = current
    return atr


def scan_structure(candles: list[Candle]) -> dict[str, Any]:
    atr = calculate_atr(candles, ATR_PERIOD)
    direction = 1
    candidate_high: Level | None = None
    candidate_low: Level | None = None
    candidate_high_atr: float | None = None
    candidate_low_atr: float | None = None
    swing_high: Level | None = None
    swing_low: Level | None = None
    swing_high_broken = False
    swing_low_broken = False
    trend = 0
    last_event: dict[str, Any] | None = None

    for index, candle in enumerate(candles):
        if swing_high is not None and not swing_high_broken and candle.close > swing_high.price:
            kind = "CHoCH" if trend == -1 else "BOS"
            last_event = {"kind": kind, "direction": "bullish", "index": index}
            swing_high_broken = True
            trend = 1
        if swing_low is not None and not swing_low_broken and candle.close < swing_low.price:
            kind = "CHoCH" if trend == 1 else "BOS"
            last_event = {"kind": kind, "direction": "bearish", "index": index}
            swing_low_broken = True
            trend = -1

        current_atr = atr[index]
        if current_atr is None:
            continue
        if candidate_high is None or candidate_low is None:
            candidate_high = Level(candle.high, index)
            candidate_low = Level(candle.low, index)
            candidate_high_atr = current_atr
            candidate_low_atr = current_atr
            continue

        if direction == 1:
            if candle.high >= candidate_high.price:
                candidate_high = Level(candle.high, index)
                candidate_high_atr = current_atr
            if candle.close <= candidate_high.price - (candidate_high_atr or current_atr) * ATR_MULTIPLIER:
                swing_high = candidate_high
                swing_high_broken = False
                direction = -1
                candidate_low = Level(candle.low, index)
                candidate_low_atr = current_atr
        else:
            if candle.low <= candidate_low.price:
                candidate_low = Level(candle.low, index)
                candidate_low_atr = current_atr
            if candle.close >= candidate_low.price + (candidate_low_atr or current_atr) * ATR_MULTIPLIER:
                swing_low = candidate_low
                swing_low_broken = False
                direction = 1
                candidate_high = Level(candle.high, index)
                candidate_high_atr = current_atr

    if last_event is not None:
        last_event["bars_ago"] = len(candles) - 1 - last_event["index"]
    return {"event": last_event}


def scan_indicator_zones(candles: list[Candle]) -> dict[str, bool]:
    high_points: list[Level] = []
    low_points: list[Level] = []
    low_index_history: list[int | None] = []
    trend = 1
    market = 1
    last_l0_at_market_change: float | None = None
    last_h0_at_market_change: float | None = None
    last_up_index: int | None = None
    last_down_index: int | None = None
    active_ob: list[tuple[float, float]] = []
    active_be_ob: list[tuple[float, float]] = []
    ftr_zone: tuple[float, float] | None = None
    reversal_zone: tuple[float, float] | None = None

    for index, candle in enumerate(candles):
        start = max(0, index - ZIGZAG_LENGTH + 1)
        window = candles[start : index + 1]
        to_up = candle.high >= max(item.high for item in window)
        to_down = candle.low <= min(item.low for item in window)

        previous_up = last_up_index
        previous_down = last_down_index
        bars_since_up = index - 1 - previous_up if previous_up is not None else 0
        bars_since_down = index - 1 - previous_down if previous_down is not None else 0
        if index > 0 and to_up:
            last_up_index = index
        if index > 0 and to_down:
            last_down_index = index

        low_window_size = bars_since_up if bars_since_up > 0 else 1
        high_window_size = bars_since_down if bars_since_down > 0 else 1
        low_start = max(0, index - low_window_size + 1)
        high_start = max(0, index - high_window_size + 1)
        low_value = min(item.low for item in candles[low_start : index + 1])
        high_value = max(item.high for item in candles[high_start : index + 1])
        low_index = max(
            i for i in range(low_start, index + 1) if candles[i].low == low_value
        )
        high_index = max(
            i for i in range(high_start, index + 1) if candles[i].high == high_value
        )

        previous_trend = trend
        if trend == 1 and to_down:
            trend = -1
        elif trend == -1 and to_up:
            trend = 1
        if trend != previous_trend:
            (low_points if trend == 1 else high_points).append(
                Level(low_value if trend == 1 else high_value, low_index if trend == 1 else high_index)
            )

        enough_data = len(high_points) >= 2 and len(low_points) >= 2
        h0 = high_points[-1] if enough_data else None
        h1 = high_points[-2] if enough_data else None
        l0 = low_points[-1] if enough_data else None
        l1 = low_points[-2] if enough_data else None
        low_index_history.append(l0.index if l0 else None)

        bullish_msb = False
        bearish_msb = False
        if enough_data and h0 and h1 and l0 and l1:
            if l0.price != last_l0_at_market_change and h0.price != last_h0_at_market_change:
                new_market = market
                if (
                    market == 1
                    and l0.price < l1.price
                    and l0.price < l1.price - abs(h0.price - l1.price) * FIB_FACTOR
                ):
                    new_market = -1
                elif (
                    market == -1
                    and h0.price > h1.price
                    and h0.price > h1.price + abs(h1.price - l0.price) * FIB_FACTOR
                ):
                    new_market = 1
                if new_market != market:
                    market = new_market
                    last_l0_at_market_change = l0.price
                    last_h0_at_market_change = h0.price
                    bullish_msb = market == 1
                    bearish_msb = market == -1

        if bullish_msb or bearish_msb:
            ftr_zone = None
            reversal_zone = None
            start_index = l0.index if bullish_msb and l0 else h0.index if h0 else None
            break_level = h1.price if bullish_msb and h1 else l1.price if l1 else None
            break_index: int | None = None

            if start_index is not None and break_level is not None:
                for candidate in range(start_index + 1, index + 1):
                    crossed = (
                        candles[candidate].close > break_level
                        if bullish_msb
                        else candles[candidate].close < break_level
                    )
                    if crossed:
                        break_index = candidate
                        break

            if break_index is not None and start_index is not None:
                for candidate in range(break_index - 1, start_index, -1):
                    item = candles[candidate]
                    opposite_color = item.close < item.open if bullish_msb else item.close > item.open
                    if not opposite_color:
                        continue
                    retouched = any(
                        candles[later].low <= item.high
                        if bullish_msb
                        else candles[later].high >= item.low
                        for later in range(break_index + 1, index + 1)
                    )
                    if not retouched:
                        ftr_zone = (item.high, item.low)
                    break

                origin = candles[start_index]
                reversal_retouched = any(
                    candles[later].low <= origin.high
                    if bullish_msb
                    else candles[later].high >= origin.low
                    for later in range(break_index + 1, index + 1)
                )
                if not reversal_retouched:
                    reversal_zone = (origin.high, origin.low)

            if bullish_msb and enough_data and h1 and l0:
                delayed_low_index = (
                    low_index_history[index - ZIGZAG_LENGTH]
                    if index >= ZIGZAG_LENGTH
                    else None
                )
                end_index = delayed_low_index if delayed_low_index is not None else h1.index
                ob_index: int | None = None
                for candidate in range(h1.index, end_index + 1):
                    item = candles[candidate]
                    if item.open > item.close:
                        ob_index = candidate
                if ob_index is not None:
                    ob = candles[ob_index]
                    active_ob.append((ob.high, ob.low))
                    active_ob = active_ob[-MAX_ACTIVE_BU_OBS:]

            if bearish_msb and enough_data and h1 and l1:
                ob_index: int | None = None
                for candidate in range(l1.index, h1.index + 1):
                    item = candles[candidate]
                    if item.open < item.close:
                        ob_index = candidate
                if ob_index is not None:
                    ob = candles[ob_index]
                    active_be_ob.append((ob.high, ob.low))
                    active_be_ob = active_be_ob[-MAX_ACTIVE_BE_OBS:]

        active_ob = [zone for zone in active_ob if candle.close >= zone[1]]
        active_be_ob = [zone for zone in active_be_ob if candle.close <= zone[0]]

    current = candles[-1]
    previous_close = candles[-2].close if len(candles) > 1 else current.close
    in_bu_ob = False
    bu_ob_confirm = False
    for top, bottom in active_ob:
        came_from_above = previous_close > top
        close_inside = bottom <= current.close <= top
        touched_zone = bottom <= current.low <= top
        lower_wick = min(current.open, current.close) - current.low
        upper_wick = current.high - max(current.open, current.close)
        if came_from_above and close_inside:
            in_bu_ob = True
        if (
            came_from_above
            and touched_zone
            and current.close > bottom
            and current.close > current.open
            and lower_wick > upper_wick
        ):
            bu_ob_confirm = True

    in_be_ob = False
    for top, bottom in active_be_ob:
        came_from_below = previous_close < bottom
        close_inside = bottom <= current.close <= top
        if came_from_below and close_inside:
            in_be_ob = True

    return {
        "bu_ob_inside": in_bu_ob,
        "bu_ob_confirm": bu_ob_confirm,
        "be_ob_inside": in_be_ob,
        "ftr": ftr_zone is not None and ftr_zone[1] <= current.close <= ftr_zone[0],
        "reversal": reversal_zone is not None
        and reversal_zone[1] <= current.close <= reversal_zone[0],
    }


def scan_pair(
    symbol: str,
    timeframe: str,
    pacer: RequestPacer,
    mexc_symbol: str | None = None,
) -> dict[str, Any]:
    try:
        candles, data_source = get_closed_candles(
            symbol, timeframe, pacer, mexc_symbol
        )
        structure = scan_structure(candles)["event"]
        zones = scan_indicator_zones(candles)
        current_event = structure if structure and structure["bars_ago"] == 0 else None
        conditions = {
            "bu_ob_inside": zones["bu_ob_inside"],
            "bu_ob_confirm": zones["bu_ob_confirm"],
            "be_ob_inside": zones["be_ob_inside"],
            "choch": bool(
                current_event
                and current_event["direction"] == "bullish"
                and current_event["kind"] == "CHoCH"
            ),
            "bos": bool(
                current_event
                and current_event["direction"] == "bullish"
                and current_event["kind"] == "BOS"
            ),
            "ftr": zones["ftr"],
            "reversal": zones["reversal"],
        }
        matches = [name for key, name in CONDITIONS if conditions[key]]
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "candle": candles[-1].open_time,
            "price": candles[-1].close,
            "conditions": conditions,
            "matches": matches,
            "data_source": data_source,
            "error": "",
        }
    except Exception as error:
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "candle": None,
            "price": None,
            "conditions": {},
            "matches": [],
            "data_source": "",
            "error": str(error),
        }


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {
            "active_conditions": [],
            "hourly_seen": [],
            "hourly_bucket": "",
            "hourly_sent_bucket": "",
        }
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Could not read alert state: {error}") from error
    if not isinstance(data, dict):
        raise RuntimeError("Alert state file has an invalid format")
    for key in ("active_conditions", "hourly_seen"):
        value = data.get(key, [])
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise RuntimeError("Alert state file has an invalid format")
    if not isinstance(data.get("hourly_bucket", ""), str):
        raise RuntimeError("Alert state file has an invalid format")
    if not isinstance(data.get("hourly_sent_bucket", ""), str):
        raise RuntimeError("Alert state file has an invalid format")
    return data


def save_state(state: dict[str, Any]) -> None:
    temporary = STATE_FILE.with_suffix(".tmp")
    try:
        temporary.write_text(
            json.dumps(state),
            encoding="utf-8",
        )
        temporary.replace(STATE_FILE)
    except OSError as error:
        raise RuntimeError(f"Could not save alert state: {error}") from error


def send_telegram(message: str) -> None:
    token = os.environ.get("TELEGRAM_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        raise RuntimeError("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID GitHub secrets are required")

    body = urllib.parse.urlencode(
        {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    ).encode("utf-8")
    request = urllib.request.Request(
        f"{TELEGRAM_URL}/bot{token}/sendMessage",
        data=body,
        headers={"User-Agent": "Futures-Structure-Alert/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            result = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"Telegram HTTP {error.code}: {error.reason}") from None
    except (urllib.error.URLError, TimeoutError) as error:
        reason = error.reason if isinstance(error, urllib.error.URLError) else error
        raise RuntimeError(f"Telegram connection failed: {reason}") from None
    if not result.get("ok"):
        raise RuntimeError(f"Telegram rejected message: {result}")


def notify_hourly_summary(
    hourly_keys: set[str], results: dict[str, dict[str, dict[str, Any]]]
) -> None:
    symbols = {key.split("|", 2)[0] for key in hourly_keys}
    ranked = []
    for symbol in symbols:
        per_timeframe = {
            timeframe: results.get(timeframe, {}).get(symbol)
            for timeframe in TIMEFRAMES
        }
        total = sum(len(row["matches"]) for row in per_timeframe.values() if row)
        if total:
            ranked.append((total, symbol, per_timeframe))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    if not ranked:
        return

    lines = [
        "🔥 <b>4 SAATLİK BINANCE FUTURES TARAMA</b>",
        "📡 Veri: Binance öncelikli · MEXC yedek",
        f"⏰ Saatlik toplam · {datetime.now(timezone.utc).strftime('%d.%m.%Y %H:52')} UTC",
        f"📊 <b>{len(ranked)} parite / {len(CONDITIONS)} koşul</b>",
        "━━━━━━━━━━━━━━━━",
    ]
    for total, symbol, per_timeframe in ranked:
        display_symbol = symbol.split("/", 1)[0]
        filled = "◆" * total
        empty = "◇" * (len(CONDITIONS) - total)
        row = per_timeframe["4h"]
        matches = row["matches"] if row else []
        lines.append(
            f"\n<b>{display_symbol}</b>"
            f"\n{filled}{empty} <b>{total}/7</b>"
            f"\n✅ {', '.join(matches)}"
        )

    message = ""
    for line in lines:
        if len(message) + len(line) + 1 > 3800:
            send_telegram(message)
            message = "Binance Futures — devamı\n"
        message += line + "\n"
    if message.strip():
        send_telegram(message)


def write_html_report(
    symbols: list[str],
    results_by_timeframe: dict[str, dict[str, dict[str, Any]]],
    scan_errors: dict[tuple[str, str], str],
    generated_at: datetime | None = None,
    output_path: Path = HTML_REPORT_FILE,
) -> None:
    generated_at = generated_at or datetime.now(timezone.utc)
    rows: list[str] = []
    successful_count = 0
    signal_count = 0
    fallback_count = 0
    error_count = 0

    for symbol in symbols:
        for timeframe in TIMEFRAMES:
            result = results_by_timeframe.get(timeframe, {}).get(symbol)
            error = scan_errors.get((timeframe, symbol), "")
            if result:
                successful_count += 1
                matches = result["matches"]
                signal_count += bool(matches)
                fallback_count += result["data_source"] == "MEXC yedek"
                status = "Sinyal var" if matches else "Sinyal yok"
                status_class = "signal" if matches else "clear"
                source = result["data_source"]
                price = f'{float(result["price"]):,.8g}'
                conditions = result["conditions"]
                candle_time = datetime.fromtimestamp(
                    result["candle"] / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M UTC")
                match_labels = ", ".join(matches) if matches else "—"
            else:
                error_count += 1
                status = "Veri alınamadı"
                status_class = "error"
                source = "—"
                price = "—"
                conditions = {}
                candle_time = "—"
                match_labels = error or "Tarama sonucu yok"

            condition_cells = "".join(
                '<td class="condition '
                + ("active" if conditions.get(key) else "")
                + '">'
                + ("✓" if conditions.get(key) else "—")
                + "</td>"
                for key, _ in CONDITIONS
            )
            rows.append(
                f'<tr data-status="{status_class}">'
                f"<td><strong>{html.escape(symbol)}</strong></td>"
                f"<td>{html.escape(timeframe)}</td>"
                f'<td><span class="badge {status_class}">{html.escape(status)}</span></td>'
                f"<td>{html.escape(source)}</td>"
                f"<td>{html.escape(price)}</td>"
                f"<td>{html.escape(candle_time)}</td>"
                f"<td>{html.escape(match_labels)}</td>"
                f"{condition_cells}</tr>"
            )

    template = """<!doctype html>
<html lang="tr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Binance Futures Tarama Raporu</title>
<style>
:root{color-scheme:light dark;font:15px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif}
body{margin:0;background:#f6f8fa;color:#1f2328}
main{max-width:1600px;margin:0 auto;padding:24px}
h1{margin:0 0 6px;font-size:1.7rem}.muted{color:#656d76}
.summary{display:flex;gap:12px;flex-wrap:wrap;margin:20px 0}
.card{background:white;border:1px solid #d1d9e0;border-radius:8px;padding:12px 16px;min-width:125px}
.card strong{display:block;font-size:1.35rem}
.controls{display:flex;gap:10px;margin:16px 0}
input,select{padding:9px 11px;border:1px solid #d1d9e0;border-radius:6px;background:white;color:inherit}
input{min-width:240px;flex:1}
.table-wrap{overflow:auto;background:white;border:1px solid #d1d9e0;border-radius:8px}
table{border-collapse:collapse;width:100%;min-width:1250px}
th,td{padding:9px 10px;text-align:left;border-bottom:1px solid #d8dee4;white-space:nowrap}
th{position:sticky;top:0;background:#f6f8fa;z-index:1}
tbody tr:hover{background:#f6f8fa}.badge{padding:3px 8px;border-radius:12px;font-size:.85rem}
.badge.signal{background:#dafbe1;color:#116329}.badge.clear{background:#ddf4ff;color:#0550ae}
.badge.error{background:#ffebe9;color:#cf222e}.condition{text-align:center;color:#8c959f}
.condition.active{background:#dafbe1;color:#116329;font-weight:bold}
@media(prefers-color-scheme:dark){body{background:#0d1117;color:#e6edf3}.card,.table-wrap{background:#161b22;border-color:#30363d}th,input,select{background:#161b22;border-color:#30363d;color:#e6edf3}th{background:#21262d}th,td{border-color:#30363d}tbody tr:hover{background:#21262d}.muted{color:#8b949e}}
</style>
</head>
<body><main>
<h1>Binance Futures tarama raporu</h1>
<div class="muted">Oluşturulma: __GENERATED_AT__ · Kaynak evren: aktif Binance USDⓈ-M USDT perpetual pariteleri · MEXC yalnızca Binance verisi alınamadığında yedektir.</div>
<section class="summary">
<div class="card"><strong>__SYMBOL_COUNT__</strong>Binance paritesi</div>
<div class="card"><strong>__SUCCESS_COUNT__</strong>Başarılı tarama</div>
<div class="card"><strong>__SIGNAL_COUNT__</strong>Sinyalli parite</div>
<div class="card"><strong>__FALLBACK_COUNT__</strong>MEXC yedek</div>
<div class="card"><strong>__ERROR_COUNT__</strong>Veri alınamayan</div>
</section>
<div class="controls">
<input id="search" type="search" placeholder="Sembol veya sinyal ara">
<select id="status"><option value="">Tüm durumlar</option><option value="signal">Sinyal var</option><option value="clear">Sinyal yok</option><option value="error">Veri alınamadı</option></select>
</div>
<div class="table-wrap"><table>
<thead><tr><th>Parite</th><th>Zaman</th><th>Durum</th><th>Veri kaynağı</th><th>Son fiyat</th><th>Son kapanış</th><th>Sinyaller</th>__CONDITION_HEADERS__</tr></thead>
<tbody id="results">__ROWS__</tbody>
</table></div>
</main>
<script>
const search=document.getElementById("search"),status=document.getElementById("status");
function filterRows(){const q=search.value.toLocaleLowerCase("tr"),s=status.value;for(const row of document.querySelectorAll("#results tr")){row.hidden=(s&&row.dataset.status!==s)||!row.textContent.toLocaleLowerCase("tr").includes(q)}}
search.addEventListener("input",filterRows);status.addEventListener("change",filterRows);
</script>
</body></html>"""
    headers = "".join(f"<th>{html.escape(label)}</th>" for _, label in CONDITIONS)
    report = (
        template.replace("__GENERATED_AT__", html.escape(generated_at.strftime("%Y-%m-%d %H:%M UTC")))
        .replace("__SYMBOL_COUNT__", str(len(symbols)))
        .replace("__SUCCESS_COUNT__", str(successful_count))
        .replace("__SIGNAL_COUNT__", str(signal_count))
        .replace("__FALLBACK_COUNT__", str(fallback_count))
        .replace("__ERROR_COUNT__", str(error_count))
        .replace("__CONDITION_HEADERS__", headers)
        .replace("__ROWS__", "".join(rows))
    )
    output_path.write_text(report, encoding="utf-8")


def run() -> int:
    if not os.environ.get("TELEGRAM_TOKEN") or not os.environ.get("TELEGRAM_CHAT_ID"):
        raise RuntimeError("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID GitHub secrets are required")

    pacer = RequestPacer(REQUEST_INTERVAL)
    symbols = binance_perpetual_symbols(pacer)
    print(f"Binance USD-M: {len(symbols)} aktif USDT-M perpetual parite.")
    try:
        mexc_symbols = mexc_symbol_map(symbols, pacer)
        print(f"MEXC yedek eşleşmesi: {len(mexc_symbols)} parite.")
    except Exception as error:
        mexc_symbols = {}
        print(f"MEXC yedeği kullanılamıyor; Binance taraması sürecek: {error}")

    results_by_timeframe: dict[str, dict[str, dict[str, Any]]] = {}
    scan_errors: dict[tuple[str, str], str] = {}
    errors = 0
    data_source_counts = {"Binance": 0, "MEXC yedek": 0}
    for timeframe in TIMEFRAMES:
        print(f"{timeframe} taraması başlıyor...")
        results: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {
                executor.submit(
                    scan_pair, symbol, timeframe, pacer, mexc_symbols.get(symbol)
                ): symbol
                for symbol in symbols
            }
            for completed, future in enumerate(as_completed(futures), start=1):
                result = future.result()
                if result["error"]:
                    errors += 1
                    scan_errors[(timeframe, result["symbol"])] = result["error"]
                    print(f"  Hata {result['symbol']} {timeframe}: {result['error']}")
                else:
                    results[result["symbol"]] = result
                    data_source_counts[result["data_source"]] += 1
                if completed % 100 == 0 or completed == len(symbols):
                    print(f"  {timeframe}: {completed}/{len(symbols)} tamamlandı")
        results_by_timeframe[timeframe] = results

    write_html_report(symbols, results_by_timeframe, scan_errors)
    print(f"HTML tarama raporu yazıldı: {HTML_REPORT_FILE}")

    state = load_state()
    previous = set(state["active_conditions"])
    current_hour = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")
    hourly_sent_bucket = state.get("hourly_sent_bucket", "")
    hourly_seen = (
        set(state["hourly_seen"])
        if state.get("hourly_bucket") == current_hour
        else set()
    )
    successful_contexts = {
        (timeframe, symbol)
        for timeframe, results in results_by_timeframe.items()
        for symbol in results
    }
    current: set[str] = set()
    for timeframe, results in results_by_timeframe.items():
        for symbol, row in results.items():
            for key, _ in CONDITIONS:
                if row["conditions"][key]:
                    current.add(f"{symbol}|{timeframe}|{key}")

    retained = {
        key
        for key in previous
        if len(parts := key.split("|", 2)) == 3
        and (parts[1], parts[0]) not in successful_contexts
    }
    newly_matched = current - previous
    active = retained | current
    hourly_seen.update(
        key for key in current if key.split("|", 2)[1] == "4h"
    )

    ranked: dict[str, tuple[int, str]] = {}
    for symbol in symbols:
        per_timeframe = {
            timeframe: results_by_timeframe[timeframe].get(symbol)
            for timeframe in TIMEFRAMES
        }
        count = sum(len(row["matches"]) for row in per_timeframe.values() if row)
        if count:
            details = " | ".join(
                f"{timeframe}: {', '.join(row['matches'])}"
                for timeframe, row in per_timeframe.items()
                if row and row["matches"]
            )
            ranked[symbol] = (count, details)

    print(f"\nBirleşik tarama: {len(symbols)} parite; {len(ranked)} eşleşme.")
    for rank, (symbol, (count, details)) in enumerate(
        sorted(ranked.items(), key=lambda item: (-item[1][0], item[0])), start=1
    ):
        display_symbol = symbol.split("/", 1)[0]
        print(
            f"{rank}. {display_symbol} — {count}/{len(CONDITIONS) * len(TIMEFRAMES)} "
            f"koşul | {details}"
        )

    print(f"Yeni koşul eşleşmesi: {len(newly_matched)}")
    print(
        "Veri kaynağı kullanımı: "
        f"Binance {data_source_counts['Binance']}, "
        f"MEXC yedek {data_source_counts['MEXC yedek']}."
    )
    if datetime.now(timezone.utc).minute >= 52 and hourly_sent_bucket != current_hour:
        notify_hourly_summary(hourly_seen, results_by_timeframe)
        hourly_sent_bucket = current_hour
    else:
        print("Saatlik özet gönderim penceresi dışında veya bu saat gönderildi.")
    save_state({
        "active_conditions": sorted(active),
        "hourly_seen": sorted(hourly_seen),
        "hourly_bucket": current_hour,
        "hourly_sent_bucket": hourly_sent_bucket,
    })
    if errors:
        print(f"{errors} parite/zaman diliminde veri alınamadı.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except Exception as error:
        print(f"Tarama başarısız: {error}", file=sys.stderr)
        raise SystemExit(1)

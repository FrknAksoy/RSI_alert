#!/usr/bin/env python3
"""Scan closed Binance USD-M perpetual candles and send structure matches to Telegram."""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
import urllib.error
import urllib.parse
import urllib.request
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
CONDITIONS = (
    ("bu_ob_inside", "BU-OB İÇİNDE"),
    ("bu_ob_confirm", "BU-OB TEYİT"),
    ("be_ob_inside", "BE-OB İÇİNDE"),
    ("choch", "CHoCH"),
    ("bos", "BOS"),
    ("ftr", "FTR BÖLGESİ"),
    ("reversal", "REVERSAL BÖLGESİ"),
)


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


def get_closed_candles(
    symbol: str, timeframe: str, pacer: RequestPacer
) -> list[Candle]:
    rows = None
    last_error = ""
    for base_url in BINANCE_FUTURES_URLS:
        try:
            rows = http_json(
                base_url,
                "/fapi/v1/klines",
                {"symbol": symbol, "interval": timeframe, "limit": KLINE_LIMIT},
                pacer,
            )
            break
        except Exception as error:
            last_error = str(error)
    if rows is None:
        raise RuntimeError(f"Binance candles unavailable: {last_error}")
    if len(rows) < 3:
        raise RuntimeError(f"Not enough {timeframe} candles returned")
    rows.sort(key=lambda row: int(row[0]))
    if len(rows) < 3:
        raise RuntimeError(f"Not enough {timeframe} candles returned")
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
    symbol: str, timeframe: str, pacer: RequestPacer
) -> dict[str, Any]:
    try:
        candles = get_closed_candles(symbol, timeframe, pacer)
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


def run() -> int:
    if not os.environ.get("TELEGRAM_TOKEN") or not os.environ.get("TELEGRAM_CHAT_ID"):
        raise RuntimeError("TELEGRAM_TOKEN and TELEGRAM_CHAT_ID GitHub secrets are required")

    pacer = RequestPacer(REQUEST_INTERVAL)
    symbols = binance_perpetual_symbols(pacer)
    print(f"Binance USD-M: {len(symbols)} aktif USDT-M perpetual parite.")

    results_by_timeframe: dict[str, dict[str, dict[str, Any]]] = {}
    errors = 0
    for timeframe in TIMEFRAMES:
        print(f"{timeframe} taraması başlıyor...")
        results: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {
                executor.submit(scan_pair, symbol, timeframe, pacer): symbol
                for symbol in symbols
            }
            for completed, future in enumerate(as_completed(futures), start=1):
                result = future.result()
                if result["error"]:
                    errors += 1
                    print(f"  Hata {result['symbol']} {timeframe}: {result['error']}")
                else:
                    results[result["symbol"]] = result
                if completed % 100 == 0 or completed == len(symbols):
                    print(f"  {timeframe}: {completed}/{len(symbols)} tamamlandı")
        results_by_timeframe[timeframe] = results

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

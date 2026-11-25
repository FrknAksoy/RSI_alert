import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import ANY, call, patch

import futures_structure_alert as scanner


def mexc_market(
    symbol: str,
    base_id: str,
    *,
    active: bool = True,
    contract: bool = True,
    swap: bool = True,
    linear: bool = True,
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "baseId": base_id,
        "quoteId": "USDT",
        "quote": "USDT",
        "active": active,
        "contract": contract,
        "swap": swap,
        "linear": linear,
    }


class BinancePerpetualSymbolsTests(unittest.TestCase):
    @patch.object(
        scanner,
        "http_json",
        return_value={
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "quoteAsset": "USDT",
                    "contractType": "PERPETUAL",
                },
                {
                    "symbol": "ETHUSDT",
                    "status": "TRADING",
                    "quoteCoin": "USDT",
                    "contractType": "TRADIFI_PERPETUAL",
                },
                {
                    "symbol": "DOGEUSDT",
                    "status": "BREAK",
                    "quoteCoin": "USDT",
                    "contractType": "PERPETUAL",
                },
                {
                    "symbol": "BTCUSD",
                    "status": "TRADING",
                    "quoteAsset": "USD",
                    "contractType": "PERPETUAL",
                },
                {
                    "symbol": "BTCUSDT_260327",
                    "status": "TRADING",
                    "quoteCoin": "USDT",
                    "contractType": "CURRENT_QUARTER",
                },
            ]
        },
    )
    def test_uses_only_active_binance_usdt_perpetuals(self, _http_json) -> None:
        self.assertEqual(
            scanner.binance_perpetual_symbols(scanner.RequestPacer(0)),
            ["BTCUSDT", "ETHUSDT"],
        )

    @patch.object(
        scanner,
        "http_json",
        side_effect=RuntimeError("Binance API unavailable"),
    )
    def test_uses_checked_in_snapshot_if_binance_api_is_unavailable(
        self, _http_json
    ) -> None:
        with patch.object(
            scanner,
            "BINANCE_SYMBOL_SNAPSHOT_FILE",
            new=Path(__file__).parent.parent / "binance_usdt_perpetuals.txt",
        ):
            symbols = scanner.binance_perpetual_symbols(scanner.RequestPacer(0))

        self.assertGreater(len(symbols), 500)
        self.assertIn("BTCUSDT", symbols)


class HtmlReportTests(unittest.TestCase):
    def test_writes_standalone_report_with_signals_errors_and_escaped_symbols(self) -> None:
        symbol = "BTC<script>alert(1)</script>USDT"
        conditions = {key: key == "bos" for key, _ in scanner.CONDITIONS}
        results = {
            "4h": {
                symbol: {
                    "symbol": symbol,
                    "timeframe": "4h",
                    "candle": 1_000,
                    "price": 123.45,
                    "conditions": conditions,
                    "matches": ["BOS"],
                    "data_source": "MEXC yedek",
                },
                "QUIETUSDT": {
                    "symbol": "QUIETUSDT",
                    "timeframe": "4h",
                    "candle": 1_000,
                    "price": 0.25,
                    "conditions": {key: False for key, _ in scanner.CONDITIONS},
                    "matches": [],
                    "data_source": "Binance",
                },
            }
        }
        errors = {("4h", "BADUSDT"): "HTTP <unavailable>"}

        with TemporaryDirectory() as directory:
            output_path = Path(directory) / "report.html"
            scanner.write_html_report(
                [symbol, "BADUSDT", "QUIETUSDT"],
                results,
                errors,
                datetime(2026, 11, 25, 10, 0, tzinfo=timezone.utc),
                output_path,
            )
            report = output_path.read_text(encoding="utf-8")

        self.assertIn("3</strong>Binance paritesi", report)
        self.assertIn("2</strong>Başarılı tarama", report)
        self.assertIn("1</strong>Sinyalli parite", report)
        self.assertIn("1</strong>MEXC yedek", report)
        self.assertIn("1</strong>Veri alınamayan", report)
        self.assertIn("BTC&lt;script&gt;alert(1)&lt;/script&gt;USDT", report)
        self.assertNotIn("<td><strong>BTC<script>", report)
        self.assertIn("HTTP &lt;unavailable&gt;", report)
        self.assertIn('data-status="clear"', report)
        self.assertIn("MEXC yedek", report)


class MexcFallbackMarketTests(unittest.TestCase):
    def test_only_maps_active_mexc_perpetuals_in_binance_universe(self) -> None:
        markets = {
            "btc": mexc_market("BTC/USDT:USDT", "BTC"),
            "inactive": mexc_market("ETH/USDT:USDT", "ETH", active=False),
            "future": mexc_market("SOL/USDT:USDT", "SOL", swap=False),
            "inverse": mexc_market("ADA/USDT:USDT", "ADA", linear=False),
            "mexc-only": mexc_market("AMDSTOCK/USDT:USDT", "AMDSTOCK"),
        }

        self.assertEqual(
            scanner.match_mexc_symbols(["BTCUSDT", "ETHUSDT"], markets),
            {"BTCUSDT": "BTC/USDT:USDT"},
        )

    def test_available_source_list_stays_binance_list(self) -> None:
        binance_symbols = ["BTCUSDT", "NOTONMEXCUSDT"]

        with (
            patch.object(
                scanner, "binance_perpetual_symbols", return_value=binance_symbols
            ),
            patch.object(scanner, "mexc_symbol_map", return_value={"BTCUSDT": "BTC/USDT:USDT"}),
        ):
            symbols, fallback_map = scanner.available_symbols(
                scanner.RequestPacer(0)
            )

        self.assertEqual(symbols, binance_symbols)
        self.assertNotIn("AMDSTOCKUSDT", symbols)
        self.assertEqual(fallback_map, {"BTCUSDT": "BTC/USDT:USDT"})


class BinanceCandleTests(unittest.TestCase):
    def test_loads_4h_candles_and_excludes_the_open_candle(self) -> None:
        rows = [
            [1000, "1", "3", "0.5", "2"],
            [2000, "2", "4", "1.5", "3"],
            [3000, "3", "5", "2.5", "4"],
        ]

        with patch.object(scanner, "http_json", return_value=rows) as request:
            candles, source = scanner.get_closed_candles(
                "BTCUSDT", "4h", scanner.RequestPacer(0)
            )

        self.assertEqual([candle.open_time for candle in candles], [1000, 2000])
        self.assertEqual(candles[0].close, 2.0)
        self.assertEqual(source, "Binance")
        request.assert_called_once_with(
            scanner.BINANCE_FUTURES_URLS[0],
            "/fapi/v1/klines",
            {"symbol": "BTCUSDT", "interval": "4h", "limit": scanner.KLINE_LIMIT},
            ANY,
        )

    def test_uses_an_alternate_binance_host_if_primary_fails(self) -> None:
        rows = [
            [1000, "1", "3", "0.5", "2"],
            [2000, "2", "4", "1.5", "3"],
            [3000, "3", "5", "2.5", "4"],
        ]

        with patch.object(
            scanner,
            "http_json",
            side_effect=[RuntimeError("primary unavailable"), rows],
        ) as request:
            candles, source = scanner.get_closed_candles(
                "BTCUSDT", "4h", scanner.RequestPacer(0)
            )

        self.assertEqual(len(candles), 2)
        self.assertEqual(source, "Binance")
        self.assertEqual(
            request.call_args_list,
            [
                call(
                    scanner.BINANCE_FUTURES_URLS[0],
                    "/fapi/v1/klines",
                    {"symbol": "BTCUSDT", "interval": "4h", "limit": scanner.KLINE_LIMIT},
                    ANY,
                ),
                call(
                    scanner.BINANCE_FUTURES_URLS[1],
                    "/fapi/v1/klines",
                    {"symbol": "BTCUSDT", "interval": "4h", "limit": scanner.KLINE_LIMIT},
                    ANY,
                ),
            ],
        )

    def test_uses_mexc_only_after_all_binance_hosts_fail(self) -> None:
        rows = [
            [1000, 1, 3, 0.5, 2, 10],
            [2000, 2, 4, 1.5, 3, 10],
            [3000, 3, 5, 2.5, 4, 10],
        ]
        exchange = unittest.mock.Mock()
        exchange.fetch_ohlcv.return_value = rows

        with (
            patch.object(
                scanner,
                "http_json",
                side_effect=RuntimeError("Binance unavailable"),
            ),
            patch.object(scanner, "mexc_exchange", return_value=exchange),
        ):
            candles, source = scanner.get_closed_candles(
                "BTCUSDT", "4h", scanner.RequestPacer(0), "BTC/USDT:USDT"
            )

        self.assertEqual([candle.open_time for candle in candles], [1000, 2000])
        self.assertEqual(source, "MEXC yedek")
        exchange.fetch_ohlcv.assert_called_once_with(
            "BTC/USDT:USDT", "4h", limit=scanner.KLINE_LIMIT
        )

    def test_does_not_use_mexc_only_symbols_as_fallback(self) -> None:
        with (
            patch.object(
                scanner,
                "http_json",
                side_effect=RuntimeError("Binance unavailable"),
            ),
            patch.object(scanner, "mexc_exchange") as make_exchange,
        ):
            with self.assertRaisesRegex(
                RuntimeError, "no matching MEXC USDT perpetual market"
            ):
                scanner.get_closed_candles(
                    "NOTONBINANCEUSDT", "4h", scanner.RequestPacer(0)
                )

        make_exchange.assert_not_called()

    def test_scan_keeps_binance_ticker_for_mexc_fallback_candles(self) -> None:
        candles = [
            scanner.Candle(1000, 1, 2, 0.5, 1.5),
            scanner.Candle(2000, 1.5, 2, 1, 1.8),
            scanner.Candle(3000, 1.8, 2, 1.2, 1.9),
        ]
        zones = {
            "bu_ob_inside": False,
            "bu_ob_confirm": False,
            "be_ob_inside": False,
            "ftr": False,
            "reversal": False,
        }

        with (
            patch.object(
                scanner,
                "get_closed_candles",
                return_value=(candles, "MEXC yedek"),
            ) as get_candles,
            patch.object(scanner, "scan_structure", return_value={"event": None}),
            patch.object(scanner, "scan_indicator_zones", return_value=zones),
        ):
            result = scanner.scan_pair(
                "BTCUSDT", "4h", scanner.RequestPacer(0), "BTC/USDT:USDT"
            )

        get_candles.assert_called_once_with(
            "BTCUSDT", "4h", unittest.mock.ANY, "BTC/USDT:USDT"
        )
        self.assertEqual(result["symbol"], "BTCUSDT")
        self.assertEqual(result["data_source"], "MEXC yedek")
        self.assertEqual(result["error"], "")


if __name__ == "__main__":
    unittest.main()

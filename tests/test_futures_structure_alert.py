import unittest
from unittest.mock import ANY, call, patch

import futures_structure_alert as scanner


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


class BinanceCandleTests(unittest.TestCase):
    def test_loads_4h_candles_and_excludes_the_open_candle(self) -> None:
        rows = [
            [1000, "1", "3", "0.5", "2"],
            [2000, "2", "4", "1.5", "3"],
            [3000, "3", "5", "2.5", "4"],
        ]

        with patch.object(scanner, "http_json", return_value=rows) as request:
            candles = scanner.get_closed_candles(
                "BTCUSDT", "4h", scanner.RequestPacer(0)
            )

        self.assertEqual([candle.open_time for candle in candles], [1000, 2000])
        self.assertEqual(candles[0].close, 2.0)
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
            scanner.get_closed_candles("BTCUSDT", "4h", scanner.RequestPacer(0))

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


if __name__ == "__main__":
    unittest.main()

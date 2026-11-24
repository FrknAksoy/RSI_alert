import unittest
from unittest.mock import patch

import futures_structure_alert as scanner


def mexc_market(
    symbol: str,
    base_id: str,
    *,
    active: bool = True,
    contract: bool = True,
    swap: bool = True,
    linear: bool = True,
    quote: str = "USDT",
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "baseId": base_id,
        "quoteId": quote,
        "quote": quote,
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
            ]
        },
    )
    def test_uses_only_active_binance_usdt_perpetuals(self, _http_json) -> None:
        self.assertEqual(
            scanner.binance_perpetual_symbols(scanner.RequestPacer(0)),
            ["BTCUSDT", "ETHUSDT"],
        )


class MexcSymbolIntersectionTests(unittest.TestCase):
    def test_intersects_binance_list_without_adding_mexc_only_markets(self) -> None:
        markets = {
            "btc": mexc_market("BTC/USDT:USDT", "BTC"),
            "mexc-only": mexc_market("AMDSTOCK/USDT:USDT", "AMDSTOCK"),
        }

        self.assertEqual(
            scanner.match_mexc_symbols(
                ["BTCUSDT", "EMESTOCKUSDT"], markets
            ),
            {"BTCUSDT": "BTC/USDT:USDT"},
        )

    def test_keeps_unavailable_pairs_in_source_but_does_not_map_them(self) -> None:
        binance_symbols = ["BTCUSDT", "EMESTOCKUSDT"]
        markets = {"btc": mexc_market("BTC/USDT:USDT", "BTC")}

        with (
            patch.object(scanner, "binance_perpetual_symbols", return_value=binance_symbols),
            patch.object(scanner, "mexc_exchange") as get_exchange,
        ):
            get_exchange.return_value.load_markets.return_value = markets
            available, mapping = scanner.available_symbols(scanner.RequestPacer(0))

        self.assertEqual(available, binance_symbols)
        self.assertEqual(mapping, {"BTCUSDT": "BTC/USDT:USDT"})

    def test_excludes_inactive_non_swap_inverse_and_non_usdt_markets(self) -> None:
        markets = {
            "inactive": mexc_market("BTC/USDT:USDT", "BTC", active=False),
            "future": mexc_market("ETH/USDT:USDT", "ETH", swap=False),
            "inverse": mexc_market("XRP/USDT:USDT", "XRP", linear=False),
            "spot": mexc_market("SOL/USDT:USDT", "SOL", contract=False),
            "other-quote": mexc_market(
                "ADA/USDC:USDC", "ADA", quote="USDC"
            ),
        }

        self.assertEqual(
            scanner.match_mexc_symbols(
                ["BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "ADAUSDC"],
                markets,
            ),
            {},
        )


class MexcCandleTests(unittest.TestCase):
    def test_loads_mexc_ohlcv_and_excludes_the_open_candle(self) -> None:
        rows = [
            [3000, 3, 5, 2.5, 4, 10],
            [1000, 1, 3, 0.5, 2, 10],
            [2000, 2, 4, 1.5, 3, 10],
        ]
        exchange = unittest.mock.Mock()
        exchange.fetch_ohlcv.return_value = rows

        with patch.object(scanner, "mexc_exchange", return_value=exchange):
            candles = scanner.get_closed_candles(
                "BTC/USDT:USDT", "4h", scanner.RequestPacer(0)
            )

        self.assertEqual([candle.open_time for candle in candles], [1000, 2000])
        self.assertEqual(candles[0].close, 2.0)
        exchange.fetch_ohlcv.assert_called_once_with(
            "BTC/USDT:USDT", "4h", limit=scanner.KLINE_LIMIT
        )

    def test_scan_uses_mexc_symbol_but_retains_binance_ticker(self) -> None:
        candles = [
            scanner.Candle(1, 1, 2, 0.5, 1.5),
            scanner.Candle(2, 1.5, 2, 1, 1.8),
            scanner.Candle(3, 1.8, 2, 1.2, 1.9),
        ]
        zones = {
            "bu_ob_inside": False,
            "bu_ob_confirm": False,
            "be_ob_inside": False,
            "ftr": False,
            "reversal": False,
        }

        with (
            patch.object(scanner, "get_closed_candles", return_value=candles) as fetch,
            patch.object(scanner, "scan_structure", return_value={"event": None}),
            patch.object(scanner, "scan_indicator_zones", return_value=zones),
        ):
            result = scanner.scan_pair(
                "BTCUSDT", "4h", scanner.RequestPacer(0), "BTC/USDT:USDT"
            )

        fetch.assert_called_once_with(
            "BTC/USDT:USDT", "4h", unittest.mock.ANY
        )
        self.assertEqual(result["symbol"], "BTCUSDT")
        self.assertFalse(result["error"])


if __name__ == "__main__":
    unittest.main()

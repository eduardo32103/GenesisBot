from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

from api.main import create_app
from api.main import _yahoo_asset_chart_payload
from services.dashboard.get_asset_chart_series import get_asset_chart_series


class DashboardAssetChartSeriesTests(unittest.TestCase):
    @patch("services.dashboard.get_asset_chart_series.FmpClient")
    @patch("services.dashboard.get_asset_chart_series.load_settings")
    def test_does_not_call_fmp_when_live_is_disabled(self, mock_settings: Mock, mock_fmp_client: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=False)

        payload = get_asset_chart_series("NVDA", "1Y")

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["status"], "fmp_disabled")
        self.assertEqual(payload["ticker"], "NVDA")
        self.assertEqual(payload["timeframe"], "1Y")
        self.assertEqual(payload["selected_range"], "1Y")
        self.assertEqual(payload["points"], [])
        mock_fmp_client.assert_not_called()

    @patch("services.dashboard.get_asset_chart_series.FmpClient")
    @patch("services.dashboard.get_asset_chart_series.load_settings")
    def test_builds_annual_series_from_fmp_eod_history(self, mock_settings: Mock, mock_fmp_client: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=True)
        client = mock_fmp_client.return_value
        client.get_quote.return_value = {
            "price": 120,
            "change": 5,
            "changesPercentage": 4.3,
            "name": "NVIDIA",
        }
        client.get_profile.return_value = {"companyName": "NVIDIA Corporation"}
        client.get_full_historical_eod.return_value = [
            {"date": "2026-01-03", "open": 115, "high": 122, "low": 114, "close": 120, "volume": 3000},
            {"date": "2026-01-01", "open": 98, "high": 101, "low": 97, "close": 100, "volume": 1000},
            {"date": "2026-01-02", "open": 100, "high": 112, "low": 99, "close": 110, "volume": 2000},
        ]

        payload = get_asset_chart_series("nvda", "1Y")

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["ticker"], "NVDA")
        self.assertEqual(payload["timeframe"], "1Y")
        self.assertEqual([point["date"] for point in payload["points"]], ["2026-01-01", "2026-01-02", "2026-01-03"])
        self.assertEqual(payload["ohlc"][0]["open"], 98)
        self.assertEqual(payload["returns"]["MAX"], 20.0)
        self.assertEqual(payload["summary"]["start_price"], 100)
        self.assertEqual(payload["summary"]["end_price"], 120)
        self.assertEqual(payload["summary"]["change"], 20)
        self.assertEqual(payload["summary"]["change_pct"], 20.0)
        self.assertEqual(payload["quote"]["price"], 120)
        self.assertIn("indicators", payload)
        self.assertEqual(payload["first_date"], "2026-01-01")
        self.assertEqual(payload["last_date"], "2026-01-03")
        self.assertEqual(payload["first_close"], 100)
        self.assertEqual(payload["last_close"], 120)
        self.assertEqual(payload["return_details"]["MAX"]["first_close"], 100)
        self.assertEqual(payload["return_details"]["MAX"]["last_close"], 120)
        self.assertEqual(payload["return_details"]["MAX"]["points_used"], 3)
        self.assertEqual(payload["source"]["endpoint"], "historical-price-eod/full")
        self.assertEqual(payload["fmp_endpoint_used"], "historical-price-eod/full")
        self.assertFalse(payload["has_full_history"])
        client.get_full_historical_eod.assert_called_once_with("NVDA", symbol_map=None)

    @patch("services.dashboard.get_asset_chart_series.FmpClient")
    @patch("services.dashboard.get_asset_chart_series.load_settings")
    def test_intraday_series_uses_five_minute_fmp_history(self, mock_settings: Mock, mock_fmp_client: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=True)
        client = mock_fmp_client.return_value
        client.get_quote.return_value = {"price": 608.75, "name": "Meta"}
        client.get_profile.return_value = {"companyName": "Meta Platforms"}
        client.get_full_historical_eod.return_value = [
            {"date": "2026-04-30", "open": 590, "high": 602, "low": 588, "close": 600},
            {"date": "2026-05-01", "open": 600, "high": 610, "low": 599, "close": 606},
        ]
        client.get_intraday_history.return_value = [
            {"date": "2026-05-01 09:35:00", "open": 601, "high": 607, "low": 600, "close": 606},
            {"date": "2026-05-01 09:30:00", "open": 600, "high": 602, "low": 599, "close": 601},
        ]

        payload = get_asset_chart_series("META", "1D")

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["timeframe"], "1D")
        self.assertEqual(payload["source"]["endpoint"], "historical-chart/5min")
        self.assertEqual(payload["summary"]["change_pct"], 0.8319)
        self.assertEqual(payload["ohlc"][0]["open"], 600)
        client.get_intraday_history.assert_called_once_with("META", interval="5min", limit=160, symbol_map=None)
        client.get_full_historical_eod.assert_called_once_with("META", symbol_map=None)

    @patch("services.dashboard.get_asset_chart_series.FmpClient")
    @patch("services.dashboard.get_asset_chart_series.load_settings")
    def test_crypto_chart_uses_fmp_usd_symbol_for_history(self, mock_settings: Mock, mock_fmp_client: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=True)
        client = mock_fmp_client.return_value
        client.get_quote.return_value = {"price": 64000, "name": "Bitcoin"}
        client.get_profile.return_value = {}
        client.get_full_historical_eod.return_value = [
            {"date": "2026-01-01", "open": 59000, "high": 61000, "low": 58000, "close": 60000},
            {"date": "2026-01-02", "open": 60000, "high": 65000, "low": 59800, "close": 64000},
        ]

        payload = get_asset_chart_series("BTC", "1W")

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["ticker"], "BTC")
        client.get_full_historical_eod.assert_called_once_with("BTC", symbol_map={"BTC": "BTCUSD"})

    @patch("services.dashboard.get_asset_chart_series.FmpClient")
    @patch("services.dashboard.get_asset_chart_series.load_settings")
    def test_max_uses_full_available_history_not_five_year_slice(self, mock_settings: Mock, mock_fmp_client: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=True)
        client = mock_fmp_client.return_value
        client.get_quote.return_value = {"price": 320, "name": "Long History"}
        client.get_profile.return_value = {}
        start = date(2016, 1, 1)
        rows = []
        for index in range(0, 2300):
            current = start + timedelta(days=index)
            price = 50 + index * 0.1
            rows.append({"date": current.isoformat(), "open": price, "high": price + 2, "low": price - 2, "close": price + 1})
        client.get_full_historical_eod.return_value = rows

        payload = get_asset_chart_series("NVDA", "MAX")

        self.assertTrue(payload["ok"])
        self.assertGreater(payload["max_history_years"], 5)
        self.assertGreater(payload["history_points"], 1260)
        self.assertEqual(payload["raw_eod_points"], 2300)
        self.assertNotEqual(payload["returns"]["MAX"], payload["returns"]["5Y"])
        self.assertEqual(payload["source"]["raw_eod_points"], 2300)
        self.assertEqual(payload["fmp_endpoint_used"], "historical-price-eod/full")
        self.assertTrue(payload["has_full_history"])
        self.assertFalse(payload["is_max_truncated"])
        self.assertFalse(payload["max_truncated"])
        self.assertFalse(payload["source"]["is_max_truncated"])
        self.assertFalse(payload["source"]["max_truncated"])
        self.assertEqual(payload["truncation_reason"], "")
        self.assertIn("MAX usa", payload["max_history_note"])
        self.assertEqual(payload["first_date"], "2016-01-01")
        self.assertEqual(payload["first_close"], 51)
        self.assertEqual(payload["return_details"]["MAX"]["first_close"], 51)
        self.assertEqual(payload["return_details"]["MAX"]["last_close"], 280.9)
        self.assertEqual(payload["return_details"]["MAX"]["points_used"], 2300)

    @patch("services.dashboard.get_asset_chart_series.FmpClient")
    @patch("services.dashboard.get_asset_chart_series.load_settings")
    def test_returns_clean_fallback_without_real_ohlc(self, mock_settings: Mock, mock_fmp_client: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=True)
        client = mock_fmp_client.return_value
        client.get_quote.return_value = {"price": 120, "name": "NVIDIA"}
        client.get_profile.return_value = {}
        client.get_full_historical_eod.return_value = [{"date": "2026-01-01", "close": 100}]

        payload = get_asset_chart_series("NVDA", "1Y")

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["status"], "no_data")
        self.assertEqual(payload["ohlc"], [])
        self.assertIn("OHLC", payload["message"])
        self.assertNotIn("fmp_devolvio", payload["truncation_reason"])
        self.assertIn("MAX", payload["max_history_note"])

    @patch("services.dashboard.get_asset_chart_series.FmpClient")
    @patch("services.dashboard.get_asset_chart_series.load_settings")
    def test_uses_fmp_light_price_history_when_full_ohlc_is_missing(self, mock_settings: Mock, mock_fmp_client: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=True)
        client = mock_fmp_client.return_value
        client.get_quote.return_value = {"price": 132, "change": 2, "changesPercentage": 1.54, "name": "NVIDIA"}
        client.get_profile.return_value = {"companyName": "NVIDIA Corporation"}
        client.get_full_historical_eod.return_value = []
        client.get_historical_price_light.return_value = [
            {"date": "2026-01-01", "price": 100, "volume": 1000},
            {"date": "2026-01-02", "price": 120, "volume": 2500},
            {"date": "2026-01-03", "price": 132, "volume": 4000},
        ]

        payload = get_asset_chart_series("NVDA", "1Y")

        self.assertTrue(payload["ok"])
        self.assertTrue(payload["price_only"])
        self.assertTrue(payload["source"]["price_only"])
        self.assertEqual(payload["source"]["fmp_endpoint_used"], "historical-price-eod/light")
        self.assertEqual(payload["ohlc"][0]["open"], 100)
        self.assertEqual(payload["ohlc"][0]["high"], 100)
        self.assertEqual(payload["ohlc"][0]["low"], 100)
        self.assertEqual(payload["ohlc"][0]["close"], 100)
        self.assertEqual(payload["ohlc"][-1]["volume"], 4000)
        self.assertEqual(payload["summary"]["end_price"], 132)
        client.get_historical_price_light.assert_called_once_with("NVDA", symbol_map=None)

    @patch("services.dashboard.get_asset_chart_series.FmpClient")
    @patch("services.dashboard.get_asset_chart_series.load_settings")
    def test_short_max_history_uses_clean_human_note(self, mock_settings: Mock, mock_fmp_client: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=True)
        client = mock_fmp_client.return_value
        client.get_quote.return_value = {"price": 120, "name": "Short History"}
        client.get_profile.return_value = {}
        client.get_full_historical_eod.return_value = [
            {"date": "2025-01-01", "open": 90, "high": 101, "low": 88, "close": 100},
            {"date": "2026-01-01", "open": 100, "high": 125, "low": 99, "close": 120},
        ]

        payload = get_asset_chart_series("NVDA", "MAX")

        self.assertTrue(payload["ok"])
        self.assertTrue(payload["is_max_truncated"])
        self.assertEqual(payload["truncation_reason"], "max_disponible_menor_o_igual_5y")
        self.assertIn("MAX disponible", payload["max_history_note"])
        self.assertNotIn("fmp_devolvio", payload["max_history_note"].lower())

    def test_app_config_exposes_asset_chart_endpoint(self) -> None:
        app_config = create_app()

        self.assertEqual(app_config["asset_chart_endpoint"], "/api/dashboard/asset/chart?ticker={symbol}&range={range}")

    @patch("api.main._yahoo_quote_row")
    @patch("api.main._yahoo_fetch_chart")
    def test_yahoo_fallback_populates_return_tiles_from_broader_series(self, mock_fetch: Mock, mock_quote: Mock) -> None:
        start = datetime(2025, 1, 1, tzinfo=timezone.utc)

        def chart_payload(days: int, start_price: float, step: float) -> dict:
            timestamps = []
            opens = []
            highs = []
            lows = []
            closes = []
            volumes = []
            for index in range(days):
                stamp = start + timedelta(days=index)
                close = start_price + index * step
                timestamps.append(int(stamp.timestamp()))
                opens.append(close - 0.5)
                highs.append(close + 1)
                lows.append(close - 1)
                closes.append(close)
                volumes.append(1_000 + index)
            return {"timestamp": timestamps, "indicators": {"quote": [{"open": opens, "high": highs, "low": lows, "close": closes, "volume": volumes}]}}

        def fake_fetch(ticker: str, timeframe: str = "1D") -> dict:
            if timeframe == "5Y":
                return chart_payload(520, 100, 0.5)
            return chart_payload(30, 200, 1)

        mock_fetch.side_effect = fake_fetch
        mock_quote.return_value = {"price": 229, "previous_close": 228, "quote_timestamp": "2026-05-11T00:00:00+00:00"}

        payload = _yahoo_asset_chart_payload("NVDA", "1M")

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["returns"]["1M"], 14.5)
        self.assertIsNotNone(payload["returns"]["1W"])
        self.assertIsNotNone(payload["returns"]["1Y"])
        self.assertIsNotNone(payload["returns"]["MAX"])
        self.assertEqual(payload["return_details"]["1M"]["points_used"], 30)


if __name__ == "__main__":
    unittest.main()

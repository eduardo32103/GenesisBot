from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from api.main import create_app
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
        client.get_historical_eod.return_value = [
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
        self.assertEqual(payload["source"]["endpoint"], "historical-price-eod/full")
        client.get_historical_eod.assert_called_once_with("NVDA", limit=None, symbol_map=None)

    @patch("services.dashboard.get_asset_chart_series.FmpClient")
    @patch("services.dashboard.get_asset_chart_series.load_settings")
    def test_intraday_series_uses_five_minute_fmp_history(self, mock_settings: Mock, mock_fmp_client: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=True)
        client = mock_fmp_client.return_value
        client.get_quote.return_value = {"price": 608.75, "name": "Meta"}
        client.get_profile.return_value = {"companyName": "Meta Platforms"}
        client.get_historical_eod.return_value = [
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
        client.get_historical_eod.assert_called_once_with("META", limit=None, symbol_map=None)

    @patch("services.dashboard.get_asset_chart_series.FmpClient")
    @patch("services.dashboard.get_asset_chart_series.load_settings")
    def test_crypto_chart_uses_fmp_usd_symbol_for_history(self, mock_settings: Mock, mock_fmp_client: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=True)
        client = mock_fmp_client.return_value
        client.get_quote.return_value = {"price": 64000, "name": "Bitcoin"}
        client.get_profile.return_value = {}
        client.get_historical_eod.return_value = [
            {"date": "2026-01-01", "open": 59000, "high": 61000, "low": 58000, "close": 60000},
            {"date": "2026-01-02", "open": 60000, "high": 65000, "low": 59800, "close": 64000},
        ]

        payload = get_asset_chart_series("BTC", "1W")

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["ticker"], "BTC")
        client.get_historical_eod.assert_called_once_with("BTC", limit=None, symbol_map={"BTC": "BTCUSD"})

    @patch("services.dashboard.get_asset_chart_series.FmpClient")
    @patch("services.dashboard.get_asset_chart_series.load_settings")
    def test_returns_clean_fallback_without_real_ohlc(self, mock_settings: Mock, mock_fmp_client: Mock) -> None:
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=True)
        client = mock_fmp_client.return_value
        client.get_quote.return_value = {"price": 120, "name": "NVIDIA"}
        client.get_profile.return_value = {}
        client.get_historical_eod.return_value = [{"date": "2026-01-01", "close": 100}]

        payload = get_asset_chart_series("NVDA", "1Y")

        self.assertFalse(payload["ok"])
        self.assertEqual(payload["status"], "no_data")
        self.assertEqual(payload["ohlc"], [])
        self.assertIn("OHLC", payload["message"])

    def test_app_config_exposes_asset_chart_endpoint(self) -> None:
        app_config = create_app()

        self.assertEqual(app_config["asset_chart_endpoint"], "/api/dashboard/asset/chart?ticker={symbol}&range={range}")


if __name__ == "__main__":
    unittest.main()

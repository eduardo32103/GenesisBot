from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from services.dashboard.get_radar_ticker_drilldown import get_dashboard_portfolio, get_dashboard_radar_ticker_drilldown
from services.dashboard.get_radar_snapshot import _shape_item, get_radar_snapshot


class DashboardRadarDrilldownTests(unittest.TestCase):
    @patch("services.dashboard.get_radar_snapshot.load_settings")
    @patch("services.dashboard.get_radar_snapshot.FmpClient")
    @patch("services.dashboard.get_radar_snapshot._fetch_wallet_rows")
    @patch("services.dashboard.get_radar_snapshot._parse_portfolio_fallback")
    def test_radar_snapshot_calculates_real_portfolio_when_units_exist(
        self,
        mock_fallback: Mock,
        mock_wallet: Mock,
        mock_fmp_client: Mock,
        mock_settings: Mock,
    ) -> None:
        mock_wallet.return_value = []
        mock_fallback.return_value = [
            _shape_item(
                "NVDA",
                display_name="NVIDIA",
                is_investment=True,
                units=10,
                entry_price=150,
                reference_price=150,
                origin="portfolio_fallback",
            )
        ]
        mock_settings.return_value = SimpleNamespace(
            database_url="",
            chat_id="",
            fmp_api_key="test-key",
            fmp_live_enabled=True,
        )
        client_instance = mock_fmp_client.return_value
        client_instance.get_quote.return_value = {
            "price": 200.0,
            "change": 4.0,
            "changesPercentage": 2.0,
            "name": "NVIDIA Corporation",
            "timestamp": "2026-05-01T14:30:00+00:00",
        }
        client_instance.get_profile.return_value = {
            "companyName": "NVIDIA Corporation",
            "sector": "Technology",
        }

        snapshot = get_radar_snapshot()

        item = snapshot["items"][0]
        self.assertEqual(item["ticker"], "NVDA")
        self.assertEqual(item["current_price"], 200.0)
        self.assertEqual(item["market_value"], 2000.0)
        self.assertEqual(item["cost_basis"], 1500.0)
        self.assertEqual(item["unrealized_pnl"], 500.0)
        self.assertEqual(item["unrealized_pnl_pct"], 33.33)
        self.assertEqual(item["weight_pct"], 100.0)
        self.assertEqual(item["status"], "en_alza")
        self.assertEqual(snapshot["summary"]["total_value"], 2000.0)
        self.assertEqual(snapshot["summary"]["number_of_positions"], 1)
        self.assertEqual(snapshot["summary"]["top_concentration"]["ticker"], "NVDA")
        client_instance.get_quote.assert_called_once_with("NVDA")
        client_instance.get_profile.assert_called_once_with("NVDA")

    @patch("services.dashboard.get_radar_snapshot.load_settings")
    @patch("services.dashboard.get_radar_snapshot.FmpClient")
    @patch("services.dashboard.get_radar_snapshot._fetch_wallet_rows")
    @patch("services.dashboard.get_radar_snapshot._parse_portfolio_fallback")
    def test_radar_snapshot_adds_live_price_to_watchlist_without_units(
        self,
        mock_fallback: Mock,
        mock_wallet: Mock,
        mock_fmp_client: Mock,
        mock_settings: Mock,
    ) -> None:
        mock_wallet.return_value = []
        mock_fallback.return_value = [
            _shape_item(
                "BNO",
                reference_price=61.39,
                origin="portfolio_fallback",
            )
        ]
        mock_settings.return_value = SimpleNamespace(
            database_url="",
            chat_id="",
            fmp_api_key="test-key",
            fmp_live_enabled=True,
        )
        client_instance = mock_fmp_client.return_value
        client_instance.get_quote.return_value = {
            "price": 64.25,
            "change": 1.25,
            "changesPercentage": 1.98,
            "previousClose": 63.0,
            "dayHigh": 65.0,
            "dayLow": 62.5,
            "volume": 123456,
            "name": "United States Brent Oil Fund",
            "timestamp": 1777618398,
        }

        snapshot = get_radar_snapshot()

        item = snapshot["items"][0]
        self.assertEqual(item["ticker"], "BNO")
        self.assertEqual(item["current_price"], 64.25)
        self.assertEqual(item["daily_change"], 1.25)
        self.assertEqual(item["daily_change_pct"], 1.98)
        self.assertEqual(item["previous_close"], 63.0)
        self.assertEqual(item["day_high"], 65.0)
        self.assertEqual(item["day_low"], 62.5)
        self.assertEqual(item["volume"], 123456.0)
        self.assertEqual(item["source"], "live")
        self.assertEqual(item["status"], "en_alza")
        self.assertEqual(item["market_value"], 0.0)
        self.assertIsNone(item["weight_pct"])
        self.assertIn("T", item["quote_timestamp"])
        self.assertEqual(snapshot["summary"]["total_value"], 0.0)
        self.assertEqual(snapshot["summary"]["watchlist_count"], 1)
        self.assertIn("watchlist con datos directos activos", snapshot["summary"]["genesis_perspective"])
        client_instance.get_quote.assert_called_once_with("BNO")
        client_instance.get_profile.assert_not_called()

    @patch("services.dashboard.get_radar_ticker_drilldown.load_settings")
    @patch("services.dashboard.get_radar_ticker_drilldown.FmpClient")
    @patch("services.dashboard.get_radar_ticker_drilldown.get_radar_snapshot")
    def test_dashboard_drilldown_uses_radar_snapshot_and_quote(
        self,
        mock_snapshot: Mock,
        mock_fmp_client: Mock,
        mock_settings: Mock,
    ) -> None:
        mock_snapshot.return_value = {
            "items": [
                {
                    "ticker": "NVDA",
                    "is_investment": True,
                    "amount_usd": 1200.0,
                    "reference_price": 480.0,
                    "updated_at": "2026-04-20T10:00:00+00:00",
                },
                {
                    "ticker": "IAU",
                    "is_investment": False,
                    "amount_usd": 0.0,
                    "reference_price": 61.0,
                    "updated_at": "2026-04-20T11:00:00+00:00",
                },
            ]
        }
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=True)
        client_instance = mock_fmp_client.return_value
        client_instance.get_quote.return_value = {
            "price": 525.0,
            "timestamp": "2026-04-23T15:00:00+00:00",
        }

        detail = get_dashboard_radar_ticker_drilldown("nvda")

        self.assertTrue(detail["found"])
        self.assertEqual(detail["symbol"], "NVDA")
        self.assertEqual(detail["entry_price"], 480.0)
        self.assertEqual(detail["current_price"], 525.0)
        self.assertEqual(detail["status"], "gain")
        client_instance.get_quote.assert_called_once_with("NVDA")

    @patch("services.dashboard.get_radar_ticker_drilldown.load_settings")
    @patch("services.dashboard.get_radar_ticker_drilldown.FmpClient")
    @patch("services.dashboard.get_radar_ticker_drilldown.get_radar_snapshot")
    def test_dashboard_drilldown_does_not_use_quote_when_fmp_live_is_disabled(
        self,
        mock_snapshot: Mock,
        mock_fmp_client: Mock,
        mock_settings: Mock,
    ) -> None:
        mock_snapshot.return_value = {
            "items": [
                {
                    "ticker": "NVDA",
                    "is_investment": True,
                    "amount_usd": 1200.0,
                    "reference_price": 480.0,
                    "updated_at": "2026-04-20T10:00:00+00:00",
                }
            ]
        }
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=False)

        detail = get_dashboard_radar_ticker_drilldown("nvda")

        self.assertTrue(detail["found"])
        self.assertIsNone(detail["current_price"])
        self.assertEqual(detail["status"], "unpriced")
        mock_fmp_client.assert_not_called()

    @patch("services.dashboard.get_radar_ticker_drilldown.load_settings")
    @patch("services.dashboard.get_radar_ticker_drilldown.FmpClient")
    @patch("services.dashboard.get_radar_ticker_drilldown.get_radar_snapshot")
    def test_dashboard_portfolio_snapshot_uses_radar_positions_without_quotes(
        self,
        mock_snapshot: Mock,
        mock_fmp_client: Mock,
        mock_settings: Mock,
    ) -> None:
        mock_snapshot.return_value = {
            "items": [
                {
                    "ticker": "BNO",
                    "is_investment": False,
                    "amount_usd": 0.0,
                    "reference_price": 61.39,
                    "updated_at": "2026-04-20T11:00:00+00:00",
                }
            ]
        }
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=False)

        snapshot = get_dashboard_portfolio()

        self.assertEqual(snapshot["tickers"], ["BNO"])
        self.assertEqual(snapshot["summary"]["position_count"], 1)
        self.assertEqual(snapshot["summary"]["investment_count"], 0)
        mock_fmp_client.assert_not_called()


if __name__ == "__main__":
    unittest.main()

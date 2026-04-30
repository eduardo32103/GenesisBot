from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from services.dashboard.get_radar_ticker_drilldown import get_dashboard_portfolio, get_dashboard_radar_ticker_drilldown


class DashboardRadarDrilldownTests(unittest.TestCase):
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

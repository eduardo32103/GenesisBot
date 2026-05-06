from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from services.dashboard.get_alerts_snapshot import get_alerts_snapshot


class DashboardAlertsEnrichedTests(unittest.TestCase):
    @patch("services.dashboard.get_radar_snapshot.get_radar_snapshot")
    @patch("services.dashboard.get_alerts_snapshot._fetch_alerts_snapshot")
    @patch("services.dashboard.get_alerts_snapshot.load_settings")
    def test_derived_alerts_include_price_volume_and_detail_fields(
        self,
        mock_settings: Mock,
        mock_fetch_alerts: Mock,
        mock_radar_snapshot: Mock,
    ) -> None:
        mock_settings.return_value = SimpleNamespace(database_url="")
        mock_fetch_alerts.return_value = {
            "generated_at": "2026-05-06T12:00:00+00:00",
            "summary": {
                "total_recent": 0,
                "active_alerts": 0,
                "engine_summary": "Todavia no hay alertas recientes registradas.",
            },
            "items": [],
            "recent_alerts": [],
        }
        mock_radar_snapshot.return_value = {
            "items": [
                {
                    "ticker": "NVDA",
                    "current_price": 100.0,
                    "daily_change": 4.0,
                    "daily_change_pct": 4.0,
                    "volume": 2000,
                    "avg_volume": 1000,
                    "dayHigh": 101.0,
                    "dayLow": 90.0,
                }
            ]
        }

        payload = get_alerts_snapshot()
        alert = payload["items"][0]

        self.assertEqual(alert["ticker"], "NVDA")
        self.assertEqual(alert["price"], 100.0)
        self.assertEqual(alert["change"], 4.0)
        self.assertEqual(alert["change_pct"], 4.0)
        self.assertEqual(alert["volume"], 2000)
        self.assertEqual(alert["relative_volume"], 2.0)
        self.assertEqual(alert["dollar_volume"], 200000.0)
        self.assertEqual(alert["support"], 90.0)
        self.assertEqual(alert["resistance"], 101.0)
        self.assertEqual(alert["source"], "technical")
        self.assertIn("mini_series", alert)
        self.assertIn("genesis_reading", alert)


if __name__ == "__main__":
    unittest.main()

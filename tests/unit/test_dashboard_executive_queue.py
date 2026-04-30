from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from services.dashboard.get_executive_queue_snapshot import get_executive_queue_snapshot


class DashboardExecutiveQueueTests(unittest.TestCase):
    @patch("services.dashboard.get_executive_queue_snapshot.load_settings")
    @patch("services.dashboard.get_executive_queue_snapshot._fetch_related_alerts")
    @patch("services.dashboard.get_executive_queue_snapshot.get_operational_reliability_snapshot")
    @patch("services.dashboard.get_executive_queue_snapshot.get_radar_snapshot")
    def test_queue_groups_assets_by_existing_decision_layer(
        self,
        mock_radar: Mock,
        mock_reliability: Mock,
        mock_related_alerts: Mock,
        mock_settings: Mock,
    ) -> None:
        mock_settings.return_value = SimpleNamespace(database_url="postgresql://local")
        mock_radar.return_value = {
            "summary": {
                "data_origin": "database",
            },
            "items": [
                {
                    "ticker": "NVDA",
                    "is_investment": True,
                    "amount_usd": 2400.0,
                    "reference_price": 525.0,
                    "source": "live",
                    "source_label": "live",
                    "updated_at": "2026-04-24T15:00:00+00:00",
                    "origin": "database",
                    "signal": "Posicion abierta",
                },
                {
                    "ticker": "BNO",
                    "is_investment": False,
                    "amount_usd": 0.0,
                    "reference_price": 61.39,
                    "source": "contingency",
                    "source_label": "contingencia",
                    "updated_at": "2026-04-20T11:00:00+00:00",
                    "origin": "database",
                    "signal": "En radar con referencia",
                },
            ],
        }
        mock_reliability.return_value = {
            "reliability": {
                "level": "ALTA",
                "decision_note": "Usable para decidir",
            }
        }
        mock_related_alerts.side_effect = [
            [
                {
                    "alert_id": "alert-001",
                    "ticker": "NVDA",
                    "alert_type_label": "Geo / Macro",
                    "summary": "Momentum y macro alineados.",
                    "status_label": "Validada positiva",
                    "created_at": "2026-04-20T14:30:00+00:00",
                    "evaluated_at": "2026-04-23T14:30:00+00:00",
                    "validation": "Validada | SWING_5D",
                }
            ],
            [],
        ]

        payload = get_executive_queue_snapshot()

        self.assertEqual(payload["summary"]["total_assets"], 2)
        self.assertEqual(payload["summary"]["review_now_count"], 1)
        self.assertEqual(payload["summary"]["wait_count"], 1)
        self.assertEqual(payload["buckets"]["revisar ahora"][0]["ticker"], "NVDA")
        self.assertEqual(payload["buckets"]["revisar ahora"][0]["priority"], "alta")
        self.assertEqual(payload["buckets"]["revisar ahora"][0]["decision"], "revisar ahora")
        self.assertEqual(payload["buckets"]["revisar ahora"][0]["signal_or_context"], "Geo / Macro | Validada positiva")
        self.assertEqual(payload["buckets"]["esperar"][0]["ticker"], "BNO")
        self.assertFalse(payload["meta"]["uses_live_quotes"])

    @patch("services.dashboard.get_executive_queue_snapshot.load_settings")
    @patch("services.dashboard.get_executive_queue_snapshot._fetch_related_alerts")
    @patch("services.dashboard.get_executive_queue_snapshot.get_operational_reliability_snapshot")
    @patch("services.dashboard.get_executive_queue_snapshot.get_radar_snapshot")
    def test_queue_stays_honest_when_reliability_is_low(
        self,
        mock_radar: Mock,
        mock_reliability: Mock,
        mock_related_alerts: Mock,
        mock_settings: Mock,
    ) -> None:
        mock_settings.return_value = SimpleNamespace(database_url="")
        mock_radar.return_value = {
            "summary": {
                "data_origin": "portfolio_fallback",
            },
            "items": [
                {
                    "ticker": "IAU",
                    "is_investment": False,
                    "amount_usd": 0.0,
                    "reference_price": 0.0,
                    "source": "unavailable",
                    "source_label": "unavailable",
                    "updated_at": "",
                    "origin": "portfolio_fallback",
                    "signal": "Solo vigilancia",
                }
            ],
        }
        mock_reliability.return_value = {
            "reliability": {
                "level": "BAJA",
                "decision_note": "No concluyente",
            }
        }
        mock_related_alerts.return_value = []

        payload = get_executive_queue_snapshot()

        self.assertEqual(payload["summary"]["total_assets"], 1)
        self.assertEqual(payload["summary"]["reliability_level"], "baja")
        self.assertEqual(payload["buckets"]["no concluyente"][0]["ticker"], "IAU")
        self.assertEqual(payload["buckets"]["no concluyente"][0]["decision"], "no concluyente")
        self.assertEqual(payload["buckets"]["no concluyente"][0]["main_risk"], "Confiabilidad operativa baja")


if __name__ == "__main__":
    unittest.main()

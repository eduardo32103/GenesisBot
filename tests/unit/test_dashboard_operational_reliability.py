from __future__ import annotations

import unittest
from unittest.mock import patch

from services.dashboard.get_operational_reliability_snapshot import get_operational_reliability_snapshot


class DashboardOperationalReliabilityTests(unittest.TestCase):
    @patch("services.dashboard.get_operational_reliability_snapshot.get_operational_health")
    @patch("services.dashboard.get_operational_reliability_snapshot.get_fmp_dependencies_snapshot")
    @patch("services.dashboard.get_operational_reliability_snapshot.get_alerts_snapshot")
    @patch("services.dashboard.get_operational_reliability_snapshot.get_radar_snapshot")
    def test_reliability_is_high_when_db_backed_and_provider_is_stable(
        self,
        mock_radar,
        mock_alerts,
        mock_fmp,
        mock_health,
    ) -> None:
        mock_health.return_value = {
            "system": {
                "status": "online",
                "summary": "Persistencia activa.",
            },
            "bot": {
                "heartbeat_age_seconds": 12,
            },
        }
        mock_radar.return_value = {
            "summary": {
                "data_origin": "database",
                "tracked_count": 2,
                "reference_count": 2,
            },
            "items": [
                {"ticker": "NVDA", "source": "live"},
                {"ticker": "BNO", "source": "live"},
            ],
        }
        mock_alerts.return_value = {
            "summary": {
                "data_origin": "database",
                "total_recent": 4,
                "validated_alerts": 3,
            }
        }
        mock_fmp.return_value = {
            "provider": {
                "status": "OK",
                "degraded": False,
            },
            "usage": {
                "quote": {"fetch": 4},
                "intraday": {"fetch": 1},
                "eod": {},
                "news": {},
            },
            "signals": {
                "cooldown_active": 0,
                "quota": 0,
                "access": 0,
            },
            "meta": {
                "source": "runtime_snapshot",
            },
        }

        payload = get_operational_reliability_snapshot()

        self.assertEqual(payload["reliability"]["level"], "ALTA")
        self.assertEqual(payload["reliability"]["decision_note"], "Usable para decidir")
        self.assertIn("Salud operativa online", payload["reliability"]["live_parts"])
        self.assertIn("Radar / cartera desde DB", payload["reliability"]["live_parts"])
        self.assertIn("Alertas desde DB", payload["reliability"]["live_parts"])
        self.assertIn("Cotizaciones quote", payload["reliability"]["fmp_dependent_parts"])
        self.assertEqual(payload["reliability"]["degraded_count"], 0)
        self.assertEqual(payload["signals"]["health_status"], "online")

    @patch("services.dashboard.get_operational_reliability_snapshot.get_operational_health")
    @patch("services.dashboard.get_operational_reliability_snapshot.get_fmp_dependencies_snapshot")
    @patch("services.dashboard.get_operational_reliability_snapshot.get_alerts_snapshot")
    @patch("services.dashboard.get_operational_reliability_snapshot.get_radar_snapshot")
    def test_reliability_is_low_when_radar_and_alerts_are_not_db_backed(
        self,
        mock_radar,
        mock_alerts,
        mock_fmp,
        mock_health,
    ) -> None:
        mock_health.return_value = {
            "system": {
                "status": "degraded",
                "summary": "Sin persistencia confirmada.",
            },
            "bot": {
                "heartbeat_age_seconds": 0,
            },
        }
        mock_radar.return_value = {
            "summary": {
                "data_origin": "portfolio_fallback",
                "tracked_count": 3,
                "reference_count": 1,
            },
            "items": [
                {"ticker": "BNO", "source": "contingency"},
                {"ticker": "IAU", "source": "unavailable"},
                {"ticker": "IXC", "source": "cache"},
            ],
        }
        mock_alerts.return_value = {
            "summary": {
                "data_origin": "unavailable",
                "total_recent": 0,
                "validated_alerts": 0,
            }
        }
        mock_fmp.return_value = {
            "provider": {
                "status": "DEGRADED",
                "degraded": True,
            },
            "usage": {
                "quote": {"throttle": 2, "quota": 1},
                "intraday": {},
                "eod": {},
                "news": {},
            },
            "signals": {
                "cooldown_active": 1,
                "quota": 1,
                "access": 0,
            },
            "meta": {
                "source": "runtime_snapshot",
            },
        }

        payload = get_operational_reliability_snapshot()

        self.assertEqual(payload["reliability"]["level"], "BAJA")
        self.assertEqual(payload["reliability"]["decision_note"], "No concluyente")
        self.assertIn("Radar / cartera desde portfolio.json", payload["reliability"]["fallback_parts"])
        self.assertIn("Salud operativa degradada", payload["reliability"]["degraded_parts"])
        self.assertIn("Alertas sin lectura real de DB", payload["reliability"]["degraded_parts"])
        self.assertIn("Proveedor FMP degradado", payload["reliability"]["degraded_parts"])
        self.assertEqual(payload["reliability"]["fmp_status_label"], "Degradado")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from services.dashboard.get_radar_ticker_drilldown import get_dashboard_radar_ticker_drilldown


class DashboardAssetTacticalTests(unittest.TestCase):
    @patch("services.dashboard.get_radar_ticker_drilldown.get_operational_reliability_snapshot")
    @patch("services.dashboard.get_radar_ticker_drilldown._fetch_related_alerts")
    @patch("services.dashboard.get_radar_ticker_drilldown.load_settings")
    @patch("services.dashboard.get_radar_ticker_drilldown.FmpClient")
    @patch("services.dashboard.get_radar_ticker_drilldown.get_radar_snapshot")
    def test_unified_asset_card_combines_portfolio_and_related_alerts(
        self,
        mock_snapshot: Mock,
        mock_fmp_client: Mock,
        mock_settings: Mock,
        mock_related_alerts: Mock,
        mock_reliability: Mock,
    ) -> None:
        mock_snapshot.return_value = {
            "items": [
                {
                    "ticker": "NVDA",
                    "is_investment": True,
                    "amount_usd": 2400.0,
                    "reference_price": 480.0,
                    "source": "contingency",
                    "source_label": "contingencia",
                    "source_note": "Ultima referencia persistida.",
                    "updated_at": "2026-04-20T10:00:00+00:00",
                    "origin": "database",
                    "signal": "Posicion abierta",
                }
            ]
        }
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=True, database_url="postgresql://local")
        client_instance = mock_fmp_client.return_value
        client_instance.get_quote.return_value = {
            "price": 525.0,
            "timestamp": "2026-04-24T15:00:00+00:00",
        }
        mock_related_alerts.return_value = [
            {
                "alert_id": "alert-001",
                "ticker": "NVDA",
                "alert_type_label": "Geo / Macro",
                "title": "Breakout confirmado",
                "summary": "Momentum y macro alineados.",
                "status": "completed",
                "status_label": "Validada positiva",
                "created_at": "2026-04-20T14:30:00+00:00",
                "evaluated_at": "2026-04-23T14:30:00+00:00",
                "score": 1.75,
                "validation": "Validada | SWING_5D",
                "result": "Ganadora",
            }
        ]
        mock_reliability.return_value = {
            "reliability": {
                "level": "ALTA",
                "decision_note": "Usable para decidir",
            }
        }

        detail = get_dashboard_radar_ticker_drilldown("nvda")

        self.assertTrue(detail["found"])
        self.assertEqual(detail["symbol"], "NVDA")
        self.assertEqual(detail["source_label"], "contingencia")
        self.assertEqual(detail["current_price"], 525.0)
        self.assertEqual(detail["related_alerts_count"], 1)
        self.assertEqual(detail["alert_state_summary"], "1 alerta reciente | Validada positiva")
        self.assertEqual(detail["context_note"], "Momentum y macro alineados.")
        self.assertEqual(detail["latest_alert_created_at"], "2026-04-20T14:30:00+00:00")
        self.assertEqual(detail["latest_alert_evaluated_at"], "2026-04-23T14:30:00+00:00")
        self.assertIn("Radar y cartera leidos desde la base principal.", detail["reliability_note"])
        self.assertIn("alert_events y alert_validations", detail["reliability_note"])
        self.assertEqual(detail["priority"], "alta")
        self.assertEqual(detail["decision"], "revisar ahora")
        self.assertEqual(detail["main_reason"], "Validada positiva")
        self.assertEqual(detail["dominant_signal"], "Geo / Macro | Validada positiva")
        self.assertEqual(detail["main_risk"], "Dato apoyado en fallback o contingencia")
        self.assertEqual(detail["current_reliability"], "alta")
        self.assertEqual(detail["decision_timestamp"], "2026-04-23T14:30:00+00:00")
        self.assertIn("senal reciente", detail["executive_note"])
        self.assertEqual(detail["dominant_factor"], "Alerta relacionada reciente")
        self.assertIn("1 alerta reciente | Validada positiva", detail["supporting_signals"])
        self.assertIn("Momentum y macro alineados.", detail["supporting_signals"])
        self.assertIn("Dato apoyado en fallback o contingencia", detail["blocking_signals"])
        self.assertIn("Fuente live o DB sin contingencia", detail["upgrade_requirements"])
        self.assertIn("revisar ahora", detail["decision_explanation"])

    @patch("services.dashboard.get_radar_ticker_drilldown.get_operational_reliability_snapshot")
    @patch("services.dashboard.get_radar_ticker_drilldown._fetch_related_alerts")
    @patch("services.dashboard.get_radar_ticker_drilldown.load_settings")
    @patch("services.dashboard.get_radar_ticker_drilldown.FmpClient")
    @patch("services.dashboard.get_radar_ticker_drilldown.get_radar_snapshot")
    def test_unified_asset_card_stays_honest_without_alerts_or_live_quote(
        self,
        mock_snapshot: Mock,
        mock_fmp_client: Mock,
        mock_settings: Mock,
        mock_related_alerts: Mock,
        mock_reliability: Mock,
    ) -> None:
        mock_snapshot.return_value = {
            "items": [
                {
                    "ticker": "BNO",
                    "is_investment": False,
                    "amount_usd": 0.0,
                    "reference_price": 61.39,
                    "source": "contingency",
                    "source_label": "contingencia",
                    "source_note": "Ultima referencia persistida.",
                    "updated_at": "2026-04-20T11:00:00+00:00",
                    "origin": "database",
                    "signal": "En radar con referencia",
                }
            ]
        }
        mock_settings.return_value = SimpleNamespace(fmp_api_key="test-key", fmp_live_enabled=True, database_url="postgresql://local")
        client_instance = mock_fmp_client.return_value
        client_instance.get_quote.return_value = {}
        mock_related_alerts.return_value = []
        mock_reliability.return_value = {
            "reliability": {
                "level": "BAJA",
                "decision_note": "No concluyente",
            }
        }

        detail = get_dashboard_radar_ticker_drilldown("BNO")

        self.assertTrue(detail["found"])
        self.assertEqual(detail["status"], "watchlist")
        self.assertEqual(detail["related_alerts_count"], 0)
        self.assertEqual(detail["alert_state_summary"], "Sin alertas recientes asociadas")
        self.assertEqual(detail["context_note"], "Sin contexto corto persistido para este activo.")
        self.assertEqual(detail["latest_alert_created_at"], "")
        self.assertEqual(detail["latest_alert_evaluated_at"], "")
        self.assertIsNone(detail["current_price"])
        self.assertIn("No hay alertas recientes asociadas", detail["reliability_note"])
        self.assertEqual(detail["priority"], "baja")
        self.assertEqual(detail["decision"], "no concluyente")
        self.assertEqual(detail["main_reason"], "La confiabilidad operativa esta baja para sostener una lectura firme.")
        self.assertEqual(detail["dominant_signal"], "En radar con referencia")
        self.assertEqual(detail["main_risk"], "Confiabilidad operativa baja")
        self.assertEqual(detail["current_reliability"], "baja")
        self.assertEqual(detail["decision_timestamp"], "2026-04-20T11:00:00+00:00")
        self.assertIn("no alcanza base suficiente", detail["executive_note"])
        self.assertEqual(detail["dominant_factor"], "Confiabilidad operativa baja")
        self.assertIn("Sin alertas recientes asociadas", detail["blocking_signals"])
        self.assertIn("Sin precio live confirmado", detail["blocking_signals"])
        self.assertIn("Confiabilidad operativa media o alta", detail["upgrade_requirements"])
        self.assertIn("no concluyente", detail["decision_explanation"])


if __name__ == "__main__":
    unittest.main()

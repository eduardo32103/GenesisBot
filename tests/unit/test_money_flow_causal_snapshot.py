from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from api.main import create_app
from services.dashboard.get_money_flow_causal_snapshot import get_money_flow_causal_snapshot


class MoneyFlowCausalSnapshotTests(unittest.TestCase):
    @patch("services.dashboard.get_money_flow_causal_snapshot.load_settings")
    @patch("services.dashboard.get_money_flow_causal_snapshot._fetch_related_alerts")
    @patch("services.dashboard.get_money_flow_causal_snapshot.get_macro_activity_snapshot")
    @patch("services.dashboard.get_money_flow_causal_snapshot.get_money_flow_detection_snapshot")
    def test_assigns_probable_earnings_cause_from_related_alert_context(
        self,
        mock_detection: Mock,
        mock_macro: Mock,
        mock_related_alerts: Mock,
        mock_settings: Mock,
    ) -> None:
        mock_settings.return_value = SimpleNamespace(database_url="postgresql://local")
        mock_detection.return_value = {
            "status": "detection_ready_causality_disabled",
            "items": [
                {
                    "ticker": "NVDA",
                    "primary_signal": "strong_inflow",
                    "primary_label": "compatible con entrada fuerte",
                    "detected_signal_count": 1,
                    "timestamp": "2026-04-25T15:00:00+00:00",
                    "signals": [
                        {
                            "type": "strong_inflow",
                            "detected": True,
                            "label": "compatible con entrada fuerte",
                        }
                    ],
                }
            ],
        }
        mock_macro.return_value = {
            "macro": {"available": False},
            "meta": {"macro_source": "unavailable", "activity_source": "unavailable"},
        }
        mock_related_alerts.return_value = [
            {
                "alert_id": "alert-nvda-001",
                "ticker": "NVDA",
                "alert_type_label": "Momentum",
                "title": "Earnings y guidance mejor de lo esperado",
                "summary": "El movimiento es compatible con resultados y revision de guidance.",
                "source": "runtime",
                "status_label": "Validada positiva",
                "created_at": "2026-04-25T14:00:00+00:00",
            }
        ]

        payload = get_money_flow_causal_snapshot()
        item = payload["items"][0]

        self.assertEqual(payload["phase"], "5.3")
        self.assertEqual(payload["status"], "probable_causality_ready")
        self.assertTrue(payload["summary"]["probable_causal_layer_enabled"])
        self.assertFalse(payload["summary"]["causality_confirmed"])
        self.assertFalse(payload["summary"]["institutional_claims_enabled"])
        self.assertFalse(payload["summary"]["recommendation_enabled"])
        self.assertEqual(item["probable_cause"], "earnings")
        self.assertEqual(item["probable_cause_label"], "compatible con reaccion a earnings")
        self.assertEqual(item["confidence"], "medium")
        self.assertEqual(item["related_alerts_count"], 1)
        self.assertIn("Lectura causal probable", item["honesty_note"])

    @patch("services.dashboard.get_money_flow_causal_snapshot.load_settings")
    @patch("services.dashboard.get_money_flow_causal_snapshot._fetch_related_alerts")
    @patch("services.dashboard.get_money_flow_causal_snapshot.get_macro_activity_snapshot")
    @patch("services.dashboard.get_money_flow_causal_snapshot.get_money_flow_detection_snapshot")
    def test_uses_macro_and_proxy_context_without_confirming_causality(
        self,
        mock_detection: Mock,
        mock_macro: Mock,
        mock_related_alerts: Mock,
        mock_settings: Mock,
    ) -> None:
        mock_settings.return_value = SimpleNamespace(database_url="postgresql://local")
        mock_detection.return_value = {
            "status": "detection_ready_causality_disabled",
            "items": [
                {
                    "ticker": "BNO",
                    "primary_signal": "risk_on_risk_off",
                    "primary_label": "compatible con risk-off",
                    "detected_signal_count": 2,
                    "timestamp": "2026-04-25T15:00:00+00:00",
                    "signals": [
                        {"type": "risk_on_risk_off", "detected": True},
                        {"type": "sector_pressure", "detected": True},
                    ],
                }
            ],
        }
        mock_macro.return_value = {
            "macro": {
                "available": True,
                "bias_label": "macro defensivo",
                "summary": "Risk-off por tasas y energia.",
                "dominant_risk": "riesgo geopolitico en petroleo",
                "high_risk_tickers": ["BNO"],
                "sensitive_tickers": [],
                "headlines": [],
            },
            "meta": {"macro_source": "runtime_snapshot", "activity_source": "runtime_snapshot"},
        }
        mock_related_alerts.return_value = []

        payload = get_money_flow_causal_snapshot()
        item = payload["items"][0]
        candidate_types = {candidate["cause_type"] for candidate in item["candidates"]}

        self.assertEqual(item["probable_cause"], "macro")
        self.assertEqual(item["probable_cause_label"], "consistente con contexto macro")
        self.assertIn("geopolitics", candidate_types)
        self.assertIn("etf_index_proxy", candidate_types)
        self.assertFalse(payload["source_status"]["fmp_live_queries_enabled"])
        self.assertEqual(payload["source_status"]["macro_source"], "runtime_snapshot")

    @patch("services.dashboard.get_money_flow_causal_snapshot.load_settings")
    @patch("services.dashboard.get_money_flow_causal_snapshot._fetch_related_alerts")
    @patch("services.dashboard.get_money_flow_causal_snapshot.get_macro_activity_snapshot")
    @patch("services.dashboard.get_money_flow_causal_snapshot.get_money_flow_detection_snapshot")
    def test_keeps_inconclusive_when_money_flow_confirmation_is_insufficient(
        self,
        mock_detection: Mock,
        mock_macro: Mock,
        mock_related_alerts: Mock,
        mock_settings: Mock,
    ) -> None:
        mock_settings.return_value = SimpleNamespace(database_url="")
        mock_detection.return_value = {
            "status": "detection_ready_causality_disabled",
            "items": [
                {
                    "ticker": "IAU",
                    "primary_signal": "insufficient_confirmation",
                    "primary_label": "confirmacion insuficiente",
                    "detected_signal_count": 1,
                    "timestamp": "",
                    "signals": [
                        {
                            "type": "insufficient_confirmation",
                            "detected": True,
                        }
                    ],
                }
            ],
        }
        mock_macro.return_value = {
            "macro": {"available": False},
            "meta": {"macro_source": "unavailable", "activity_source": "unavailable"},
        }
        mock_related_alerts.return_value = []

        payload = get_money_flow_causal_snapshot()
        item = payload["items"][0]

        self.assertEqual(item["probable_cause"], "inconclusive")
        self.assertEqual(item["probable_cause_label"], "no concluyente")
        self.assertEqual(item["candidates"], [])
        self.assertEqual(payload["summary"]["assets_inconclusive"], 1)
        self.assertEqual(payload["source_status"]["alert_context_source"], "unavailable")

    def test_payload_and_route_keep_honesty_guardrails_visible(self) -> None:
        app_config = create_app()

        self.assertEqual(app_config["money_flow_causal_endpoint"], "/api/dashboard/money-flow/causal")
        self.assertNotIn("buy", app_config["money_flow_causal_endpoint"])

    @patch("services.dashboard.get_money_flow_causal_snapshot.load_settings")
    @patch("services.dashboard.get_money_flow_causal_snapshot._fetch_related_alerts")
    @patch("services.dashboard.get_money_flow_causal_snapshot.get_macro_activity_snapshot")
    @patch("services.dashboard.get_money_flow_causal_snapshot.get_money_flow_detection_snapshot")
    def test_output_does_not_claim_institutionality_or_confirmed_cause(
        self,
        mock_detection: Mock,
        mock_macro: Mock,
        mock_related_alerts: Mock,
        mock_settings: Mock,
    ) -> None:
        mock_settings.return_value = SimpleNamespace(database_url="")
        mock_detection.return_value = {"status": "detection_ready_causality_disabled", "items": []}
        mock_macro.return_value = {
            "macro": {"available": False},
            "meta": {"macro_source": "unavailable", "activity_source": "unavailable"},
        }
        mock_related_alerts.return_value = []

        payload = get_money_flow_causal_snapshot()
        serialized = json.dumps(payload, ensure_ascii=False).lower()

        self.assertNotIn("institucionales comprando", serialized)
        self.assertNotIn("institucionales vendiendo", serialized)
        self.assertNotIn("causa confirmada", serialized)
        self.assertNotIn("compra recomendada", serialized)
        self.assertFalse(payload["summary"]["causality_confirmed"])


if __name__ == "__main__":
    unittest.main()

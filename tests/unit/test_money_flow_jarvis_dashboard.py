from __future__ import annotations

import json
import unittest
from unittest.mock import Mock, patch

from services.dashboard.get_money_flow_jarvis_answer import get_money_flow_jarvis_answer


class MoneyFlowJarvisDashboardTests(unittest.TestCase):
    @patch("services.dashboard.get_money_flow_jarvis_answer.get_money_flow_causal_snapshot")
    @patch("services.dashboard.get_money_flow_jarvis_answer.get_money_flow_detection_snapshot")
    def test_answers_simple_asset_question_conservatively(self, mock_detection: Mock, mock_causal: Mock) -> None:
        mock_detection.return_value = {
            "status": "detection_ready_causality_disabled",
            "items": [
                {
                    "ticker": "NVDA",
                    "primary_signal": "strong_inflow",
                    "primary_label": "compatible con entrada fuerte",
                    "timestamp": "2026-04-29T12:00:00+00:00",
                }
            ],
        }
        mock_causal.return_value = {
            "status": "probable_causality_ready",
            "items": [
                {
                    "ticker": "NVDA",
                    "money_flow_primary_signal": "strong_inflow",
                    "money_flow_primary_label": "compatible con entrada fuerte",
                    "probable_cause": "earnings",
                    "probable_cause_label": "compatible con reaccion a earnings",
                    "confidence": "medium",
                    "money_flow_timestamp": "2026-04-29T12:00:00+00:00",
                }
            ],
        }

        payload = get_money_flow_jarvis_answer("que pasa con nvda")

        self.assertEqual(payload["phase"], "5.5")
        self.assertEqual(payload["status"], "jarvis_money_flow_ready")
        self.assertEqual(payload["matched_ticker"], "NVDA")
        self.assertIn("NVDA merece atencion", payload["answer"])
        self.assertIn("strong_inflow", payload["answer"])
        self.assertIn("compatible con reaccion a earnings", payload["answer"])
        self.assertFalse(payload["source_status"]["fmp_live_queries_enabled"])

    @patch("services.dashboard.get_money_flow_jarvis_answer.get_money_flow_causal_snapshot")
    @patch("services.dashboard.get_money_flow_jarvis_answer.get_money_flow_detection_snapshot")
    def test_keeps_answer_inconclusive_when_money_flow_is_insufficient(self, mock_detection: Mock, mock_causal: Mock) -> None:
        mock_detection.return_value = {
            "status": "detection_ready_causality_disabled",
            "items": [{"ticker": "BNO", "primary_signal": "insufficient_confirmation"}],
        }
        mock_causal.return_value = {
            "status": "probable_causality_ready",
            "items": [
                {
                    "ticker": "BNO",
                    "money_flow_primary_signal": "insufficient_confirmation",
                    "probable_cause": "inconclusive",
                    "probable_cause_label": "No concluyente",
                    "confidence": "low",
                }
            ],
        }

        payload = get_money_flow_jarvis_answer("flujo de capital bno")
        serialized = json.dumps(payload, ensure_ascii=False).lower()

        self.assertIn("bno queda no concluyente", serialized)
        self.assertIn("faltan datos suficientes", serialized)
        self.assertNotIn("institucionales comprando", serialized)
        self.assertNotIn("causa confirmada", serialized)
        self.assertNotIn("compra recomendada", serialized)


if __name__ == "__main__":
    unittest.main()

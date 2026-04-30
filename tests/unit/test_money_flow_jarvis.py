from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from services.analysis.money_flow_jarvis import (
    build_money_flow_jarvis_briefing,
    extract_money_flow_ticker,
    should_handle_money_flow_intent,
)


class MoneyFlowJarvisTests(unittest.TestCase):
    def test_detects_money_flow_intent_and_optional_ticker(self) -> None:
        self.assertTrue(should_handle_money_flow_intent("flujo de capital nvda"))
        self.assertTrue(should_handle_money_flow_intent("money flow"))
        self.assertFalse(should_handle_money_flow_intent("radar de ballenas"))
        self.assertEqual(extract_money_flow_ticker("flujo de capital nvda"), "NVDA")

    @patch("services.analysis.money_flow_jarvis.get_money_flow_causal_snapshot")
    @patch("services.analysis.money_flow_jarvis.get_money_flow_detection_snapshot")
    def test_builds_conservative_briefing_from_5_2_and_5_3(self, mock_detection: Mock, mock_causal: Mock) -> None:
        mock_detection.return_value = {
            "summary": {"total_assets": 1, "assets_with_detected_flow": 1},
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
            "summary": {"total_assets": 1, "assets_with_probable_cause": 1},
            "items": [
                {
                    "ticker": "NVDA",
                    "money_flow_primary_signal": "strong_inflow",
                    "money_flow_primary_label": "compatible con entrada fuerte",
                    "probable_cause": "earnings",
                    "probable_cause_label": "compatible con reaccion a earnings",
                    "confidence": "medium",
                    "reason": "El contexto persistido menciona resultados.",
                    "money_flow_timestamp": "2026-04-29T12:00:00+00:00",
                }
            ],
        }

        briefing = build_money_flow_jarvis_briefing("money flow nvda")

        self.assertIn("<b>Flujo de Capital</b>", briefing)
        self.assertIn("<b>NVDA</b>", briefing)
        self.assertIn("<b>strong_inflow</b>", briefing)
        self.assertIn("compatible con reaccion a earnings", briefing)
        self.assertIn("Confiabilidad: Media", briefing)
        self.assertIn("No confirma causa final", briefing)
        self.assertNotIn("institucionales comprando", briefing.lower())
        self.assertNotIn("causa confirmada", briefing.lower())
        self.assertNotIn("compra recomendada", briefing.lower())

    @patch("services.analysis.money_flow_jarvis.get_money_flow_causal_snapshot")
    @patch("services.analysis.money_flow_jarvis.get_money_flow_detection_snapshot")
    def test_keeps_briefing_inconclusive_when_evidence_is_missing(self, mock_detection: Mock, mock_causal: Mock) -> None:
        mock_detection.return_value = {
            "summary": {"total_assets": 1, "assets_with_detected_flow": 0},
            "items": [
                {
                    "ticker": "IAU",
                    "primary_signal": "insufficient_confirmation",
                    "primary_label": "confirmacion insuficiente",
                    "timestamp": "",
                }
            ],
        }
        mock_causal.return_value = {
            "summary": {"total_assets": 1, "assets_inconclusive": 1},
            "items": [
                {
                    "ticker": "IAU",
                    "money_flow_primary_signal": "insufficient_confirmation",
                    "money_flow_primary_label": "confirmacion insuficiente",
                    "probable_cause": "inconclusive",
                    "probable_cause_label": "no concluyente",
                    "confidence": "low",
                    "reason": "Faltan insumos reales suficientes.",
                }
            ],
        }

        briefing = build_money_flow_jarvis_briefing("flujo de capital iau")

        self.assertIn("IAU", briefing)
        self.assertIn("insufficient_confirmation", briefing)
        self.assertIn("No concluyente", briefing)
        self.assertIn("faltan datos suficientes", briefing)


if __name__ == "__main__":
    unittest.main()

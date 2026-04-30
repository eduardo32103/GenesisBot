from __future__ import annotations

import unittest

from services.dashboard.get_money_flow_signal_model import get_money_flow_signal_model


class MoneyFlowSignalModelTests(unittest.TestCase):
    def test_model_declares_expected_signal_types(self) -> None:
        payload = get_money_flow_signal_model()
        signal_ids = {signal["id"] for signal in payload["signal_types"]}

        self.assertEqual(payload["phase"], "5.1")
        self.assertEqual(payload["status"], "contract_ready_detection_pending")
        self.assertEqual(
            signal_ids,
            {
                "strong_inflow",
                "strong_outflow",
                "volume_breakout",
                "price_volume_divergence",
                "sector_pressure",
                "risk_on_risk_off",
                "rotation",
                "insufficient_confirmation",
            },
        )

    def test_model_is_contract_only_and_keeps_detection_disabled(self) -> None:
        payload = get_money_flow_signal_model()
        summary = payload["summary"]

        self.assertFalse(summary["detection_enabled"])
        self.assertFalse(summary["causality_enabled"])
        self.assertFalse(summary["fmp_live_queries_enabled"])
        self.assertTrue(summary["dashboard_ready"])
        self.assertTrue(summary["bot_ready"])

    def test_model_keeps_honesty_guardrails_explicit(self) -> None:
        payload = get_money_flow_signal_model()

        self.assertIn("No afirmar institucionalidad si la fuente solo muestra volumen o precio.", payload["honesty_rules"])
        self.assertIn("institucionales comprando", payload["forbidden_claims"])
        self.assertIn("causa confirmada", payload["forbidden_claims"])
        for signal in payload["signal_types"]:
            self.assertIn("no concluyente", " ".join(signal["honest_language"]).lower())

    def test_model_lists_real_or_planned_sources_without_querying_them(self) -> None:
        payload = get_money_flow_signal_model()
        source_ids = {source["id"] for source in payload["source_catalog"]}

        self.assertIn("fmp_runtime_snapshot", source_ids)
        self.assertIn("radar_snapshot", source_ids)
        self.assertIn("alert_events", source_ids)
        self.assertIn("macro_activity_snapshot", source_ids)
        self.assertIn("sector_or_index_proxy", source_ids)


if __name__ == "__main__":
    unittest.main()

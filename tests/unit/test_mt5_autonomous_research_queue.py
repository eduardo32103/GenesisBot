from __future__ import annotations

import contextlib
import io
import unittest

from scripts.run_autonomous_research_queue import main as queue_main
from services.mt5.mt5_autonomous_research_queue import run_autonomous_research_queue


class MT5AutonomousResearchQueueTests(unittest.TestCase):
    def test_loads_persistent_research_lessons(self) -> None:
        result = run_autonomous_research_queue(
            persistent_events={
                "recent_research_lessons": [
                    {"family": "btc_h1", "lesson_type": "deep_validation_failure"},
                    {"family": "eurusd_h1_vwap", "lesson_type": "proxy_false_positive"},
                ],
                "db_degraded": False,
            },
            harvester_result=_harvester([]),
            intelligence_result=_intelligence(),
        )

        self.assertEqual(result["lessons_loaded"], 2)
        self.assertFalse(result["db_state"]["db_degraded"])
        self.assertEqual(result["db_state"]["source"], "injected")

    def test_avoids_rejected_and_degraded_families(self) -> None:
        result = run_autonomous_research_queue(
            persistent_events={},
            harvester_result=_harvester([]),
            intelligence_result=_intelligence(),
        )

        avoided = " ".join(row["label"] for row in result["avoided_families"])
        hypotheses = {row["family_name"] for row in result["next_hypotheses"]}
        self.assertIn("ETHUSD M30", avoided)
        self.assertIn("BTCUSD H1", avoided)
        self.assertIn("EURUSD H1 session_vwap_reclaim", avoided)
        self.assertNotIn("session_vwap_reclaim", hypotheses)
        self.assertIn("volatility_compression_breakout", hypotheses)
        self.assertIn("multi_timeframe_trend_pullback", hypotheses)
        self.assertGreaterEqual(result["rejected_families_loaded"], 1)
        self.assertGreaterEqual(result["degraded_profiles_loaded"], 1)

    def test_does_not_return_unknown_profile_candidate(self) -> None:
        row = {**_clean_candidate(), "profile": "unknown_profile", "source_identity_resolved": False}
        result = run_autonomous_research_queue(
            persistent_events={},
            harvester_result=_harvester([row]),
            intelligence_result=_intelligence(),
        )

        self.assertEqual(result["candidates_found"], 0)
        self.assertIsNone(result["top_candidate"])
        reasons = result["evaluated_candidates"][0]["rejection_reasons"]
        self.assertIn("unknown_profile", reasons)
        self.assertIn("source_identity_unresolved", reasons)

    def test_enforces_min_sample_gates(self) -> None:
        row = {**_clean_candidate(), "recent_closed": 12, "total_closed": 49}
        result = run_autonomous_research_queue(
            persistent_events={},
            harvester_result=_harvester([row]),
            intelligence_result=_intelligence(),
        )

        reasons = result["evaluated_candidates"][0]["rejection_reasons"]
        self.assertIn("recent_closed_below_20", reasons)
        self.assertIn("total_closed_below_50", reasons)
        self.assertEqual(result["recommendation"], "continue_research")

    def test_enforces_monte_carlo_gates(self) -> None:
        row = {
            **_clean_candidate(),
            "monte_carlo_stressed_pf": 0.91,
            "monte_carlo_stressed_expectancy": -0.01,
        }
        result = run_autonomous_research_queue(
            persistent_events={},
            harvester_result=_harvester([row]),
            intelligence_result=_intelligence(),
        )

        reasons = result["evaluated_candidates"][0]["rejection_reasons"]
        self.assertIn("monte_carlo_stressed_pf_below_1_05", reasons)
        self.assertIn("monte_carlo_stressed_expectancy_not_positive", reasons)
        self.assertEqual(result["candidates_found"], 0)

    def test_enforces_remove_best_gate(self) -> None:
        row = {**_clean_candidate(), "remove_best_5_pf": 0.98}
        result = run_autonomous_research_queue(
            persistent_events={},
            harvester_result=_harvester([row]),
            intelligence_result=_intelligence(),
        )

        self.assertIn("remove_best_5_pf_below_1", result["evaluated_candidates"][0]["rejection_reasons"])
        self.assertEqual(result["candidates_found"], 0)

    def test_clean_candidate_is_review_only_without_activation(self) -> None:
        result = run_autonomous_research_queue(
            persistent_events={},
            harvester_result=_harvester([_clean_candidate()]),
            intelligence_result=_intelligence(),
            run_fast_scans=True,
        )

        self.assertEqual(result["recommendation"], "paper_forward_candidate_review")
        self.assertEqual(result["candidates_found"], 1)
        self.assertEqual(result["top_candidate"]["candidate_status"], "queue_candidate_ready")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["paper_rotation_applied"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_dry_run_does_not_execute_heavy_scan(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = queue_main(["--no-persistent"])

        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("research_queue_state=dry_run_plan", text)
        self.assertIn("heavy_backtests_run=False", text)
        self.assertIn("candidate_activated=False", text)
        self.assertIn("broker_touched=False", text)
        self.assertIn("order_policy=journal_only_no_broker", text)

    def test_max_evaluations_respected(self) -> None:
        rows = [
            {**_clean_candidate(), "profile": f"gbpusd_h1_clean_{index}"}
            for index in range(5)
        ]
        result = run_autonomous_research_queue(
            persistent_events={},
            harvester_result=_harvester(rows),
            intelligence_result=_intelligence(),
            run_fast_scans=True,
            max_evaluations=2,
        )

        self.assertTrue(result["max_evaluations_respected"])
        self.assertEqual(result["candidate_evaluations_considered"], 2)
        self.assertEqual(len(result["evaluated_candidates"]), 2)


def _harvester(rows: list[dict[str, object]]) -> dict[str, object]:
    return {
        "status": "robust_candidate_harvester_ready",
        "recommendation": "paper_observation_review" if rows else "continue_research",
        "loaded_sources": ["processed.csv"],
        "missing_sources": [],
        "raw_rows": len(rows),
        "useful_rows": len(rows),
        "top_candidates": rows,
        "rejected_candidates": [],
    }


def _intelligence() -> dict[str, object]:
    return {
        "status": "research_intelligence_core_ready",
        "recommendation": "research_plan_ready",
        "recommended_next_research_phase": "design_volatility_compression_breakout_processed_feature_scan",
        "priority_queue": [
            {"family_name": "session_vwap_reclaim", "priority_score": 96},
            {"family_name": "volatility_compression_breakout", "priority_score": 88},
            {"family_name": "multi_timeframe_trend_pullback", "priority_score": 84},
        ],
        "next_hypotheses": [],
    }


def _clean_candidate() -> dict[str, object]:
    return {
        "symbol": "GBPUSD",
        "timeframe": "H1",
        "profile": "gbpusd_h1_volatility_compression_breakout_clean",
        "family": "volatility_compression_breakout",
        "source_identity_resolved": True,
        "total_closed": 90,
        "recent_closed": 28,
        "total_pf": 1.32,
        "recent_pf": 1.26,
        "expectancy": 0.18,
        "recent_expectancy": 0.11,
        "monte_carlo_stressed_pf": 1.08,
        "monte_carlo_stressed_expectancy": 0.05,
        "spread_x2_pf": 1.01,
        "remove_best_5_pf": 1.03,
        "single_trade_dependency": False,
        "fragile_regime_dependency": False,
        "degraded_by_registry": False,
        "rejected_by_research_registry": False,
        "sibling_risk": False,
    }


if __name__ == "__main__":
    unittest.main()

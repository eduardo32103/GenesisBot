from __future__ import annotations

import contextlib
import io
import unittest

from scripts.run_fast_edge_factory import main as factory_main
from services.mt5.mt5_fast_edge_factory import run_fast_edge_factory


class MT5FastEdgeFactoryTests(unittest.TestCase):
    def test_dry_run_does_not_execute_heavy_scans(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = factory_main(["--no-persistent"])

        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("factory_state=dry_run_plan", text)
        self.assertIn("heavy_backtests_run=False", text)
        self.assertIn("candidate_activated=False", text)
        self.assertIn("broker_touched=False", text)
        self.assertIn("order_policy=journal_only_no_broker", text)

    def test_max_evaluations_is_respected(self) -> None:
        rows = [{**_clean_candidate(), "profile": f"gbpusd_h1_vcb_{index}"} for index in range(5)]
        result = run_fast_edge_factory(
            run_fast_scans=True,
            max_evaluations=2,
            harvester_result=_harvester(rows),
            queue_result=_queue_result(),
        )

        self.assertTrue(result["max_evaluations_respected"])
        self.assertEqual(result["evaluations_count"], 2)
        self.assertEqual(result["unique_evaluations_count"], 2)

    def test_rejected_and_degraded_families_are_avoided(self) -> None:
        rows = [
            {
                **_clean_candidate(),
                "symbol": "ETHUSD",
                "timeframe": "M30",
                "profile": "eth_m30_vol_breakout_chop_guard_v1",
                "family": "volatility_breakout",
                "degraded_by_registry": True,
            }
        ]
        result = run_fast_edge_factory(
            run_fast_scans=True,
            harvester_result=_harvester(rows),
            queue_result=_queue_result(),
        )

        avoided = " ".join(row["label"] for row in result["avoided_families"])
        self.assertIn("ETHUSD M30", avoided)
        self.assertEqual(result["candidates_found"], 0)
        reasons = result["top_rejected"][0]["rejection_reasons"]
        self.assertIn("degraded_profile", reasons)
        self.assertIn("degraded_by_registry", reasons)

    def test_unknown_profile_is_rejected(self) -> None:
        result = run_fast_edge_factory(
            run_fast_scans=True,
            harvester_result=_harvester([{**_clean_candidate(), "profile": "unknown_profile", "source_identity_resolved": False}]),
            queue_result=_queue_result(),
        )

        reasons = result["top_rejected"][0]["rejection_reasons"]
        self.assertIn("unknown_profile", reasons)
        self.assertIn("source_identity_unresolved", reasons)
        self.assertEqual(result["candidates_found"], 0)

    def test_sample_gates_are_applied(self) -> None:
        row = {**_clean_candidate(), "total_closed": 45, "recent_closed": 12}
        result = run_fast_edge_factory(
            run_fast_scans=True,
            harvester_result=_harvester([row]),
            queue_result=_queue_result(),
        )

        reasons = result["top_rejected"][0]["rejection_reasons"]
        self.assertIn("total_closed_below_50", reasons)
        self.assertIn("recent_closed_below_20", reasons)

    def test_pf_and_expectancy_gates_are_applied(self) -> None:
        row = {**_clean_candidate(), "total_pf": 1.0, "recent_pf": 1.02, "expectancy": -0.1, "recent_expectancy": 0.0}
        result = run_fast_edge_factory(
            run_fast_scans=True,
            harvester_result=_harvester([row]),
            queue_result=_queue_result(),
        )

        reasons = result["top_rejected"][0]["rejection_reasons"]
        self.assertIn("total_pf_below_1_15", reasons)
        self.assertIn("recent_pf_below_1_15", reasons)
        self.assertIn("expectancy_not_positive", reasons)
        self.assertIn("recent_expectancy_not_positive", reasons)

    def test_remove_best_dependency_is_rejected(self) -> None:
        row = {**_clean_candidate(), "remove_best_5_pf": 0.8}
        result = run_fast_edge_factory(
            run_fast_scans=True,
            harvester_result=_harvester([row]),
            queue_result=_queue_result(),
        )

        self.assertIn("remove_best_5_pf_below_1", result["top_rejected"][0]["rejection_reasons"])

    def test_single_trade_dependency_is_rejected(self) -> None:
        row = {**_clean_candidate(), "single_trade_dependency": True}
        result = run_fast_edge_factory(
            run_fast_scans=True,
            harvester_result=_harvester([row]),
            queue_result=_queue_result(),
        )

        self.assertIn("single_trade_dependency", result["top_rejected"][0]["rejection_reasons"])

    def test_fragile_regime_dependency_is_rejected(self) -> None:
        row = {**_clean_candidate(), "fragile_regime_dependency": True}
        result = run_fast_edge_factory(
            run_fast_scans=True,
            harvester_result=_harvester([row]),
            queue_result=_queue_result(),
        )

        self.assertIn("fragile_regime_dependency", result["top_rejected"][0]["rejection_reasons"])

    def test_output_dedupe_collapses_repeated_dead_candidate(self) -> None:
        row = {**_clean_candidate(), "recent_closed": 5}
        result = run_fast_edge_factory(
            run_fast_scans=True,
            harvester_result=_harvester([row, dict(row), dict(row)]),
            queue_result=_queue_result(),
        )

        self.assertEqual(result["evaluations_count"], 3)
        self.assertEqual(result["unique_evaluations_count"], 1)
        self.assertEqual(len(result["top_rejected"]), 1)
        self.assertEqual(result["rejected_summary"][0]["rejected_count"], 1)

    def test_candidate_without_monte_carlo_needs_deep_validation_without_activation(self) -> None:
        row = _clean_candidate()
        result = run_fast_edge_factory(
            run_fast_scans=True,
            harvester_result=_harvester([row]),
            queue_result=_queue_result(),
        )

        self.assertEqual(result["recommendation"], "deep_validation_candidate_found")
        self.assertEqual(result["candidates_found"], 1)
        self.assertEqual(result["deep_validation_candidates"][0]["candidate_status"], "needs_deep_validation")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_candidate_with_passing_monte_carlo_is_deep_validation_candidate(self) -> None:
        row = {**_clean_candidate(), "monte_carlo_stressed_pf": 1.08}
        result = run_fast_edge_factory(
            run_fast_scans=True,
            harvester_result=_harvester([row]),
            queue_result=_queue_result(),
        )

        self.assertEqual(result["deep_validation_candidates"][0]["candidate_status"], "deep_validation_candidate")
        self.assertEqual(result["recommendation"], "deep_validation_candidate_found")


def _harvester(rows: list[dict[str, object]]) -> dict[str, object]:
    return {
        "ok": True,
        "status": "robust_candidate_harvester_ready",
        "recommendation": "paper_observation_review" if rows else "continue_research",
        "loaded_sources": ["processed.csv"],
        "missing_sources": [],
        "raw_rows": len(rows),
        "useful_rows": len(rows),
        "top_candidates": rows,
        "rejected_candidates": [],
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _queue_result() -> dict[str, object]:
    return {
        "db_state": {"source": "injected", "db_degraded": False, "status_endpoints_write_free": True},
        "lessons_loaded": 2,
        "rejected_families_loaded": 9,
        "degraded_profiles_loaded": 1,
        "avoided_families": [
            {"label": "ETHUSD M30 eth_m30_vol_breakout_chop_guard_v1", "source": "degradation_registry", "reason": "early_forward_edge_failed"},
            {"label": "BTCUSD H1 recent_liquidity_sweep", "source": "research_rejection_registry", "reason": "deep_validation_failed"},
        ],
        "recommended_next_research_phase": "volatility_compression_breakout",
        "recommended_next_script": "design_volatility_compression_breakout_processed_feature_scan",
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
        "spread_x2_pf": 1.01,
        "single_trade_dependency": False,
        "fragile_regime_dependency": False,
        "degraded_by_registry": False,
        "rejected_by_research_registry": False,
        "sibling_risk": False,
    }


if __name__ == "__main__":
    unittest.main()

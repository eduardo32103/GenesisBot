from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from scripts.run_robust_candidate_harvester import main as harvester_main
from services.mt5.mt5_robust_candidate_harvester import run_robust_candidate_harvester


class MT5RobustCandidateHarvesterTests(unittest.TestCase):
    def test_clean_high_sample_candidate_is_review_only(self) -> None:
        result = run_robust_candidate_harvester(rows=[_clean_candidate()], load_persistent=False)

        self.assertEqual(result["recommendation"], "paper_observation_review")
        self.assertEqual(len(result["top_candidates"]), 1)
        top = result["top_candidates"][0]
        self.assertEqual(top["candidate_status"], "robust_candidate_ready")
        self.assertEqual(top["rejection_reasons"], [])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["paper_rotation_applied"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_btc_h1_failed_deep_validation_profiles_are_rejected_by_registry(self) -> None:
        result = run_robust_candidate_harvester(
            rows=[
                {
                    **_clean_candidate(),
                    "symbol": "BTCUSD",
                    "timeframe": "H1",
                    "profile": "btcusd_h1_tournament_edge_candidate_paper_review_v1",
                    "family": "tournament_edge",
                },
                {
                    **_clean_candidate(),
                    "symbol": "BTCUSD",
                    "timeframe": "H1",
                    "profile": "btcusd_h1_recent_liquidity_sweep_baseline_source_1_deep_validation",
                    "family": "recent_liquidity_sweep",
                },
            ],
            load_persistent=False,
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertEqual(result["top_candidates"], [])
        reasons = {row["profile"]: row["rejection_reasons"] for row in result["rejected_candidates"]}
        self.assertIn("rejected_by_research_registry", reasons["btcusd_h1_tournament_edge_candidate_paper_review_v1"])
        self.assertIn("rejected_by_research_registry", reasons["btcusd_h1_recent_liquidity_sweep_baseline_source_1_deep_validation"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["broker_touched"])

    def test_unresolved_or_dependency_candidates_are_rejected(self) -> None:
        result = run_robust_candidate_harvester(
            rows=[
                {**_clean_candidate(), "profile": "unknown_profile", "source_identity_resolved": False},
                {**_clean_candidate(), "profile": "gbpusd_h1_dependency", "single_trade_dependency": True},
                {**_clean_candidate(), "profile": "gbpusd_h1_fragile", "fragile_regime_dependency": True},
                {**_clean_candidate(), "profile": "gbpusd_h1_weak_remove_best", "remove_best_5_pf": 0.8},
            ],
            load_persistent=False,
        )

        self.assertEqual(result["recommendation"], "continue_research")
        all_reasons = [reason for row in result["rejected_candidates"] for reason in row["rejection_reasons"]]
        self.assertIn("source_identity_unresolved", all_reasons)
        self.assertIn("single_trade_dependency", all_reasons)
        self.assertIn("fragile_regime_dependency", all_reasons)
        self.assertIn("remove_best_5_pf_below_1", all_reasons)
        self.assertFalse(result["paper_forward_onboarding_started"])

    def test_no_robust_candidate_recommends_research_intelligence(self) -> None:
        result = run_robust_candidate_harvester(
            rows=[{**_clean_candidate(), "recent_closed": 10, "recent_pf": 0.9}],
            load_persistent=False,
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertEqual(result["recommended_next_research_phase"], "run_research_intelligence_core_for_next_hypothesis")
        self.assertEqual(result["top_candidates"], [])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_script_smoke_runs_without_activation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "processed.csv"
            source.write_text(
                "symbol,timeframe,profile,family,recent_closed,total_closed,recent_pf,total_pf,expectancy,source_identity_resolved\n"
                "GBPUSD,H1,gbpusd_h1_clean,trend_pullback,25,80,1.3,1.4,0.2,true\n",
                encoding="utf-8",
            )
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = harvester_main(["--processed-source-paths", str(source), "--no-persistent"])

        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("MT5 Robust Candidate Harvester", text)
        self.assertIn("candidate_activated=False", text)
        self.assertIn("broker_touched=False", text)
        self.assertIn("order_policy=journal_only_no_broker", text)


def _clean_candidate() -> dict[str, object]:
    return {
        "symbol": "GBPUSD",
        "timeframe": "H1",
        "profile": "gbpusd_h1_clean_candidate",
        "family": "trend_pullback",
        "source_identity_resolved": True,
        "recent_closed": 24,
        "total_closed": 80,
        "recent_pf": 1.28,
        "total_pf": 1.35,
        "expectancy": 0.16,
        "monte_carlo_stressed_pf": 1.08,
        "remove_best_5_pf": 1.02,
        "spread_x2_pf": 1.01,
        "single_trade_dependency": False,
        "fragile_regime_dependency": False,
    }


if __name__ == "__main__":
    unittest.main()

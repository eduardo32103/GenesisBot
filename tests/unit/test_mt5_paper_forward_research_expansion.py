from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.run_paper_forward_research_expansion import main as expansion_main
from services.mt5.mt5_paper_forward_research_expansion import run_paper_forward_research_expansion


class MT5PaperForwardResearchExpansionTests(unittest.TestCase):
    def test_clean_non_sibling_candidate_is_recommended_for_review_only(self) -> None:
        result = run_paper_forward_research_expansion(
            rows=[
                {
                    "symbol": "XAUUSD",
                    "timeframe": "M15",
                    "profile": "xauusd_m15_recent_session_open_continuation",
                    "family": "recent_session_open_continuation",
                    "recent_closed": 24,
                    "total_closed": 72,
                    "recent_pf": 1.22,
                    "total_pf": 1.34,
                    "expectancy": 0.12,
                    "monte_carlo_stressed_pf": 1.08,
                    "spread_x2_pf": 1.01,
                    "remove_best_5_pf": 1.04,
                    "fragile_regime_dependency": False,
                    "single_trade_dependency": False,
                }
            ],
            include_priority_candidates=False,
            load_default_sources=False,
        )

        self.assertEqual(result["recommendation"], "paper_forward_candidate_review")
        recommended = result["recommended_candidate"]
        self.assertEqual(recommended["symbol"], "XAUUSD")
        self.assertEqual(recommended["candidate_status"], "paper_forward_review_ready")
        self.assertEqual(recommended["recommended_next_action"], "paper_forward_candidate_review")
        self.assertFalse(recommended["degraded_by_registry"])
        self.assertFalse(recommended["sibling_risk"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_eth_m30_sibling_candidate_is_blocked_and_not_recommended(self) -> None:
        result = run_paper_forward_research_expansion(
            rows=[
                _degraded_eth_chop_guard_row(),
                {
                    "symbol": "ETHUSD",
                    "timeframe": "M30",
                    "profile": "eth_m30_vol_breakout_regime_filtered_v1",
                    "family": "recent_volatility_breakout",
                    "recent_closed": 21,
                    "total_closed": 82,
                    "recent_pf": 1.4045,
                    "total_pf": 1.8814,
                    "expectancy": 0.2344,
                    "monte_carlo_stressed_pf": 1.1464,
                    "spread_x2_pf": 1.934,
                    "remove_best_5_pf": 1.4835,
                    "fragile_regime_dependency": False,
                    "single_trade_dependency": False,
                },
            ],
            include_priority_candidates=False,
            load_default_sources=False,
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertIsNone(result["recommended_candidate"])
        blocked = next(row for row in result["ranking"] if row["profile"] == "eth_m30_vol_breakout_regime_filtered_v1")
        self.assertTrue(blocked["sibling_risk"])
        self.assertEqual(blocked["sibling_of_degraded_profile"], "eth_m30_vol_breakout_chop_guard_v1")
        self.assertEqual(blocked["candidate_status"], "blocked_by_sibling_risk")
        self.assertEqual(blocked["recommended_next_action"], "manual_review_or_new_family_required")
        excluded = next(row for row in result["ranking"] if row["profile"] == "eth_m30_vol_breakout_chop_guard_v1")
        self.assertEqual(excluded["candidate_status"], "excluded_by_degradation_registry")
        self.assertTrue(result["excluded_by_registry_or_sibling_risk"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])

    def test_near_miss_is_ranked_for_targeted_hardening(self) -> None:
        result = run_paper_forward_research_expansion(
            rows=[
                {
                    "symbol": "US500",
                    "timeframe": "H1",
                    "profile": "us500_h1_recent_session_open_continuation",
                    "family": "recent_session_open_continuation",
                    "recent_closed": 30,
                    "total_closed": 76,
                    "recent_pf": 1.18,
                    "total_pf": 1.22,
                    "expectancy": 0.07,
                    "monte_carlo_stressed_pf": 0.72,
                    "spread_x2_pf": 1.4,
                    "remove_best_5_pf": 1.08,
                    "fragile_regime_dependency": False,
                    "single_trade_dependency": False,
                }
            ],
            include_priority_candidates=False,
            load_default_sources=False,
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertIsNone(result["recommended_candidate"])
        self.assertEqual(len(result["near_misses"]), 1)
        near_miss = result["near_misses"][0]
        self.assertEqual(near_miss["candidate_status"], "near_miss_hardening_candidate")
        self.assertIn("monte_carlo_stressed_pf_below_1_05", near_miss["failed_gates"])
        self.assertEqual(near_miss["hardening_recommendation"], "worth_targeted_hardening")
        self.assertEqual(result["top_3_hardening_families"][0]["family"], "recent_session_open_continuation")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_eth_m30_volatility_breakout_cluster_is_blocked_from_expansion(self) -> None:
        result = run_paper_forward_research_expansion(
            rows=[
                {
                    "symbol": "ETHUSD",
                    "timeframe": "M30",
                    "profile": "eth_m30_vol_breakout_ny_core_session_v1",
                    "family": "recent_volatility_breakout",
                    "recent_closed": 22,
                    "total_closed": 75,
                    "recent_pf": 1.1261,
                    "total_pf": 1.7836,
                    "expectancy": 0.1757,
                    "monte_carlo_stressed_pf": 1.06,
                    "spread_x2_pf": 1.6904,
                    "remove_best_5_pf": 1.3432,
                    "fragile_regime_dependency": False,
                    "single_trade_dependency": False,
                }
            ],
            include_priority_candidates=False,
            load_default_sources=False,
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertIsNone(result["recommended_candidate"])
        row = result["ranking"][0]
        self.assertTrue(row["sibling_risk"])
        self.assertEqual(row["sibling_of_degraded_profile"], "eth_m30_vol_breakout_chop_guard_v1")
        self.assertEqual(row["sibling_risk_reason"], "same_degraded_eth_m30_volatility_breakout_cluster")
        self.assertEqual(row["candidate_status"], "blocked_by_sibling_risk")
        self.assertEqual(row["recommended_next_action"], "manual_review_or_new_family_required")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])

    def test_dependency_and_remove_best_gates_block_clean_review(self) -> None:
        result = run_paper_forward_research_expansion(
            rows=[
                {
                    "symbol": "BTCUSD",
                    "timeframe": "M30",
                    "profile": "btcusd_m30_recent_liquidity_sweep",
                    "family": "recent_liquidity_sweep",
                    "recent_closed": 22,
                    "total_closed": 65,
                    "recent_pf": 1.3,
                    "total_pf": 1.4,
                    "expectancy": 0.1,
                    "monte_carlo_stressed_pf": 1.12,
                    "spread_x2_pf": 1.02,
                    "remove_best_5_pf": 0.91,
                    "fragile_regime_dependency": True,
                    "single_trade_dependency": True,
                }
            ],
            include_priority_candidates=False,
            load_default_sources=False,
        )

        self.assertEqual(result["recommendation"], "continue_research")
        row = result["ranking"][0]
        self.assertEqual(row["candidate_status"], "research_gate_failed")
        self.assertIn("remove_best_5_pf_below_1_0", row["failed_gates"])
        self.assertIn("fragile_regime_dependency", row["failed_gates"])
        self.assertIn("single_trade_dependency", row["failed_gates"])
        self.assertEqual(row["recommended_next_action"], "continue_research")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])

    def test_script_reads_processed_sources_without_activation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result_file = root / "multi_symbol_recent_first_results.csv"
            result_file.write_text(
                "\n".join(
                    [
                        "symbol,timeframe,profile,family,recent_closed,total_closed,recent_pf,total_pf,expectancy,monte_carlo_stressed_pf,spread_x2_pf,remove_best_5_pf,fragile_regime_dependency,single_trade_dependency",
                        "XAUUSD,M15,xauusd_m15_recent_session_open_continuation,recent_session_open_continuation,24,72,1.22,1.34,0.12,1.08,1.01,1.04,False,False",
                    ]
                ),
                encoding="utf-8",
            )

            code = expansion_main(["--results-dir", str(root)])

        self.assertEqual(code, 0)


def _degraded_eth_chop_guard_row() -> dict[str, object]:
    return {
        "symbol": "ETHUSD",
        "timeframe": "M30",
        "profile": "eth_m30_vol_breakout_chop_guard_v1",
        "family": "recent_volatility_breakout",
        "recent_closed": 21,
        "total_closed": 82,
        "recent_pf": 1.4045,
        "total_pf": 1.8823,
        "expectancy": 0.2349,
        "monte_carlo_stressed_pf": 1.1472,
        "spread_x2_pf": 1.9349,
        "remove_best_5_pf": 1.4846,
        "fragile_regime_dependency": False,
        "single_trade_dependency": False,
    }


if __name__ == "__main__":
    unittest.main()

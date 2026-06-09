from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from scripts.run_new_family_edge_discovery import main as discovery_main
from services.mt5.mt5_new_family_edge_discovery import run_new_family_edge_discovery


class MT5NewFamilyEdgeDiscoveryTests(unittest.TestCase):
    def test_clean_new_family_candidate_is_recommended_for_review_only(self) -> None:
        result = run_new_family_edge_discovery(
            rows=[
                {
                    "symbol": "US500",
                    "timeframe": "M30",
                    "family": "recent_failed_breakout_reversal",
                    "profile": "us500_m30_failed_breakout_reversal_clean",
                    "recent_closed": 22,
                    "total_closed": 70,
                    "recent_pf": 1.22,
                    "total_pf": 1.34,
                    "expectancy": 0.12,
                    "monte_carlo_stressed_pf": 1.08,
                    "monte_carlo_stressed_expectancy": 0.03,
                    "spread_x2_pf": 1.02,
                    "remove_best_5_pf": 1.01,
                    "fragile_regime_dependency": False,
                    "single_trade_dependency": False,
                }
            ],
            load_default_sources=False,
        )

        self.assertEqual(result["recommendation"], "paper_forward_candidate_review")
        recommended = result["recommended_candidate"]
        self.assertEqual(recommended["symbol"], "US500")
        self.assertEqual(recommended["candidate_status"], "paper_forward_review_ready")
        self.assertEqual(recommended["conceptual_family"], "range_breakout_failed_retest")
        self.assertEqual(recommended["rejection_reasons"], [])
        self.assertFalse(recommended["degraded_by_registry"])
        self.assertFalse(recommended["sibling_risk"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_failed_clusters_are_excluded_by_research_rejection_registry(self) -> None:
        result = run_new_family_edge_discovery(
            rows=[
                {
                    "symbol": "ETHUSD",
                    "timeframe": "M30",
                    "family": "recent_volatility_breakout",
                    "profile": "eth_m30_vol_breakout_regime_filtered_v1",
                    "recent_closed": 25,
                    "total_closed": 80,
                    "recent_pf": 1.4,
                    "total_pf": 1.8,
                    "expectancy": 0.2,
                    "monte_carlo_stressed_pf": 1.1,
                    "monte_carlo_stressed_expectancy": 0.04,
                    "spread_x2_pf": 1.2,
                    "remove_best_5_pf": 1.1,
                },
                {
                    "symbol": "XAUUSD",
                    "timeframe": "M15",
                    "family": "recent_session_open_continuation",
                    "profile": "xau_m15_session_baseline",
                    "recent_closed": 25,
                    "total_closed": 80,
                    "recent_pf": 1.4,
                    "total_pf": 1.8,
                    "expectancy": 0.2,
                    "monte_carlo_stressed_pf": 1.1,
                    "monte_carlo_stressed_expectancy": 0.04,
                    "spread_x2_pf": 1.2,
                    "remove_best_5_pf": 1.1,
                },
                {
                    "symbol": "BTCUSD",
                    "timeframe": "H1",
                    "family": "recent_ema_reclaim",
                    "profile": "btc_h1_ema_reclaim_volatility_guard",
                    "recent_closed": 25,
                    "total_closed": 80,
                    "recent_pf": 1.4,
                    "total_pf": 1.8,
                    "expectancy": 0.2,
                    "monte_carlo_stressed_pf": 1.1,
                    "monte_carlo_stressed_expectancy": 0.04,
                    "spread_x2_pf": 1.2,
                    "remove_best_5_pf": 1.1,
                },
                {
                    "symbol": "BTCUSD",
                    "timeframe": "M30",
                    "family": "recent_london_us_breakout",
                    "profile": "btc_m30_london_us_breakout_strict_trailing",
                    "recent_closed": 25,
                    "total_closed": 80,
                    "recent_pf": 1.4,
                    "total_pf": 1.8,
                    "expectancy": 0.2,
                    "monte_carlo_stressed_pf": 1.1,
                    "monte_carlo_stressed_expectancy": 0.04,
                    "spread_x2_pf": 1.2,
                    "remove_best_5_pf": 1.1,
                },
            ],
            load_default_sources=False,
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertIsNone(result["recommended_candidate"])
        self.assertEqual(result["ranking"], [])
        excluded = result["excluded_by_registry_or_sibling_risk"]
        reasons = {row["research_rejection_reason"] for row in excluded}
        self.assertIn("eth_m30_volatility_breakout_cluster_degraded_or_sibling_risk", reasons)
        self.assertIn("xau_m15_session_open_continuation_failed_mc_and_remove_best_5", reasons)
        self.assertIn("btc_h1_ema_reclaim_failed_pf_mc_remove_best_and_dependency_gates", reasons)
        self.assertIn("btc_m30_london_us_breakout_failed_deep_sample_validation", reasons)
        self.assertTrue(all(row["candidate_status"] == "excluded_by_research_rejection_registry" for row in excluded))
        self.assertTrue(all(row["recommended_next_action"] == "skip_rejected_family" for row in excluded))
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])

    def test_near_miss_ranking_reports_shortfalls(self) -> None:
        result = run_new_family_edge_discovery(
            rows=[
                {
                    "symbol": "EURUSD",
                    "timeframe": "H1",
                    "family": "recent_liquidity_sweep",
                    "profile": "eurusd_h1_liquidity_sweep_near_miss",
                    "recent_closed": 18,
                    "total_closed": 55,
                    "recent_pf": 1.12,
                    "total_pf": 1.18,
                    "expectancy": 0.05,
                    "monte_carlo_stressed_pf": 0.97,
                    "monte_carlo_stressed_expectancy": 0.01,
                    "spread_x2_pf": 1.0,
                    "remove_best_5_pf": 1.02,
                    "fragile_regime_dependency": False,
                    "single_trade_dependency": False,
                }
            ],
            load_default_sources=False,
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertEqual(len(result["top_near_misses"]), 1)
        near_miss = result["top_near_misses"][0]
        self.assertEqual(near_miss["candidate_status"], "near_miss")
        self.assertEqual(near_miss["conceptual_family"], "liquidity_sweep_reversal")
        self.assertIn("monte_carlo_stressed_pf_below_1_05", near_miss["rejection_reasons"])
        self.assertIn("monte_carlo_stressed_pf", near_miss["gate_shortfalls"])
        self.assertEqual(result["next_expansion"][0]["action"], "targeted_hardening_review")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_dependency_and_remove_best_gates_block_clean_review(self) -> None:
        result = run_new_family_edge_discovery(
            rows=[
                {
                    "symbol": "GBPUSD",
                    "timeframe": "M30",
                    "family": "recent_range_reversion",
                    "profile": "gbpusd_m30_range_reversion_dependency",
                    "recent_closed": 22,
                    "total_closed": 65,
                    "recent_pf": 1.3,
                    "total_pf": 1.4,
                    "expectancy": 0.1,
                    "monte_carlo_stressed_pf": 1.12,
                    "monte_carlo_stressed_expectancy": 0.03,
                    "spread_x2_pf": 1.02,
                    "remove_best_5_pf": 0.91,
                    "fragile_regime_dependency": True,
                    "single_trade_dependency": True,
                }
            ],
            load_default_sources=False,
        )

        row = result["ranking"][0]
        self.assertEqual(result["recommendation"], "continue_research")
        self.assertEqual(row["candidate_status"], "research_gate_failed")
        self.assertIn("remove_best_5_pf_below_1", row["rejection_reasons"])
        self.assertIn("fragile_regime_dependency", row["rejection_reasons"])
        self.assertIn("single_trade_dependency", row["rejection_reasons"])
        self.assertEqual(row["recommended_next_action"], "continue_research")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])

    def test_no_useful_rows_continues_research_without_activation(self) -> None:
        result = run_new_family_edge_discovery(load_default_sources=False)

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertIsNone(result["recommended_candidate"])
        self.assertEqual(result["useful_rows"], 0)
        self.assertEqual(result["families_evaluated"], [])
        self.assertEqual(result["symbol_timeframes_evaluated"], [])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_script_reads_processed_sources_without_activation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result_file = root / "multi_symbol_recent_first_results.csv"
            result_file.write_text(
                "\n".join(
                    [
                        "symbol,timeframe,family,hardening_mode,recent_closed,total_closed,recent_pf,total_pf,total_expectancy,monte_carlo_stressed_pf,monte_carlo_stressed_expectancy,spread_x2_pf,remove_best_5_pf,fragile_regime_dependency,single_trade_dependency",
                        "US500,M30,recent_failed_breakout_reversal,baseline,24,72,1.22,1.34,0.12,1.08,0.03,1.01,1.04,False,False",
                    ]
                ),
                encoding="utf-8",
            )

            with contextlib.redirect_stdout(io.StringIO()):
                code = discovery_main(["--results-dir", str(root)])

        self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main()

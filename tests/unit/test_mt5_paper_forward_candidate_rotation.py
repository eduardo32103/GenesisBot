from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.run_paper_forward_candidate_rotation import main as rotation_main
from services.mt5.mt5_paper_forward_candidate_rotation import run_paper_forward_candidate_rotation


class MT5PaperForwardCandidateRotationTests(unittest.TestCase):
    def test_registry_degraded_eth_m30_profile_is_excluded_even_if_metrics_pass(self) -> None:
        result = run_paper_forward_candidate_rotation(
            rows=[
                {
                    "symbol": "ETHUSD",
                    "timeframe": "M30",
                    "profile": "eth_m30_vol_breakout_chop_guard_v1",
                    "family": "recent_volatility_breakout",
                    "recent_closed": 30,
                    "total_closed": 80,
                    "recent_pf": 1.8,
                    "total_pf": 1.7,
                    "expectancy": 0.22,
                    "monte_carlo_stressed_pf": 1.2,
                    "spread_x2_pf": 1.1,
                }
            ],
            include_priority_candidates=False,
            load_default_sources=False,
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertFalse(result["candidate_activated"])
        self.assertEqual(len(result["excluded_by_degradation_registry"]), 1)
        excluded = result["excluded_by_degradation_registry"][0]
        self.assertEqual(excluded["profile"], "eth_m30_vol_breakout_chop_guard_v1")
        self.assertTrue(excluded["degraded_by_registry"])
        self.assertEqual(excluded["candidate_status"], "excluded_by_degradation_registry")
        self.assertEqual(excluded["recommended_next_action"], "skip_degraded_profile")
        self.assertFalse(excluded["sibling_risk"])
        self.assertIn("degraded_by_persistent_registry", excluded["rejection_reasons"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_passing_non_degraded_candidate_is_recommended_for_human_review_only(self) -> None:
        result = run_paper_forward_candidate_rotation(
            rows=[
                {
                    "symbol": "XAUUSD",
                    "timeframe": "M15",
                    "profile": "xauusd_m15_recent_session_open_continuation",
                    "family": "recent_session_open_continuation",
                    "recent_closed": 22,
                    "total_closed": 70,
                    "recent_pf": 1.28,
                    "total_pf": 1.42,
                    "expectancy": 0.18,
                    "monte_carlo_stressed_pf": 1.11,
                    "spread_x2_pf": 1.02,
                    "fragile_regime_dependency": False,
                }
            ],
            include_priority_candidates=False,
            load_default_sources=False,
        )

        self.assertEqual(result["recommendation"], "paper_forward_candidate_review")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["automatic_promotion"])
        self.assertFalse(result["promoted_profile_mutated"])
        recommended = result["recommended_candidate"]
        self.assertEqual(recommended["symbol"], "XAUUSD")
        self.assertEqual(recommended["timeframe"], "M15")
        self.assertEqual(recommended["candidate_status"], "paper_forward_review_ready")
        self.assertEqual(recommended["recommended_next_action"], "paper_forward_candidate_review")
        self.assertFalse(recommended["degraded_by_registry"])
        self.assertFalse(recommended["sibling_risk"])
        self.assertEqual(recommended["rejection_reasons"], [])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_no_candidate_passes_gate_continues_research_without_activation(self) -> None:
        result = run_paper_forward_candidate_rotation(
            rows=[
                {
                    "symbol": "BTCUSD",
                    "timeframe": "M30",
                    "profile": "btcusd_m30_recent_liquidity_sweep",
                    "family": "recent_liquidity_sweep",
                    "recent_closed": 10,
                    "total_closed": 44,
                    "recent_pf": 1.04,
                    "total_pf": 1.16,
                    "expectancy": 0.05,
                    "monte_carlo_stressed_pf": 1.08,
                    "spread_x2_pf": 0.94,
                }
            ],
            include_priority_candidates=False,
            load_default_sources=False,
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertIsNone(result["recommended_candidate"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        row = result["ranking"][0]
        self.assertEqual(row["candidate_status"], "gate_failed")
        self.assertIn("recent_closed_below_15", row["rejection_reasons"])
        self.assertIn("total_closed_below_45", row["rejection_reasons"])
        self.assertIn("recent_pf_below_1_05", row["rejection_reasons"])
        self.assertIn("spread_x2_pf_below_0_95", row["rejection_reasons"])

    def test_eth_m30_sibling_of_degraded_profile_is_blocked_from_review(self) -> None:
        result = run_paper_forward_candidate_rotation(
            rows=[
                {
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
                },
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
                },
            ],
            include_priority_candidates=False,
            load_default_sources=False,
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertIsNone(result["recommended_candidate"])
        sibling = next(row for row in result["ranking"] if row["profile"] == "eth_m30_vol_breakout_regime_filtered_v1")
        self.assertTrue(sibling["sibling_risk"])
        self.assertEqual(sibling["sibling_of_degraded_profile"], "eth_m30_vol_breakout_chop_guard_v1")
        self.assertEqual(sibling["sibling_risk_reason"], "similar_to_degraded_forward_profile")
        self.assertEqual(sibling["candidate_status"], "blocked_by_sibling_risk")
        self.assertEqual(sibling["recommended_next_action"], "manual_review_or_new_family_required")
        self.assertIn("sibling_risk_similar_to_degraded_forward_profile", sibling["rejection_reasons"])
        excluded = result["excluded_by_degradation_registry"][0]
        self.assertEqual(excluded["candidate_status"], "excluded_by_degradation_registry")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_rotation_prefers_non_sibling_candidate_when_available(self) -> None:
        result = run_paper_forward_candidate_rotation(
            rows=[
                {
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
                },
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
                },
                {
                    "symbol": "XAUUSD",
                    "timeframe": "M15",
                    "profile": "xauusd_m15_recent_session_open_continuation",
                    "family": "recent_session_open_continuation",
                    "recent_closed": 22,
                    "total_closed": 70,
                    "recent_pf": 1.28,
                    "total_pf": 1.42,
                    "expectancy": 0.18,
                    "monte_carlo_stressed_pf": 1.11,
                    "spread_x2_pf": 1.02,
                },
            ],
            include_priority_candidates=False,
            load_default_sources=False,
        )

        self.assertEqual(result["recommendation"], "paper_forward_candidate_review")
        recommended = result["recommended_candidate"]
        self.assertEqual(recommended["symbol"], "XAUUSD")
        self.assertEqual(recommended["profile"], "xauusd_m15_recent_session_open_continuation")
        self.assertFalse(recommended["sibling_risk"])
        sibling = next(row for row in result["ranking"] if row["profile"] == "eth_m30_vol_breakout_regime_filtered_v1")
        self.assertEqual(sibling["candidate_status"], "blocked_by_sibling_risk")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])

    def test_rotation_without_useful_sources_requests_data_source_repair(self) -> None:
        result = run_paper_forward_candidate_rotation(load_default_sources=False)

        profiles = {row["profile"] for row in result["ranking"]}
        self.assertIn("eth_m30_vol_breakout_chop_guard_v1", profiles)
        self.assertIn("eth_m30_vol_breakout_mae_guard_v1", profiles)
        self.assertIn("xauusd_m15_recent_session_open_continuation", profiles)
        self.assertIn("us500_h1_recent_session_open_continuation", profiles)
        self.assertIn("btcusd_m30_recent_liquidity_sweep", profiles)
        self.assertEqual(result["recommendation"], "repair_data_sources")
        self.assertEqual(result["loaded_sources"], [])
        self.assertEqual(result["useful_rows"], 0)
        self.assertTrue(any(row["degraded_by_registry"] for row in result["excluded_by_degradation_registry"]))
        self.assertFalse(result["live_runtime_mutated"])
        self.assertFalse(result["shadow_trades_mutated"])
        self.assertFalse(result["martingale_enabled"])
        self.assertFalse(result["grid_enabled"])
        self.assertFalse(result["averaging_down_enabled"])
        self.assertFalse(result["increase_size_after_loss_enabled"])

    def test_script_reads_small_existing_results_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result_file = root / "multi_symbol_recent_first_cost_calibrated_results.csv"
            result_file.write_text(
                "\n".join(
                    [
                        "symbol,timeframe,profile,family,recent_closed,total_closed,recent_pf,total_pf,expectancy,monte_carlo_stressed_pf,spread_x2_pf",
                        "US500,H1,us500_h1_recent_session_open_continuation,recent_session_open_continuation,18,60,1.11,1.22,0.09,1.06,0.99",
                    ]
                ),
                encoding="utf-8",
            )

            code = rotation_main(["--results-dir", str(root)])

        self.assertEqual(code, 0)

    def test_alias_columns_are_loaded_from_processed_result_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result_file = root / "multi_symbol_recent_first_results.csv"
            result_file.write_text(
                "\n".join(
                    [
                        "Symbol,Timeframe,strategy_profile,recent_trades,trades,recent_profit_factor,profit_factor,total_expectancy,mc_pf,spread_stress_pf",
                        "XAUUSD,M15,xauusd_m15_recent_session_open_continuation,20,55,1.25,1.31,0.11,1.08,0.98",
                    ]
                ),
                encoding="utf-8",
            )

            result = run_paper_forward_candidate_rotation(
                search_root=root,
                include_priority_candidates=False,
            )

        self.assertEqual(result["recommendation"], "paper_forward_candidate_review")
        self.assertEqual(result["useful_rows"], 1)
        self.assertEqual(len(result["loaded_sources"]), 1)
        self.assertGreater(len(result["missing_sources"]), 0)
        recommended = result["recommended_candidate"]
        self.assertEqual(recommended["symbol"], "XAUUSD")
        self.assertEqual(recommended["timeframe"], "M15")
        self.assertEqual(recommended["recent_closed"], 20)
        self.assertEqual(recommended["total_closed"], 55)
        self.assertEqual(recommended["recent_pf"], 1.25)
        self.assertEqual(recommended["total_pf"], 1.31)
        self.assertEqual(recommended["monte_carlo_stressed_pf"], 1.08)
        self.assertEqual(recommended["spread_x2_pf"], 0.98)
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_ohlc_csv_is_skipped_even_when_explicitly_passed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ohlc_file = root / "ETHUSD_M30_20000.csv"
            ohlc_file.write_text(
                "\n".join(
                    [
                        "time,open,high,low,close,tick_volume",
                        "2026-01-01 00:00:00,1,2,0.5,1.5,100",
                    ]
                ),
                encoding="utf-8",
            )

            result = run_paper_forward_candidate_rotation(
                result_paths=[ohlc_file],
                include_priority_candidates=False,
                load_default_sources=False,
            )

        self.assertEqual(result["recommendation"], "repair_data_sources")
        self.assertEqual(result["loaded_sources"], [])
        self.assertEqual(result["useful_rows"], 0)
        self.assertEqual(result["skipped_sources"][0]["reason"], "not_processed_results_file")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from scripts.run_btc_h1_ema_reclaim_hardening import main as hardening_main
from services.mt5.mt5_btc_h1_ema_reclaim_hardening import (
    _hardening_configs,
    run_btc_h1_ema_reclaim_hardening,
)


class MT5BtcH1EmaReclaimHardeningTests(unittest.TestCase):
    def test_clean_variant_is_recommended_for_review_only(self) -> None:
        result = run_btc_h1_ema_reclaim_hardening(
            {
                "rows": [
                    {
                        "symbol": "BTCUSD",
                        "timeframe": "H1",
                        "target_name": "btc_h1_ema_reclaim_london_us_mae_guard",
                        "profile": "btc_h1_ema_reclaim_london_us_mae_guard",
                        "family": "recent_ema_reclaim",
                        "hardening_actions": ["london_us_only", "mae_guard"],
                        "recent_closed": 18,
                        "total_closed": 58,
                        "recent_pf": 1.24,
                        "total_pf": 1.31,
                        "expectancy": 0.12,
                        "monte_carlo_stressed_pf": 1.08,
                        "monte_carlo_stressed_expectancy": 0.03,
                        "spread_x2_pf": 1.02,
                        "remove_best_5_pf": 1.01,
                        "max_drawdown": 420.0,
                        "fragile_regime_dependency": False,
                        "single_trade_dependency": False,
                    }
                ]
            }
        )

        self.assertEqual(result["recommendation"], "paper_forward_candidate_review")
        self.assertEqual(result["research_conclusion"], "paper_forward_candidate_review")
        self.assertFalse(result["discard_candidate_for_now"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["automatic_promotion"])
        self.assertFalse(result["promoted_profile_mutated"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")
        recommended = result["recommended_candidate"]
        self.assertEqual(recommended["target_name"], "btc_h1_ema_reclaim_london_us_mae_guard")
        self.assertEqual(recommended["candidate_status"], "paper_forward_review_ready")
        self.assertEqual(recommended["rejection_reasons"], [])
        self.assertFalse(recommended["degraded_by_registry"])
        self.assertFalse(recommended["sibling_risk"])

    def test_many_gate_failures_continue_research_and_discard_for_now(self) -> None:
        result = run_btc_h1_ema_reclaim_hardening(
            {
                "rows": [
                    {
                        "target_name": "btc_h1_ema_reclaim_baseline",
                        "recent_closed": 12,
                        "total_closed": 40,
                        "recent_pf": 1.02,
                        "total_pf": 1.1,
                        "expectancy": 0.03,
                        "monte_carlo_stressed_pf": 0.9,
                        "monte_carlo_stressed_expectancy": -0.02,
                        "spread_x2_pf": 0.93,
                        "remove_best_5_pf": 0.8,
                        "fragile_regime_dependency": False,
                        "single_trade_dependency": False,
                    }
                ]
            }
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertEqual(result["research_conclusion"], "discard_btc_h1_ema_reclaim_for_now")
        self.assertTrue(result["discard_candidate_for_now"])
        self.assertIsNone(result["recommended_candidate"])
        best = result["best_variant"]
        self.assertEqual(best["candidate_status"], "gate_failed")
        self.assertIn("recent_closed_below_15", best["rejection_reasons"])
        self.assertIn("total_closed_below_45", best["rejection_reasons"])
        self.assertIn("recent_pf_below_1_05", best["rejection_reasons"])
        self.assertIn("monte_carlo_stressed_pf_below_1_05", best["rejection_reasons"])
        self.assertIn("monte_carlo_stressed_expectancy_not_positive", best["rejection_reasons"])
        self.assertIn("spread_x2_pf_below_0_95", best["rejection_reasons"])
        self.assertIn("remove_best_5_pf_below_1", best["rejection_reasons"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_dependency_gates_block_review(self) -> None:
        result = run_btc_h1_ema_reclaim_hardening(
            {
                "rows": [
                    {
                        "target_name": "btc_h1_ema_reclaim_momentum_guard",
                        "recent_closed": 22,
                        "total_closed": 75,
                        "recent_pf": 1.3,
                        "total_pf": 1.38,
                        "expectancy": 0.1,
                        "monte_carlo_stressed_pf": 1.09,
                        "monte_carlo_stressed_expectancy": 0.02,
                        "spread_x2_pf": 1.01,
                        "remove_best_5_pf": 1.04,
                        "fragile_regime_dependency": True,
                        "single_trade_dependency": True,
                    }
                ]
            }
        )

        row = result["best_variant"]
        self.assertEqual(result["recommendation"], "continue_research")
        self.assertEqual(row["candidate_status"], "gate_failed")
        self.assertIn("fragile_regime_dependency", row["rejection_reasons"])
        self.assertIn("single_trade_dependency", row["rejection_reasons"])
        self.assertFalse(row["applies_to_real_trading"])
        self.assertFalse(row["broker_touched"])
        self.assertFalse(row["order_executed"])
        self.assertEqual(row["order_policy"], "journal_only_no_broker")

    def test_variants_cover_required_defensive_actions_with_two_filter_limit(self) -> None:
        configs = _hardening_configs()
        target_names = {item.target_name for item in configs}
        actions = {action for item in configs for action in item.hardening_actions}

        self.assertIn("btc_h1_ema_reclaim_baseline", target_names)
        for action in {
            "trend_guard",
            "volatility_guard",
            "momentum_guard",
            "spread_guard",
            "mae_guard",
            "fast_loss_cut",
            "trailing_defensive",
            "london_us_only",
            "ny_core_only",
            "asia_only",
        }:
            self.assertIn(action, actions)
        self.assertTrue(all(len(item.hardening_actions) <= 2 for item in configs))

    def test_missing_csv_reports_error_without_broker_touch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "missing_btc.csv"
            result = run_btc_h1_ema_reclaim_hardening({"csv_path": str(missing)})

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertEqual(result["variants_evaluated"], 0)
        self.assertEqual(result["errors"][0]["error"], "csv_not_found")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_script_runs_without_activation_for_missing_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "missing_btc.csv"
            with contextlib.redirect_stdout(io.StringIO()):
                code = hardening_main(["--csv-path", str(missing)])

        self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main()

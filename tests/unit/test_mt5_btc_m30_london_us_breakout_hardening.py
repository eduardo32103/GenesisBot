from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from scripts.run_btc_m30_london_us_breakout_hardening import main as hardening_main
from services.mt5.mt5_btc_m30_london_us_breakout_hardening import (
    _hardening_configs,
    run_btc_m30_london_us_breakout_hardening,
)


class MT5BtcM30LondonUsBreakoutHardeningTests(unittest.TestCase):
    def test_clean_variant_is_recommended_for_review_only(self) -> None:
        result = run_btc_m30_london_us_breakout_hardening(
            {
                "rows": [
                    {
                        "symbol": "BTCUSD",
                        "timeframe": "M30",
                        "target_name": "btc_m30_london_us_breakout_strict_momentum",
                        "profile": "btc_m30_london_us_breakout_strict_momentum",
                        "family": "recent_london_us_breakout",
                        "hardening_actions": ["stricter_london_us_session", "momentum_guard"],
                        "recent_closed": 16,
                        "total_closed": 64,
                        "recent_pf": 1.42,
                        "total_pf": 1.5,
                        "expectancy": 0.2,
                        "monte_carlo_stressed_pf": 1.08,
                        "monte_carlo_stressed_expectancy": 4.2,
                        "spread_x2_pf": 1.2,
                        "remove_best_5_pf": 1.1,
                        "max_drawdown": 900.0,
                        "fragile_regime_dependency": False,
                        "single_trade_dependency": False,
                    }
                ]
            }
        )

        self.assertEqual(result["recommendation"], "paper_forward_candidate_review")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["automatic_promotion"])
        self.assertFalse(result["promoted_profile_mutated"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")
        recommended = result["recommended_candidate"]
        self.assertEqual(recommended["target_name"], "btc_m30_london_us_breakout_strict_momentum")
        self.assertEqual(recommended["candidate_status"], "paper_forward_review_ready")
        self.assertEqual(recommended["rejection_reasons"], [])
        self.assertFalse(recommended["degraded_by_registry"])
        self.assertFalse(recommended["sibling_risk"])

    def test_original_near_miss_fails_recent_sample_and_monte_carlo_pf(self) -> None:
        result = run_btc_m30_london_us_breakout_hardening(
            {
                "rows": [
                    {
                        "target_name": "btc_m30_london_us_breakout_baseline",
                        "recent_closed": 12,
                        "total_closed": 64,
                        "recent_pf": 1.76,
                        "total_pf": 1.6536,
                        "expectancy": 0.2197,
                        "monte_carlo_stressed_pf": 1.0277,
                        "monte_carlo_stressed_expectancy": 7.491498,
                        "spread_x2_pf": 1.4575,
                        "remove_best_5_pf": 1.33,
                        "fragile_regime_dependency": False,
                        "single_trade_dependency": False,
                    }
                ]
            }
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertIsNone(result["recommended_candidate"])
        best = result["best_variant"]
        self.assertEqual(best["candidate_status"], "gate_failed")
        self.assertIn("recent_closed_below_15", best["rejection_reasons"])
        self.assertIn("monte_carlo_stressed_pf_below_1_05", best["rejection_reasons"])
        self.assertNotIn("remove_best_5_pf_below_1", best["rejection_reasons"])
        self.assertNotIn("spread_x2_pf_below_0_95", best["rejection_reasons"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_dependency_gates_block_review(self) -> None:
        result = run_btc_m30_london_us_breakout_hardening(
            {
                "rows": [
                    {
                        "target_name": "btc_m30_london_us_breakout_momentum_guard",
                        "recent_closed": 22,
                        "total_closed": 75,
                        "recent_pf": 1.3,
                        "total_pf": 1.38,
                        "expectancy": 0.1,
                        "monte_carlo_stressed_pf": 1.09,
                        "monte_carlo_stressed_expectancy": 3.0,
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

        self.assertIn("btc_m30_london_us_breakout_baseline", target_names)
        for action in {
            "stricter_london_us_session",
            "london_open_only",
            "ny_open_only",
            "london_us_overlap_only",
            "volatility_guard",
            "trend_guard",
            "momentum_guard",
            "spread_guard",
            "mae_guard",
            "fast_loss_cut",
            "trailing_defensive",
            "time_stop_guard",
            "remove_low_atr",
            "remove_chop_regime",
        }:
            self.assertIn(action, actions)
        self.assertTrue(all(len(item.hardening_actions) <= 2 for item in configs))

    def test_missing_csv_reports_error_without_broker_touch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "missing_btc_m30.csv"
            result = run_btc_m30_london_us_breakout_hardening({"csv_path": str(missing)})

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
            missing = Path(tmp) / "missing_btc_m30.csv"
            with contextlib.redirect_stdout(io.StringIO()):
                code = hardening_main(["--csv-path", str(missing)])

        self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main()

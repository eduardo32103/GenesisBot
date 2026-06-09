from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from scripts.run_xau_m15_session_hardening import main as hardening_main
from services.mt5.mt5_xau_m15_session_hardening import _hardening_configs, run_xau_m15_session_hardening


class MT5XauM15SessionHardeningTests(unittest.TestCase):
    def test_clean_variant_is_recommended_for_review_only(self) -> None:
        result = run_xau_m15_session_hardening(
            {
                "rows": [
                    {
                        "symbol": "XAUUSD",
                        "timeframe": "M15",
                        "target_name": "xau_m15_session_london_us_mae_guard",
                        "profile": "xau_m15_session_london_us_mae_guard",
                        "family": "recent_session_open_continuation",
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
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["automatic_promotion"])
        self.assertFalse(result["promoted_profile_mutated"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")
        recommended = result["recommended_candidate"]
        self.assertEqual(recommended["target_name"], "xau_m15_session_london_us_mae_guard")
        self.assertEqual(recommended["candidate_status"], "paper_forward_review_ready")
        self.assertEqual(recommended["rejection_reasons"], [])
        self.assertFalse(recommended["degraded_by_registry"])
        self.assertFalse(recommended["sibling_risk"])

    def test_near_miss_fails_monte_carlo_and_remove_best_without_activation(self) -> None:
        result = run_xau_m15_session_hardening(
            {
                "rows": [
                    {
                        "target_name": "xau_m15_session_baseline",
                        "recent_closed": 17,
                        "total_closed": 53,
                        "recent_pf": 1.704,
                        "total_pf": 1.4046,
                        "expectancy": 0.08,
                        "monte_carlo_stressed_pf": 0.92,
                        "monte_carlo_stressed_expectancy": -0.01,
                        "spread_x2_pf": 1.03,
                        "remove_best_5_pf": 0.84,
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
        self.assertIn("monte_carlo_stressed_pf_below_1_05", best["rejection_reasons"])
        self.assertIn("monte_carlo_stressed_expectancy_not_positive", best["rejection_reasons"])
        self.assertIn("remove_best_5_pf_below_1", best["rejection_reasons"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_dependency_gates_block_review(self) -> None:
        result = run_xau_m15_session_hardening(
            {
                "rows": [
                    {
                        "target_name": "xau_m15_session_momentum_guard",
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

        self.assertIn("xau_m15_session_baseline", target_names)
        for action in {
            "spread_guard",
            "volatility_guard",
            "trend_guard",
            "momentum_guard",
            "mae_guard",
            "fast_loss_cut",
            "trailing_defensive",
            "remove_off_session",
            "london_us_only",
            "ny_core_only",
            "asia_only",
        }:
            self.assertIn(action, actions)
        self.assertTrue(all(len(item.hardening_actions) <= 2 for item in configs))

    def test_missing_csv_reports_error_without_broker_touch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "missing_xau.csv"
            result = run_xau_m15_session_hardening({"csv_path": str(missing)})

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
            missing = Path(tmp) / "missing_xau.csv"
            with contextlib.redirect_stdout(io.StringIO()):
                code = hardening_main(["--csv-path", str(missing)])

        self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main()

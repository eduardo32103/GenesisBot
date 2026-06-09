from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from scripts.run_btc_m30_london_us_breakout_deep_validation import main as deep_validation_main
from services.mt5.mt5_btc_m30_london_us_breakout_deep_validation import (
    _deep_configs,
    run_btc_m30_london_us_breakout_deep_validation,
)


class MT5BtcM30LondonUsBreakoutDeepValidationTests(unittest.TestCase):
    def test_clean_deep_window_is_recommended_for_review_only(self) -> None:
        result = run_btc_m30_london_us_breakout_deep_validation(
            {
                "rows": [
                    {
                        "target_name": "btc_m30_london_us_breakout_stricter_london_us_session",
                        "profile": "btc_m30_london_us_breakout_stricter_london_us_session",
                        "total_closed": 59,
                        "total_pf": 1.8343,
                        "expectancy": 0.2597,
                        "monte_carlo_stressed_pf": 1.1343,
                        "monte_carlo_stressed_expectancy": 33.818342,
                        "spread_x2_pf": 1.7862,
                        "remove_best_5_pf": 1.4679,
                        "max_drawdown": 620.0,
                        "fragile_regime_dependency": False,
                        "single_trade_dependency": False,
                        "window_results": [
                            {"window": "recent_10_pct", "closed": 12, "profit_factor": 1.76, "expectancy": 0.2},
                            {"window": "last_90_days", "closed": 16, "profit_factor": 1.2, "expectancy": 0.08},
                        ],
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
        self.assertEqual(recommended["validation_window"], "last_90_days")
        self.assertEqual(recommended["recent_closed"], 16)
        self.assertEqual(recommended["candidate_status"], "paper_forward_review_ready")
        self.assertEqual(recommended["rejection_reasons"], [])

    def test_strict_session_near_miss_still_fails_only_recent_sample(self) -> None:
        result = run_btc_m30_london_us_breakout_deep_validation(
            {
                "rows": [
                    {
                        "target_name": "btc_m30_london_us_breakout_stricter_london_us_session",
                        "total_closed": 59,
                        "total_pf": 1.8343,
                        "expectancy": 0.2597,
                        "monte_carlo_stressed_pf": 1.1343,
                        "monte_carlo_stressed_expectancy": 33.818342,
                        "spread_x2_pf": 1.7862,
                        "remove_best_5_pf": 1.4679,
                        "fragile_regime_dependency": False,
                        "single_trade_dependency": False,
                        "window_results": [
                            {"window": "recent_25_pct", "closed": 12, "profit_factor": 1.76, "expectancy": 0.12},
                            {"window": "last_90_days", "closed": 11, "profit_factor": 1.3, "expectancy": 0.09},
                        ],
                    }
                ]
            }
        )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertIsNone(result["recommended_candidate"])
        best = result["best_variant"]
        self.assertEqual(best["candidate_status"], "gate_failed")
        self.assertEqual(best["recent_closed"], 12)
        self.assertEqual(best["rejection_reasons"], ["recent_closed_below_15"])
        self.assertFalse(best["broker_touched"])
        self.assertFalse(best["order_executed"])
        self.assertEqual(best["order_policy"], "journal_only_no_broker")

    def test_dependency_gates_block_even_when_sample_passes(self) -> None:
        result = run_btc_m30_london_us_breakout_deep_validation(
            {
                "rows": [
                    {
                        "target_name": "btc_m30_london_us_breakout_strict_time_stop",
                        "total_closed": 70,
                        "total_pf": 1.4,
                        "expectancy": 0.11,
                        "monte_carlo_stressed_pf": 1.08,
                        "monte_carlo_stressed_expectancy": 6.0,
                        "spread_x2_pf": 1.1,
                        "remove_best_5_pf": 1.02,
                        "fragile_regime_dependency": True,
                        "single_trade_dependency": True,
                        "window_results": [
                            {"window": "last_60_days", "closed": 18, "profit_factor": 1.18, "expectancy": 0.05},
                        ],
                    }
                ]
            }
        )

        row = result["best_variant"]
        self.assertEqual(result["recommendation"], "continue_research")
        self.assertIn("fragile_regime_dependency", row["rejection_reasons"])
        self.assertIn("single_trade_dependency", row["rejection_reasons"])
        self.assertFalse(row["applies_to_real_trading"])
        self.assertFalse(row["broker_touched"])
        self.assertFalse(row["order_executed"])
        self.assertEqual(row["order_policy"], "journal_only_no_broker")

    def test_missing_deep_csvs_prepare_read_only_export_without_broker_touch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing_40k = Path(tmp) / "BTCUSD_M30_40000.csv"
            missing_60k = Path(tmp) / "BTCUSD_M30_60000.csv"
            result = run_btc_m30_london_us_breakout_deep_validation(
                {"csv_paths": f"{missing_40k},{missing_60k}"}
            )

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertEqual(result["variants_evaluated"], 0)
        self.assertEqual(result["csv_used"], [])
        self.assertEqual(result["export_readiness"]["missing_depths"], ["40000", "60000"])
        self.assertTrue(result["export_readiness"]["prepared_read_only"])
        self.assertFalse(result["export_readiness"]["broker_touched"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_variants_cover_requested_deep_validation_actions(self) -> None:
        configs = _deep_configs()
        target_names = {item.target_name for item in configs}
        actions = {action for item in configs for action in item.hardening_actions}

        for target in {
            "btc_m30_london_us_breakout_stricter_london_us_session",
            "btc_m30_london_us_breakout_baseline",
            "btc_m30_london_us_breakout_no_offsession",
            "btc_m30_london_us_breakout_overlap_only",
            "btc_m30_london_us_breakout_strict_volatility",
            "btc_m30_london_us_breakout_strict_time_stop",
            "btc_m30_london_us_breakout_strict_mae",
            "btc_m30_london_us_breakout_strict_trailing",
        }:
            self.assertIn(target, target_names)
        for action in {
            "stricter_london_us_session",
            "no_offsession",
            "london_us_overlap_only",
            "volatility_guard",
            "time_stop_guard",
            "mae_guard",
            "trailing_defensive",
        }:
            self.assertIn(action, actions)
        self.assertTrue(all(len(item.hardening_actions) <= 2 for item in configs))

    def test_script_runs_without_activation_for_missing_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "BTCUSD_M30_40000.csv"
            with contextlib.redirect_stdout(io.StringIO()):
                code = deep_validation_main(["--csv-paths", str(missing)])

        self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main()

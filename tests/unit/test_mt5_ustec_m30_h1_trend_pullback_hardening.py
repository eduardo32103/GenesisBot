from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from scripts.run_ustec_m30_h1_trend_pullback_hardening import main as hardening_main
from services.mt5.mt5_ustec_m30_h1_trend_pullback_hardening import (
    _VARIANTS,
    run_ustec_m30_h1_trend_pullback_hardening,
)


class MT5UstecM30H1TrendPullbackHardeningTests(unittest.TestCase):
    def test_clean_variant_is_recommended_for_review_only(self) -> None:
        result = run_ustec_m30_h1_trend_pullback_hardening(
            {
                "rows": [
                    {
                        "profile": "ustec_m30_h1_trend_pullback_rsi_filter",
                        "hardening_actions": ["rsi_filter"],
                        "recent_closed": 24,
                        "total_closed": 80,
                        "recent_pf": 1.22,
                        "total_pf": 1.34,
                        "expectancy": 0.0008,
                        "monte_carlo_stressed_pf": 1.08,
                        "monte_carlo_stressed_expectancy": 0.0002,
                        "spread_x2_pf": 1.14,
                        "remove_best_5_pf": 1.04,
                        "max_drawdown": 0.01,
                        "fragile_regime_dependency": False,
                        "single_trade_dependency": False,
                        "data_quality": "ok",
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
        self.assertEqual(recommended["profile"], "ustec_m30_h1_trend_pullback_rsi_filter")
        self.assertEqual(recommended["candidate_status"], "paper_forward_review_ready")
        self.assertEqual(recommended["rejection_reasons"], [])
        self.assertFalse(recommended["degraded_by_registry"])
        self.assertFalse(recommended["rejected_by_research_registry"])
        self.assertFalse(recommended["sibling_risk"])
        self.assertFalse(recommended["candidate_activated"])
        self.assertFalse(recommended["paper_forward_onboarding_started"])
        self.assertFalse(recommended["applies_to_real_trading"])
        self.assertFalse(recommended["broker_touched"])
        self.assertFalse(recommended["order_executed"])
        self.assertEqual(recommended["order_policy"], "journal_only_no_broker")

    def test_missing_required_timeframe_requests_data_repair_without_activation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing_m30 = Path(tmp) / "USTEC_M30_missing.csv"
            missing_h1 = Path(tmp) / "USTEC_H1_missing.csv"
            result = run_ustec_m30_h1_trend_pullback_hardening(
                {"m30_csv_paths": str(missing_m30), "h1_csv_paths": str(missing_h1)}
            )

        self.assertEqual(result["recommendation"], "repair_data_sources")
        self.assertEqual(result["variants_evaluated"], 0)
        self.assertEqual(result["csv_used"], [])
        self.assertTrue(result["missing_csvs"])
        row = result["best_variant"]
        self.assertEqual(row["data_quality"], "missing_required_timeframe")
        self.assertIn("missing_required_timeframe", row["rejection_reasons"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_dependency_and_robustness_gates_block_review(self) -> None:
        result = run_ustec_m30_h1_trend_pullback_hardening(
            {
                "rows": [
                    {
                        "profile": "ustec_m30_h1_trend_pullback_rsi_atr_filter",
                        "recent_closed": 20,
                        "total_closed": 70,
                        "recent_pf": 1.2,
                        "total_pf": 1.3,
                        "expectancy": 0.0003,
                        "monte_carlo_stressed_pf": 0.94,
                        "monte_carlo_stressed_expectancy": -0.00001,
                        "spread_x2_pf": 0.91,
                        "remove_best_5_pf": 0.88,
                        "fragile_regime_dependency": True,
                        "single_trade_dependency": True,
                        "data_quality": "ok",
                    }
                ]
            }
        )

        row = result["best_variant"]
        self.assertEqual(result["recommendation"], "continue_research")
        self.assertEqual(row["candidate_status"], "gate_failed")
        self.assertIn("monte_carlo_stressed_pf_below_1_05", row["rejection_reasons"])
        self.assertIn("monte_carlo_stressed_expectancy_not_positive", row["rejection_reasons"])
        self.assertIn("spread_x2_pf_below_0_95", row["rejection_reasons"])
        self.assertIn("remove_best_5_pf_below_1", row["rejection_reasons"])
        self.assertIn("fragile_regime_dependency", row["rejection_reasons"])
        self.assertIn("single_trade_dependency", row["rejection_reasons"])
        self.assertFalse(row["applies_to_real_trading"])
        self.assertFalse(row["broker_touched"])
        self.assertFalse(row["order_executed"])
        self.assertEqual(row["order_policy"], "journal_only_no_broker")

    def test_variants_cover_required_actions_with_two_filter_limit(self) -> None:
        modes = {str(item["mode"]) for item in _VARIANTS}
        actions = {action for item in _VARIANTS for action in item.get("actions", ())}

        for mode in {
            "baseline",
            "rsi_filter",
            "rsi_atr_filter",
            "trend_strength_guard",
            "pullback_depth_guard",
            "volatility_guard",
            "spread_guard",
            "mae_guard",
            "fast_loss_cut",
            "trailing_defensive",
            "time_stop_guard",
            "long_only",
            "short_only",
        }:
            self.assertIn(mode, modes)
        for action in {
            "rsi_filter",
            "atr_filter",
            "trend_strength_guard",
            "pullback_depth_guard",
            "volatility_guard",
            "spread_guard",
            "mae_guard",
            "fast_loss_cut",
            "trailing_defensive",
            "time_stop_guard",
            "long_only",
            "short_only",
        }:
            self.assertIn(action, actions)
        self.assertTrue(all(len(item.get("actions", ())) <= 2 for item in _VARIANTS))

    def test_script_runs_without_activation_for_missing_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = hardening_main(
                    [
                        "--m30-csv-paths",
                        str(Path(tmp) / "USTEC_M30_missing.csv"),
                        "--h1-csv-paths",
                        str(Path(tmp) / "USTEC_H1_missing.csv"),
                    ]
                )

        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("csv_used=none", text)
        self.assertIn("recommendation=repair_data_sources", text)
        self.assertIn("candidate_activated=False", text)
        self.assertIn("paper_forward_onboarding_started=False", text)
        self.assertIn("broker_touched=False", text)
        self.assertIn("order_executed=False", text)
        self.assertIn("order_policy=journal_only_no_broker", text)


if __name__ == "__main__":
    unittest.main()

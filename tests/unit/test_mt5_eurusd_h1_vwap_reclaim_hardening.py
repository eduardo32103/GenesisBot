from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.run_eurusd_h1_vwap_reclaim_hardening import main as hardening_main
from services.mt5.mt5_eurusd_h1_vwap_reclaim_hardening import (
    _VARIANTS,
    run_eurusd_h1_vwap_reclaim_hardening,
)


class MT5EurUsdH1VwapReclaimHardeningTests(unittest.TestCase):
    def test_clean_variant_is_recommended_for_review_only(self) -> None:
        result = run_eurusd_h1_vwap_reclaim_hardening(
            {
                "rows": [
                    {
                        "profile": "eurusd_h1_vwap_reclaim_distance_filter",
                        "hardening_actions": ["distance_filter"],
                        "recent_closed": 24,
                        "total_closed": 80,
                        "recent_pf": 1.25,
                        "total_pf": 1.36,
                        "expectancy": 0.00012,
                        "monte_carlo_stressed_pf": 1.09,
                        "monte_carlo_stressed_expectancy": 0.00004,
                        "spread_x2_pf": 1.2,
                        "remove_best_5_pf": 1.05,
                        "max_drawdown": 0.002,
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
        self.assertEqual(recommended["profile"], "eurusd_h1_vwap_reclaim_distance_filter")
        self.assertEqual(recommended["candidate_status"], "paper_forward_review_ready")
        self.assertEqual(recommended["rejection_reasons"], [])
        self.assertFalse(recommended["degraded_by_registry"])
        self.assertFalse(recommended["rejected_by_research_registry"])
        self.assertFalse(recommended["sibling_risk"])

    def test_missing_volume_blocks_review_from_real_csv_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "EURUSD_H1_20000.csv"
            _write_csv(csv_path, _bars(include_volume=False))
            result = run_eurusd_h1_vwap_reclaim_hardening({"csv_paths": str(csv_path), "targets": "distance_filter"})

        self.assertEqual(result["recommendation"], "continue_research")
        self.assertEqual(result["variants_evaluated"], 1)
        row = result["best_variant"]
        self.assertEqual(row["data_quality"], "missing_volume")
        self.assertIn("missing_volume", row["rejection_reasons"])
        self.assertEqual(row["candidate_status"], "gate_failed")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_dependency_and_robustness_gates_block_review(self) -> None:
        result = run_eurusd_h1_vwap_reclaim_hardening(
            {
                "rows": [
                    {
                        "profile": "eurusd_h1_vwap_reclaim_momentum_distance_filter",
                        "recent_closed": 20,
                        "total_closed": 70,
                        "recent_pf": 1.2,
                        "total_pf": 1.3,
                        "expectancy": 0.0001,
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
            "distance_filter",
            "momentum_distance_filter",
            "trend_guard",
            "volatility_guard",
            "spread_guard",
            "mae_guard",
            "fast_loss_cut",
            "trailing_defensive",
            "time_stop_guard",
            "session_filter_london",
            "session_filter_ny",
            "session_filter_london_ny",
        }:
            self.assertIn(mode, modes)
        for action in {
            "distance_filter",
            "momentum_guard",
            "trend_guard",
            "volatility_guard",
            "spread_guard",
            "mae_guard",
            "fast_loss_cut",
            "trailing_defensive",
            "time_stop_guard",
            "session_filter_london",
            "session_filter_ny",
            "session_filter_london_ny",
        }:
            self.assertIn(action, actions)
        self.assertTrue(all(len(item.get("actions", ())) <= 2 for item in _VARIANTS))

    def test_script_runs_without_activation_for_missing_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "EURUSD_H1_missing.csv"
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = hardening_main(["--csv-paths", str(missing)])

        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("csv_used=none", text)
        self.assertIn("candidate_activated=False", text)
        self.assertIn("paper_forward_onboarding_started=False", text)
        self.assertIn("broker_touched=False", text)
        self.assertIn("order_executed=False", text)
        self.assertIn("order_policy=journal_only_no_broker", text)


def _bars(*, include_volume: bool) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for index in range(150):
        close = 1.1 + (index % 8) * 0.0001
        row: dict[str, object] = {
            "time": (start + timedelta(hours=index)).isoformat(),
            "open": close,
            "high": close + 0.0002,
            "low": close - 0.0002,
            "close": close,
        }
        if include_volume:
            row["volume"] = 100 + index
        rows.append(row)
    return rows


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    headers = ["time", "open", "high", "low", "close"]
    if any("volume" in row for row in rows):
        headers.append("volume")
    with path.open("w", encoding="utf-8", newline="") as handle:
        handle.write(",".join(headers) + "\n")
        for row in rows:
            handle.write(",".join(str(row.get(header, "")) for header in headers) + "\n")


if __name__ == "__main__":
    unittest.main()

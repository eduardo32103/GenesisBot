from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from scripts.run_range_breakout_refinement_from_csv import main as refinement_main
from services.mt5.mt5_range_breakout_refinement import (
    _loss_cluster_stats,
    _mae_mfe_stats,
    _reason_loss_clusters,
    run_range_breakout_refinement,
    write_range_breakout_refinement_outputs,
)


def _bars_csv(count: int = 950) -> str:
    rows = ["time,open,high,low,close,volume"]
    price = 100.0
    for index in range(count):
        phase = index % 48
        compressed = phase < 18
        direction = 1 if (index // 48) % 2 == 0 else -1
        impulse = 0.08 if compressed else (0.68 if phase in {18, 19, 20} else 0.18)
        open_price = price
        close = price + direction * impulse
        high = max(open_price, close) + (0.18 if compressed else 0.54)
        low = min(open_price, close) - (0.18 if compressed else 0.54)
        price = close
        rows.append(f"2025-03-{(index % 28) + 1:02d}T{index % 24:02d}:30:00+00:00,{open_price:.4f},{high:.4f},{low:.4f},{close:.4f},1")
    return "\n".join(rows)


class MT5RangeBreakoutRefinementTests(unittest.TestCase):
    def test_refinement_is_paper_only_and_does_not_mutate_live_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M30_20000.csv").write_text(_bars_csv(), encoding="utf-8")

            result = run_range_breakout_refinement(
                {
                    "csv_dir": str(root),
                    "targets": "m30_range_breakout_both_all,range_breakout_anti_chop_m30_no_offsession_v2",
                    "max_bars": 800,
                    "per_evaluation_timeout_seconds": 1.0,
                }
            )

            self.assertTrue(result["ok"])
            self.assertFalse(result["broker_touched"])
            self.assertFalse(result["order_executed"])
            self.assertEqual(result["order_policy"], "journal_only_no_broker")
            self.assertFalse(result["automatic_promotion"])
            self.assertFalse(result["promoted_profile_mutated"])
            self.assertFalse(result["forward_state_mutated"])
            self.assertFalse(result["live_runtime_mutated"])
            self.assertFalse(result["shadow_trades_mutated"])
            self.assertFalse(result["martingale_enabled"])
            self.assertFalse(result["grid_enabled"])
            self.assertFalse(result["averaging_down_enabled"])
            self.assertFalse(result["increase_size_after_loss_enabled"])
            self.assertEqual(result["max_open_trades"], 1)
            self.assertEqual(len(result["results"]), 2)

    def test_refinement_reports_exits_clusters_and_segments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M30_20000.csv").write_text(_bars_csv(), encoding="utf-8")

            result = run_range_breakout_refinement(
                {
                    "csv_dir": str(root),
                    "targets": "m30_range_breakout_both_all",
                    "max_bars": 850,
                    "per_evaluation_timeout_seconds": 1.0,
                }
            )
            row = result["results"][0]

            for key in [
                "exit_reason_counts",
                "loss_cause_counts",
                "stop_loss_cluster_count",
                "momentum_loss_exit_cluster_count",
                "time_stop_cluster_count",
                "loss_cluster_count",
                "avg_MAE_R",
                "avg_MFE_R",
                "session_stats",
                "side_stats",
                "hour_stats",
                "atr_regime_stats",
                "volatility_stats",
                "weakest_segment",
                "strongest_segment",
                "reject_reasons",
            ]:
                self.assertIn(key, row)
            self.assertFalse(row["broker_touched"])
            self.assertFalse(row["order_executed"])

    def test_cluster_helpers_count_consecutive_loss_runs(self) -> None:
        trades = [
            {"status": "loss", "exit_reason": "stop_loss", "lifecycle_status": "closed", "initial_risk": 10, "max_adverse_excursion": -7, "max_favorable_excursion": 3},
            {"status": "loss", "exit_reason": "stop_loss", "lifecycle_status": "closed", "initial_risk": 10, "max_adverse_excursion": -8, "max_favorable_excursion": 2},
            {"status": "win", "exit_reason": "take_profit", "lifecycle_status": "closed", "initial_risk": 10, "max_adverse_excursion": -2, "max_favorable_excursion": 13},
            {"status": "loss", "exit_reason": "momentum_loss_exit", "lifecycle_status": "closed", "initial_risk": 10, "max_adverse_excursion": -5, "max_favorable_excursion": 4},
        ]

        self.assertEqual(_loss_cluster_stats(trades)["cluster_count"], 1)
        self.assertEqual(_reason_loss_clusters(trades, "stop_loss")["cluster_count"], 1)
        self.assertEqual(_reason_loss_clusters(trades, "momentum_loss_exit")["cluster_count"], 0)
        self.assertGreater(_mae_mfe_stats(trades)["avg_MFE_R"], 0)

    def test_outputs_and_script_smoke_finish_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M30_20000.csv").write_text(_bars_csv(), encoding="utf-8")
            result = run_range_breakout_refinement(
                {
                    "csv_dir": str(root),
                    "targets": "m30_range_breakout_both_all",
                    "max_bars": 750,
                    "per_evaluation_timeout_seconds": 1.0,
                }
            )

            csv_path, json_path, summary_path = write_range_breakout_refinement_outputs(result, root)
            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertIn("Range Breakout", summary_path.read_text(encoding="utf-8"))

            started = time.monotonic()
            code = refinement_main(["--smoke", "--csv-dir", str(root), "--output-dir", str(root)])

            self.assertEqual(code, 0)
            self.assertLess(time.monotonic() - started, 30)
            self.assertTrue((root / "range_breakout_refinement_results.csv").exists())


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from scripts.run_range_breakout_deep_sample_from_csv import main as deep_sample_main
from services.mt5.mt5_range_breakout_deep_sample import (
    _readiness,
    run_range_breakout_deep_sample,
    write_range_breakout_deep_sample_outputs,
)


def _bars_csv(count: int = 1300) -> str:
    rows = ["time,open,high,low,close,volume"]
    price = 100.0
    for index in range(count):
        phase = index % 54
        compressed = phase < 20
        direction = 1 if (index // 54) % 2 == 0 else -1
        impulse = 0.06 if compressed else (0.74 if phase in {20, 21, 22} else 0.16)
        open_price = price
        close = price + direction * impulse
        high = max(open_price, close) + (0.16 if compressed else 0.56)
        low = min(open_price, close) - (0.16 if compressed else 0.56)
        price = close
        rows.append(f"2025-04-{(index % 28) + 1:02d}T{index % 24:02d}:30:00+00:00,{open_price:.4f},{high:.4f},{low:.4f},{close:.4f},1")
    return "\n".join(rows)


class MT5RangeBreakoutDeepSampleTests(unittest.TestCase):
    def test_deep_sample_uses_available_csv_and_reports_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M30_20000.csv").write_text(_bars_csv(), encoding="utf-8")

            result = run_range_breakout_deep_sample(
                {
                    "csv_dir": str(root),
                    "include_baseline_20000": True,
                    "targets": "range_breakout_anti_chop_m30_no_offsession_v2",
                    "max_bars": 900,
                    "per_evaluation_timeout_seconds": 1.0,
                }
            )

            self.assertTrue(result["ok"])
            self.assertEqual(len(result["csvs_evaluated"]), 1)
            self.assertEqual(len(result["csvs_missing"]), 2)
            row = result["results"][0]
            self.assertEqual(row["sample_label"], "20000")
            self.assertIn("csv_path_used", row)
            self.assertIn("max_bars_requested", row)
            self.assertIn("bars_loaded", row)
            self.assertIn("bars_evaluated", row)
            self.assertIn("first_bar_time", row)
            self.assertIn("last_bar_time", row)
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

    def test_readiness_distinguishes_capital_ready_and_paper_forward(self) -> None:
        self.assertEqual(_readiness({"closed": 39, "candidate": True})["readiness"], "sample_too_small")
        self.assertEqual(_readiness({"closed": 40, "candidate": True})["readiness"], "capital_preservation_ready")
        self.assertEqual(_readiness({"closed": 75, "candidate": True})["readiness"], "paper_forward_candidate_recommended")
        self.assertFalse(_readiness({"closed": 75, "candidate": False})["paper_forward_candidate_recommended"])

    def test_outputs_and_script_smoke_finish_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M30_20000.csv").write_text(_bars_csv(), encoding="utf-8")
            result = run_range_breakout_deep_sample(
                {
                    "csv_dir": str(root),
                    "include_baseline_20000": True,
                    "targets": "range_breakout_anti_chop_m30_no_offsession_v2",
                    "max_bars": 850,
                    "per_evaluation_timeout_seconds": 1.0,
                }
            )

            csv_path, json_path, summary_path = write_range_breakout_deep_sample_outputs(result, root)
            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertIn("Deep-Sample", summary_path.read_text(encoding="utf-8"))

            started = time.monotonic()
            code = deep_sample_main(["--smoke", "--csv-dir", str(root), "--output-dir", str(root)])

            self.assertEqual(code, 0)
            self.assertLess(time.monotonic() - started, 30)
            self.assertTrue((root / "range_breakout_deep_sample_results.csv").exists())


if __name__ == "__main__":
    unittest.main()

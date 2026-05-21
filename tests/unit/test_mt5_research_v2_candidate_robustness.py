from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from scripts.run_research_v2_candidate_robustness_from_csv import main as robustness_main
from services.mt5.mt5_research_v2_candidate_robustness import (
    run_research_v2_candidate_robustness,
    write_research_v2_candidate_robustness_outputs,
)


def _bars_csv(count: int = 900) -> str:
    rows = ["time,open,high,low,close,volume"]
    price = 100.0
    for index in range(count):
        regime = (index // 150) % 3
        drift = [0.12, -0.1, 0.02][regime]
        impulse = 0.62 if index % 27 in {0, 1, 2} else 0.21
        direction = 1 if index % 19 < 10 else -1
        open_price = price
        close = price + drift + direction * impulse
        high = max(open_price, close) + 0.5
        low = min(open_price, close) - 0.5
        price = close
        rows.append(f"2025-02-{(index % 28) + 1:02d}T{index % 24:02d}:00:00+00:00,{open_price:.4f},{high:.4f},{low:.4f},{close:.4f},1")
    return "\n".join(rows)


class MT5ResearchV2CandidateRobustnessTests(unittest.TestCase):
    def test_robustness_is_paper_only_and_does_not_mutate_live_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M30_20000.csv").write_text(_bars_csv(), encoding="utf-8")

            result = run_research_v2_candidate_robustness(
                {
                    "csv_dir": str(root),
                    "targets": "m30_range_breakout_both_all",
                    "max_bars": 700,
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
            self.assertEqual(len(result["results"]), 1)

    def test_robustness_reports_segments_and_candidate_gates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M30_20000.csv").write_text(_bars_csv(), encoding="utf-8")

            result = run_research_v2_candidate_robustness(
                {
                    "csv_dir": str(root),
                    "targets": "m30_range_breakout_both_all",
                    "max_bars": 750,
                    "per_evaluation_timeout_seconds": 1.0,
                }
            )
            row = result["results"][0]

            for key in [
                "tercile_stats",
                "half_stats",
                "quarter_stats",
                "session_stats",
                "side_stats",
                "regime_stats",
                "volatility_stats",
                "atr_regime_stats",
                "rsi_regime_stats",
                "exit_reason_stats",
                "weakest_segment",
                "strongest_segment",
                "filter_hint",
                "reject_reasons",
            ]:
                self.assertIn(key, row)
            self.assertIn("tercile_1", row["tercile_stats"])
            self.assertFalse(row["broker_touched"])
            self.assertFalse(row["order_executed"])

    def test_outputs_and_script_smoke_finish_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M30_20000.csv").write_text(_bars_csv(), encoding="utf-8")
            (root / "BTCUSD_M15_20000.csv").write_text(_bars_csv(), encoding="utf-8")
            result = run_research_v2_candidate_robustness(
                {
                    "csv_dir": str(root),
                    "targets": "m30_range_breakout_both_all,m15_momentum_continuation_sell_london",
                    "max_bars": 700,
                    "per_evaluation_timeout_seconds": 1.0,
                }
            )

            csv_path, json_path, summary_path = write_research_v2_candidate_robustness_outputs(result, root)
            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertIn("Candidate Robustness", summary_path.read_text(encoding="utf-8"))

            started = time.monotonic()
            code = robustness_main(["--smoke", "--csv-dir", str(root), "--output-dir", str(root)])

            self.assertEqual(code, 0)
            self.assertLess(time.monotonic() - started, 30)
            self.assertTrue((root / "research_v2_candidate_robustness_results.csv").exists())


if __name__ == "__main__":
    unittest.main()

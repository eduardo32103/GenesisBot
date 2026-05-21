from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from scripts.run_strategy_research_v2_from_csv import main as research_v2_main
from services.mt5.mt5_strategy_research_v2 import (
    run_strategy_research_v2,
    write_strategy_research_v2_outputs,
)


def _bars_csv(count: int = 900) -> str:
    rows = ["time,open,high,low,close,volume"]
    price = 100.0
    for index in range(count):
        regime = (index // 120) % 4
        drift = [0.18, -0.16, 0.04, -0.03][regime]
        pulse = 0.55 if index % 31 in {0, 1, 2} else 0.18
        direction = 1 if index % 17 < 9 else -1
        open_price = price
        close = price + drift + direction * pulse
        high = max(open_price, close) + 0.45
        low = min(open_price, close) - 0.45
        price = close
        rows.append(f"2025-01-{(index % 28) + 1:02d}T{index % 24:02d}:00:00+00:00,{open_price:.4f},{high:.4f},{low:.4f},{close:.4f},1")
    return "\n".join(rows)


class MT5StrategyResearchV2Tests(unittest.TestCase):
    def test_research_v2_is_paper_only_and_does_not_mutate_live_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M15_20000.csv").write_text(_bars_csv(), encoding="utf-8")

            result = run_strategy_research_v2(
                {
                    "csv_dir": str(root),
                    "timeframes": "M15",
                    "families": "mean_reversion_safe,trend_pullback",
                    "max_bars": 600,
                    "max_evaluations": 8,
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
            self.assertGreater(len(result["results"]), 0)

    def test_research_rows_include_required_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M30_20000.csv").write_text(_bars_csv(), encoding="utf-8")

            result = run_strategy_research_v2(
                {
                    "csv_dir": str(root),
                    "timeframes": "M30",
                    "families": "mean_reversion_safe",
                    "max_bars": 650,
                    "max_evaluations": 4,
                    "per_evaluation_timeout_seconds": 1.0,
                }
            )
            row = result["results"][0]

            for key in [
                "generated_signal_count",
                "actionable_signal_count",
                "closed",
                "profit_factor",
                "expectancy",
                "max_drawdown",
                "tercile_stats",
                "monte_carlo_stressed_pf",
                "monte_carlo_p95_drawdown",
                "fragile_regime_dependency",
                "single_trade_dependency",
                "reject_reasons",
                "candidate_profile_name",
            ]:
                self.assertIn(key, row)
            self.assertFalse(row["broker_touched"])
            self.assertFalse(row["order_executed"])
            self.assertEqual(row["order_policy"], "journal_only_no_broker")

    def test_outputs_and_script_smoke_finish_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M15_20000.csv").write_text(_bars_csv(), encoding="utf-8")
            result = run_strategy_research_v2(
                {
                    "csv_dir": str(root),
                    "timeframes": "M15",
                    "families": "mean_reversion_safe",
                    "max_bars": 500,
                    "max_evaluations": 4,
                    "per_evaluation_timeout_seconds": 1.0,
                }
            )

            csv_path, json_path, summary_path = write_strategy_research_v2_outputs(result, root)
            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertIn("Strategy Research V2", summary_path.read_text(encoding="utf-8"))

            started = time.monotonic()
            code = research_v2_main(["--smoke", "--csv-dir", str(root), "--output-dir", str(root)])

            self.assertEqual(code, 0)
            self.assertLess(time.monotonic() - started, 30)
            self.assertTrue((root / "strategy_research_v2_results.csv").exists())


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from scripts.run_recent_first_research_from_csv import main as recent_first_main
from services.mt5.mt5_recent_first_research import (
    _build_variants,
    _gate,
    run_recent_first_research,
    write_recent_first_research_outputs,
)


def _bars_csv(count: int = 1300) -> str:
    rows = ["time,open,high,low,close,volume"]
    price = 100.0
    for index in range(count):
        cycle = index % 72
        direction = 1 if (index // 72) % 2 == 0 else -1
        drift = 0.02 if index < count * 0.75 else 0.08
        impulse = 0.05 if cycle < 24 else (0.72 if cycle in {24, 25, 26, 54, 55} else 0.14)
        open_price = price
        close = price + drift + direction * impulse
        high = max(open_price, close) + (0.2 if cycle < 24 else 0.62)
        low = min(open_price, close) - (0.2 if cycle < 24 else 0.62)
        price = close
        rows.append(f"2025-06-{(index % 28) + 1:02d}T{index % 24:02d}:00:00+00:00,{open_price:.4f},{high:.4f},{low:.4f},{close:.4f},1")
    return "\n".join(rows)


class MT5RecentFirstResearchTests(unittest.TestCase):
    def test_recent_first_is_paper_only_and_reports_required_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ["BTCUSD_M15_20000.csv", "BTCUSD_M30_60000.csv", "BTCUSD_M30_40000.csv"]:
                (root / name).write_text(_bars_csv(), encoding="utf-8")

            result = run_recent_first_research(
                {
                    "csv_dir": str(root),
                    "timeframes": "M15,M30",
                    "families": "recent_momentum_pullback,recent_ema_reclaim",
                    "max_bars": 900,
                    "max_evaluations": 8,
                    "per_evaluation_timeout_seconds": 1.0,
                }
            )

            self.assertTrue(result["ok"])
            self.assertLessEqual(result["evaluations"], 8)
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
            row = result["results"][0]
            for key in [
                "recent_closed",
                "recent_pf",
                "recent_expectancy",
                "recent_max_drawdown",
                "total_closed",
                "total_pf",
                "oldest_pf",
                "middle_pf",
                "previous_pf",
                "monte_carlo_stressed_pf",
                "rejection_reasons",
            ]:
                self.assertIn(key, row)

    def test_gate_rejects_bad_recent_window(self) -> None:
        total = {"closed": 80, "profit_factor": 1.6, "expectancy": 0.2, "max_drawdown": 1000}
        split = {
            "oldest": {"closed": 20, "profit_factor": 1.3, "expectancy": 0.1, "max_drawdown": 300},
            "middle": {"closed": 20, "profit_factor": 1.4, "expectancy": 0.1, "max_drawdown": 300},
            "previous": {"closed": 25, "profit_factor": 1.3, "expectancy": 0.1, "max_drawdown": 300},
            "recent": {"closed": 15, "profit_factor": 0.9, "expectancy": -0.1, "max_drawdown": 400},
        }
        monte_carlo = {"profit_factor_stressed": 1.2, "max_drawdown_p95": 900, "expectancy_stressed": 0.1}

        gate = _gate(total, split, monte_carlo, fragile=False, single_trade=False)

        self.assertFalse(gate["passed"])
        self.assertIn("recent_pf_below_1_05", gate["reasons"])
        self.assertIn("recent_expectancy_not_positive", gate["reasons"])

    def test_variant_builder_round_robins_families(self) -> None:
        variants = _build_variants(
            ["M15", "M30"],
            ["recent_momentum_pullback", "recent_range_reversion", "recent_volatility_breakout"],
            max_evaluations=12,
            dataset_counts={"M15": 1, "M30": 2},
        )

        families = {variant.family for variant in variants}

        self.assertIn("recent_momentum_pullback", families)
        self.assertIn("recent_range_reversion", families)
        self.assertIn("recent_volatility_breakout", families)

    def test_outputs_and_script_smoke_finish_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ["BTCUSD_M15_20000.csv", "BTCUSD_M30_60000.csv", "BTCUSD_M30_40000.csv"]:
                (root / name).write_text(_bars_csv(), encoding="utf-8")
            result = run_recent_first_research(
                {
                    "csv_dir": str(root),
                    "timeframes": "M15,M30",
                    "families": "recent_momentum_pullback,recent_ema_reclaim",
                    "max_bars": 850,
                    "max_evaluations": 8,
                    "per_evaluation_timeout_seconds": 1.0,
                }
            )

            csv_path, json_path, summary_path = write_recent_first_research_outputs(result, root)
            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertIn("Recent-First", summary_path.read_text(encoding="utf-8"))

            started = time.monotonic()
            code = recent_first_main(["--smoke", "--csv-dir", str(root), "--output-dir", str(root)])

            self.assertEqual(code, 0)
            self.assertLess(time.monotonic() - started, 30)
            self.assertTrue((root / "recent_first_research_results.csv").exists())


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from scripts.run_recent_first_hardening_from_csv import main as hardening_main
from services.mt5.mt5_recent_first_hardening import (
    _gate,
    run_recent_first_hardening,
    write_recent_first_hardening_outputs,
)


def _bars_csv(count: int = 1500) -> str:
    rows = ["time,open,high,low,close,volume"]
    price = 100.0
    for index in range(count):
        cycle = index % 96
        direction = 1 if (index // 96) % 2 == 0 else -1
        london_impulse = 0.62 if 8 <= index % 24 <= 17 and cycle in {18, 19, 20, 52, 53, 54} else 0.08
        pull = -0.16 if cycle in {21, 55} else 0.03
        open_price = price
        close = price + direction * london_impulse + pull
        high = max(open_price, close) + 0.42
        low = min(open_price, close) - 0.42
        price = close
        rows.append(f"2025-07-{(index % 28) + 1:02d}T{index % 24:02d}:00:00+00:00,{open_price:.4f},{high:.4f},{low:.4f},{close:.4f},1")
    return "\n".join(rows)


class MT5RecentFirstHardeningTests(unittest.TestCase):
    def test_hardening_is_paper_only_and_reports_stress_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ["BTCUSD_M30_60000.csv", "BTCUSD_M30_40000.csv", "BTCUSD_H1_30000.csv"]:
                (root / name).write_text(_bars_csv(), encoding="utf-8")

            result = run_recent_first_hardening(
                {
                    "csv_dir": str(root),
                    "targets": "recent_london_us_breakout_m30_both_hardened_v1,recent_liquidity_sweep_h1_hardened_v1",
                    "max_bars": 1000,
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
            row = result["results"][0]
            for key in [
                "recent_closed",
                "recent_pf",
                "total_closed",
                "total_pf",
                "monte_carlo_stressed_pf",
                "monte_carlo_stressed_expectancy",
                "monte_carlo_p95_drawdown",
                "remove_best_5_pf",
                "spread_x2_pf",
                "rejection_reasons",
            ]:
                self.assertIn(key, row)
            self.assertFalse(row["broker_touched"])
            self.assertFalse(row["order_executed"])

    def test_gate_rejects_failed_hardening_stress(self) -> None:
        total = {"closed": 80, "profit_factor": 1.5, "expectancy": 0.2, "max_drawdown": 1200}
        recent = {"closed": 30, "profit_factor": 1.4, "expectancy": 0.15, "max_drawdown": 600}
        monte_carlo = {"profit_factor_stressed": 0.9, "expectancy_stressed": -0.02, "max_drawdown_p95": 900}
        remove_best_5 = {"profit_factor": 0.95}
        spread_x2 = {"profit_factor": 0.92}

        gate = _gate(total, recent, monte_carlo, remove_best_5, spread_x2, fragile=False, single_trade=False)

        self.assertFalse(gate["passed"])
        self.assertIn("monte_carlo_stressed_pf_below_1_05", gate["reasons"])
        self.assertIn("monte_carlo_stressed_expectancy_negative", gate["reasons"])
        self.assertIn("remove_best_5_pf_below_1", gate["reasons"])
        self.assertIn("spread_x2_pf_below_1", gate["reasons"])

    def test_outputs_and_script_smoke_finish_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ["BTCUSD_M30_60000.csv", "BTCUSD_M30_40000.csv", "BTCUSD_H1_30000.csv"]:
                (root / name).write_text(_bars_csv(), encoding="utf-8")
            result = run_recent_first_hardening(
                {
                    "csv_dir": str(root),
                    "targets": "recent_london_us_breakout_m30_both_hardened_v1,recent_liquidity_sweep_h1_hardened_v1",
                    "max_bars": 950,
                    "per_evaluation_timeout_seconds": 1.0,
                }
            )

            csv_path, json_path, summary_path = write_recent_first_hardening_outputs(result, root)
            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertIn("Recent-First Hardening", summary_path.read_text(encoding="utf-8"))

            started = time.monotonic()
            code = hardening_main(["--smoke", "--csv-dir", str(root), "--output-dir", str(root)])

            self.assertEqual(code, 0)
            self.assertLess(time.monotonic() - started, 30)
            self.assertTrue((root / "recent_first_hardening_results.csv").exists())


if __name__ == "__main__":
    unittest.main()

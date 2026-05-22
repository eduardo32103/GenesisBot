from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from scripts.run_eth_m30_volatility_hardening_from_csv import main as eth_hardening_main
from services.mt5.mt5_eth_m30_volatility_hardening import (
    _gate,
    run_eth_m30_volatility_hardening,
    write_eth_m30_volatility_hardening_outputs,
)


def _bars_csv(count: int = 1500) -> str:
    rows = ["time,open,high,low,close,volume"]
    price = 3200.0
    for index in range(count):
        cycle = index % 96
        direction = 1 if (index // 96) % 2 == 0 else -1
        breakout = 7.4 if cycle in {18, 19, 20, 52, 53, 54} else 1.1
        pull = -1.8 if cycle in {21, 55} else 0.35
        open_price = price
        close = price + direction * breakout + pull
        high = max(open_price, close) + 4.2
        low = min(open_price, close) - 4.2
        price = close
        rows.append(f"2025-09-{(index % 28) + 1:02d}T{index % 24:02d}:00:00+00:00,{open_price:.2f},{high:.2f},{low:.2f},{close:.2f},1")
    return "\n".join(rows)


class MT5EthM30VolatilityHardeningTests(unittest.TestCase):
    def test_hardening_is_paper_only_and_reports_required_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_path = root / "ETHUSD_M30_20000.csv"
            csv_path.write_text(_bars_csv(), encoding="utf-8")

            result = run_eth_m30_volatility_hardening(
                {
                    "csv_path": str(csv_path),
                    "targets": "eth_m30_vol_breakout_both_baseline,eth_m30_vol_breakout_mc_hardened_v1",
                    "max_bars": 1100,
                    "monte_carlo_simulations": 120,
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
                "total_closed",
                "monte_carlo_stressed_pf",
                "monte_carlo_stressed_expectancy",
                "monte_carlo_p95_drawdown",
                "spread_x1_5_pf",
                "spread_x2_pf",
                "remove_best_3_pf",
                "remove_best_5_pf",
                "buy_sell_stats",
                "session_stats",
                "hour_stats",
                "volatility_regime_stats",
                "atr_regime_stats",
                "trend_chop_range_stats",
                "exit_reason_counts",
                "loss_cluster_stats",
                "rejection_reasons",
            ]:
                self.assertIn(key, row)
            self.assertFalse(row["broker_touched"])
            self.assertFalse(row["order_executed"])

    def test_gate_rejects_monte_carlo_and_remove_best_failures(self) -> None:
        total = {"closed": 85, "profit_factor": 1.5, "expectancy": 0.2, "max_drawdown": 1200}
        recent = {"closed": 28, "profit_factor": 1.2, "expectancy": 0.12, "max_drawdown": 700}
        monte_carlo = {"profit_factor_stressed": 0.94, "expectancy_stressed": -0.01, "max_drawdown_p95": 1500}
        remove_best_5 = {"profit_factor": 0.91}
        spread_x2 = {"profit_factor": 1.2}

        gate = _gate(total, recent, monte_carlo, remove_best_5, spread_x2, fragile=False, single_trade=False)

        self.assertFalse(gate["passed"])
        self.assertIn("monte_carlo_stressed_pf_below_1_05", gate["reasons"])
        self.assertIn("monte_carlo_stressed_expectancy_negative", gate["reasons"])
        self.assertIn("remove_best_5_pf_below_1", gate["reasons"])

    def test_outputs_and_script_smoke_finish_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_path = root / "ETHUSD_M30_20000.csv"
            csv_path.write_text(_bars_csv(), encoding="utf-8")
            result = run_eth_m30_volatility_hardening(
                {
                    "csv_path": str(csv_path),
                    "targets": "eth_m30_vol_breakout_both_baseline,eth_m30_vol_breakout_mae_guard_v1",
                    "max_bars": 1000,
                    "monte_carlo_simulations": 120,
                    "per_evaluation_timeout_seconds": 1.0,
                }
            )

            csv_out, json_out, summary_out = write_eth_m30_volatility_hardening_outputs(result, root)
            self.assertTrue(csv_out.exists())
            self.assertTrue(json_out.exists())
            self.assertTrue(summary_out.exists())
            self.assertIn("ETHUSD M30 Volatility Breakout", summary_out.read_text(encoding="utf-8"))

            started = time.monotonic()
            code = eth_hardening_main(["--smoke", "--csv-path", str(csv_path), "--output-dir", str(root)])

            self.assertEqual(code, 0)
            self.assertLess(time.monotonic() - started, 30)
            self.assertTrue((root / "eth_m30_volatility_hardening_results.csv").exists())


if __name__ == "__main__":
    unittest.main()

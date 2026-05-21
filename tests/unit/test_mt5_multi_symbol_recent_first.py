from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from scripts.run_multi_symbol_recent_first_from_csv import main as multi_symbol_main
from services.mt5.mt5_multi_symbol_recent_first import (
    _gate,
    run_multi_symbol_recent_first,
    write_multi_symbol_recent_first_outputs,
)


def _bars_csv(count: int = 1500, *, bias: float = 0.0) -> str:
    rows = ["time,open,high,low,close,volume"]
    price = 100.0 + bias
    for index in range(count):
        cycle = index % 96
        direction = 1 if (index // 96) % 2 == 0 else -1
        session_push = 0.64 if 7 <= index % 24 <= 20 and cycle in {18, 19, 20, 52, 53, 54} else 0.07
        mean_pull = -0.14 if cycle in {21, 55} else 0.025
        open_price = price
        close = price + direction * session_push + mean_pull
        high = max(open_price, close) + 0.44
        low = min(open_price, close) - 0.44
        price = close
        rows.append(f"2025-08-{(index % 28) + 1:02d}T{index % 24:02d}:00:00+00:00,{open_price:.5f},{high:.5f},{low:.5f},{close:.5f},1")
    return "\n".join(rows)


class MT5MultiSymbolRecentFirstTests(unittest.TestCase):
    def test_multi_symbol_research_is_paper_only_and_skips_missing_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M15_20000.csv").write_text(_bars_csv(), encoding="utf-8")
            (root / "ETHUSD_M15_20000.csv").write_text(_bars_csv(bias=30.0), encoding="utf-8")

            result = run_multi_symbol_recent_first(
                {
                    "csv_dir": str(root),
                    "fallback_csv_dir": str(root),
                    "symbols": "BTCUSD,ETHUSD,MISSING",
                    "timeframes": "M15",
                    "families": "recent_momentum_pullback,recent_ema_reclaim",
                    "bars": 1000,
                    "max_evaluations_per_symbol_timeframe": 8,
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
            self.assertEqual([item["symbol"] for item in result["skipped_symbols"]], ["MISSING"])
            row = result["results"][0]
            for key in [
                "symbol",
                "recent_closed",
                "recent_pf",
                "total_closed",
                "total_pf",
                "monte_carlo_stressed_pf",
                "spread_x1_5_pf",
                "spread_x2_pf",
                "remove_best_5_pf",
                "rejection_reasons",
            ]:
                self.assertIn(key, row)

    def test_gate_rejects_spread_and_remove_best_stress(self) -> None:
        total = {"closed": 80, "profit_factor": 1.6, "expectancy": 0.2, "max_drawdown": 1200}
        recent = {"closed": 25, "profit_factor": 1.3, "expectancy": 0.12, "max_drawdown": 700}
        monte_carlo = {"profit_factor_stressed": 1.1, "expectancy_stressed": 0.04, "max_drawdown_p95": 1500}
        remove_best_5 = {"profit_factor": 0.91}
        spread_x1_5 = {"profit_factor": 0.94}
        spread_x2 = {"profit_factor": 0.90}

        gate = _gate(total, recent, monte_carlo, remove_best_5, spread_x1_5, spread_x2, fragile=False, single_trade=False)

        self.assertFalse(gate["passed"])
        self.assertIn("spread_x1_5_pf_below_1", gate["reasons"])
        self.assertIn("spread_x2_pf_below_0_95", gate["reasons"])
        self.assertIn("remove_best_5_pf_below_1", gate["reasons"])

    def test_outputs_and_script_smoke_finish_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M15_20000.csv").write_text(_bars_csv(), encoding="utf-8")
            result = run_multi_symbol_recent_first(
                {
                    "csv_dir": str(root),
                    "fallback_csv_dir": str(root),
                    "symbols": "BTCUSD",
                    "timeframes": "M15",
                    "families": "recent_momentum_pullback,recent_ema_reclaim",
                    "bars": 900,
                    "max_evaluations_per_symbol_timeframe": 6,
                    "per_evaluation_timeout_seconds": 1.0,
                }
            )

            csv_path, json_path, summary_path = write_multi_symbol_recent_first_outputs(result, root)
            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertIn("Multi-Symbol Recent-First", summary_path.read_text(encoding="utf-8"))

            started = time.monotonic()
            code = multi_symbol_main(["--smoke", "--symbols", "BTCUSD", "--csv-dir", str(root), "--fallback-csv-dir", str(root), "--output-dir", str(root)])

            self.assertEqual(code, 0)
            self.assertLess(time.monotonic() - started, 30)
            self.assertTrue((root / "multi_symbol_recent_first_results.csv").exists())


if __name__ == "__main__":
    unittest.main()

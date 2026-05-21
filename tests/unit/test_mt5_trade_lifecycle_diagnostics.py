from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from scripts.run_trade_lifecycle_diagnostics_from_csv import main as lifecycle_main
from services.mt5.mt5_backtester import _load_bars, _settings
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_trade_lifecycle_diagnostics import (
    diagnose_trade_lifecycle,
    run_trade_lifecycle_diagnostics,
    write_trade_lifecycle_outputs,
)


def _bars_csv(count: int = 260) -> str:
    rows = ["time,open,high,low,close,volume"]
    price = 100.0
    for index in range(count):
        direction = 1 if (index // 34) % 2 == 0 else -1
        pulse = 0.55 if index % 9 in {0, 1, 2} else 0.22
        open_price = price
        close = price + direction * pulse
        high = max(open_price, close) + 0.35
        low = min(open_price, close) - 0.35
        price = close
        rows.append(f"2026-01-01 {index % 24:02d}:00:00,{open_price:.4f},{high:.4f},{low:.4f},{close:.4f},1")
    return "\n".join(rows)


class MT5TradeLifecycleDiagnosticsTests(unittest.TestCase):
    def test_diagnostics_are_paper_only_and_do_not_mutate_live_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M30_5000.csv").write_text(_bars_csv(), encoding="utf-8")

            result = run_trade_lifecycle_diagnostics(
                {
                    "csv_dir": str(root),
                    "pairs": "M30:capital_preservation_v4_side_filtered",
                    "max_bars": 220,
                    "timeout_seconds": 10,
                }
            )

            self.assertTrue(result["ok"])
            self.assertFalse(result["broker_touched"])
            self.assertFalse(result["order_executed"])
            self.assertEqual(result["order_policy"], "journal_only_no_broker")
            self.assertFalse(result["promoted_profile_mutated"])
            self.assertFalse(result["forward_state_mutated"])
            self.assertFalse(result["shadow_trades_mutated"])
            self.assertFalse(result["martingale_enabled"])
            self.assertFalse(result["grid_enabled"])
            self.assertFalse(result["averaging_down_enabled"])

    def test_lifecycle_counts_trade_occupancy_and_exit_metrics(self) -> None:
        settings = _settings(
            {
                "symbol": "BTCUSD",
                "timeframe": "M30",
                "csv_text": _bars_csv(320),
                "max_bars": 320,
                "save_results": False,
                "spread_points": 20,
            },
            get_mt5_config(),
        )
        bars, _warnings = _load_bars({"csv_text": _bars_csv(320)}, settings)

        row = diagnose_trade_lifecycle(bars, settings, "capital_preservation_v4_side_filtered", timeout_seconds=10)

        for key in [
            "skipped_due_max_open_trades",
            "avg_bars_in_trade",
            "median_bars_in_trade",
            "max_bars_in_trade",
            "exit_reason_counts",
            "side_stats",
            "session_hour_stats",
        ]:
            self.assertIn(key, row)
        self.assertIsInstance(row["exit_reason_counts"], dict)
        self.assertGreaterEqual(row["skipped_due_max_open_trades"], 0)
        self.assertGreaterEqual(row["avg_bars_in_trade"], 0)

    def test_write_outputs_and_script_smoke_finish_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M30_5000.csv").write_text(_bars_csv(), encoding="utf-8")
            result = run_trade_lifecycle_diagnostics(
                {
                    "csv_dir": str(root),
                    "pairs": "M30:capital_preservation_v4_side_filtered",
                    "max_bars": 180,
                }
            )

            csv_path, json_path, summary_path = write_trade_lifecycle_outputs(result, root)
            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertIn("MT5 Trade Lifecycle Diagnostics", summary_path.read_text(encoding="utf-8"))

            started = time.monotonic()
            code = lifecycle_main(["--smoke", "--csv-dir", str(root), "--output-dir", str(root)])

            self.assertEqual(code, 0)
            self.assertLess(time.monotonic() - started, 30)
            self.assertTrue((root / "trade_lifecycle_diagnostics.csv").exists())


if __name__ == "__main__":
    unittest.main()

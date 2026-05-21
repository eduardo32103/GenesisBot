from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from scripts.run_counterfactual_diagnostics_from_csv import main as counterfactual_main
from services.mt5.mt5_counterfactual_diagnostics import (
    diagnose_counterfactual_profile,
    run_counterfactual_diagnostics,
    write_counterfactual_outputs,
)
from services.mt5.mt5_capital_preservation_optimizer import CAPITAL_PRESERVATION_PROFILES, _PROFILE_PARAMS
from services.mt5.mt5_backtester import _load_bars, _settings
from services.mt5.mt5_config import get_mt5_config


def _bars_csv(count: int = 260) -> str:
    rows = ["time,open,high,low,close,volume"]
    price = 100.0
    for index in range(count):
        direction = 1 if (index // 32) % 2 == 0 else -1
        pulse = 0.6 if index % 11 in {0, 1, 2} else 0.22
        open_price = price
        close = price + direction * pulse
        high = max(open_price, close) + 0.35
        low = min(open_price, close) - 0.35
        price = close
        rows.append(f"2026-01-01 {index % 24:02d}:00:00,{open_price:.4f},{high:.4f},{low:.4f},{close:.4f},1")
    return "\n".join(rows)


class MT5CounterfactualDiagnosticsTests(unittest.TestCase):
    def test_counterfactual_diagnostics_are_paper_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M30_5000.csv").write_text(_bars_csv(), encoding="utf-8")

            result = run_counterfactual_diagnostics(
                {
                    "csv_dir": str(root),
                    "pairs": "M30:capital_preservation_v4_side_filtered",
                    "max_bars": 220,
                    "timeout_seconds": 15,
                }
            )

            self.assertTrue(result["ok"])
            self.assertFalse(result["broker_touched"])
            self.assertFalse(result["order_executed"])
            self.assertEqual(result["order_policy"], "journal_only_no_broker")
            self.assertFalse(result["promoted_profile_mutated"])
            self.assertFalse(result["forward_state_mutated"])
            self.assertFalse(result["martingale_enabled"])
            self.assertFalse(result["grid_enabled"])
            self.assertFalse(result["averaging_down_enabled"])
            self.assertFalse(result["automatic_promotion"])

    def test_counterfactual_row_contains_required_comparisons(self) -> None:
        settings = _settings(
            {
                "symbol": "BTCUSD",
                "timeframe": "M30",
                "csv_text": _bars_csv(300),
                "max_bars": 300,
                "save_results": False,
                "spread_points": 20,
            },
            get_mt5_config(),
        )
        bars, _warnings = _load_bars({"csv_text": _bars_csv(300)}, settings)

        row = diagnose_counterfactual_profile(bars, settings, "capital_preservation_v4_side_filtered", timeout_seconds=15)

        for key in [
            "baseline_closed",
            "counterfactual_closed",
            "blocked_trades_that_would_win",
            "blocked_trades_that_would_lose",
            "net_value_of_block",
            "risk_saved_by_block",
            "opportunity_lost_by_block",
            "session_window_stats",
            "side_stats",
            "whether_block_is_protective",
        ]:
            self.assertIn(key, row)
        scenarios = {item["scenario"] for item in row["counterfactuals"]}
        self.assertIn("cooldown_one_bar_shorter", scenarios)
        self.assertIn("consecutive_losses_extra_confirmation", scenarios)
        self.assertIn("session_expand_positive_hours", scenarios)

    def test_write_outputs_and_script_smoke_finish_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M30_5000.csv").write_text(_bars_csv(), encoding="utf-8")
            result = run_counterfactual_diagnostics(
                {
                    "csv_dir": str(root),
                    "pairs": "M30:capital_preservation_v4_side_filtered",
                    "max_bars": 180,
                }
            )

            csv_path, json_path, summary_path = write_counterfactual_outputs(result, root)
            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertIn("MT5 Counterfactual Diagnostics", summary_path.read_text(encoding="utf-8"))

            started = time.monotonic()
            code = counterfactual_main(["--smoke", "--csv-dir", str(root), "--output-dir", str(root)])

            self.assertEqual(code, 0)
            self.assertLess(time.monotonic() - started, 30)
            self.assertTrue((root / "counterfactual_diagnostics.csv").exists())

    def test_counterfactual_safe_profiles_are_registered_paper_only(self) -> None:
        for profile in ["capital_preservation_v5_counterfactual_safe", "low_drawdown_v6_counterfactual_safe"]:
            self.assertIn(profile, CAPITAL_PRESERVATION_PROFILES)
            params = _PROFILE_PARAMS[profile]
            self.assertTrue(params["counterfactual_safe"])
            self.assertEqual(params["allowed_sides"], ["buy"])
            self.assertTrue(params["session_filter"])


if __name__ == "__main__":
    unittest.main()

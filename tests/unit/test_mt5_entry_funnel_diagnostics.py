from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from scripts.run_entry_funnel_diagnostics_from_csv import main as funnel_main
from services.mt5.mt5_capital_preservation_optimizer import CAPITAL_PRESERVATION_PROFILES
from services.mt5.mt5_entry_funnel_diagnostics import run_entry_funnel_diagnostics, write_entry_funnel_outputs


def _bars_csv(count: int = 220) -> str:
    rows = ["time,open,high,low,close,volume"]
    price = 100.0
    for index in range(count):
        direction = 1 if (index // 30) % 2 == 0 else -1
        pulse = 0.5 if index % 17 == 0 else 0.18
        open_price = price
        close = price + direction * pulse
        high = max(open_price, close) + 0.25
        low = min(open_price, close) - 0.25
        price = close
        rows.append(f"2026-01-01 {index % 24:02d}:00:00,{open_price:.4f},{high:.4f},{low:.4f},{close:.4f},1")
    return "\n".join(rows)


class MT5EntryFunnelDiagnosticsTests(unittest.TestCase):
    def test_diagnostics_are_paper_only_and_do_not_mutate_live_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M15_5000.csv").write_text(_bars_csv(), encoding="utf-8")

            result = run_entry_funnel_diagnostics(
                {
                    "csv_dir": str(root),
                    "timeframes": ["M15"],
                    "profiles": ["trend_continuation_v3_balanced"],
                    "max_bars": 180,
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

    def test_each_filter_reports_counts_and_reasons_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M15_5000.csv").write_text(_bars_csv(), encoding="utf-8")

            result = run_entry_funnel_diagnostics(
                {
                    "csv_dir": str(root),
                    "timeframes": ["M15"],
                    "profiles": ["breakout_pullback_v3_balanced"],
                    "max_bars": 180,
                }
            )
            row = result["results"][0]

            for key in [
                "passed_regime_filter",
                "failed_regime_filter",
                "passed_spread_filter",
                "failed_spread_filter",
                "passed_volatility_filter",
                "failed_volatility_filter",
                "passed_trend_filter",
                "failed_trend_filter",
                "passed_pullback_filter",
                "failed_pullback_filter",
                "passed_rsi_filter",
                "failed_rsi_filter",
                "passed_ema_filter",
                "failed_ema_filter",
                "passed_score_threshold",
                "failed_score_threshold",
            ]:
                self.assertIn(key, row)
                self.assertIsInstance(row[key], int)
            self.assertIn("no_trade_reason_counts", row)
            self.assertIsInstance(row["no_trade_reason_counts"], dict)
            self.assertGreater(row["bars_evaluated"], 0)

    def test_zero_trade_profile_explains_why(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M15_5000.csv").write_text(_bars_csv(120), encoding="utf-8")

            result = run_entry_funnel_diagnostics(
                {
                    "csv_dir": str(root),
                    "timeframes": ["M15"],
                    "profiles": ["capital_preservation_v2"],
                    "max_bars": 100,
                    "spread_points": 999,
                }
            )
            row = result["results"][0]

            self.assertEqual(row["opened_shadow_trade_count"], 0)
            self.assertTrue(row["top_no_trade_reasons"])
            self.assertIn("spread_too_high", row["no_trade_reason_counts"])

    def test_write_outputs_and_script_smoke_finish_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M15_5000.csv").write_text(_bars_csv(), encoding="utf-8")
            result = run_entry_funnel_diagnostics(
                {
                    "csv_dir": str(root),
                    "timeframes": ["M15"],
                    "profiles": ["trend_continuation_v3_balanced"],
                    "max_bars": 140,
                }
            )
            csv_path, json_path, summary_path = write_entry_funnel_outputs(result, root)
            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertTrue(summary_path.exists())

            started = time.monotonic()
            code = funnel_main(["--smoke", "--csv-dir", str(root), "--output-dir", str(root)])

            self.assertEqual(code, 0)
            self.assertLess(time.monotonic() - started, 30)
            self.assertTrue((root / "entry_funnel_diagnostics.csv").exists())

    def test_balanced_profiles_are_registered(self) -> None:
        for profile in [
            "trend_continuation_v3_balanced",
            "breakout_pullback_v3_balanced",
            "low_drawdown_v3_more_trades",
            "anti_chop_v4_balanced",
            "capital_preservation_v3_balanced",
            "liquidity_sweep_v2_confirmed",
        ]:
            self.assertIn(profile, CAPITAL_PRESERVATION_PROFILES)


if __name__ == "__main__":
    unittest.main()

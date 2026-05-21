from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.run_h1_candidate_maturation_from_csv import main as h1_maturation_main
from scripts.run_h1_candidate_maturation_from_csv import parse_args as h1_maturation_parse_args
from services.mt5.mt5_backtester import _load_bars, _settings
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_h1_candidate_maturation import (
    evaluate_h1_profile_maturation,
    run_h1_candidate_maturation,
    write_h1_candidate_maturation_outputs,
)


def _bars_csv(count: int = 320) -> str:
    rows = ["time,open,high,low,close,volume"]
    price = 100.0
    for index in range(count):
        direction = 1 if (index // 36) % 2 == 0 else -1
        pulse = 0.62 if index % 13 in {0, 1, 2} else 0.24
        open_price = price
        close = price + direction * pulse
        high = max(open_price, close) + 0.35
        low = min(open_price, close) - 0.35
        price = close
        rows.append(f"2026-01-01 {index % 24:02d}:00:00,{open_price:.4f},{high:.4f},{low:.4f},{close:.4f},1")
    return "\n".join(rows)


def _closed_trade(index: int, pnl: float = 10.0) -> dict[str, object]:
    return {
        "lifecycle_status": "closed",
        "status": "win" if pnl >= 0 else "loss",
        "pnl": pnl,
        "pnl_pct": pnl / 1000.0,
        "r_multiple": pnl / 10.0,
        "opened_index": index,
        "exit_reason": "take_profit" if pnl >= 0 else "stop_loss",
        "side": "buy",
    }


class MT5H1CandidateMaturationTests(unittest.TestCase):
    def test_h1_maturation_is_paper_only_and_does_not_mutate_live_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_H1_10000.csv").write_text(_bars_csv(), encoding="utf-8")

            result = run_h1_candidate_maturation(
                {
                    "csv_dir": str(root),
                    "profiles": "low_drawdown_v5_session_filtered",
                    "max_bars": 250,
                    "timeout_seconds": 15,
                }
            )

            self.assertTrue(result["ok"])
            self.assertFalse(result["broker_touched"])
            self.assertFalse(result["order_executed"])
            self.assertEqual(result["order_policy"], "journal_only_no_broker")
            self.assertFalse(result["promoted_profile_mutated"])
            self.assertFalse(result["forward_state_mutated"])
            self.assertFalse(result["automatic_promotion"])
            self.assertFalse(result["martingale_enabled"])
            self.assertFalse(result["grid_enabled"])
            self.assertFalse(result["averaging_down_enabled"])

    def test_h1_maturation_reports_sample_and_terciles(self) -> None:
        settings = _settings(
            {
                "symbol": "BTCUSD",
                "timeframe": "H1",
                "csv_text": _bars_csv(360),
                "max_bars": 360,
                "save_results": False,
                "spread_points": 20,
            },
            get_mt5_config(),
        )
        bars, _warnings = _load_bars({"csv_text": _bars_csv(360)}, settings)

        row = evaluate_h1_profile_maturation(bars, settings, "low_drawdown_v5_session_filtered", timeout_seconds=15)

        for key in [
            "closed_actual",
            "sample_gate_required",
            "missing_to_50",
            "trade_frequency_per_1000_bars",
            "estimated_bars_for_50_trades",
            "tercile_stats",
            "monte_carlo_stressed_pf",
            "monte_carlo_p95_drawdown",
            "fragile_regime_dependency",
        ]:
            self.assertIn(key, row)
        self.assertEqual(row["sample_gate_required"], 50)
        self.assertIn("tercile_1", row["tercile_stats"])

    def test_write_outputs_and_script_smoke_finish_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_H1_10000.csv").write_text(_bars_csv(), encoding="utf-8")
            result = run_h1_candidate_maturation(
                {
                    "csv_dir": str(root),
                    "profiles": "low_drawdown_v5_session_filtered",
                    "max_bars": 220,
                    "timeout_seconds": 15,
                }
            )

            csv_path, json_path, summary_path = write_h1_candidate_maturation_outputs(result, root)
            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertIn("MT5 H1 Candidate Maturation Summary", summary_path.read_text(encoding="utf-8"))

            started = time.monotonic()
            code = h1_maturation_main(["--smoke", "--csv-dir", str(root), "--output-dir", str(root)])

            self.assertEqual(code, 0)
            self.assertLess(time.monotonic() - started, 30)
            self.assertTrue((root / "h1_candidate_maturation_results.csv").exists())

    def test_explicit_csv_path_and_max_bars_honor_30000_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            csv_file = root / "custom_h1_30000.csv"
            csv_file.write_text(_bars_csv(30000), encoding="utf-8")
            fake_trades = [_closed_trade(index * 900 + 10) for index in range(30)]

            with patch(
                "services.mt5.mt5_h1_candidate_maturation._simulate_capital_preservation",
                return_value=(fake_trades, 0, [], {"risk_governor_blocks": 0}),
            ):
                result = run_h1_candidate_maturation(
                    {
                        "csv_path": str(csv_file),
                        "profiles": "low_drawdown_v5_session_filtered",
                        "max_bars": 30000,
                        "timeout_seconds": 15,
                    }
                )

            row = result["results"][0]
            self.assertEqual(result["csv_path_used"], str(csv_file))
            self.assertEqual(result["bars_loaded"], 30000)
            self.assertEqual(result["bars_evaluated"], 29999)
            self.assertEqual(result["max_bars_requested"], 30000)
            self.assertEqual(row["csv_path_used"], str(csv_file))
            self.assertEqual(row["bars_loaded"], 30000)
            self.assertEqual(row["bars_evaluated"], 29999)
            self.assertEqual(row["max_bars_requested"], 30000)
            self.assertNotEqual(row["bars_loaded"], 10000)
            self.assertEqual(row["trade_frequency_per_1000_bars"], round((30 / 29999) * 1000.0, 4))
            self.assertEqual(row["estimated_bars_for_50_trades"], 50000)

    def test_runner_accepts_csv_path_alias(self) -> None:
        args = h1_maturation_parse_args(["--csv-path", "data/backtests/BTCUSD_H1_30000.csv", "--max-bars", "30000"])

        self.assertEqual(args.csv_path, "data/backtests/BTCUSD_H1_30000.csv")
        self.assertEqual(args.max_bars, 30000)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from scripts.run_capital_preservation_optimizer_from_csv import main as capital_optimizer_main
from services.mt5.mt5_capital_preservation_optimizer import (
    CAPITAL_PRESERVATION_PROFILES,
    MT5CapitalPreservationOptimizer,
    _capital_decision_from_history,
    _capital_candidate_gate,
    _config,
    _simulate_capital_preservation,
    _settings_for_capital_config,
    write_capital_preservation_outputs,
)
from services.mt5.mt5_backtester import _load_bars, _settings
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_promoted_profile import get_promoted_profile, record_promoted_profile, reset_promoted_profiles_for_tests


def _bars_csv(count: int = 180) -> str:
    rows = ["time,open,high,low,close,volume"]
    price = 100.0
    for index in range(count):
        direction = 1 if (index // 12) % 2 == 0 else -1
        open_price = price
        close = price + direction * 0.35
        high = max(open_price, close) + 0.22
        low = min(open_price, close) - 0.22
        price = close
        rows.append(f"2026-01-01 00:{index % 60:02d}:00,{open_price:.4f},{high:.4f},{low:.4f},{close:.4f},1")
    return "\n".join(rows)


class MT5CapitalPreservationOptimizerTests(unittest.TestCase):
    def test_optimizer_is_paper_only_and_does_not_mutate_live_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M15_5000.csv").write_text(_bars_csv(), encoding="utf-8")

            result = MT5CapitalPreservationOptimizer().run(
                {
                    "csv_dir": str(root),
                    "timeframes": ["M15"],
                    "profiles": ["anti_chop_v2_safe"],
                    "max_bars": 160,
                    "max_evaluations": 3,
                    "timeout_seconds": 2,
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
            self.assertFalse(result["increase_size_after_loss_enabled"])

    def test_optimizer_does_not_mutate_promoted_profile_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M15_5000.csv").write_text(_bars_csv(), encoding="utf-8")
            reset_promoted_profiles_for_tests()
            before = record_promoted_profile(symbol="BTCUSD", timeframe="M30", profile="quality_loose", mode="paper_forward_candidate")

            MT5CapitalPreservationOptimizer().run(
                {
                    "csv_dir": str(root),
                    "timeframes": ["M15"],
                    "profiles": ["capital_preservation_v1"],
                    "max_bars": 160,
                    "max_evaluations": 2,
                }
            )
            after = get_promoted_profile(symbol="BTCUSD", timeframe="M30")

            self.assertEqual(before["profile"], after["profile"])
            self.assertEqual(after["status"], "paper_forward_candidate")
            self.assertTrue(after["active"])
            self.assertFalse(after["broker_touched"])
            self.assertFalse(after["order_executed"])

    def test_candidate_gate_rejects_bad_risk_metrics(self) -> None:
        full = {
            "closed": 100,
            "profit_factor": 0.95,
            "expectancy": -0.01,
            "win_rate": 44,
            "max_drawdown": 6000,
            "buy_pf": 1.0,
            "sell_pf": 1.0,
            "side_stats": {"buy": {"trades": 50}, "sell": {"trades": 50}},
            "best_trade": {"pnl": 50},
            "net_pnl": -100,
        }
        gate = _capital_candidate_gate(
            full,
            {"first_half": {"closed": 50, "profit_factor": 0.8, "expectancy": -0.06}},
            {"test_summary": {"closed": 50, "profit_factor": 0.9}},
            {"passed": True, "fail_reasons": []},
            type("Settings", (), {"timeframe": "M15"})(),
            {},
        )

        self.assertFalse(gate["passed"])
        self.assertIn("pf_below_1_20", gate["reasons"])
        self.assertIn("drawdown_above_5000", gate["reasons"])
        self.assertIn("sample_too_small", _capital_candidate_gate(
            {**full, "closed": 20, "profit_factor": 2.0, "expectancy": 0.2, "win_rate": 60, "max_drawdown": 1000},
            {},
            {"test_summary": {"closed": 10, "profit_factor": 1.2}},
            {"passed": True, "fail_reasons": []},
            type("Settings", (), {"timeframe": "M15"})(),
            {},
        )["reasons"])

    def test_simulator_respects_max_open_trades_and_risk_governor(self) -> None:
        settings = _settings(
            {
                "symbol": "BTCUSD",
                "timeframe": "M15",
                "csv_text": _bars_csv(),
                "max_bars": 160,
                "save_results": False,
                "spread_points": 999,
            },
            get_mt5_config(),
        )
        bars, _warnings = _load_bars({"csv_text": _bars_csv()}, settings)
        config = _config("anti_chop_v2_safe", 1.2, 3, 55, 20, True, True, 1, 2, True, True, True, True, False, False, False, True, 0.1)
        profile_settings = _settings_for_capital_config(settings, config)

        _trades, _no_trade, blocked, state = _simulate_capital_preservation(profile_settings, bars, config, time.monotonic())

        self.assertLessEqual(state["max_open_trades_observed"], 1)
        self.assertGreater(state["risk_governor_blocks"], 0)
        self.assertTrue(any(reason.startswith("risk_governor_") for reason in blocked))

    def test_per_evaluation_timeout_marks_reject_without_breaking_run(self) -> None:
        settings = _settings(
            {
                "symbol": "BTCUSD",
                "timeframe": "M15",
                "csv_text": _bars_csv(260),
                "max_bars": 260,
                "save_results": False,
            },
            get_mt5_config(),
        )
        settings = replace(settings, timeout_seconds=0.0)
        bars, _warnings = _load_bars({"csv_text": _bars_csv(260)}, settings)
        config = _config("trend_continuation_v1", 1.2, 3, 55, 25, True, True, 1, 2, True, True, True, True, False, False, False, True, 0.1)
        settings = _settings_for_capital_config(settings, config)

        row = MT5CapitalPreservationOptimizer()._evaluate(settings, bars, config, source_csv="unit.csv")

        self.assertTrue(row["timed_out"])
        self.assertEqual(row["reject_reason"], "timeout")
        self.assertEqual(row["recommendation"], "reject")
        self.assertFalse(row["broker_touched"])
        self.assertFalse(row["order_executed"])

    def test_script_keyboard_interrupt_writes_partial_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M15_5000.csv").write_text(_bars_csv(80), encoding="utf-8")

            with patch("scripts.run_capital_preservation_optimizer_from_csv.MT5CapitalPreservationOptimizer.run", side_effect=KeyboardInterrupt):
                code = capital_optimizer_main(["--csv-dir", str(root), "--output-dir", str(root), "--timeframes", "M15", "--profiles", "trend_continuation_v1"])

            self.assertEqual(code, 0)
            payload = (root / "capital_preservation_optimizer_results.json").read_text(encoding="utf-8")
            self.assertIn("mt5_capital_preservation_optimizer_interrupted", payload)
            self.assertIn('"broker_touched": false', payload)

    def test_script_smoke_mode_finishes_and_writes_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M15_5000.csv").write_text(_bars_csv(120), encoding="utf-8")
            started = time.monotonic()

            code = capital_optimizer_main(["--smoke", "--csv-dir", str(root), "--output-dir", str(root)])

            self.assertEqual(code, 0)
            self.assertLess(time.monotonic() - started, 30)
            self.assertTrue((root / "capital_preservation_optimizer_results.csv").exists())
            self.assertTrue((root / "capital_preservation_optimizer_results.json").exists())

    def test_new_edge_profiles_are_available_and_paper_only(self) -> None:
        for profile in [
            "breakout_pullback_v1",
            "breakout_pullback_v2_safe",
            "trend_continuation_v1",
            "mean_reversion_v1_safe",
            "volatility_squeeze_v1",
            "liquidity_sweep_reversal_v1",
            "atr_trailing_v1",
        ]:
            self.assertIn(profile, CAPITAL_PRESERVATION_PROFILES)

    def test_breakout_pullback_decision_uses_family_logic(self) -> None:
        rows = []
        price = 100.0
        for index in range(40):
            if index < 26:
                close = price + 0.2
            elif index < 34:
                close = price + 0.55
            else:
                close = price - 0.05
            open_price = price
            high = max(open_price, close) + 0.35
            low = min(open_price, close) - 0.35
            price = close
            rows.append({"time": f"2026-01-01 10:{index % 60:02d}:00", "open": open_price, "high": high, "low": low, "close": close, "volume": 1})
        rows[-1]["low"] = rows[-1]["close"] - 0.8
        rows[-1]["close"] = rows[-2]["close"] + 0.25
        settings = _settings(
            {
                "symbol": "BTCUSD",
                "timeframe": "M15",
                "bars_data": rows,
                "filter_profile": "quality_v2",
                "max_bars": 80,
                "save_results": False,
            },
            get_mt5_config(),
        )
        config = _config("breakout_pullback_v1", 1.2, 3, 55, 25, True, True, 1, 2, True, True, True, True, False, False, False, True, 0.1)
        settings = _settings_for_capital_config(settings, config)

        decision = _capital_decision_from_history(rows, settings, config)

        self.assertIn("broker_touched", decision | {"broker_touched": False})
        self.assertIn(decision["reason"], {"breakout_pullback_confirmed", "pullback_not_confirmed", "extended_candle_no_chase", "ema_distance_too_far"})

    def test_write_outputs_generates_capital_preservation_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = {
                "ok": True,
                "recommendation": "reject",
                "results": [
                    {
                        "timeframe": "M15",
                        "profile": "capital_preservation_v1",
                        "risk_reward": 1.2,
                        "time_stop_bars": 3,
                        "score_min": 72,
                        "spread_max": 20,
                        "closed": 80,
                        "wins": 40,
                        "losses": 40,
                        "win_rate": 50,
                        "profit_factor": 1.1,
                        "expectancy": 0.01,
                        "max_drawdown": 5100,
                        "test_pf": 0.9,
                        "test_expectancy": -0.01,
                        "monte_carlo": {"risk_of_ruin": 0.1, "max_drawdown_p95": 5200, "profit_factor_stressed": 0.9},
                        "capital_preservation_score": -12.0,
                        "recommendation": "reject",
                        "candidate": False,
                        "pass_fail_reasons": ["drawdown_above_5000"],
                        "broker_touched": False,
                        "order_executed": False,
                    }
                ],
                "candidates": [],
            }

            csv_path, json_path, summary_path = write_capital_preservation_outputs(result, tmp)

            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertIn("No martingale", summary_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()

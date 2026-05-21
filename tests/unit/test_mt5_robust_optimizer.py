from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.run_robust_profile_optimizer_from_csv import write_outputs
from services.mt5.mt5_promoted_profile import get_promoted_profile, record_promoted_profile, reset_promoted_profiles_for_tests
from services.mt5.mt5_robust_optimizer import MT5RobustOptimizer, _candidate_gate, _monte_carlo


def _bars_csv(count: int = 160) -> str:
    rows = ["time,open,high,low,close,volume"]
    price = 100.0
    for index in range(count):
        direction = 1 if (index // 8) % 2 == 0 else -1
        open_price = price
        close = price + direction * 0.45
        high = max(open_price, close) + 0.25
        low = min(open_price, close) - 0.25
        price = close
        rows.append(f"2026-01-01 00:{index % 60:02d}:00,{open_price:.4f},{high:.4f},{low:.4f},{close:.4f},1")
    return "\n".join(rows)


class MT5RobustOptimizerTests(unittest.TestCase):
    def test_optimizer_is_cold_path_and_never_mutates_live_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M15_5000.csv").write_text(_bars_csv(), encoding="utf-8")

            result = MT5RobustOptimizer().run(
                {
                    "csv_dir": str(root),
                    "timeframes": ["M15"],
                    "profiles": ["anti_chop_v2_safe"],
                    "rr_values": [1.2],
                    "time_stop_minutes": [15],
                    "max_bars": 160,
                }
            )

            self.assertTrue(result["ok"])
            self.assertFalse(result["broker_touched"])
            self.assertFalse(result["order_executed"])
            self.assertFalse(result["live_runtime_mutated"])
            self.assertFalse(result["promoted_profile_mutated"])
            self.assertFalse(result["shadow_trades_mutated"])
            self.assertEqual(result["results"][0]["applies_to_real_trading"], False)

    def test_optimizer_does_not_mutate_promoted_profile_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "BTCUSD_M15_5000.csv").write_text(_bars_csv(), encoding="utf-8")
            reset_promoted_profiles_for_tests()
            before = record_promoted_profile(symbol="BTCUSD", timeframe="M30", profile="quality_loose", mode="paper_forward_candidate")

            MT5RobustOptimizer().run(
                {
                    "csv_dir": str(root),
                    "timeframes": ["M15"],
                    "profiles": ["anti_chop_v2_safe"],
                    "rr_values": [1.2],
                    "time_stop_minutes": [15],
                    "max_bars": 160,
                }
            )
            after = get_promoted_profile(symbol="BTCUSD", timeframe="M30")

            self.assertEqual(before["profile"], after["profile"])
            self.assertEqual(after["status"], "paper_forward_candidate")
            self.assertTrue(after["active"])
            self.assertFalse(after["broker_touched"])
            self.assertFalse(after["order_executed"])

    def test_candidate_gate_rejects_small_sample_or_bad_windows(self) -> None:
        full = {
            "closed": 30,
            "profit_factor": 1.5,
            "expectancy": 0.1,
            "win_rate": 55,
            "max_drawdown": 1000,
            "buy_pf": 1.2,
            "sell_pf": 1.1,
        }
        windows = {"first_half": {"closed": 10, "profit_factor": 0.8, "expectancy": 0.01}}
        gate = _candidate_gate(
            full,
            windows,
            {"closed": 20, "profit_factor": 1.2},
            {"closed": 20, "profit_factor": 1.2},
            {"passed": True, "fail_reasons": []},
            type("Settings", (), {"timeframe": "M15"})(),
        )

        self.assertFalse(gate["passed"])
        self.assertIn("sample_too_small", gate["reasons"])
        self.assertIn("first_half_pf_below_1", gate["reasons"])

    def test_monte_carlo_stress_flags_bad_trade_distribution(self) -> None:
        trades = [
            {"lifecycle_status": "closed", "pnl": 10 if index < 5 else -50}
            for index in range(30)
        ]

        result = _monte_carlo(trades, initial_balance=100000, max_drawdown_limit=100, simulations=100)

        self.assertFalse(result["passed"])
        self.assertTrue(result["fail_reasons"])
        self.assertGreaterEqual(result["risk_of_ruin"], 0)

    def test_write_outputs_generates_required_robust_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = {
                "ok": True,
                "recommendation": "reject",
                "results": [
                    {
                        "timeframe": "M15",
                        "profile": "anti_chop_v2_safe",
                        "rr": 1.2,
                        "time_stop_min": 15,
                        "closed": 80,
                        "wins": 45,
                        "losses": 35,
                        "win_rate": 56.25,
                        "profit_factor": 1.25,
                        "expectancy": 0.03,
                        "max_drawdown": 2000,
                        "test_pf": 1.1,
                        "test_expectancy": 0.02,
                        "monte_carlo": {"risk_of_ruin": 0.0, "max_drawdown_p95": 1200},
                        "institutional_score": 12.3,
                        "recommendation": "reject",
                        "candidate": False,
                        "pass_fail_reasons": ["test_pf_below_1"],
                        "broker_touched": False,
                        "order_executed": False,
                    }
                ],
                "candidates": [],
            }

            csv_path, json_path, summary_path = write_outputs(result, tmp)

            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertIn("never recommends real trading", summary_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()

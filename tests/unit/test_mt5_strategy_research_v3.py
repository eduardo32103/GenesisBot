from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from scripts.run_strategy_research_v3_from_csv import main as research_v3_main
from services.mt5.mt5_strategy_research_v3 import (
    _apply_cross_sample_recent_guard,
    _gate,
    run_strategy_research_v3,
    write_strategy_research_v3_outputs,
)


def _bars_csv(count: int = 1300) -> str:
    rows = ["time,open,high,low,close,volume"]
    price = 100.0
    for index in range(count):
        cycle = index % 64
        drift = 0.05 if (index // 180) % 2 == 0 else -0.04
        impulse = 0.08 if cycle < 26 else (0.72 if cycle in {26, 27, 28} else 0.17)
        direction = 1 if (index // 64) % 2 == 0 else -1
        open_price = price
        close = price + drift + direction * impulse
        high = max(open_price, close) + (0.18 if cycle < 26 else 0.58)
        low = min(open_price, close) - (0.18 if cycle < 26 else 0.58)
        price = close
        rows.append(f"2025-05-{(index % 28) + 1:02d}T{index % 24:02d}:00:00+00:00,{open_price:.4f},{high:.4f},{low:.4f},{close:.4f},1")
    return "\n".join(rows)


class MT5StrategyResearchV3Tests(unittest.TestCase):
    def test_research_v3_is_paper_only_and_reports_walk_forward_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ["BTCUSD_M15_20000.csv", "BTCUSD_M30_60000.csv", "BTCUSD_M30_40000.csv"]:
                (root / name).write_text(_bars_csv(), encoding="utf-8")

            result = run_strategy_research_v3(
                {
                    "csv_dir": str(root),
                    "timeframes": "M15,M30",
                    "families": "range_breakout_anti_chop,ema_reclaim_pullback",
                    "max_bars": 900,
                    "max_evaluations": 8,
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
                "closed_train",
                "closed_validation",
                "closed_recent_holdout",
                "pf_train",
                "pf_validation",
                "pf_recent_holdout",
                "rolling_window_pf_min",
                "rolling_window_expectancy_min",
                "monte_carlo_stressed_pf",
                "rejection_reasons",
            ]:
                self.assertIn(key, row)

    def test_gate_rejects_recent_holdout_failure(self) -> None:
        total = {"closed": 80, "profit_factor": 1.5, "expectancy": 0.2, "max_drawdown": 1000}
        split = {
            "train": {"closed": 40, "profit_factor": 1.7, "expectancy": 0.3, "max_drawdown": 500},
            "validation": {"closed": 25, "profit_factor": 1.4, "expectancy": 0.1, "max_drawdown": 400},
            "recent_holdout": {"closed": 15, "profit_factor": 0.9, "expectancy": -0.1, "max_drawdown": 300},
        }
        rolling = {"pf_min": 1.1, "expectancy_min": 0.05, "drawdown_max": 700}
        monte_carlo = {"profit_factor_stressed": 1.2, "max_drawdown_p95": 900, "expectancy_stressed": 0.1}

        gate = _gate(total, split, rolling, monte_carlo, fragile=False, single_trade=False)

        self.assertFalse(gate["passed"])
        self.assertIn("recent_holdout_pf_below_1_05", gate["reasons"])
        self.assertIn("recent_holdout_expectancy_not_positive", gate["reasons"])

    def test_cross_sample_recent_guard_blocks_old_history_candidate(self) -> None:
        rows = [
            {
                "family": "range_breakout_anti_chop",
                "timeframe": "M30",
                "side_mode": "both",
                "session_filter": "all",
                "volatility_regime": "normal_high",
                "trend_regime": "any",
                "rsi_regime": "any",
                "score_threshold": 57.0,
                "risk_reward": 1.15,
                "time_stop_bars": 2,
                "mae_exit_r": 0.85,
                "sample_label": "M30_60000",
                "candidate": True,
                "recommendation": "research_candidate",
                "rejection_reasons": [],
                "reject_reasons": [],
                "research_score": 100.0,
            },
            {
                "family": "range_breakout_anti_chop",
                "timeframe": "M30",
                "side_mode": "both",
                "session_filter": "all",
                "volatility_regime": "normal_high",
                "trend_regime": "any",
                "rsi_regime": "any",
                "score_threshold": 57.0,
                "risk_reward": 1.15,
                "time_stop_bars": 2,
                "mae_exit_r": 0.85,
                "sample_label": "M30_40000",
                "profit_factor_total": 0.8,
                "expectancy_total": -0.1,
                "pf_recent_holdout": 0.8,
                "expectancy_recent_holdout": -0.1,
                "candidate": False,
                "recommendation": "reject",
                "rejection_reasons": ["recent_holdout_pf_below_1_05"],
                "reject_reasons": ["recent_holdout_pf_below_1_05"],
                "research_score": -20.0,
            },
        ]

        _apply_cross_sample_recent_guard(rows)

        self.assertFalse(rows[0]["candidate"])
        self.assertIn("recent_40000_failed", rows[0]["rejection_reasons"])

    def test_outputs_and_script_smoke_finish_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ["BTCUSD_M15_20000.csv", "BTCUSD_M30_60000.csv", "BTCUSD_M30_40000.csv"]:
                (root / name).write_text(_bars_csv(), encoding="utf-8")
            result = run_strategy_research_v3(
                {
                    "csv_dir": str(root),
                    "timeframes": "M15,M30",
                    "families": "range_breakout_anti_chop,ema_reclaim_pullback",
                    "max_bars": 850,
                    "max_evaluations": 8,
                    "per_evaluation_timeout_seconds": 1.0,
                }
            )

            csv_path, json_path, summary_path = write_strategy_research_v3_outputs(result, root)
            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertTrue(summary_path.exists())
            self.assertIn("Strategy Research V3", summary_path.read_text(encoding="utf-8"))

            started = time.monotonic()
            code = research_v3_main(["--smoke", "--csv-dir", str(root), "--output-dir", str(root)])

            self.assertEqual(code, 0)
            self.assertLess(time.monotonic() - started, 30)
            self.assertTrue((root / "strategy_research_v3_results.csv").exists())


if __name__ == "__main__":
    unittest.main()

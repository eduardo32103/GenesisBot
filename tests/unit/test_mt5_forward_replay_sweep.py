from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.run_forward_replay_sweep_from_csv import is_candidate, run_sweep, write_outputs


class _FakeForwardReplay:
    def run(self, payload: dict) -> dict:
        profile = str(payload.get("profile") or "")
        timeframe = str(payload.get("timeframe") or "")
        if profile == "trend_v1" and timeframe == "M30":
            metrics = {
                "closed": 32,
                "wins": 18,
                "losses": 14,
                "win_rate": 56.25,
                "profit_factor": 1.34,
                "expectancy": 0.041,
                "max_drawdown": 2300,
                "degraded": False,
                "degradation_reason": "",
            }
        elif profile == "baseline":
            metrics = {
                "closed": 40,
                "wins": 18,
                "losses": 22,
                "win_rate": 45.0,
                "profit_factor": 1.05,
                "expectancy": 0.004,
                "max_drawdown": 1200,
                "degraded": False,
                "degradation_reason": "",
            }
        else:
            metrics = {
                "closed": 10,
                "wins": 3,
                "losses": 7,
                "win_rate": 30.0,
                "profit_factor": 0.7,
                "expectancy": -0.02,
                "max_drawdown": 600,
                "degraded": True,
                "degradation_reason": "early_forward_underperformance",
            }
        return {
            "ok": True,
            "status": "paper_forward_candidate" if not metrics["degraded"] else "observation_only",
            "symbol": payload.get("symbol"),
            "timeframe": timeframe,
            "profile": profile,
            "bars_loaded": 120,
            **metrics,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }


class ForwardReplaySweepTests(unittest.TestCase):
    def test_candidate_rule_requires_minimum_quality(self) -> None:
        self.assertTrue(
            is_candidate(
                {
                    "closed": 25,
                    "profit_factor": 1.15,
                    "expectancy": 0.001,
                    "win_rate": 40,
                    "max_drawdown": 5000,
                    "degraded": False,
                    "broker_touched": False,
                    "order_executed": False,
                }
            )
        )
        self.assertFalse(
            is_candidate(
                {
                    "closed": 24,
                    "profit_factor": 2.0,
                    "expectancy": 0.1,
                    "win_rate": 70,
                    "max_drawdown": 100,
                    "degraded": False,
                    "broker_touched": False,
                    "order_executed": False,
                }
            )
        )
        self.assertFalse(
            is_candidate(
                {
                    "closed": 30,
                    "profit_factor": 1.5,
                    "expectancy": 0.05,
                    "win_rate": 60,
                    "max_drawdown": 100,
                    "degraded": True,
                    "broker_touched": False,
                    "order_executed": False,
                }
            )
        )

    def test_run_sweep_orders_candidates_and_degraded_last_without_live_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for timeframe in ("M15", "M30"):
                (root / f"BTCUSD_{timeframe}_5000.csv").write_text(
                    "time,open,high,low,close,volume\n"
                    "2026-01-01 00:00:00,100,101,99,100,1\n"
                    "2026-01-01 00:30:00,100,102,99,101,1\n",
                    encoding="utf-8",
                )

            result = run_sweep(
                csv_dir=root,
                timeframes=["M15", "M30"],
                profiles=["baseline", "trend_v1", "anti_chop_v1"],
                runner_factory=_FakeForwardReplay,
            )

            self.assertTrue(result["ok"])
            self.assertFalse(result["broker_touched"])
            self.assertFalse(result["order_executed"])
            self.assertEqual(result["results"][0]["timeframe"], "M30")
            self.assertEqual(result["results"][0]["profile"], "trend_v1")
            self.assertTrue(result["results"][0]["candidate"])
            self.assertTrue(result["results"][-1]["degraded"])
            self.assertEqual(result["results"][-1]["degradation_reason"], "early_forward_underperformance")

    def test_write_outputs_creates_csv_and_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = {
                "ok": True,
                "results": [
                    {
                        "timeframe": "M30",
                        "profile": "trend_v1",
                        "closed": 32,
                        "wins": 18,
                        "losses": 14,
                        "win_rate": 56.25,
                        "profit_factor": 1.34,
                        "expectancy": 0.041,
                        "max_drawdown": 2300,
                        "degraded": False,
                        "degradation_reason": "",
                        "score": 100,
                        "candidate": True,
                        "bars_loaded": 120,
                        "broker_touched": False,
                        "order_executed": False,
                    }
                ],
            }

            csv_path, json_path = write_outputs(result, tmp)

            self.assertTrue(csv_path.exists())
            self.assertTrue(json_path.exists())
            self.assertIn("trend_v1", csv_path.read_text(encoding="utf-8"))
            self.assertIn("broker_touched", json_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()

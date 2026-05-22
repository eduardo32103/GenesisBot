from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from services.mt5.mt5_eth_m30_paper_forward_analytics import (
    analyze_eth_m30_paper_forward_snapshots,
    load_eth_m30_monitor_snapshots,
    run_eth_m30_paper_forward_analytics,
)


class MT5EthM30PaperForwardAnalyticsTests(unittest.TestCase):
    def test_analyzes_score_components_and_safety(self) -> None:
        result = analyze_eth_m30_paper_forward_snapshots(
            [
                _snapshot(score=54.0, trend=66.0, momentum=47.0, volatility=46.0),
                _snapshot(score=56.0, trend=64.0, momentum=49.0, volatility=45.0),
                _snapshot(score=57.0, trend=62.0, momentum=49.5, volatility=44.0),
            ]
        )

        self.assertEqual(result["samples_total"], 3)
        self.assertEqual(result["runtime_snapshot_complete_pct"], 100.0)
        self.assertEqual(result["bar_context_pct"], 100.0)
        self.assertEqual(result["active_true_count"], 3)
        self.assertEqual(result["applies_to_paper_shadow_count"], 3)
        self.assertEqual(result["decision_counts"]["NO_TRADE"], 3)
        self.assertEqual(result["top_decision_reasons"]["profile_conditions_not_met:score_too_low"], 3)
        self.assertEqual(result["near_threshold_counts"]["score"]["within_5_below"], 3)
        self.assertEqual(result["near_threshold_counts"]["momentum_score"]["below"], 3)
        self.assertEqual(result["near_miss_counts"]["within_1"], 1)
        self.assertEqual(result["near_miss_counts"]["within_2"], 2)
        self.assertEqual(result["near_miss_counts"]["within_3"], 2)
        self.assertEqual(result["near_miss_counts"]["within_5"], 3)
        self.assertEqual(result["score_gap_distribution"]["count"], 3)
        self.assertEqual(result["score_component_bottleneck"]["dominant_component"], "momentum_score")
        self.assertEqual(result["bottleneck_component_ranking"][0]["component"], "momentum_score")
        self.assertEqual(result["top_near_miss_timestamps"][0]["score"], 57.0)
        self.assertIn("investigate_score_components", result["recommendation_actions"])
        self.assertIn("investigate_momentum_component", result["recommendation_actions"])
        self.assertIn("do_not_relax_thresholds_yet", result["recommendation_actions"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_counts_risk_blocks_and_context_distribution(self) -> None:
        blocked = _snapshot(reason="risk_governor_block:spread_too_high", risk_allowed=False)
        tick_only = _snapshot(context="tick_only", complete=False, active=False, applies=False)

        result = analyze_eth_m30_paper_forward_snapshots([blocked, tick_only])

        self.assertEqual(result["samples_total"], 2)
        self.assertEqual(result["runtime_snapshot_complete_count"], 1)
        self.assertEqual(result["bar_context_count"], 1)
        self.assertEqual(result["risk_governor_block_count"], 1)
        self.assertEqual(result["session_distribution"]["london_us"], 2)
        self.assertEqual(result["regime_distribution"]["trend"], 2)
        self.assertIn("review_risk_governor_blocks_without_relaxing", result["recommendation_actions"])

    def test_json_loader_and_outputs_write_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            json_path = root / "monitor.json"
            json_path.write_text(json.dumps({"snapshots": [_snapshot()]}), encoding="utf-8")

            snapshots = load_eth_m30_monitor_snapshots(csv_path=root / "missing.csv", json_path=json_path)
            result = run_eth_m30_paper_forward_analytics(csv_path=root / "missing.csv", json_path=json_path, output_dir=root)

            self.assertEqual(len(snapshots), 1)
            self.assertTrue(Path(result["output_paths"]["summary"]).exists())
            self.assertTrue(Path(result["output_paths"]["json"]).exists())
            self.assertIn("ETHUSD M30 Paper-Forward Analytics", Path(result["output_paths"]["summary"]).read_text(encoding="utf-8"))
            self.assertFalse(result["automatic_promotion"])
            self.assertFalse(result["promoted_profile_mutated"])
            self.assertFalse(result["forward_state_mutated"])

    def test_csv_loader_fallback_counts_decisions_without_raw(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "monitor.csv"
            csv_path.write_text(
                "timestamp,symbol,timeframe,active,decision,decision_reason,broker_touched,order_executed,order_policy\n"
                "2026-05-22T00:00:00+00:00,ETHUSD,M30,True,NO_TRADE,no_runtime_snapshot_for_requested_timeframe,False,False,journal_only_no_broker\n",
                encoding="utf-8",
            )

            snapshots = load_eth_m30_monitor_snapshots(csv_path=csv_path, json_path=Path(tmp) / "missing.json")
            result = analyze_eth_m30_paper_forward_snapshots(snapshots)

            self.assertEqual(result["samples_total"], 1)
            self.assertEqual(result["decision_counts"]["NO_TRADE"], 1)
            self.assertEqual(result["top_decision_reasons"]["no_runtime_snapshot_for_requested_timeframe"], 1)
            self.assertEqual(result["runtime_snapshot_complete_pct"], 0.0)

    def test_shadow_trade_stats_are_paper_only(self) -> None:
        result = analyze_eth_m30_paper_forward_snapshots(
            [_snapshot()],
            shadow_trades=[
                {"status": "closed", "pnl": 12.5, "broker_touched": False, "order_executed": False},
                {"status": "open", "pnl": 0.0, "broker_touched": False, "order_executed": False},
            ],
        )

        self.assertEqual(result["closed_shadow_trades"], 1)
        self.assertEqual(result["paper_pnl"], 12.5)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])


def _snapshot(
    *,
    score: float = 54.0,
    trend: float = 66.0,
    momentum: float = 47.0,
    volatility: float = 46.0,
    reason: str = "profile_conditions_not_met:score_too_low",
    context: str = "bar_context",
    complete: bool = True,
    active: bool = True,
    applies: bool = True,
    risk_allowed: bool = True,
) -> dict:
    return {
        "timestamp": "2026-05-22T00:00:00+00:00",
        "symbol": "ETHUSD",
        "timeframe": "M30",
        "active": active,
        "applies_to_paper_shadow": applies,
        "risk_governor_allowed": risk_allowed,
        "risk_governor_reason": "risk_governor_pass" if risk_allowed else "spread_too_high",
        "decision": "NO_TRADE",
        "decision_reason": reason,
        "open_shadow_count": 0,
        "runtime_snapshot_complete": complete,
        "runtime_snapshot_context": context,
        "bars_count": 100 if complete else 0,
        "tick_merged_into_bar_context": complete,
        "strategy_score": score,
        "min_score": 58.0,
        "score_gap_to_threshold": round(score - 58.0, 4),
        "trend_score": trend,
        "momentum_score": momentum,
        "volatility_score": volatility,
        "market_regime": "trend",
        "session": "london_us",
        "spread": 1.7,
        "failed_components": "score_below_threshold,momentum_below_threshold" if momentum < 50 else "score_below_threshold",
        "component_thresholds": '{"momentum_score": 50.0, "score": 58.0, "trend_score": 50.0, "volatility_score": 35.0}',
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "raw": {
            "decision": {
                "decision": "NO_TRADE",
                "reason": reason,
                "runtime_snapshot_complete": complete,
                "runtime_snapshot_context": context,
                "risk_governor_allowed": risk_allowed,
                "risk_governor_reason": "risk_governor_pass" if risk_allowed else "spread_too_high",
                "strategy_score": score,
                "min_score": 58.0,
                "score_gap_to_threshold": round(score - 58.0, 4),
                "trend_score": trend,
                "momentum_score": momentum,
                "volatility_score": volatility,
                "failed_components": ["score_below_threshold", "momentum_below_threshold"] if momentum < 50 else ["score_below_threshold"],
                "component_thresholds": {
                    "score": 58.0,
                    "momentum_score": 50.0,
                    "trend_score": 50.0,
                    "volatility_score": 35.0,
                },
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
                "last_tick": {
                    "score": score,
                    "trend_score": trend,
                    "momentum_score": momentum,
                    "volatility_score": volatility,
                    "runtime_snapshot_complete": complete,
                    "runtime_snapshot_context": context,
                    "market_regime": "trend",
                    "session": "london_us",
                    "spread": 1.7,
                    "last": 2124.0,
                },
            },
            "forward_profile_state": {
                "active": active,
                "applies_to_paper_shadow": applies,
                "runtime_snapshot_complete": complete,
                "runtime_snapshot_context": context,
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            },
            "shadow_trades_open": {"open_count": 0, "trades": []},
        },
    }


if __name__ == "__main__":
    unittest.main()

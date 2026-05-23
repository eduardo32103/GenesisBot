from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from services.mt5.mt5_eth_m30_paper_forward_monitor import (
    collect_eth_m30_paper_forward_snapshot,
    run_eth_m30_paper_forward_monitor,
    summarize_monitor_snapshots,
    write_eth_m30_paper_forward_monitor_outputs,
)


class MT5EthM30PaperForwardMonitorTests(unittest.TestCase):
    def test_monitor_logs_risk_decision_and_paper_safety_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = run_eth_m30_paper_forward_monitor(
                base_url="http://genesis.local",
                iterations=2,
                interval_sec=0,
                output_dir=tmp,
                fetcher=_fake_fetcher(),
                sleep_fn=lambda _: None,
            )

            self.assertTrue(result["ok"])
            self.assertFalse(result["broker_touched"])
            self.assertFalse(result["order_executed"])
            self.assertEqual(result["order_policy"], "journal_only_no_broker")
            self.assertFalse(result["automatic_promotion"])
            self.assertFalse(result["promoted_profile_mutated"])
            self.assertFalse(result["forward_state_mutated"])
            self.assertEqual(result["summary"]["samples"], 2)
            self.assertEqual(result["summary"]["active_true_count"], 2)
            self.assertEqual(result["summary"]["decision_counts"]["NO_TRADE"], 2)
            self.assertEqual(result["summary"]["risk_governor_block_count"], 0)
            row = result["snapshots"][0]
            for key in [
                "risk_state",
                "risk_allowed",
                "risk_governor_allowed",
                "risk_governor_reason",
                "decision",
                "decision_reason",
                "paper_forward_candidate_profile",
                "runtime_snapshot_complete",
                "runtime_snapshot_context",
                "bars_count",
                "tick_merged_into_bar_context",
                "strategy_score",
                "min_score",
                "score_gap_to_threshold",
                "trend_score",
                "min_trend_score",
                "trend_gap_to_threshold",
                "momentum_score",
                "min_momentum_score",
                "momentum_gap_to_threshold",
                "volatility_score",
                "min_volatility_score",
                "volatility_gap_to_threshold",
                "market_regime",
                "session",
                "spread",
                "failed_components",
                "component_thresholds",
                "open_shadow_count",
                "open_shadow_trade_ids",
                "blocking_shadow_trade_id",
                "risk_governor_open_trades_count",
                "risk_governor_open_trades_source",
                "shadow_open_endpoint_count",
                "source_of_open_trade_count",
                "shadow_occupancy_inconsistent",
            ]:
                self.assertIn(key, row)
            self.assertTrue(row["runtime_snapshot_complete"])
            self.assertEqual(row["runtime_snapshot_context"], "bar_context")
            self.assertEqual(row["bars_count"], 100)
            self.assertTrue(row["tick_merged_into_bar_context"])
            self.assertEqual(row["strategy_score"], 54.0)
            self.assertEqual(row["min_score"], 58.0)
            self.assertEqual(row["score_gap_to_threshold"], -4.0)
            self.assertEqual(row["min_momentum_score"], 50.0)
            self.assertEqual(row["momentum_gap_to_threshold"], -3.0)
            self.assertEqual(row["min_trend_score"], 50.0)
            self.assertEqual(row["trend_gap_to_threshold"], 16.0)
            self.assertEqual(row["min_volatility_score"], 35.0)
            self.assertEqual(row["volatility_gap_to_threshold"], 11.0)
            self.assertIn("momentum_below_threshold", row["failed_components"])
            self.assertTrue(Path(result["output_paths"]["csv"]).exists())
            self.assertTrue(Path(result["output_paths"]["json"]).exists())
            self.assertTrue(Path(result["output_paths"]["summary"]).exists())

    def test_monitor_handles_endpoint_failure_safely_and_marks_degraded(self) -> None:
        def failing_fetcher(url: str, timeout: int) -> dict:
            if "decision" in url:
                raise TimeoutError("decision timeout")
            return _fake_fetcher()(url, timeout)

        snapshot = collect_eth_m30_paper_forward_snapshot(base_url="http://genesis.local", fetcher=failing_fetcher)

        self.assertFalse(snapshot["ok"])
        self.assertTrue(snapshot["degraded"])
        self.assertEqual(snapshot["endpoint_failures"], 1)
        self.assertEqual(snapshot["degradation_reason"], "endpoint_failure")
        self.assertFalse(snapshot["broker_touched"])
        self.assertFalse(snapshot["order_executed"])
        self.assertEqual(snapshot["order_policy"], "journal_only_no_broker")

    def test_repeated_failures_summary_recommends_observation_only(self) -> None:
        snapshots = [
            {
                "decision": "",
                "decision_reason": "",
                "risk_governor_allowed": False,
                "risk_governor_reason": "endpoint_failure",
                "endpoint_failures": 5,
                "broker_touched": False,
                "order_executed": False,
                "open_shadow_count": 0,
            }
            for _ in range(2)
        ]

        summary = summarize_monitor_snapshots(snapshots)

        self.assertEqual(summary["status"], "degraded_api_failures")
        self.assertTrue(summary["should_degrade_to_observation_only"])
        self.assertEqual(summary["recommendation"], "observation_only")
        self.assertFalse(summary["broker_touched"])
        self.assertFalse(summary["order_executed"])

    def test_monitor_detects_unsafe_broker_or_order_fields(self) -> None:
        fetcher = _fake_fetcher(decision_patch={"broker_touched": True})

        snapshot = collect_eth_m30_paper_forward_snapshot(base_url="http://genesis.local", fetcher=fetcher)

        self.assertTrue(snapshot["broker_touched"])
        self.assertFalse(snapshot["order_executed"])
        self.assertTrue(snapshot["degraded"])
        self.assertEqual(snapshot["degradation_reason"], "broker_touched_detected")

    def test_monitor_logs_shadow_occupancy_source_for_max_open_blocks(self) -> None:
        fetcher = _fake_fetcher(
            decision_patch={
                "reason": "risk_governor_block:max_open_trades_reached",
                "risk_governor_allowed": False,
                "risk_governor_reason": "max_open_trades_reached",
                "blocking_shadow_trade_id": "stale-shadow-1",
                "risk_governor_open_trades_count": 1,
                "risk_governor_open_trades_source": "runtime_snapshot_open_shadow_trade",
                "risk_governor_open_trade_id": "stale-shadow-1",
                "risk_governor_open_trade_status": "closed",
            }
        )

        snapshot = collect_eth_m30_paper_forward_snapshot(base_url="http://genesis.local", fetcher=fetcher)

        self.assertEqual(snapshot["decision_reason"], "risk_governor_block:max_open_trades_reached")
        self.assertEqual(snapshot["open_shadow_count"], 0)
        self.assertEqual(snapshot["shadow_open_endpoint_count"], 0)
        self.assertEqual(snapshot["risk_governor_open_trades_count"], 1)
        self.assertEqual(snapshot["blocking_shadow_trade_id"], "stale-shadow-1")
        self.assertEqual(snapshot["source_of_open_trade_count"], "runtime_snapshot_open_shadow_trade")
        self.assertTrue(snapshot["shadow_occupancy_inconsistent"])
        self.assertFalse(snapshot["broker_touched"])
        self.assertFalse(snapshot["order_executed"])

    def test_outputs_include_human_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = {
                "summary": {
                    "samples": 1,
                    "active_true_count": 0,
                    "decision_counts": {"NO_TRADE": 1},
                    "risk_governor_block_count": 0,
                    "no_runtime_snapshot_count": 1,
                    "paper_shadow_open_attempts_observed": 0,
                    "shadow_open_count_latest": 0,
                    "endpoint_failure_count": 0,
                    "status": "healthy_observation",
                    "recommendation": "continue_paper_forward_observation",
                    "broker_touched": False,
                    "order_executed": False,
                    "order_policy": "journal_only_no_broker",
                },
                "snapshots": [
                    {
                        "timestamp": "2026-05-22T00:00:00+00:00",
                        "symbol": "ETHUSD",
                        "timeframe": "M30",
                        "forward_status": "paper_forward_candidate",
                        "forward_profile": "eth_m30_vol_breakout_chop_guard_v1",
                        "active": False,
                        "risk_state": "normal",
                        "risk_allowed": True,
                "decision": "NO_TRADE",
                "decision_reason": "no_runtime_snapshot_for_requested_timeframe",
                "paper_forward_candidate_profile": "eth_m30_vol_breakout_chop_guard_v1",
                "runtime_snapshot_complete": True,
                "runtime_snapshot_context": "bar_context",
                "bars_count": 100,
                "tick_merged_into_bar_context": True,
                "strategy_score": 54.0,
                "min_score": 58.0,
                "score_gap_to_threshold": -4.0,
                "trend_score": 66.0,
                "min_trend_score": 50.0,
                "trend_gap_to_threshold": 16.0,
                "momentum_score": 47.0,
                "min_momentum_score": 50.0,
                "momentum_gap_to_threshold": -3.0,
                "volatility_score": 46.0,
                "min_volatility_score": 35.0,
                "volatility_gap_to_threshold": 11.0,
                "market_regime": "trend",
                "session": "london_us",
                "spread": 1.7,
                "failed_components": "score_below_threshold,momentum_below_threshold",
                "component_thresholds": '{"momentum_score": 50.0, "score": 58.0, "trend_score": 50.0, "volatility_score": 35.0}',
                "open_shadow_count": 0,
                "open_shadow_trade_ids": [],
                "blocking_shadow_trade_id": "",
                "risk_governor_open_trades_count": 0,
                "risk_governor_open_trades_source": "",
                "risk_governor_open_trade_id": "",
                "risk_governor_open_trade_status": "",
                "shadow_open_endpoint_count": 0,
                "source_of_open_trade_count": "none",
                "shadow_occupancy_inconsistent": False,
                "broker_touched": False,
                "order_executed": False,
                        "order_policy": "journal_only_no_broker",
                    }
                ],
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
            paths = write_eth_m30_paper_forward_monitor_outputs(result, tmp)

            self.assertIn("ETHUSD M30 Paper-Forward Monitor", paths["summary"].read_text(encoding="utf-8"))
            self.assertIn("NO_TRADE", paths["csv"].read_text(encoding="utf-8"))


def _fake_fetcher(decision_patch: dict | None = None):
    def fetch(url: str, timeout: int) -> dict:
        if url.endswith("/health"):
            return {"ok": True, "status": "mt5_bridge_ready", "broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}
        if "forward-profile-state" in url:
            return {
                "ok": True,
                "status": "paper_forward_candidate",
                "profile": "eth_m30_vol_breakout_chop_guard_v1",
                "active": True,
                "applies_to_paper_shadow": True,
                "applies_to_real_trading": False,
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        if "risk-state" in url:
            return {
                "ok": True,
                "risk_state": "normal",
                "allowed": True,
                "reason": "risk_governor_pass",
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        if "decision" in url:
            payload = {
                "ok": True,
                "decision": "NO_TRADE",
                "reason": "fast_path_snapshot_only",
                "risk_governor_allowed": True,
                "risk_governor_reason": "risk_governor_pass",
                "paper_forward_candidate_profile": "eth_m30_vol_breakout_chop_guard_v1",
                "runtime_snapshot_complete": True,
                "runtime_snapshot_context": "bar_context",
                "bars_count": 100,
                "tick_merged_into_bar_context": True,
                "strategy_score": 54.0,
                "min_score": 58.0,
                "score_gap_to_threshold": -4.0,
                "trend_score": 66.0,
                "min_trend_score": 50.0,
                "trend_gap_to_threshold": 16.0,
                "momentum_score": 47.0,
                "min_momentum_score": 50.0,
                "momentum_gap_to_threshold": -3.0,
                "volatility_score": 46.0,
                "min_volatility_score": 35.0,
                "volatility_gap_to_threshold": 11.0,
                "failed_components": ["score_below_threshold", "momentum_below_threshold"],
                "component_thresholds": {
                    "score": 58.0,
                    "momentum_score": 50.0,
                    "trend_score": 50.0,
                    "volatility_score": 35.0,
                },
                "risk_governor_open_trades_count": 0,
                "risk_governor_open_trades_source": "",
                "risk_governor_open_trade_id": "",
                "risk_governor_open_trade_status": "",
                "last_tick": {
                    "score": 54.0,
                    "trend_score": 66.0,
                    "momentum_score": 47.0,
                    "volatility_score": 46.0,
                    "runtime_snapshot_complete": True,
                    "runtime_snapshot_context": "bar_context",
                    "bars_count": 100,
                    "tick_merged_into_bar_context": True,
                    "market_regime": "trend",
                    "session": "london_us",
                    "spread": 1.7,
                },
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
            payload.update(decision_patch or {})
            return payload
        if "shadow-trades/open" in url:
            return {
                "ok": True,
                "open_count": 0,
                "trades": [],
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        return {}

    return fetch


if __name__ == "__main__":
    unittest.main()

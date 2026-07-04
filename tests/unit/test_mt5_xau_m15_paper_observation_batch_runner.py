from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from services.mt5.mt5_xau_m15_paper_observation_batch_runner import (
    compute_xau_m15_paper_batch_stats,
    run_xau_m15_paper_observation_batch_runner,
    run_xau_m15_paper_observation_batch_step,
)


class MT5XauM15PaperObservationBatchRunnerTests(unittest.TestCase):
    def test_dry_run_does_not_open_shadow(self) -> None:
        client = _FakeClient()

        result = run_xau_m15_paper_observation_batch_runner(client=client, dry_run=True, target_trades=3, max_cycles=5, state_file=None, results_file=None)

        self.assertEqual(result["runner_state"], "idle_no_shadow")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_no_open_if_db_degraded(self) -> None:
        client = _FakeClient(db={**_db(), "db_degraded": True})

        result = _step(client)

        self.assertEqual(result["runner_state"], "stopped_by_db")
        self.assertEqual(result["stop_reason"], "db_degraded")
        self.assertEqual(client.open_calls, 0)

    def test_no_open_if_queue_depth_positive(self) -> None:
        client = _FakeClient(db={**_db(), "queue_depth": 1})

        result = _step(client)

        self.assertEqual(result["runner_state"], "stopped_by_db")
        self.assertEqual(result["stop_reason"], "queue_depth_high")
        self.assertEqual(client.open_calls, 0)

    def test_no_open_if_shadow_already_open(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_open())

        result = _step(client)

        self.assertEqual(result["runner_state"], "shadow_open_monitoring")
        self.assertEqual(result["open_shadow_count"], 1)
        self.assertEqual(client.open_calls, 0)

    def test_opens_one_shadow_when_gates_green_and_confirmed(self) -> None:
        client = _FakeClient()

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "opening_shadow")
        self.assertTrue(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 1)
        self.assertEqual(result["shadow_trade_id"], "xau-batch-opened")
        self.assertFalse(result["candidate_activated"])

    def test_recent_edge_negative_does_not_blindly_open_paper(self) -> None:
        client = _FakeClient(readiness=_recent_edge_ready(strict_passed=False))

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "waiting_for_high_quality_paper_signal")
        self.assertEqual(result["current_phase"], "adaptive_paper_cooldown")
        self.assertTrue(result["recent_edge_negative"])
        self.assertEqual(result["entry_block_type"], "adaptive_paper_cooldown")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_recent_edge_negative_can_wait_for_high_quality_paper_signal(self) -> None:
        client = _FakeClient(readiness=_recent_edge_ready(strict_passed=False, failed_strict=["trend_alignment_ok", "signal_direction"]))

        result = _step(client, dry_run=False, paper_only_confirmed=True, strict_paper_probe=True)

        self.assertEqual(result["runner_state"], "waiting_for_high_quality_paper_signal")
        self.assertIn("trend_alignment_ok", result["stop_reason"])
        self.assertIn("signal_direction", result["stop_reason"])
        self.assertEqual(result["next_action"], "wait_for_high_quality_paper_signal")
        self.assertFalse(result["entry_allowed_for_paper_test"])
        self.assertEqual(client.open_calls, 0)

    def test_legacy_readiness_recent_edge_negative_is_normalized_to_adaptive_wait(self) -> None:
        client = _FakeClient(readiness=_legacy_recent_edge_ready())

        result = _step(client, dry_run=False, paper_only_confirmed=True, strict_paper_probe=True)

        self.assertEqual(result["runner_state"], "waiting_for_high_quality_paper_signal")
        self.assertEqual(result["current_phase"], "adaptive_paper_cooldown")
        self.assertEqual(result["risk_governor_reason"], "recent_edge_negative")
        self.assertTrue(result["recent_edge_negative"])
        self.assertEqual(result["entry_block_type"], "adaptive_paper_cooldown")
        self.assertEqual(result["next_action"], "wait_for_high_quality_paper_signal")
        self.assertIn("signal_direction", result["stop_reason"])
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["paper_shadow_created"])

    def test_strict_paper_probe_opens_only_when_stricter_gates_pass(self) -> None:
        client = _FakeClient(readiness=_recent_edge_ready(strict_passed=True))

        result = _step(client, dry_run=False, paper_only_confirmed=True, strict_paper_probe=True)

        self.assertEqual(result["runner_state"], "opening_shadow")
        self.assertEqual(result["current_phase"], "strict_paper_probe")
        self.assertTrue(result["entry_allowed_for_paper_test"])
        self.assertTrue(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 1)
        self.assertTrue(client.last_strict_paper_probe)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_no_trade_signal_under_recent_edge_does_not_open(self) -> None:
        client = _FakeClient(readiness=_recent_edge_ready(strict_passed=False, failed_strict=["signal_direction"]))

        result = _step(client, dry_run=False, paper_only_confirmed=True, strict_paper_probe=True)

        self.assertEqual(result["runner_state"], "waiting_for_high_quality_paper_signal")
        self.assertIn("signal_direction", result["stop_reason"])
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 0)

    def test_open_persistence_failure_does_not_claim_shadow_created(self) -> None:
        client = _FakeClient(
            open_result={
                "ok": False,
                "paper_shadow_created": False,
                "shadow_trade_id": "xau-open-failed",
                "open_persistence_failed": True,
                "open_write_retained_critical": True,
                "reason": "open_persistence_failed",
                **_safety(),
            }
        )

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "stopped_by_open_persistence_failed")
        self.assertFalse(result["paper_shadow_created"])
        self.assertTrue(result["open_persistence_failed"])
        self.assertTrue(result["open_write_retained_critical"])
        self.assertEqual(result["stop_reason"], "open_persistence_failed")
        self.assertEqual(result["next_action"], "drain_queue_or_backfill_runtime_open_shadow")
        self.assertEqual(client.open_calls, 1)

    def test_does_not_close_entry_block_only(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_open(category="entry_block_only", should_watch=True, risk_block_type="entry_block"))

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "watch_only")
        self.assertTrue(result["should_watch_only"])
        self.assertFalse(result["paper_close_applied"])
        self.assertEqual(client.close_calls, 0)

    def test_does_not_close_caution_watch(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_open(category="caution_watch", should_watch=True))

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "watch_only")
        self.assertFalse(result["paper_close_applied"])
        self.assertEqual(client.close_calls, 0)

    def test_closes_take_profit_when_should_close_true(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_close("take_profit_hit", pnl=12.0, r=1.2))

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "close_applied")
        self.assertTrue(result["paper_close_applied"])
        self.assertEqual(client.close_calls, 1)
        self.assertEqual(result["closed_trade"]["exit_reason"], "take_profit_hit")

    def test_closes_stop_loss_when_should_close_true(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_close("stop_loss_hit", pnl=-10.0, r=-1.0))

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "close_applied")
        self.assertEqual(result["closed_trade"]["exit_reason"], "stop_loss_hit")
        self.assertEqual(client.close_calls, 1)

    def test_closes_trailing_exit_when_should_close_true(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_close("trailing_defensive_exit", pnl=4.0, r=0.4))

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "close_applied")
        self.assertEqual(result["closed_trade"]["exit_reason"], "trailing_defensive_exit")
        self.assertEqual(client.close_calls, 1)

    def test_closes_critical_safety_exit_when_should_close_true(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_close("critical_safety_exit", pnl=-2.0, r=-0.2, category="critical_safety_exit"))

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "close_applied")
        self.assertEqual(result["closed_trade"]["safety_exit_category"], "critical_safety_exit")
        self.assertEqual(client.close_calls, 1)

    def test_stops_on_multiple_open_shadows(self) -> None:
        client = _FakeClient(open_count=2, monitor={**_monitor_open(), "open_shadow_count": 2})

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "stopped_by_duplicate_shadow")
        self.assertEqual(result["stop_reason"], "multiple_open_shadows")
        self.assertEqual(client.open_calls, 0)

    def test_restart_safe_resume_open_shadow(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_open())
        state = {"current_open_shadow_id": "existing-shadow"}

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=[],
            cycle_number=1,
            target_trades=20,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["runner_state"], "shadow_open_monitoring")
        self.assertEqual(client.open_calls, 0)

    def test_orphan_does_not_invent_pnl(self) -> None:
        client = _FakeClient(open_count=0, monitor=_monitor_none())
        state = {"current_open_shadow_id": "lost-shadow"}

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=[],
            cycle_number=1,
            target_trades=20,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["anomaly"], "opened_shadow_missing_close_record")
        self.assertEqual(result["anomaly_type"], "opened_shadow_missing_close_record")
        self.assertEqual(result["closed_trade"], {})
        self.assertEqual(client.open_calls, 0)

    def test_broker_touched_true_blocks(self) -> None:
        client = _FakeClient(readiness={**_ready(), "broker_touched": True})

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "stopped_by_safety")
        self.assertEqual(client.open_calls, 0)

    def test_order_executed_true_blocks(self) -> None:
        client = _FakeClient(monitor={**_monitor_none(), "order_executed": True})

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "stopped_by_safety")
        self.assertEqual(client.open_calls, 0)

    def test_no_forbidden_execution_reference_added(self) -> None:
        forbidden = "order" + "_send"
        service = Path("services/mt5/mt5_xau_m15_paper_observation_batch_runner.py").read_text(encoding="utf-8")
        script = Path("scripts/run_xau_m15_paper_observation_batch_runner.py").read_text(encoding="utf-8")

        self.assertNotIn(forbidden, service)
        self.assertNotIn(forbidden, script)

    def test_stats_win_rate_profit_factor_expectancy(self) -> None:
        stats = compute_xau_m15_paper_batch_stats(
            [
                {"shadow_trade_id": "a", "pnl": 12.0, "r_multiple": 1.2, "exit_reason": "take_profit_hit", "age_minutes": 15},
                {"shadow_trade_id": "b", "pnl": -4.0, "r_multiple": -0.4, "exit_reason": "stop_loss_hit", "age_minutes": 30},
            ]
        )

        self.assertEqual(stats["trades_closed"], 2)
        self.assertEqual(stats["wins"], 1)
        self.assertEqual(stats["losses"], 1)
        self.assertEqual(stats["win_rate"], 50.0)
        self.assertEqual(stats["profit_factor"], 3.0)
        self.assertEqual(stats["expectancy"], 4.0)
        self.assertEqual(stats["avg_r"], 0.4)

    def test_max_cycles_limit_is_respected(self) -> None:
        client = _FakeClient()

        result = run_xau_m15_paper_observation_batch_runner(
            client=client,
            dry_run=False,
            paper_only_confirmed=True,
            max_cycles=2,
            target_trades=20,
            interval_seconds=0,
            state_file=None,
            results_file=None,
            sleep_fn=lambda _: None,
        )

        self.assertEqual(result["cycles_completed"], 2)
        self.assertLessEqual(result["cycles_completed"], 2)

    def test_wait_for_signal_dry_run_can_poll_multiple_cycles_without_opening(self) -> None:
        client = _FakeClient(readiness=_legacy_recent_edge_ready())

        result = run_xau_m15_paper_observation_batch_runner(
            client=client,
            dry_run=True,
            wait_for_signal=True,
            strict_paper_probe=True,
            max_cycles=2,
            target_trades=20,
            interval_seconds=0,
            state_file=None,
            results_file=None,
            sleep_fn=lambda _: None,
        )

        self.assertEqual(result["runner_state"], "waiting_for_high_quality_paper_signal")
        self.assertEqual(result["cycles_completed"], 2)
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["paper_shadow_created"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_target_trades_limit_stops(self) -> None:
        client = _FakeClient()

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state={},
            trades=[{"shadow_trade_id": "done", "pnl": 1.0, "r_multiple": 0.1, "exit_reason": "take_profit_hit"}],
            cycle_number=1,
            target_trades=1,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["runner_state"], "stopped_by_target_trades")
        self.assertEqual(client.open_calls, 0)

    def test_session_target_not_satisfied_by_old_history(self) -> None:
        client = _FakeClient()
        state = {"session_id": "session-new", "session_started_at": "2026-07-02T00:00:00+00:00"}
        old_history = [{"shadow_trade_id": "old", "pnl": 10.0, "r_multiple": 1.0, "exit_reason": "take_profit_hit", "session_id": "old-session"}]

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=old_history,
            cycle_number=1,
            target_trades=1,
            dry_run=True,
            paper_only_confirmed=False,
        )

        self.assertEqual(result["runner_state"], "idle_no_shadow")
        self.assertEqual(result["batch_stats"]["session_trades_closed"], 0)
        self.assertEqual(result["batch_stats"]["historical_closed_count"], 1)
        self.assertEqual(client.open_calls, 0)

    def test_target_not_reached_while_current_shadow_open(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_open())
        state = {
            "session_id": "session-live",
            "session_started_at": "2026-07-02T00:00:00+00:00",
            "current_open_shadow_id": "existing-shadow",
            "session_shadow_trade_ids": ["existing-shadow", "closed-one"],
        }
        trades = [{"shadow_trade_id": "closed-one", "pnl": 1.0, "r_multiple": 0.1, "exit_reason": "take_profit_hit", "session_id": "session-live"}]

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=trades,
            cycle_number=1,
            target_trades=1,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["runner_state"], "shadow_open_monitoring")
        self.assertEqual(result["open_shadow_count"], 1)
        self.assertEqual(client.open_calls, 0)

    def test_runtime_and_persistent_same_shadow_merges_to_one(self) -> None:
        client = _FakeClient(
            open_payload={
                "ok": True,
                "open_count": 2,
                "runtime_open_count": 1,
                "persistent_open_count": 1,
                "merged_open_count": 1,
                "duplicate_detected": True,
                "open_source": "merged",
                "trades": [{"shadow_trade_id": "same-shadow", "symbol": "XAUUSD", "timeframe": "M15"}],
                **_safety(),
            },
            monitor={**_monitor_open(), "shadow_trade_id": "same-shadow", "open_shadow_count": 1, "shadow_source": "merged"},
        )

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "shadow_open_monitoring")
        self.assertEqual(result["open_shadow_count"], 1)
        self.assertEqual(result["current_shadow_source"], "merged")
        self.assertEqual(client.open_calls, 0)

    def test_stats_are_session_scoped_and_side_stats_are_correct(self) -> None:
        stats = compute_xau_m15_paper_batch_stats(
            [
                {"shadow_trade_id": "buy-win", "side": "buy", "pnl": 4.0, "r_multiple": 0.4, "exit_reason": "take_profit_hit", "session_id": "session-a"},
                {"shadow_trade_id": "sell-loss", "side": "sell", "pnl": -2.0, "r_multiple": -0.2, "exit_reason": "stop_loss_hit", "session_id": "session-a"},
                {"shadow_trade_id": "old-win", "side": "buy", "pnl": 100.0, "r_multiple": 10.0, "exit_reason": "take_profit_hit", "session_id": "old-session"},
            ],
            state={"session_id": "session-a", "session_started_at": "2026-07-02T00:00:00+00:00"},
        )

        self.assertEqual(stats["session_trades_closed"], 2)
        self.assertEqual(stats["historical_closed_count"], 1)
        self.assertEqual(stats["wins"], 1)
        self.assertEqual(stats["losses"], 1)
        self.assertEqual(stats["win_rate"], 50.0)
        self.assertEqual(stats["side_stats"]["buy_count"], 1)
        self.assertEqual(stats["side_stats"]["sell_count"], 1)
        self.assertEqual(stats["side_stats"]["buy_win_rate"], 100.0)
        self.assertEqual(stats["side_stats"]["sell_win_rate"], 0.0)

    def test_pending_state_live_open_same_id_monitors_existing(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_open())
        state = {"current_open_shadow_id": "existing-shadow", "trades_opened": 1}

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=[],
            cycle_number=1,
            target_trades=20,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["runner_state"], "shadow_open_monitoring")
        self.assertEqual(client.open_calls, 0)

    def test_pending_state_open_count_zero_history_closed_imports_result(self) -> None:
        closed = _history_closed("pending-shadow", pnl=8.0, r=0.8)
        client = _FakeClient(history=[closed])
        state = {"current_open_shadow_id": "pending-shadow", "last_shadow_trade_id": "pending-shadow", "trades_opened": 1}

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=[],
            cycle_number=1,
            target_trades=20,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["runner_state"], "reconciled_closed_shadow")
        self.assertEqual(result["reconciled_shadow_trade_id"], "pending-shadow")
        self.assertEqual(result["closed_trade"]["pnl"], 8.0)
        self.assertEqual(result["batch_stats"]["trades_closed"], 1)
        self.assertEqual(result["batch_stats"]["wins"], 1)
        self.assertEqual(client.open_calls, 0)

    def test_pending_state_open_count_zero_history_missing_stops_orphan(self) -> None:
        client = _FakeClient(history=[])
        state = {"current_open_shadow_id": "missing-shadow", "last_shadow_trade_id": "missing-shadow", "trades_opened": 1}

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=[],
            cycle_number=1,
            target_trades=20,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["runner_state"], "stopped_by_orphaned_shadow_missing_close_record")
        self.assertEqual(result["anomaly_type"], "opened_shadow_missing_close_record")
        self.assertEqual(result["orphan_shadow_trade_id"], "missing-shadow")
        self.assertEqual(result["closed_trade"], {})
        self.assertEqual(client.open_calls, 0)

    def test_pending_state_open_count_zero_history_unavailable_stops_without_inventing_pnl(self) -> None:
        client = _FakeClient(
            history_payload={
                "ok": False,
                "history_available": False,
                "reason": "history_schema_optional_column_missing",
                "closed_trades": [],
                "trades": [],
                **_safety(),
            }
        )
        state = {"current_open_shadow_id": "pending-shadow", "last_shadow_trade_id": "pending-shadow", "trades_opened": 1}

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=[],
            cycle_number=1,
            target_trades=20,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["runner_state"], "stopped_by_history_unavailable")
        self.assertEqual(result["stop_reason"], "history_schema_optional_column_missing")
        self.assertEqual(result["next_action"], "fix_history_before_next_open")
        self.assertEqual(result["closed_trade"], {})
        self.assertEqual(client.open_calls, 0)

    def test_no_new_shadow_opened_while_pending_reconciliation_exists(self) -> None:
        client = _FakeClient(history=[])
        state = {"pending_reconciliation_shadow_id": "pending-shadow", "trades_opened": 1}

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=[],
            cycle_number=1,
            target_trades=20,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["runner_state"], "stopped_by_orphaned_shadow_missing_close_record")
        self.assertEqual(client.open_calls, 0)

    def test_fast_observation_closes_timebox_exit(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_close("paper_timebox_exit", pnl=0.2, r=0.02, bars_since_entry=2))

        result = _step(client, dry_run=False, paper_only_confirmed=True, exit_policy="fast_observation", time_stop_bars=2)

        self.assertEqual(result["runner_state"], "close_applied")
        self.assertEqual(result["closed_trade"]["exit_reason"], "paper_timebox_exit")
        self.assertEqual(client.close_calls, 1)

    def test_fast_observation_closes_fast_trailing_exit(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_close("paper_fast_trailing_exit", pnl=2.0, r=0.2))

        result = _step(client, dry_run=False, paper_only_confirmed=True, exit_policy="fast_observation")

        self.assertEqual(result["runner_state"], "close_applied")
        self.assertEqual(result["closed_trade"]["exit_reason"], "paper_fast_trailing_exit")
        self.assertEqual(client.close_calls, 1)

    def test_history_endpoint_payload_is_read_only(self) -> None:
        client = _FakeClient(history=[_history_closed("closed-shadow", pnl=2.0, r=0.2)])

        history = client.shadow_trade_history()

        self.assertTrue(history["ok"])
        self.assertTrue(history["status_endpoints_write_free"])
        self.assertEqual(history["closed_count"], 1)
        self.assertFalse(history["broker_touched"])
        self.assertFalse(history["order_executed"])
        self.assertEqual(history["order_policy"], "journal_only_no_broker")


def _step(
    client: "_FakeClient",
    *,
    dry_run: bool = False,
    paper_only_confirmed: bool = False,
    exit_policy: str = "default",
    time_stop_bars: int = 2,
    strict_paper_probe: bool = False,
) -> dict[str, object]:
    return run_xau_m15_paper_observation_batch_step(
        client=client,
        state={},
        trades=[],
        cycle_number=1,
        target_trades=20,
        dry_run=dry_run,
        paper_only_confirmed=paper_only_confirmed,
        exit_policy=exit_policy,
        time_stop_bars=time_stop_bars,
        strict_paper_probe=strict_paper_probe,
    )


class _FakeClient:
    source = "fake"

    def __init__(
        self,
        *,
        db: dict[str, object] | None = None,
        readiness: dict[str, object] | None = None,
        open_count: int = 0,
        monitor: dict[str, object] | None = None,
        history: list[dict[str, object]] | None = None,
        history_payload: dict[str, object] | None = None,
        open_result: dict[str, object] | None = None,
        open_payload: dict[str, object] | None = None,
    ) -> None:
        self.db = db or _db()
        self.ready = readiness or _ready()
        self.open_count = open_count
        self.monitor_payload = monitor or _monitor_none()
        self.history_rows = [dict(row) for row in (history or [])]
        self.history_payload = dict(history_payload) if history_payload is not None else None
        self.open_result = dict(open_result) if open_result is not None else None
        self.open_payload = dict(open_payload) if open_payload is not None else None
        self.open_calls = 0
        self.close_calls = 0
        self.last_strict_paper_probe = False

    def persistent_status(self) -> dict[str, object]:
        return dict(self.db)

    def open_shadow_trades(self) -> dict[str, object]:
        if self.open_payload is not None:
            return dict(self.open_payload)
        trades = [{"shadow_trade_id": "existing-shadow", "symbol": "XAUUSD", "timeframe": "M15"}] if self.open_count else []
        if self.open_count > 1:
            trades.append({"shadow_trade_id": "existing-shadow-2", "symbol": "XAUUSD", "timeframe": "M15"})
        return {"ok": True, "open_count": self.open_count, "trades": trades, **_safety()}

    def shadow_trade_history(self) -> dict[str, object]:
        if self.history_payload is not None:
            return dict(self.history_payload)
        open_rows = [row for row in self.history_rows if row.get("status") == "open"]
        closed_rows = [row for row in self.history_rows if row.get("status") == "closed" or row.get("closed_at")]
        return {
            "ok": True,
            "status": "persistent_intelligence_shadow_trade_history_ready",
            "status_endpoints_write_free": True,
            "trades": [dict(row) for row in self.history_rows],
            "open_trades": [dict(row) for row in open_rows],
            "closed_trades": [dict(row) for row in closed_rows],
            "open_count": len(open_rows),
            "closed_count": len(closed_rows),
            **_safety(),
        }

    def readiness(self) -> dict[str, object]:
        return dict(self.ready)

    def monitor(
        self,
        *,
        apply_paper_close: bool = False,
        exit_policy: str = "default",
        time_stop_bars: int = 2,
        max_hold_minutes: float | None = None,
        min_r_to_arm_trailing: float = 0.25,
        giveback_r: float = 0.15,
        fast_loss_cut_r: float = -0.25,
    ) -> dict[str, object]:
        del exit_policy, time_stop_bars, max_hold_minutes, min_r_to_arm_trailing, giveback_r, fast_loss_cut_r
        if apply_paper_close:
            self.close_calls += 1
            self.open_count = 0
            return {**self.monitor_payload, "paper_close_applied": True, "shadow_status_after": "closed", **_safety()}
        return dict(self.monitor_payload)

    def open_shadow_once(self, *, strict_paper_probe: bool = False) -> dict[str, object]:
        self.last_strict_paper_probe = bool(strict_paper_probe)
        self.open_calls += 1
        if self.open_result is not None:
            return dict(self.open_result)
        self.open_count = 1
        return {
            "ok": True,
            "paper_shadow_created": True,
            "shadow_trade_id": "xau-batch-opened",
            "open_shadow_count_after": 1,
            "candidate_activated": False,
            "paper_forward_onboarding_started": False,
            **_safety(),
        }


def _db() -> dict[str, object]:
    return {
        "provider": "railway_postgres",
        "db_available": True,
        "db_degraded": False,
        "tables_ready": True,
        "queue_depth": 0,
        "queued_writes": 0,
        "failed_writes": 0,
        "recommendation": "persistent_intelligence_ready",
        **_safety(),
    }


def _ready() -> dict[str, object]:
    return {
        "ok": True,
        "candidate_found": True,
        "candidate_status": "paper_observation_review",
        "readiness_state": "ready_for_one_cycle_paper_observation",
        "runtime_context_available": True,
        "runtime_context_recent": True,
        "bars_count": 120,
        "tick_available": True,
        "capital_allows_observation": True,
        "risk_allows_observation": True,
        "adaptive_allows_observation": True,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        **_safety(),
    }


def _recent_edge_ready(*, strict_passed: bool, failed_strict: list[str] | None = None) -> dict[str, object]:
    failed = failed_strict if failed_strict is not None else ([] if strict_passed else ["signal_direction"])
    return {
        **_ready(),
        "readiness_state": "blocked",
        "risk_allows_observation": False,
        "risk_governor_reason": "recent_edge_negative",
        "recent_edge_negative": True,
        "recommendation": "strict_paper_probe_allowed_real_trading_still_blocked" if strict_passed else "adaptive_paper_cooldown_wait_for_high_quality_paper_signal",
        "failed_gate_names": ["risk_allows_observation"],
        "failed_gate_reasons": {"risk_allows_observation": {"actual": "recent_edge_negative", "required": "risk_governor_pass"}},
        "entry_block_type": "adaptive_paper_cooldown",
        "entry_allowed_for_paper_test": bool(strict_passed),
        "gate_summary": {
            "failed_gate_names": ["risk_allows_observation"],
            "risk_governor_reason": "recent_edge_negative",
            "recent_edge_negative": True,
        },
        "strict_paper_probe": {
            "mode": "strict_paper_probe",
            "strict_paper_probe_passed": bool(strict_passed),
            "failed_strict_gate_names": failed,
            "trend_alignment_ok": "trend_alignment_ok" not in failed,
            "spread_ok": "spread_ok" not in failed,
            "volatility_ok": "volatility_ok" not in failed,
            "no_duplicate_shadow": "no_duplicate_shadow" not in failed,
            "signal_direction": "buy" if "signal_direction" not in failed else "",
            "db_healthy_and_queue_empty": True,
            "runtime_context_recent": True,
            **_safety(),
        },
    }


def _legacy_recent_edge_ready() -> dict[str, object]:
    return {
        **_ready(),
        "readiness_state": "blocked",
        "risk_allows_observation": False,
        "risk_state": "defensive",
        "recommendation": "resolve_observation_safety_gates",
        "failed_gates": ["risk_allows_observation"],
        "gates": {
            "risk_allows_observation": {
                "actual": "recent_edge_negative",
                "passed": False,
                "required": "risk_governor_pass",
            }
        },
    }


def _monitor_none() -> dict[str, object]:
    return {
        "ok": True,
        "monitor_state": "no_action",
        "open_shadow_count": 0,
        "shadow_trade_id": "",
        "exit_signal": False,
        "exit_reason": "no_open_shadow",
        "should_close_paper": False,
        "should_watch_only": False,
        "paper_close_applied": False,
        **_safety(),
    }


def _monitor_open(*, category: str = "none", should_watch: bool = False, risk_block_type: str = "none") -> dict[str, object]:
    return {
        "ok": True,
        "monitor_state": "open_monitoring",
        "open_shadow_count": 1,
        "shadow_trade_id": "existing-shadow",
        "exit_signal": False,
        "exit_reason": "caution_watch" if should_watch else "",
        "should_close_paper": False,
        "should_watch_only": should_watch,
        "safety_exit_category": category,
        "risk_block_type": risk_block_type,
        "paper_close_applied": False,
        **_safety(),
    }


def _monitor_close(exit_reason: str, *, pnl: float, r: float, category: str = "none", bars_since_entry: int = 2) -> dict[str, object]:
    return {
        "ok": True,
        "monitor_state": "exit_pending",
        "open_shadow_count": 1,
        "shadow_trade_id": "existing-shadow",
        "side": "buy",
        "entry_price": 100.0,
        "current_price": 100.0 + pnl,
        "stop_loss": 95.0,
        "take_profit": 110.0,
        "unrealized_pnl": pnl,
        "unrealized_pnl_pct": pnl,
        "r_multiple": r,
        "age_minutes": 20,
        "bars_since_entry": bars_since_entry,
        "exit_signal": True,
        "exit_reason": exit_reason,
        "should_close_paper": True,
        "should_watch_only": False,
        "safety_exit_category": category,
        "safety_exit_reason_detail": "unit_test",
        "close_decision_reason": f"close_paper:{exit_reason}",
        "paper_close_applied": False,
        **_safety(),
    }


def _history_closed(shadow_trade_id: str, *, pnl: float, r: float) -> dict[str, object]:
    return {
        "shadow_trade_id": shadow_trade_id,
        "symbol": "XAUUSD",
        "broker_symbol": "XAUUSD.b",
        "timeframe": "M15",
        "strategy_profile": "volatility_compression_breakout|mode=nr7_trailing_defensive",
        "side": "buy",
        "entry_price": 100.0,
        "exit_price": 100.0 + pnl,
        "pnl": pnl,
        "pnl_pct": pnl,
        "r_multiple": r,
        "status": "closed",
        "opened_at": "2026-06-16T09:00:00+00:00",
        "closed_at": "2026-06-16T09:30:00+00:00",
        "exit_reason": "take_profit_hit" if pnl > 0 else "stop_loss_hit",
        **_safety(),
    }


def _safety() -> dict[str, object]:
    return {"broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}


if __name__ == "__main__":
    unittest.main()

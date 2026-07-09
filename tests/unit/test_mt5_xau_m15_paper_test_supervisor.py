from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.run_crypto_m15_paper_test_supervisor import _parse_args as _parse_crypto_args
from scripts.run_xau_m15_paper_test_supervisor import _parse_args
from services.mt5.mt5_persistent_connection_manager import persistent_write_backpressure
from services.mt5.mt5_persistent_intelligence_store import _reset_persistent_intelligence_counters_for_tests, persist_risk_event
from services.mt5.mt5_xau_m15_paper_test_supervisor import repair_orphan_state, run_xau_m15_paper_test_supervisor


class MT5XauM15PaperTestSupervisorTests(unittest.TestCase):
    def test_cli_accepts_preflight_only_flag(self) -> None:
        args = _parse_args(["--preflight-only", "--json"])

        self.assertTrue(args.preflight_only)
        self.assertTrue(args.json)

    def test_crypto_cli_accepts_explicit_m15_timeframe_only(self) -> None:
        args = _parse_crypto_args(["--symbol", "ETHUSD", "--timeframe", "M15", "--preflight-only", "--dry-run", "--json"])

        self.assertEqual(args.timeframe, "M15")
        self.assertEqual(args.symbol, "ETHUSD")
        self.assertTrue(args.preflight_only)
        self.assertTrue(args.dry_run)
        with self.assertRaises(SystemExit):
            _parse_crypto_args(["--symbol", "ETHUSD", "--timeframe", "M30", "--preflight-only"])

    def test_supervisor_dry_run_does_not_open_shadow(self) -> None:
        client = _SupervisorClient()

        result = run_xau_m15_paper_test_supervisor(client=client, dry_run=True, target_trades=3, max_cycles=5, state_file=None, results_file=None)

        self.assertEqual(result["supervisor_state"], "idle_no_shadow")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_supervisor_preflight_only_does_not_call_monitor_open_or_post_paths(self) -> None:
        client = _SupervisorClient(
            readiness={
                **_recent_edge_readiness(),
                "runtime_context_recent": False,
                "capital_state": "kill_switch",
                "capital_allows_observation": False,
                "risk_state": "defensive",
                "failed_gate_names": ["runtime_context_recent", "capital_allows_observation", "risk_allows_observation"],
            },
            open_payload={
                "ok": True,
                "open_count": 0,
                "runtime_open_count": 0,
                "persistent_open_count": 0,
                "merged_open_count": 0,
                "duplicate_detected": False,
                "open_source": "none",
                "trades": [],
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            },
            history_payload={
                "ok": True,
                "history_available": True,
                "closed_count": 16,
                "closed_trades": [],
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            },
        )

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            preflight_only=True,
            dry_run=False,
            paper_only_confirmed=True,
            state_file=None,
            results_file=None,
        )

        self.assertEqual(result["supervisor_state"], "preflight_only")
        self.assertEqual(result["decision"], "blocked_by_readiness")
        self.assertEqual(result["closed_count"], 16)
        self.assertEqual(result["open_count"], 0)
        self.assertEqual(result["merged_open_count"], 0)
        self.assertEqual(result["capital_state"], "kill_switch")
        self.assertFalse(result["capital_allows_observation"])
        self.assertEqual(result["risk_state"], "defensive")
        self.assertFalse(result["risk_allows_observation"])
        self.assertIn("runtime_context_recent", result["blockers"])
        self.assertIn("capital_allows_observation", result["blockers"])
        self.assertIn("risk_allows_observation", result["blockers"])
        self.assertEqual(result["next_safe_action"], "resolve_readiness_blockers_before_monitor_or_paper_open")
        self.assertEqual(client.monitor_calls, 0)
        self.assertEqual(client.open_calls, 0)
        self.assertEqual(client.drain_calls, 0)
        self.assertFalse(result["post_called"])
        self.assertFalse(result["monitor_called"])
        self.assertFalse(result["paper_shadow_created"])
        self.assertFalse(result["paper_close_applied"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_supervisor_preflight_only_does_not_drain_queue(self) -> None:
        client = _SupervisorClient(db_queue=2, queue_after_drain=0)

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            preflight_only=True,
            dry_run=False,
            paper_only_confirmed=True,
            state_file=None,
            results_file=None,
        )

        self.assertEqual(result["decision"], "blocked_by_db")
        self.assertIn("queue_depth_high", result["blockers"])
        self.assertEqual(result["queue_depth"], 2)
        self.assertEqual(client.drain_calls, 0)
        self.assertEqual(client.monitor_calls, 0)
        self.assertEqual(client.open_calls, 0)

    def test_crypto_preflight_dry_run_does_not_write_mt5_risk_events(self) -> None:
        _reset_persistent_intelligence_counters_for_tests()
        client = _RiskWritingSupervisorClient()

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            symbol="BTCUSD",
            broker_symbol="BTCUSD",
            timeframe="M15",
            preflight_only=True,
            dry_run=True,
            paper_only_confirmed=True,
            state_file=None,
            results_file=None,
        )
        status = persistent_write_backpressure().status()

        self.assertEqual(result["supervisor_state"], "preflight_only")
        self.assertTrue(client.risk_write["suppressed_noncritical_risk_event"])
        self.assertTrue(client.risk_write["dry_run_no_persist"])
        self.assertEqual(status["queue_depth"], 0)
        self.assertEqual(status["queued_writes"], 0)
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["paper_shadow_created"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_preflight_returns_suppressed_risk_events_in_result(self) -> None:
        _reset_persistent_intelligence_counters_for_tests()
        client = _RiskWritingSupervisorClient()

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            symbol="BTCUSD",
            broker_symbol="BTCUSD",
            timeframe="M15",
            preflight_only=True,
            dry_run=True,
            paper_only_confirmed=True,
            state_file=None,
            results_file=None,
        )

        self.assertTrue(result["dry_run_no_persist"])
        self.assertTrue(result["suppressed_noncritical_risk_events"])
        self.assertTrue(result["dry_run_risk_events"])
        self.assertEqual(result["suppressed_noncritical_risk_events"][0]["table"], "mt5_risk_events")
        self.assertEqual(result["suppressed_noncritical_risk_events"][0]["suppression_reason"], "preflight_dry_run")

    def test_no_paper_shadow_created_when_preflight_db_degraded(self) -> None:
        client = _SupervisorClient(db_degraded=True)

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            symbol="BTCUSD",
            broker_symbol="BTCUSD",
            timeframe="M15",
            preflight_only=True,
            dry_run=True,
            paper_only_confirmed=True,
            state_file=None,
            results_file=None,
        )

        self.assertEqual(result["decision"], "blocked_by_db")
        self.assertIn("db_degraded", result["blockers"])
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["paper_shadow_created"])

    def test_repeated_crypto_preflight_reuses_db_status_snapshot(self) -> None:
        client = _SnapshotAwareSupervisorClient()

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            symbol="BTCUSD",
            broker_symbol="BTCUSD",
            timeframe="M15",
            preflight_only=True,
            dry_run=False,
            paper_only_confirmed=True,
            state_file=None,
            results_file=None,
        )

        self.assertEqual(result["decision"], "preflight_ready")
        self.assertEqual(client.status_calls, 1)
        self.assertTrue(client.readiness_saw_db_snapshot)
        self.assertTrue(result["preflight"]["db_preflight_status_cache_hit"])
        self.assertEqual(client.monitor_calls, 0)
        self.assertEqual(client.open_calls, 0)

    def test_eth_symbol_preflight_uses_explicit_symbol(self) -> None:
        client = _SupervisorClient()

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            symbol="ETHUSD",
            broker_symbol="ETHUSD",
            timeframe="M15",
            preflight_only=True,
            dry_run=False,
            paper_only_confirmed=True,
            state_file=None,
            results_file=None,
        )

        self.assertEqual(result["symbol"], "ETHUSD")
        self.assertEqual(result["broker_symbol"], "ETHUSD")
        self.assertEqual(result["timeframe"], "M15")
        self.assertIn("multi_asset_paper_test|symbol=ETHUSD|timeframe=M15", result["candidate_profile"])
        self.assertIn("GET /api/genesis/mt5/shadow-trades/open?symbol=ETHUSD", result["allowed_endpoints"])
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 0)

    def test_db_degraded_still_blocks_paper_open(self) -> None:
        client = _SupervisorClient(db_degraded=True)

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            dry_run=False,
            paper_only_confirmed=True,
            once=True,
            state_file=None,
            results_file=None,
        )

        self.assertEqual(result["supervisor_state"], "stopped_by_db")
        self.assertEqual(result["stop_reason"], "db_degraded")
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["paper_shadow_created"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_no_paper_shadow_created_when_db_degraded(self) -> None:
        client = _SupervisorClient(db_degraded=True)

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            preflight_only=True,
            dry_run=False,
            paper_only_confirmed=True,
            state_file=None,
            results_file=None,
        )

        self.assertEqual(result["decision"], "blocked_by_db")
        self.assertIn("db_degraded", result["blockers"])
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["paper_shadow_created"])

    def test_supervisor_drains_queue_before_opening(self) -> None:
        client = _SupervisorClient(db_queue=2, queue_after_drain=0)

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            dry_run=False,
            paper_only_confirmed=True,
            once=True,
            target_trades=3,
            state_file=None,
            results_file=None,
        )

        self.assertEqual(client.drain_calls, 1)
        self.assertEqual(result["supervisor_state"], "opening_shadow")
        self.assertTrue(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 1)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_supervisor_does_not_open_if_queue_remains_high(self) -> None:
        client = _SupervisorClient(db_queue=2, queue_after_drain=2)

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            dry_run=False,
            paper_only_confirmed=True,
            once=True,
            target_trades=3,
            state_file=None,
            results_file=None,
        )

        self.assertEqual(result["supervisor_state"], "stopped_by_db")
        self.assertEqual(result["stop_reason"], "queue_depth_remains_after_drain")
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["paper_shadow_created"])

    def test_supervisor_does_not_open_if_failed_writes_remain_after_queue_empty(self) -> None:
        client = _SupervisorClient(failed_writes=2)

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            dry_run=False,
            paper_only_confirmed=True,
            once=True,
            target_trades=3,
            state_file=None,
            results_file=None,
        )

        self.assertEqual(result["supervisor_state"], "stopped_by_db")
        self.assertEqual(result["stop_reason"], "failed_writes_unresolved")
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["paper_shadow_created"])

    def test_preflight_blocks_failed_write_semantics_unknown_even_with_zero_active(self) -> None:
        client = _SupervisorClient(
            failed_writes=4,
            failed_writes_active=0,
            failed_writes_unresolved=0,
            failed_writes_critical=0,
            failed_write_semantics_known=False,
            dropped_noncritical_writes_total=4,
        )

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            preflight_only=True,
            dry_run=False,
            paper_only_confirmed=True,
            state_file=None,
            results_file=None,
        )

        self.assertEqual(result["decision"], "blocked_by_db")
        self.assertIn("failed_write_semantics_unknown", result["blockers"])
        self.assertEqual(client.monitor_calls, 0)
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_preflight_blocks_db_readiness_blocking_reason_failed_write_semantics_unknown(self) -> None:
        client = _SupervisorClient(
            failed_writes=4,
            failed_writes_active=0,
            failed_writes_unresolved=0,
            failed_writes_critical=0,
            failed_write_semantics_known=True,
            dropped_noncritical_writes_total=4,
            db_readiness_blocking_reason="failed_write_semantics_unknown",
        )

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            preflight_only=True,
            dry_run=False,
            paper_only_confirmed=True,
            state_file=None,
            results_file=None,
        )

        self.assertEqual(result["decision"], "blocked_by_db")
        self.assertIn("failed_write_semantics_unknown", result["blockers"])
        self.assertEqual(client.monitor_calls, 0)
        self.assertEqual(client.open_calls, 0)

    def test_preflight_allows_historical_failed_write_when_semantics_clean(self) -> None:
        client = _SupervisorClient(
            failed_writes=1,
            failed_writes_active=0,
            failed_writes_unresolved=0,
            failed_writes_critical=0,
            failed_write_semantics_known=True,
            dropped_noncritical_writes_total=0,
        )

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            preflight_only=True,
            dry_run=False,
            paper_only_confirmed=True,
            state_file=None,
            results_file=None,
        )

        self.assertEqual(result["supervisor_state"], "preflight_only")
        self.assertEqual(result["decision"], "preflight_ready")
        self.assertEqual(result["readiness_state"], "ready_for_one_cycle_paper_observation")
        self.assertEqual(result["blockers"], [])
        self.assertEqual(client.monitor_calls, 0)
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_supervisor_reports_open_persistence_failure_without_created_shadow(self) -> None:
        client = _SupervisorClient(
            open_result={
                "ok": False,
                "paper_shadow_created": False,
                "shadow_trade_id": "xau-open-failed",
                "open_persistence_failed": True,
                "open_write_retained_critical": True,
                "reason": "open_persistence_failed",
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        )

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            dry_run=False,
            paper_only_confirmed=True,
            once=True,
            target_trades=3,
            state_file=None,
            results_file=None,
        )

        self.assertEqual(result["supervisor_state"], "stopped_by_open_persistence_failed")
        self.assertFalse(result["paper_shadow_created"])
        self.assertTrue(result["open_persistence_failed"])
        self.assertTrue(result["open_write_retained_critical"])
        self.assertEqual(client.open_calls, 1)

    def test_supervisor_reports_invalid_open_response_without_opened_session_trade(self) -> None:
        client = _SupervisorClient(
            open_result={
                "ok": True,
                "paper_shadow_created": True,
                "shadow_trade_id": "",
                "open_shadow_count_after": 0,
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        )

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            dry_run=False,
            paper_only_confirmed=True,
            once=True,
            target_trades=3,
            state_file=None,
            results_file=None,
        )

        self.assertEqual(result["supervisor_state"], "stopped_by_invalid_open_response")
        self.assertEqual(result["stop_reason"], "open_response_missing_shadow_trade_id")
        self.assertEqual(result["session_trades_opened"], 0)
        self.assertEqual(result["current_shadow_id"], "")
        self.assertEqual(result["open_count"], 0)
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 1)

    def test_supervisor_exposes_recent_edge_negative_gate_summary(self) -> None:
        client = _SupervisorClient(readiness=_recent_edge_readiness())

        result = run_xau_m15_paper_test_supervisor(
            client=client,
            dry_run=False,
            paper_only_confirmed=True,
            once=True,
            strict_paper_probe=True,
            explain_gates=True,
            state_file=None,
            results_file=None,
        )

        self.assertEqual(result["supervisor_state"], "waiting_for_high_quality_paper_signal")
        self.assertEqual(result["current_phase"], "adaptive_paper_cooldown")
        self.assertEqual(result["risk_governor_reason"], "recent_edge_negative")
        self.assertTrue(result["recent_edge_negative"])
        self.assertEqual(result["entry_block_type"], "adaptive_paper_cooldown")
        self.assertIn("risk_allows_observation", result["failed_gate_names"])
        self.assertEqual(result["next_action"], "wait_for_high_quality_paper_signal")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_supervisor_stops_on_broker_safety_violation(self) -> None:
        client = _SupervisorClient(open_payload={"ok": True, "open_count": 0, "trades": [], "broker_touched": True, "order_executed": False, "order_policy": "journal_only_no_broker"})

        result = run_xau_m15_paper_test_supervisor(client=client, dry_run=False, paper_only_confirmed=True, state_file=None, results_file=None)

        self.assertEqual(result["supervisor_state"], "stopped_by_broker_safety_violation")
        self.assertEqual(client.open_calls, 0)

    def test_repair_orphan_requires_confirmation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / "state.json"
            results = Path(tmp) / "results.json"
            state.write_text('{"current_open_shadow_id":"lost","trades_opened":1}', encoding="utf-8")
            results.write_text('{"trades":[]}', encoding="utf-8")

            result = repair_orphan_state(state_file=state, results_file=results, confirm_paper_only_repair=False)

        self.assertFalse(result["repair_applied"])
        self.assertEqual(result["reason"], "confirm_paper_only_repair_required")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_repair_orphan_does_not_invent_pnl_or_winrate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp) / "state.json"
            results = Path(tmp) / "results.json"
            state.write_text('{"current_open_shadow_id":"lost","trades_opened":1}', encoding="utf-8")
            results.write_text('{"trades":[]}', encoding="utf-8")

            result = repair_orphan_state(state_file=state, results_file=results, confirm_paper_only_repair=True)
            repaired = results.read_text(encoding="utf-8")

        self.assertTrue(result["repair_applied"])
        self.assertFalse(result["pnl_invented"])
        self.assertFalse(result["winrate_changed"])
        self.assertIn('"trades_closed": 0', repaired)
        self.assertIn('"orphaned_unmeasured"', repaired)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")


class _SupervisorClient:
    source = "test_supervisor_client"

    def __init__(
        self,
        *,
        db_queue: int = 0,
        db_degraded: bool = False,
        queue_after_drain: int = 0,
        failed_writes: int = 0,
        failed_writes_active: int | None = None,
        failed_writes_unresolved: int | None = None,
        failed_writes_critical: int = 0,
        failed_write_semantics_known: bool = True,
        dropped_noncritical_writes_total: int = 0,
        db_readiness_blocking_reason: str = "",
        open_payload: dict[str, object] | None = None,
        open_result: dict[str, object] | None = None,
        history_payload: dict[str, object] | None = None,
        readiness: dict[str, object] | None = None,
    ) -> None:
        self.db_queue = db_queue
        self.db_degraded = db_degraded
        self.queue_after_drain = queue_after_drain
        self.failed_writes = failed_writes
        self.failed_writes_active = failed_writes if failed_writes_active is None and failed_writes else int(failed_writes_active or 0)
        self.failed_writes_unresolved = failed_writes if failed_writes_unresolved is None and failed_writes else int(failed_writes_unresolved or 0)
        self.failed_writes_critical = failed_writes_critical
        self.failed_write_semantics_known = failed_write_semantics_known
        self.dropped_noncritical_writes_total = dropped_noncritical_writes_total
        self.db_readiness_blocking_reason = db_readiness_blocking_reason
        self.open_payload = open_payload
        self.open_result = dict(open_result) if open_result is not None else None
        self.history_payload = dict(history_payload) if history_payload is not None else None
        self.readiness_payload = dict(readiness) if readiness is not None else None
        self.drain_calls = 0
        self.open_calls = 0
        self.monitor_calls = 0

    def persistent_status(self) -> dict[str, object]:
        return {
            "ok": True,
            "db_available": True,
            "db_degraded": self.db_degraded,
            "tables_ready": not self.db_degraded,
            "queue_depth": self.db_queue,
            "queued_writes": 0,
            "failed_writes": self.failed_writes,
            "failed_writes_total": self.failed_writes,
            "failed_writes_active": self.failed_writes_active,
            "failed_writes_unresolved": self.failed_writes_unresolved,
            "failed_writes_critical": self.failed_writes_critical,
            "failed_write_semantics_known": self.failed_write_semantics_known,
            "dropped_noncritical_writes_total": self.dropped_noncritical_writes_total,
            "last_db_error_category": "",
            "last_db_error_at": "",
            "queue_drain_succeeded": True,
            "db_readiness_blocking_reason": self.db_readiness_blocking_reason,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def queue_drain(self) -> dict[str, object]:
        self.drain_calls += 1
        self.db_queue = self.queue_after_drain
        return {
            "ok": self.db_queue == 0,
            "queue_depth_after": self.db_queue,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def open_shadow_trades(self) -> dict[str, object]:
        if self.open_payload is not None:
            return dict(self.open_payload)
        return {"ok": True, "open_count": 0, "merged_open_count": 0, "trades": [], "broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}

    def shadow_trade_history(self) -> dict[str, object]:
        if self.history_payload is not None:
            return dict(self.history_payload)
        return {"ok": True, "history_available": True, "trades": [], "closed_trades": [], "broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}

    def readiness(self) -> dict[str, object]:
        if self.readiness_payload is not None:
            return dict(self.readiness_payload)
        return {
            "ok": True,
            "readiness_state": "ready_for_one_cycle_paper_observation",
            "candidate_found": True,
            "candidate_status": "paper_observation_review",
            "runtime_context_available": True,
            "runtime_context_recent": True,
            "tick_available": True,
            "bars_count": 120,
            "capital_allows_observation": True,
            "risk_allows_observation": True,
            "adaptive_allows_observation": True,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def monitor(self, **kwargs: object) -> dict[str, object]:
        self.monitor_calls += 1
        return {
            "ok": True,
            "monitor_state": "no_action",
            "open_shadow_count": 0,
            "exit_signal": False,
            "should_close_paper": False,
            "should_watch_only": False,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def open_shadow_once(self, *, strict_paper_probe: bool = False) -> dict[str, object]:
        del strict_paper_probe
        self.open_calls += 1
        if self.open_result is not None:
            return dict(self.open_result)
        return {
            "ok": True,
            "paper_shadow_created": True,
            "shadow_trade_id": "xau-supervisor-open",
            "open_shadow_count_after": 1,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }


class _SnapshotAwareSupervisorClient(_SupervisorClient):
    def __init__(self) -> None:
        super().__init__()
        self.db_state: dict[str, object] | None = None
        self.status_calls = 0
        self.readiness_saw_db_snapshot = False

    def persistent_status(self) -> dict[str, object]:
        self.status_calls += 1
        return super().persistent_status()

    def readiness(self) -> dict[str, object]:
        self.readiness_saw_db_snapshot = isinstance(self.db_state, dict) and bool(self.db_state.get("db_available"))
        return super().readiness()


class _RiskWritingSupervisorClient(_SupervisorClient):
    def __init__(self) -> None:
        super().__init__()
        self.risk_write: dict[str, object] = {}

    def readiness(self) -> dict[str, object]:
        self.risk_write = persist_risk_event(
            {
                "symbol": "BTCUSD",
                "timeframe": "M15",
                "risk_state": "preflight_diagnostic",
                "allowed": True,
                "reason": "readiness_preflight_diagnostic",
                "recommended_action": "review_preflight",
            }
        )
        write = self.risk_write.get("write") if isinstance(self.risk_write.get("write"), dict) else {}
        return {
            **super().readiness(),
            "suppressed_noncritical_risk_events": [dict(write)],
            "dry_run_risk_events": [dict(write)],
        }


def _recent_edge_readiness() -> dict[str, object]:
    return {
        "ok": True,
        "readiness_state": "blocked",
        "candidate_found": True,
        "candidate_status": "paper_observation_review",
        "runtime_context_available": True,
        "runtime_context_recent": True,
        "tick_available": True,
        "bars_count": 120,
        "capital_allows_observation": True,
        "risk_allows_observation": False,
        "adaptive_allows_observation": True,
        "risk_governor_reason": "recent_edge_negative",
        "recent_edge_negative": True,
        "recommendation": "adaptive_paper_cooldown_wait_for_high_quality_paper_signal",
        "failed_gate_names": ["risk_allows_observation"],
        "failed_gate_reasons": {"risk_allows_observation": {"actual": "recent_edge_negative", "required": "risk_governor_pass"}},
        "entry_block_type": "adaptive_paper_cooldown",
        "entry_allowed_for_paper_test": False,
        "gate_summary": {
            "failed_gate_names": ["risk_allows_observation"],
            "risk_governor_reason": "recent_edge_negative",
            "recent_edge_negative": True,
        },
        "strict_paper_probe": {
            "strict_paper_probe_passed": False,
            "failed_strict_gate_names": ["signal_direction"],
            "signal_direction": "",
        },
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


if __name__ == "__main__":
    unittest.main()

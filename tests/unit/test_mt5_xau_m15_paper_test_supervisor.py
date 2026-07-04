from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from services.mt5.mt5_xau_m15_paper_test_supervisor import repair_orphan_state, run_xau_m15_paper_test_supervisor


class MT5XauM15PaperTestSupervisorTests(unittest.TestCase):
    def test_supervisor_dry_run_does_not_open_shadow(self) -> None:
        client = _SupervisorClient()

        result = run_xau_m15_paper_test_supervisor(client=client, dry_run=True, target_trades=3, max_cycles=5, state_file=None, results_file=None)

        self.assertEqual(result["supervisor_state"], "idle_no_shadow")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

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
        queue_after_drain: int = 0,
        open_payload: dict[str, object] | None = None,
        open_result: dict[str, object] | None = None,
        readiness: dict[str, object] | None = None,
    ) -> None:
        self.db_queue = db_queue
        self.queue_after_drain = queue_after_drain
        self.open_payload = open_payload
        self.open_result = dict(open_result) if open_result is not None else None
        self.readiness_payload = dict(readiness) if readiness is not None else None
        self.drain_calls = 0
        self.open_calls = 0

    def persistent_status(self) -> dict[str, object]:
        return {
            "ok": True,
            "db_available": True,
            "db_degraded": False,
            "tables_ready": True,
            "queue_depth": self.db_queue,
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
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
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

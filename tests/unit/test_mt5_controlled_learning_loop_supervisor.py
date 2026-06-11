from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from services.mt5.mt5_controlled_learning_loop_supervisor import run_controlled_learning_loop_supervisor


class MT5ControlledLearningLoopSupervisorTests(unittest.TestCase):
    def test_default_mode_is_status_only_and_does_not_loop(self) -> None:
        cycle_calls: list[dict[str, object]] = []

        result = run_controlled_learning_loop_supervisor(
            status_runner=lambda **_: _status(),
            orchestrator_runner=lambda **kwargs: cycle_calls.append(kwargs) or _cycle(),
        )

        self.assertEqual(result["supervisor_state"], "dry_run_ready")
        self.assertEqual(result["cycles_requested"], 0)
        self.assertEqual(result["cycles_completed"], 0)
        self.assertFalse(result["loop_started"])
        self.assertEqual(cycle_calls, [])
        _assert_safety(self, result)

    def test_cycles_limit_is_respected(self) -> None:
        cycle_calls: list[dict[str, object]] = []

        result = run_controlled_learning_loop_supervisor(
            cycles=3,
            interval_seconds=0,
            status_runner=lambda **_: _status(),
            orchestrator_runner=lambda **kwargs: cycle_calls.append(kwargs) or _cycle(),
            sleep_between_cycles=False,
        )

        self.assertEqual(result["supervisor_state"], "completed")
        self.assertEqual(result["cycles_requested"], 3)
        self.assertEqual(result["cycles_completed"], 3)
        self.assertEqual(len(cycle_calls), 3)
        _assert_safety(self, result)

    def test_lock_file_prevents_duplicate_loop(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            lock_path = Path(folder) / "learning.lock"
            lock_path.write_text("busy", encoding="utf-8")

            result = run_controlled_learning_loop_supervisor(
                cycles=1,
                lock_path=lock_path,
                status_runner=lambda **_: _status(),
                orchestrator_runner=lambda **_: _cycle(),
            )

        self.assertEqual(result["supervisor_state"], "lock_active")
        self.assertEqual(result["stop_reason"], "lock_active")
        self.assertEqual(result["cycles_completed"], 0)
        _assert_safety(self, result)

    def test_db_degraded_stops_loop_before_cycle(self) -> None:
        result = _run_blocked(_status(db_available=True, db_degraded=True, tables_ready=True))

        self.assertEqual(result["stop_reason"], "db_degraded")
        self.assertEqual(result["cycles_completed"], 0)
        _assert_safety(self, result)

    def test_capital_kill_switch_stops_loop_before_cycle(self) -> None:
        result = _run_blocked(_status(capital_state="kill_switch"))

        self.assertEqual(result["stop_reason"], "capital_protection_block")
        self.assertEqual(result["cycles_completed"], 0)
        _assert_safety(self, result)

    def test_adaptive_kill_switch_stops_loop_before_cycle(self) -> None:
        result = _run_blocked(_status(adaptive_state="kill_switch"))

        self.assertEqual(result["stop_reason"], "adaptive_governor_block")
        self.assertEqual(result["cycles_completed"], 0)
        _assert_safety(self, result)

    def test_queue_depth_high_stops_loop_before_cycle(self) -> None:
        result = _run_blocked(_status(queue_depth=999))

        self.assertEqual(result["stop_reason"], "persistent_queue_depth_high")
        self.assertEqual(result["cycles_completed"], 0)
        _assert_safety(self, result)

    def test_risk_governor_block_stops_loop_before_cycle(self) -> None:
        result = _run_blocked(_status(risk_allowed=False))

        self.assertEqual(result["stop_reason"], "risk_governor_block")
        self.assertEqual(result["cycles_completed"], 0)
        _assert_safety(self, result)

    def test_no_apply_paper_rotation_by_default(self) -> None:
        cycle_calls: list[dict[str, object]] = []

        result = run_controlled_learning_loop_supervisor(
            cycles=1,
            interval_seconds=0,
            status_runner=lambda **_: _status(),
            orchestrator_runner=lambda **kwargs: cycle_calls.append(kwargs) or _cycle(),
            sleep_between_cycles=False,
        )

        self.assertEqual(result["cycles_completed"], 1)
        self.assertFalse(cycle_calls[0]["apply_paper_rotation"])
        self.assertFalse(cycle_calls[0]["load_rotation"])
        self.assertFalse(result["paper_rotation_applied"])
        self.assertFalse(result["candidate_activated"])
        _assert_safety(self, result)

    def test_no_real_trading_flags_are_always_false(self) -> None:
        result = run_controlled_learning_loop_supervisor(
            cycles=1,
            interval_seconds=0,
            status_runner=lambda **_: _status(),
            orchestrator_runner=lambda **_: _cycle(),
            sleep_between_cycles=False,
        )

        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["automatic_promotion"])
        self.assertFalse(result["promoted_profile_mutated"])
        _assert_safety(self, result)


def _run_blocked(status: dict[str, object]) -> dict[str, object]:
    cycle_calls: list[dict[str, object]] = []
    result = run_controlled_learning_loop_supervisor(
        cycles=1,
        interval_seconds=0,
        status_runner=lambda **_: status,
        orchestrator_runner=lambda **kwargs: cycle_calls.append(kwargs) or _cycle(),
        sleep_between_cycles=False,
    )
    if cycle_calls:
        raise AssertionError("cycle should not run when a gate fails")
    return result


def _status(
    *,
    db_available: bool = True,
    db_degraded: bool = False,
    tables_ready: bool = True,
    queue_depth: int = 0,
    capital_state: str = "normal",
    adaptive_state: str = "watch",
    risk_allowed: bool = True,
    open_shadow_trades: int = 0,
) -> dict[str, object]:
    db_state = {
        "provider": "railway_postgres",
        "db_available": db_available,
        "db_degraded": db_degraded,
        "tables_ready": tables_ready,
        "missing_tables": [] if tables_ready else ["mt5_profile_state"],
        "queue_depth": queue_depth,
        "recommendation": "persistent_intelligence_ready" if db_available and tables_ready and not db_degraded else "verify_database_connection",
    }
    return {
        "ok": True,
        "status": "autonomous_learning_status_ready",
        "provider": "railway_postgres",
        "db_available": db_available,
        "db_degraded": db_degraded,
        "tables_ready": tables_ready,
        "db_state": db_state,
        "learning_state": "continue_research",
        "capital_state": capital_state,
        "capital_protection": {"capital_state": capital_state, "safe_to_trade": capital_state != "kill_switch"},
        "adaptive_state": adaptive_state,
        "adaptive_governor": {"global_state": adaptive_state},
        "risk_governor": {"allowed": risk_allowed, "reason": "" if risk_allowed else "unit_block"},
        "shadow_hygiene": {"open_shadow_trades": open_shadow_trades, "safe_to_open_new_shadow": open_shadow_trades <= 3},
        "safe_to_learn": db_available and tables_ready and not db_degraded,
        "safe_to_open_new_shadow": open_shadow_trades <= 3,
        "active_profiles": [],
        "paused_profiles": [],
        "degraded_profiles": [],
        "tournament_top_candidate": {"symbol": "BTCUSD", "timeframe": "M30", "profile": "unit_profile"},
        "paper_rotation_recommendation": "continue_research",
        "paper_rotation_applied": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "circuit_breakers": [],
        "recommended_next_action": "continue_research",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _cycle() -> dict[str, object]:
    return {
        "ok": True,
        "learning_state": "continue_research",
        "db_state": {"provider": "railway_postgres", "db_available": True, "db_degraded": False, "tables_ready": True},
        "capital_state": "normal",
        "adaptive_state": "watch",
        "tournament_top_candidate": {"symbol": "BTCUSD", "timeframe": "M30", "profile": "unit_profile"},
        "paper_rotation_recommendation": "continue_research",
        "paper_rotation_applied": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "recommended_next_action": "continue_research",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _assert_safety(test: unittest.TestCase, result: dict[str, object]) -> None:
    test.assertFalse(result["broker_touched"])
    test.assertFalse(result["order_executed"])
    test.assertEqual(result["order_policy"], "journal_only_no_broker")

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

        self.assertEqual(result["anomaly"], "orphaned_or_runtime_lost")
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


def _step(client: "_FakeClient", *, dry_run: bool = False, paper_only_confirmed: bool = False) -> dict[str, object]:
    return run_xau_m15_paper_observation_batch_step(
        client=client,
        state={},
        trades=[],
        cycle_number=1,
        target_trades=20,
        dry_run=dry_run,
        paper_only_confirmed=paper_only_confirmed,
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
    ) -> None:
        self.db = db or _db()
        self.ready = readiness or _ready()
        self.open_count = open_count
        self.monitor_payload = monitor or _monitor_none()
        self.open_calls = 0
        self.close_calls = 0

    def persistent_status(self) -> dict[str, object]:
        return dict(self.db)

    def open_shadow_trades(self) -> dict[str, object]:
        trades = [{"shadow_trade_id": "existing-shadow", "symbol": "XAUUSD", "timeframe": "M15"}] if self.open_count else []
        if self.open_count > 1:
            trades.append({"shadow_trade_id": "existing-shadow-2", "symbol": "XAUUSD", "timeframe": "M15"})
        return {"ok": True, "open_count": self.open_count, "trades": trades, **_safety()}

    def readiness(self) -> dict[str, object]:
        return dict(self.ready)

    def monitor(self, *, apply_paper_close: bool = False) -> dict[str, object]:
        if apply_paper_close:
            self.close_calls += 1
            self.open_count = 0
            return {**self.monitor_payload, "paper_close_applied": True, "shadow_status_after": "closed", **_safety()}
        return dict(self.monitor_payload)

    def open_shadow_once(self) -> dict[str, object]:
        self.open_calls += 1
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


def _monitor_close(exit_reason: str, *, pnl: float, r: float, category: str = "none") -> dict[str, object]:
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
        "bars_since_entry": 2,
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


def _safety() -> dict[str, object]:
    return {"broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}


if __name__ == "__main__":
    unittest.main()

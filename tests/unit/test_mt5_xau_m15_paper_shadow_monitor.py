from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from services.mt5.mt5_runtime_snapshot import get_snapshot, reset_runtime_snapshots_for_tests, update_bars, update_open_shadow_trade
from services.mt5.mt5_xau_m15_paper_observation_readiness import CANDIDATE_PROFILE
from services.mt5.mt5_xau_m15_paper_shadow_monitor import run_xau_m15_paper_shadow_monitor


class MT5XauM15PaperShadowMonitorTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)

    def test_no_open_shadow_returns_no_action(self) -> None:
        _seed_runtime(price=100.0)

        result = run_xau_m15_paper_shadow_monitor(db_state=_db(), risk_state=_risk(), persist_events=False)

        self.assertEqual(result["monitor_state"], "no_action")
        self.assertEqual(result["open_shadow_count"], 0)
        self.assertFalse(result["exit_signal"])
        self.assertEqual(result["exit_reason"], "no_open_shadow")
        self.assertEqual(result["safety_exit_category"], "none")
        self.assertFalse(result["should_close_paper"])
        self.assertFalse(result["should_watch_only"])
        self.assertFalse(result["paper_close_applied"])
        self.assertFalse((get_snapshot("XAUUSD", "M15") or {}).get("open_shadow_trade"))
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_monitor_does_not_open_new_shadow(self) -> None:
        result = run_xau_m15_paper_shadow_monitor(apply_paper_close=True, db_state=_db(), risk_state=_risk(), persist_events=False)

        self.assertEqual(result["open_shadow_count"], 0)
        self.assertFalse(result["paper_close_applied"])
        self.assertFalse((get_snapshot("XAUUSD", "M15") or {}).get("open_shadow_trade"))

    def test_monitor_finds_shadow_in_runtime(self) -> None:
        _seed_runtime(price=101.0)
        _seed_open_shadow(entry=100.0, stop=95.0, target=110.0)

        result = run_xau_m15_paper_shadow_monitor(db_state=_db(), risk_state=_risk(), persist_events=False)

        self.assertEqual(result["shadow_source"], "runtime_memory")
        self.assertEqual(result["open_shadow_count"], 1)
        self.assertEqual(result["shadow_trade_id"], "xau-monitor-test")
        self.assertFalse(result["paper_close_applied"])

    def test_monitor_finds_shadow_in_persistent_intelligence_when_runtime_empty(self) -> None:
        _seed_runtime(price=101.0)
        store = _FakePersistentStore([_persistent_shadow(entry=100.0, stop=95.0, target=110.0)])

        result = run_xau_m15_paper_shadow_monitor(store=store, db_state=_db(), risk_state=_risk(), persist_events=False)

        self.assertEqual(result["monitor_state"], "open_monitoring")
        self.assertEqual(result["shadow_source"], "persistent_intelligence_fallback")
        self.assertEqual(result["open_shadow_count"], 1)
        self.assertEqual(result["shadow_trade_id"], "persistent-xau-shadow")
        self.assertEqual(result["entry_price"], 100.0)
        self.assertEqual(result["current_price"], 101.0)
        self.assertGreater(result["unrealized_pnl"], 0)
        self.assertFalse(result["paper_close_applied"])

    def test_monitor_does_not_duplicate_shadow_when_using_persistent_fallback(self) -> None:
        _seed_runtime(price=101.0)
        store = _FakePersistentStore([_persistent_shadow(entry=100.0, stop=95.0, target=110.0)])

        result = run_xau_m15_paper_shadow_monitor(store=store, db_state=_db(), risk_state=_risk(), persist_events=False)
        snapshot = get_snapshot("XAUUSD", "M15") or {}

        self.assertEqual(result["shadow_source"], "persistent_intelligence_fallback")
        self.assertFalse(snapshot.get("open_shadow_trade"))
        self.assertFalse(result["paper_close_applied"])

    def test_monitor_blocks_multiple_persisted_open_shadows(self) -> None:
        _seed_runtime(price=101.0)
        first = _persistent_shadow(entry=100.0, stop=95.0, target=110.0)
        second = {**_persistent_shadow(entry=100.0, stop=95.0, target=110.0), "shadow_trade_id": "persistent-xau-shadow-2"}
        store = _FakePersistentStore([first, second])

        result = run_xau_m15_paper_shadow_monitor(store=store, db_state=_db(), risk_state=_risk(), persist_events=False)

        self.assertEqual(result["monitor_state"], "blocked_multiple_open_shadows")
        self.assertEqual(result["shadow_source"], "persistent_intelligence_fallback")
        self.assertEqual(result["open_shadow_count"], 2)
        self.assertEqual(result["exit_reason"], "multiple_open_shadows_persisted")
        self.assertEqual(result["safety_exit_category"], "critical_safety_exit")
        self.assertIn("multiple_open_shadows", result["safety_exit_reason_detail"])
        self.assertTrue(result["should_close_paper"])
        self.assertFalse(result["should_watch_only"])
        self.assertFalse(result["paper_close_applied"])

    def test_stop_loss_hit_produces_exit_signal(self) -> None:
        _seed_runtime(price=94.0)
        _seed_open_shadow(entry=100.0, stop=95.0, target=110.0)

        result = run_xau_m15_paper_shadow_monitor(db_state=_db(), risk_state=_risk(), persist_events=False)

        self.assertTrue(result["exit_signal"])
        self.assertEqual(result["exit_reason"], "stop_loss_hit")
        self.assertFalse(result["paper_close_applied"])
        self.assertEqual(result["shadow_status_after"], "open")
        self.assertLess(result["unrealized_pnl"], 0)

    def test_take_profit_hit_produces_exit_signal(self) -> None:
        _seed_runtime(price=111.0)
        _seed_open_shadow(entry=100.0, stop=95.0, target=110.0)

        result = run_xau_m15_paper_shadow_monitor(db_state=_db(), risk_state=_risk(), persist_events=False)

        self.assertTrue(result["exit_signal"])
        self.assertEqual(result["exit_reason"], "take_profit_hit")
        self.assertFalse(result["paper_close_applied"])
        self.assertGreater(result["unrealized_pnl"], 0)

    def test_stale_runtime_is_critical_safety_exit_and_apply_closes_when_price_available(self) -> None:
        stale_time = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        snapshot = {
            "symbol": "XAUUSD",
            "normalized_symbol": "XAUUSD",
            "timeframe": "M15",
            "runtime_snapshot_available": True,
            "runtime_snapshot_recent": False,
            "runtime_snapshot_context": "bar_context",
            "runtime_snapshot_complete": True,
            "last_tick_at": stale_time,
            "last_tick": {"bid": 100.0, "ask": 100.2, "last": 100.1, "timeframe": "M15"},
            "bars_count": 120,
            "open_shadow_trade": _shadow(entry=100.0, stop=95.0, target=110.0),
        }

        result = run_xau_m15_paper_shadow_monitor(
            apply_paper_close=True,
            runtime_snapshot=snapshot,
            db_state=_db(),
            risk_state=_risk(),
            persist_events=False,
        )

        self.assertEqual(result["monitor_state"], "exit_applied")
        self.assertTrue(result["exit_signal"])
        self.assertEqual(result["exit_reason"], "safety_exit")
        self.assertEqual(result["safety_exit_category"], "critical_safety_exit")
        self.assertIn("runtime_context_stale", result["safety_exit_reason_detail"])
        self.assertTrue(result["should_close_paper"])
        self.assertFalse(result["apply_blocked"])
        self.assertTrue(result["paper_close_applied"])

    def test_small_loss_with_healthy_runtime_and_adaptive_watch_does_not_close(self) -> None:
        _seed_runtime(price=99.87)
        _seed_open_shadow(entry=100.0, stop=95.0, target=110.0)

        result = run_xau_m15_paper_shadow_monitor(
            apply_paper_close=True,
            db_state=_db(),
            adaptive_state={"adaptive_state": "watch", "reason": "monitor_small_unrealized_loss"},
            risk_state=_risk(),
            persist_events=False,
        )

        self.assertEqual(result["monitor_state"], "open_monitoring")
        self.assertFalse(result["exit_signal"])
        self.assertEqual(result["exit_reason"], "caution_watch")
        self.assertEqual(result["safety_exit_category"], "caution_watch")
        self.assertIn("adaptive_state_watch", result["safety_exit_reason_detail"])
        self.assertFalse(result["should_close_paper"])
        self.assertTrue(result["should_watch_only"])
        self.assertFalse(result["paper_close_applied"])

    def test_single_current_shadow_risk_max_open_trades_is_entry_block_only(self) -> None:
        _seed_runtime(price=100.0)
        _seed_open_shadow(entry=100.0, stop=95.0, target=110.0)

        result = run_xau_m15_paper_shadow_monitor(
            apply_paper_close=True,
            db_state=_db(),
            risk_state={
                "allowed": False,
                "risk_state": "caution",
                "reason": "max_open_trades_reached",
                "max_open_trades": 1,
            },
            persist_events=False,
        )
        snapshot = get_snapshot("XAUUSD", "M15") or {}

        self.assertEqual(result["monitor_state"], "open_monitoring")
        self.assertFalse(result["exit_signal"])
        self.assertEqual(result["exit_reason"], "caution_watch")
        self.assertEqual(result["safety_exit_category"], "entry_block_only")
        self.assertEqual(result["risk_block_type"], "entry_block")
        self.assertTrue(result["risk_block_applies_to_current_shadow"])
        self.assertEqual(result["max_open_trades_limit"], 1)
        self.assertFalse(result["should_close_paper"])
        self.assertTrue(result["should_watch_only"])
        self.assertEqual(result["close_decision_reason"], "watch_only:entry_block_current_shadow")
        self.assertFalse(result["paper_close_applied"])
        self.assertEqual((snapshot.get("open_shadow_trade") or {}).get("shadow_trade_id"), "xau-monitor-test")

    def test_db_degraded_is_critical_safety_exit_and_apply_closes_paper_only(self) -> None:
        _seed_runtime(price=100.0)
        _seed_open_shadow(entry=100.0, stop=95.0, target=110.0)

        result = run_xau_m15_paper_shadow_monitor(
            apply_paper_close=True,
            db_state={**_db(), "db_degraded": True},
            risk_state=_risk(),
            persist_events=False,
        )

        self.assertEqual(result["monitor_state"], "exit_applied")
        self.assertTrue(result["exit_signal"])
        self.assertEqual(result["exit_reason"], "safety_exit")
        self.assertEqual(result["safety_exit_category"], "critical_safety_exit")
        self.assertEqual(result["risk_block_type"], "none")
        self.assertIn("db_degraded", result["safety_exit_reason_detail"])
        self.assertTrue(result["should_close_paper"])
        self.assertFalse(result["should_watch_only"])
        self.assertTrue(result["paper_close_applied"])

    def test_unknown_safety_exit_closes_but_reports_missing_detail(self) -> None:
        _seed_runtime(price=100.0)
        _seed_open_shadow(entry=100.0, stop=95.0, target=110.0)

        result = run_xau_m15_paper_shadow_monitor(
            apply_paper_close=True,
            db_state=_db(),
            risk_state={"allowed": False, "risk_state": "unknown", "reason": "safety_exit"},
            persist_events=False,
        )

        self.assertEqual(result["monitor_state"], "exit_applied")
        self.assertTrue(result["exit_signal"])
        self.assertEqual(result["exit_reason"], "safety_exit")
        self.assertEqual(result["safety_exit_category"], "unknown_safety_exit")
        self.assertEqual(result["safety_exit_reason_detail"], "missing_safety_exit_detail")
        self.assertIn("risk_governor_block_without_detail", result["safety_exit_sources"])
        self.assertTrue(result["should_close_paper"])
        self.assertFalse(result["should_watch_only"])
        self.assertTrue(result["paper_close_applied"])

    def test_dry_run_never_closes(self) -> None:
        _seed_runtime(price=111.0)
        _seed_open_shadow(entry=100.0, stop=95.0, target=110.0)

        result = run_xau_m15_paper_shadow_monitor(apply_paper_close=False, db_state=_db(), risk_state=_risk(), persist_events=False)
        snapshot = get_snapshot("XAUUSD", "M15") or {}

        self.assertEqual(result["monitor_state"], "exit_pending")
        self.assertTrue(result["exit_signal"])
        self.assertEqual(result["exit_reason"], "take_profit_hit")
        self.assertFalse(result["paper_close_applied"])
        self.assertEqual((snapshot.get("open_shadow_trade") or {}).get("status"), "open")

    def test_apply_closes_paper_only_shadow(self) -> None:
        _seed_runtime(price=111.0)
        _seed_open_shadow(entry=100.0, stop=95.0, target=110.0)

        result = run_xau_m15_paper_shadow_monitor(apply_paper_close=True, db_state=_db(), risk_state=_risk(), persist_events=False)
        snapshot = get_snapshot("XAUUSD", "M15") or {}
        closed = snapshot.get("recent_closed_shadow_trades") or []

        self.assertEqual(result["monitor_state"], "exit_applied")
        self.assertTrue(result["exit_signal"])
        self.assertEqual(result["exit_reason"], "take_profit_hit")
        self.assertTrue(result["paper_close_applied"])
        self.assertEqual(result["shadow_status_after"], "closed")
        self.assertEqual(snapshot.get("open_shadow_trade"), {})
        self.assertEqual(closed[0]["shadow_trade_id"], "xau-monitor-test")
        self.assertEqual(closed[0]["status"], "closed")
        self.assertFalse(closed[0]["broker_touched"])
        self.assertFalse(closed[0]["order_executed"])
        self.assertEqual(closed[0]["order_policy"], "journal_only_no_broker")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_close_is_atomic_db_failure_keeps_runtime_shadow_open(self) -> None:
        _seed_runtime(price=111.0)
        _seed_open_shadow(entry=100.0, stop=95.0, target=110.0)
        store = _FailingPersistentStore()

        result = run_xau_m15_paper_shadow_monitor(apply_paper_close=True, db_state=_db(), risk_state=_risk(), store=store)
        snapshot = get_snapshot("XAUUSD", "M15") or {}

        self.assertEqual(result["monitor_state"], "close_blocked_by_persistence")
        self.assertTrue(result["exit_signal"])
        self.assertEqual(result["exit_reason"], "take_profit_hit")
        self.assertFalse(result["paper_close_applied"])
        self.assertTrue(result["close_persistence_failed"])
        self.assertTrue(result["close_write_retained_critical"])
        self.assertEqual((snapshot.get("open_shadow_trade") or {}).get("shadow_trade_id"), "xau-monitor-test")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_fast_observation_timebox_closes_paper_only_shadow(self) -> None:
        _seed_runtime(price=100.0)
        trade = _shadow(entry=100.0, stop=95.0, target=110.0)
        trade["opened_at"] = (datetime.now(timezone.utc) - timedelta(minutes=75)).isoformat()
        update_open_shadow_trade("XAUUSD", trade, timeframe="M15")

        result = run_xau_m15_paper_shadow_monitor(
            apply_paper_close=True,
            exit_policy="fast_observation",
            time_stop_bars=2,
            db_state=_db(),
            risk_state=_risk(),
            persist_events=False,
        )

        self.assertEqual(result["monitor_state"], "exit_applied")
        self.assertTrue(result["exit_signal"])
        self.assertEqual(result["exit_reason"], "paper_timebox_exit")
        self.assertTrue(result["paper_close_applied"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_fast_observation_giveback_closes_paper_only_shadow(self) -> None:
        _seed_runtime(price=101.0)
        trade = _shadow(entry=100.0, stop=95.0, target=110.0)
        trade["max_favorable_excursion"] = 3.0
        update_open_shadow_trade("XAUUSD", trade, timeframe="M15")

        result = run_xau_m15_paper_shadow_monitor(
            apply_paper_close=True,
            exit_policy="fast_observation",
            min_r_to_arm_trailing=0.25,
            giveback_r=0.15,
            db_state=_db(),
            risk_state=_risk(),
            persist_events=False,
        )

        self.assertEqual(result["monitor_state"], "exit_applied")
        self.assertTrue(result["exit_signal"])
        self.assertEqual(result["exit_reason"], "paper_fast_trailing_exit")
        self.assertTrue(result["paper_close_applied"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_fast_observation_fast_loss_cut_closes_paper_only_shadow(self) -> None:
        _seed_runtime(price=98.5)
        _seed_open_shadow(entry=100.0, stop=95.0, target=110.0)

        result = run_xau_m15_paper_shadow_monitor(
            apply_paper_close=True,
            exit_policy="fast_observation",
            db_state=_db(),
            risk_state=_risk(),
            persist_events=False,
        )

        self.assertEqual(result["monitor_state"], "exit_applied")
        self.assertTrue(result["exit_signal"])
        self.assertEqual(result["exit_reason"], "paper_fast_loss_cut")
        self.assertTrue(result["paper_close_applied"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_monitor_service_adds_no_forbidden_execution_reference(self) -> None:
        service_text = Path("services/mt5/mt5_xau_m15_paper_shadow_monitor.py").read_text(encoding="utf-8")
        self.assertNotIn("order_send", service_text)


def _seed_runtime(*, price: float) -> None:
    update_bars(
        "XAUUSD.b",
        "M15",
        _bars(120, price=price),
        tick={"bid": price, "ask": price + 0.2, "last": price + 0.1, "spread": 0.2, "timeframe": "M15"},
        min_bars=100,
    )


def _seed_open_shadow(*, entry: float, stop: float, target: float) -> None:
    update_open_shadow_trade("XAUUSD", _shadow(entry=entry, stop=stop, target=target), timeframe="M15")


def _shadow(*, entry: float, stop: float, target: float) -> dict[str, object]:
    return {
        "shadow_trade_id": "xau-monitor-test",
        "symbol": "XAUUSD",
        "broker_symbol": "XAUUSD.b",
        "timeframe": "M15",
        "side": "buy",
        "action": "BUY",
        "entry_price": entry,
        "entry": entry,
        "stop_loss": stop,
        "take_profit": target,
        "initial_risk": abs(entry - stop),
        "status": "open",
        "lifecycle_status": "open",
        "opened_at": (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat(),
        "source": "paper_observation_shadow_once",
        "strategy_profile": CANDIDATE_PROFILE,
        "candidate_profile": CANDIDATE_PROFILE,
        "paper_forward_candidate": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "applies_to_real_trading": False,
    }


def _persistent_shadow(*, entry: float, stop: float, target: float) -> dict[str, object]:
    return {
        "shadow_trade_id": "persistent-xau-shadow",
        "symbol": "XAUUSD",
        "broker_symbol": "XAUUSD.b",
        "timeframe": "M15",
        "profile": CANDIDATE_PROFILE,
        "strategy_profile": CANDIDATE_PROFILE,
        "source": "paper_observation_shadow_once",
        "side": "buy",
        "entry_price": entry,
        "stop_loss": stop,
        "take_profit": target,
        "status": "open",
        "opened_at": (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat(),
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


class _FakePersistentStore:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = [dict(row) for row in rows]
        self.recorded: list[dict[str, object]] = []
        self.performance: list[dict[str, object]] = []
        self.lessons: list[dict[str, object]] = []

    def _safe_select(self, table: str, *, params: dict[str, str]) -> dict[str, object]:
        if table == "mt5_shadow_trades":
            return {"ok": True, "db_degraded": False, "rows": [dict(row) for row in self.rows]}
        return {"ok": True, "db_degraded": False, "rows": []}

    def record_shadow_trade(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        self.recorded.append(dict(payload))
        return {"ok": True, "table": "mt5_shadow_trades", "broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}

    def upsert_profile_performance(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        self.performance.append(dict(payload))
        return {"ok": True, "table": "mt5_profile_performance", "broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}

    def record_research_lesson(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        self.lessons.append(dict(payload))
        return {"ok": True, "table": "mt5_research_lessons", "broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}


class _FailingPersistentStore(_FakePersistentStore):
    def __init__(self) -> None:
        super().__init__([])

    def record_shadow_trade(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        self.recorded.append(dict(payload))
        return {
            "ok": False,
            "queued": True,
            "db_degraded": True,
            "reason": "simulated_persistent_write_failure",
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }


def _bars(count: int, *, price: float) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    start = datetime.now(timezone.utc) - timedelta(minutes=15 * count)
    for idx in range(count):
        close = price + (idx - count) * 0.01
        rows.append(
            {
                "time": (start + timedelta(minutes=15 * idx)).isoformat(),
                "open": close - 0.1,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
                "volume": 100 + idx,
                "tick_volume": 100 + idx,
            }
        )
    return rows


def _db() -> dict[str, object]:
    return {
        "provider": "railway_postgres",
        "db_available": True,
        "db_degraded": False,
        "tables_ready": True,
        "queue_depth": 0,
        "recommendation": "persistent_intelligence_ready",
    }


def _risk() -> dict[str, object]:
    return {"allowed": True, "risk_state": "normal", "reason": "risk_governor_pass"}


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from typing import Any

from services.genesis.memory_store import MemoryStore
from services.mt5.mt5_capital_protection_governor import run_capital_protection_governor
from services.mt5.mt5_persistent_db_doctor import run_persistent_db_doctor
from services.mt5.mt5_persistent_intelligence_store import (
    persistent_intelligence_recent_events,
    persistent_intelligence_schema_freeze_status,
    persistent_intelligence_status,
)
from services.mt5.mt5_risk_recovery import mt5_risk_recovery_status
from services.mt5.mt5_signal_router import MT5SignalRouter
from services.mt5.mt5_strategy_tournament import run_strategy_tournament


def build_router(memory: MemoryStore | None = None) -> MT5SignalRouter:
    return MT5SignalRouter(memory=memory)


def mt5_health(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).health()


def mt5_config(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).config_payload()


def mt5_ops_status(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).ops_status(symbol=symbol)


def mt5_risk_state(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).risk_state(symbol=symbol, timeframe=timeframe)


def mt5_risk_recovery(*, memory: MemoryStore | None = None, symbol: str = "ETHUSD", timeframe: str = "M30") -> dict[str, Any]:
    return mt5_risk_recovery_status(symbol=symbol, timeframe=timeframe)


def mt5_persistent_intelligence_status(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    return persistent_intelligence_status(write_test_event=False)


def mt5_persistent_intelligence_recent_events(*, memory: MemoryStore | None = None, limit: int = 10) -> dict[str, Any]:
    return persistent_intelligence_recent_events(limit=limit)


def mt5_persistent_db_doctor_status(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    return run_persistent_db_doctor()


def mt5_capital_protection_status(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    blocked = _schema_missing_fast_fail("capital_protection")
    if blocked:
        return blocked
    return run_capital_protection_governor()


def mt5_strategy_tournament_status(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    blocked = _schema_missing_fast_fail("strategy_tournament")
    if blocked:
        return blocked
    return run_strategy_tournament()


def _schema_missing_fast_fail(endpoint: str, *, symbol: str = "") -> dict[str, Any]:
    freeze = persistent_intelligence_schema_freeze_status()
    if not freeze.get("writes_frozen"):
        return {}
    base = {
        "ok": True,
        **freeze,
        "status": f"mt5_{endpoint}_paused_by_db_schema_missing",
        "provider": "railway_postgres",
        "symbol": str(symbol or ""),
        "db_degraded": True,
        "schema_missing_write_freeze": True,
        "writes_frozen": True,
        "learning_state": "paused_by_db_schema_missing",
        "decision": "NO_TRADE",
        "reason": "persistent_intelligence_schema_missing",
        "recommended_action": "apply_schema_sql",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "secrets_printed": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
    if endpoint == "capital_protection":
        base.update(
            {
                "capital_state": "paused_by_db_schema_missing",
                "safe_to_trade": False,
                "circuit_breakers": [
                    {
                        "name": "persistent_intelligence_schema_missing",
                        "active": True,
                        "severity": "critical",
                        "reason": "persistent_intelligence_schema_missing",
                    }
                ],
            }
        )
    if endpoint == "strategy_tournament":
        base.update(
            {
                "recommended_action": "skip_tournament_until_schema_ready",
                "top_candidate": {},
                "ranked_candidates": [],
                "candidate_activated": False,
                "paper_forward_onboarding_started": False,
            }
        )
    return base


def mt5_ui_summary(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).ui_summary(symbol=symbol, timeframe=timeframe)


def mt5_status(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).status()


def mt5_journal_recent(*, memory: MemoryStore | None = None, limit: int = 25, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).journal_recent(limit=limit, symbol=symbol)


def mt5_performance(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).performance(symbol=symbol, timeframe=timeframe)


def mt5_performance_auto(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).performance_auto(symbol=symbol, timeframe=timeframe)


def mt5_forward_test(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).forward_test(symbol=symbol, timeframe=timeframe)


def mt5_outcomes_recent(*, memory: MemoryStore | None = None, symbol: str = "", limit: int = 25) -> dict[str, Any]:
    return build_router(memory).outcomes_recent(symbol=symbol, limit=limit)


def mt5_no_trade_report(*, memory: MemoryStore | None = None, symbol: str = "", limit: int = 50) -> dict[str, Any]:
    return build_router(memory).no_trade_report(symbol=symbol, limit=limit)


def mt5_shadow_trades(*, memory: MemoryStore | None = None, symbol: str = "", limit: int = 100) -> dict[str, Any]:
    return build_router(memory).shadow_trades(symbol=symbol, limit=limit)


def mt5_shadow_trades_open(*, memory: MemoryStore | None = None, symbol: str = "", limit: int = 100) -> dict[str, Any]:
    return build_router(memory).shadow_trades_open(symbol=symbol, limit=limit)


def mt5_shadow_trades_close_expired(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).shadow_trades_close_expired(payload)


def mt5_shadow_trade_close(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).shadow_trade_close(payload)


def mt5_debug_storage(*, memory: MemoryStore | None = None, symbol: str = "", limit: int = 20) -> dict[str, Any]:
    return build_router(memory).debug_storage(symbol=symbol, limit=limit)


def mt5_instrument(*, memory: MemoryStore | None = None, symbol: str = "", payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return build_router(memory).instrument(symbol=symbol, payload=payload)


def mt5_auto_forward_status(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).auto_forward_status(symbol=symbol)


def mt5_account_sync(payload: dict[str, Any] | None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).account_sync(payload)


def mt5_signal(payload: dict[str, Any] | None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).signal(payload)


def mt5_tick(payload: dict[str, Any] | None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).tick(payload)


def mt5_bars(payload: dict[str, Any] | None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).bars(payload)


def mt5_decision(symbol: str, *, memory: MemoryStore | None = None, timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).decision(symbol, timeframe=timeframe)


def mt5_order_request(payload: dict[str, Any] | None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).order_request(payload)


def mt5_order_result(payload: dict[str, Any] | None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).order_result(payload)


def mt5_manual_tests_reset(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    body = payload or {}
    return build_router(memory).reset_manual_tests(symbol=str(body.get("symbol") or body.get("ticker") or ""))


def mt5_metrics_exclude_old_proxy(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    body = payload or {}
    return build_router(memory).exclude_old_proxy_metrics(symbol=str(body.get("symbol") or body.get("ticker") or ""))


def mt5_replay_run(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).replay_run(payload)


def mt5_replay_results(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).replay_results(symbol=symbol)


def mt5_replay_status(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).replay_status(symbol=symbol)


def mt5_replay_reset(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).replay_reset(payload)


def mt5_backtest_run(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).backtest_run(payload)


def mt5_backtest_optimize(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).backtest_optimize(payload)


def mt5_backtest_latest(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).backtest_latest(symbol=symbol)


def mt5_forward_replay_run(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).forward_replay_run(payload)


def mt5_learning_run(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    blocked = _schema_missing_fast_fail("learning_run")
    if blocked:
        return blocked
    return build_router(memory).learning_run(payload)


def mt5_memory_summary(*, memory: MemoryStore | None = None, symbol: str = "", limit: int = 50) -> dict[str, Any]:
    return build_router(memory).memory_summary(symbol=symbol, limit=limit)


def mt5_learning_status(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    blocked = _schema_missing_fast_fail("learning_status", symbol=symbol)
    if blocked:
        return blocked
    return build_router(memory).learning_status(symbol=symbol)


def mt5_adaptive_state(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).adaptive_state(symbol=symbol, timeframe=timeframe)


def mt5_strategy_profiles(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).strategy_profiles(symbol=symbol)


def mt5_adaptive_recommendations(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).adaptive_recommendations(symbol=symbol, timeframe=timeframe)


def mt5_paper_defense(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).paper_defense_status(symbol=symbol)


def mt5_promoted_profile(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).promoted_profile(symbol=symbol, timeframe=timeframe)


def mt5_forward_profile_state(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).forward_profile_state(symbol=symbol, timeframe=timeframe)

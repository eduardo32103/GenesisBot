from __future__ import annotations

from typing import Any

from services.genesis.memory_store import MemoryStore
from services.mt5.mt5_signal_router import MT5SignalRouter


def build_router(memory: MemoryStore | None = None) -> MT5SignalRouter:
    return MT5SignalRouter(memory=memory)


def mt5_health(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).health()


def mt5_config(*, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).config_payload()


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


def mt5_debug_storage(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).debug_storage(symbol=symbol)


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


def mt5_decision(symbol: str, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).decision(symbol)


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


def mt5_learning_run(payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return build_router(memory).learning_run(payload)


def mt5_memory_summary(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).memory_summary(symbol=symbol)


def mt5_adaptive_state(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).adaptive_state(symbol=symbol, timeframe=timeframe)


def mt5_strategy_profiles(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).strategy_profiles(symbol=symbol)


def mt5_adaptive_recommendations(*, memory: MemoryStore | None = None, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
    return build_router(memory).adaptive_recommendations(symbol=symbol, timeframe=timeframe)

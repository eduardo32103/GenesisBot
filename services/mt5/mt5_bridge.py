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


def mt5_shadow_trades(*, memory: MemoryStore | None = None, symbol: str = "", limit: int = 100) -> dict[str, Any]:
    return build_router(memory).shadow_trades(symbol=symbol, limit=limit)


def mt5_debug_storage(*, memory: MemoryStore | None = None, symbol: str = "") -> dict[str, Any]:
    return build_router(memory).debug_storage(symbol=symbol)


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

from __future__ import annotations

from typing import Any

from services.genesis.memory_store import MemoryStore
from services.mt5.mt5_order_model import sanitize_payload


def save_backtest_note(symbol: str, payload: dict[str, Any] | None = None, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    store = memory or MemoryStore()
    clean = {
        "symbol": str(symbol or "").upper().strip(),
        "collection": "mt5_backtest_runs",
        "broker_touched": False,
        "order_executed": False,
        **sanitize_payload(payload or {}),
    }
    return store.save_event("mt5_backtest_run", clean, "mt5_bridge", clean.get("confidence") or "media")


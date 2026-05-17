from __future__ import annotations

from typing import Any

from services.genesis.memory_store import MemoryStore
from services.mt5.mt5_order_model import sanitize_payload


class MT5Journal:
    def __init__(self, *, memory: MemoryStore | None = None) -> None:
        self.memory = memory or MemoryStore()

    def save(self, collection: str, symbol: str, payload: dict[str, Any], *, confidence: str | float = "media") -> dict[str, Any]:
        clean_symbol = str(symbol or payload.get("symbol") or "").upper().strip()
        clean = {
            "symbol": clean_symbol,
            "collection": collection,
            "broker_touched": False,
            "order_executed": False,
            **sanitize_payload(payload or {}),
        }
        if hasattr(self.memory, "save_mt5_event"):
            return self.memory.save_mt5_event(collection, clean_symbol, clean, "mt5_bridge", confidence)
        return self.memory.save_event(_event_type(collection), clean, "mt5_bridge", confidence)


def _event_type(collection: str) -> str:
    mapping = {
        "mt5_signals": "mt5_signal",
        "mt5_decisions": "mt5_decision",
        "mt5_order_requests": "mt5_order_request",
        "mt5_order_results": "mt5_order_result",
        "mt5_backtest_runs": "mt5_backtest_run",
        "mt5_forward_tests": "mt5_forward_test",
        "mt5_risk_blocks": "mt5_risk_block",
        "mt5_journal": "mt5_journal",
        "mt5_account_sync": "mt5_account_sync",
    }
    return mapping.get(collection, collection.rstrip("s") or "mt5_event")

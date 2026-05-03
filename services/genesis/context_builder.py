from __future__ import annotations

from typing import Any

from services.genesis.memory_store import MemoryStore


def build_context(question: str, tickers: list[str] | None = None, memory: MemoryStore | None = None) -> dict[str, Any]:
    store = memory or MemoryStore()
    return {
        "question": str(question or "").strip(),
        "tickers": tickers or [],
        "preferences": store.get_user_preferences(),
        "recent_memory": store.get_recent_events(10),
    }


from __future__ import annotations

from typing import Any

from services.genesis.memory_store import MemoryStore


def record_genesis_event(event_type: str, payload: dict[str, Any] | None = None, source: str = "genesis", confidence: str | float = "media") -> dict[str, Any]:
    return MemoryStore().save_event(event_type, payload or {}, source=source, confidence=confidence)


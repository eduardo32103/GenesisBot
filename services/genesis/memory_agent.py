from __future__ import annotations

from typing import Any

from services.genesis.memory_store import MemoryStore


class MemoryAgent:
    def __init__(self, store: MemoryStore | None = None) -> None:
        self.store = store or MemoryStore()

    def remember_event(self, event_type: str, payload: dict[str, Any], source: str = "genesis", confidence: str | float = "media") -> dict[str, Any]:
        return self.store.save_event(event_type, payload, source, confidence)

    def recent(self, limit: int = 20, event_type: str | None = None) -> list[dict[str, Any]]:
        return self.store.get_recent_events(limit, event_type)

    def relevant(self, query: str) -> list[dict[str, Any]]:
        return self.store.get_relevant_memory(query)

    def remember_message(self, conversation_id: str, role: str, content: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.store.save_message(conversation_id, role, content, metadata)

    def recent_messages(self, conversation_id: str = "default", limit: int = 20) -> list[dict[str, Any]]:
        return self.store.get_recent_messages(conversation_id, limit)

    def summary(self, query: str = "") -> dict[str, Any]:
        return self.store.get_memory_summary(query)


def get_memory_agent(store: MemoryStore | None = None) -> MemoryAgent:
    return MemoryAgent(store)

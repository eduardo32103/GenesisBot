from __future__ import annotations

from services.genesis.memory_store import MemoryStore
from services.genesis.whale_learning import learn_whale_events


class WhaleAgent:
    def activity(self, ticker: str | None = None, memory: MemoryStore | None = None) -> dict:
        return learn_whale_events(ticker, memory=memory)


def get_whale_agent() -> WhaleAgent:
    return WhaleAgent()

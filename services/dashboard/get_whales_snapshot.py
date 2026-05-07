from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.genesis.memory_store import MemoryStore
from services.genesis.whale_learning import learn_whale_events


def get_whales_snapshot(ticker: str | None = None) -> dict[str, Any]:
    learned = learn_whale_events(ticker, memory=MemoryStore())
    events = learned.get("events") if isinstance(learned.get("events"), list) else []
    confirmed = [event for event in events if event.get("event_type") == "whale_confirmed" and event.get("entity_name") and event.get("amount_usd") is not None]
    estimated = [event for event in events if event not in confirmed]
    return {
        "ok": True,
        "kind": "whale_flow",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "events": events,
        "confirmed": confirmed,
        "estimated": estimated,
        "summary": learned.get("summary") or {},
        "answer": learned.get("answer") or "",
        "memory": learned.get("memory") or [],
        "policy": "confirmed_whale requiere entidad y monto; smart_money_estimate usa volumen y flujo sin inventar instituciones.",
    }

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from services.genesis.memory_store import MemoryStore
from services.genesis.whale_learning import learn_whale_events

_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_WHALES_TTL_SECONDS = 10 * 60


def get_whales_snapshot(ticker: str | None = None) -> dict[str, Any]:
    cache_key = str(ticker or "__all__").upper()
    now = time.monotonic()
    cached = _CACHE.get(cache_key)
    if cached and now - cached[0] <= _WHALES_TTL_SECONDS:
        return {**cached[1], "cache_hit": True}
    started = time.monotonic()
    learned = learn_whale_events(ticker, memory=MemoryStore())
    events = learned.get("events") if isinstance(learned.get("events"), list) else []
    confirmed = [
        event
        for event in events
        if event.get("event_type") == "whale_confirmed"
        and event.get("entity_name")
        and event.get("confirmed_amount_usd") is not None
    ]
    estimated = [event for event in events if event not in confirmed]
    payload = {
        "ok": True,
        "kind": "whale_flow",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "events": events,
        "confirmed": confirmed,
        "estimated": estimated,
        "summary": learned.get("summary") or {},
        "answer": learned.get("answer") or "",
        "memory": learned.get("memory") or [],
        "source_status": {
            "source": "whale_learning",
            "status": "ok" if events else "empty",
            "elapsed_ms": int((time.monotonic() - started) * 1000),
            "cache_hit": False,
            "count": len(events),
        },
        "cache_hit": False,
        "policy": "confirmed_whale requiere entidad y monto; smart_money_estimate usa volumen y flujo sin inventar instituciones.",
    }
    _CACHE[cache_key] = (now, payload)
    return payload

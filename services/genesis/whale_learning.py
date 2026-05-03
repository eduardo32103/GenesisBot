from __future__ import annotations

from typing import Any

from services.dashboard.get_money_flow_causal_snapshot import get_money_flow_causal_snapshot
from services.dashboard.get_money_flow_detection_snapshot import get_money_flow_detection_snapshot
from services.genesis.memory_store import MemoryStore
from services.genesis.ticker_parser import normalize_ticker


def learn_whale_events(ticker: str | None = None, memory: MemoryStore | None = None) -> dict[str, Any]:
    store = memory or MemoryStore()
    normalized = normalize_ticker(ticker or "")
    rows = _extract_rows(get_money_flow_causal_snapshot()) + _extract_rows(get_money_flow_detection_snapshot())
    learned = 0
    for row in rows:
        row_ticker = normalize_ticker(row.get("ticker") or row.get("symbol"))
        if normalized and row_ticker != normalized:
            continue
        whale = row.get("whale") if isinstance(row.get("whale"), dict) else {}
        entity = whale.get("entity") or row.get("whale_entity") or ""
        if not entity:
            store.save_market_observation(row_ticker or normalized, "Ballena vigilada sin entidad institucional confirmada.")
            continue
        store.save_whale_event(
            row_ticker,
            entity=entity,
            action=whale.get("movement_type") or row.get("direction") or "No confirmado",
            amount=whale.get("movement_value") or row.get("amount_usd") or "",
            date=row.get("money_flow_timestamp") or row.get("timestamp") or "",
            confidence=whale.get("confidence") or row.get("confidence") or "media",
        )
        learned += 1
    return {
        "learned": learned,
        "memory": store.get_whale_memory(normalized or None),
        "answer": "Sin ballena institucional confirmada con la fuente activa." if learned == 0 else f"{learned} eventos de ballenas guardados.",
    }


def _extract_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    rows: list[dict[str, Any]] = []
    for key in ("items", "signals", "events"):
        if isinstance(payload.get(key), list):
            rows.extend([row for row in payload[key] if isinstance(row, dict)])
    for nested_key in ("causal", "detection"):
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            rows.extend(_extract_rows(nested))
    return rows


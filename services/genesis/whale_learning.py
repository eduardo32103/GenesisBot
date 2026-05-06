from __future__ import annotations

from typing import Any

from app.settings import load_settings
from integrations.fmp.client import FmpClient
from services.dashboard.get_money_flow_causal_snapshot import get_money_flow_causal_snapshot
from services.dashboard.get_money_flow_detection_snapshot import get_money_flow_detection_snapshot
from services.genesis.memory_store import MemoryStore
from services.genesis.ticker_parser import normalize_ticker


def learn_whale_events(ticker: str | None = None, memory: MemoryStore | None = None) -> dict[str, Any]:
    store = memory or MemoryStore()
    normalized = normalize_ticker(ticker or "")
    rows = _extract_rows(get_money_flow_causal_snapshot()) + _extract_rows(get_money_flow_detection_snapshot())
    rows.extend(_fmp_rows(normalized, rows))
    learned = 0
    watched_without_entity: set[str] = set()
    events: list[dict[str, Any]] = []
    for row in rows:
        row_ticker = normalize_ticker(row.get("ticker") or row.get("symbol"))
        if normalized and row_ticker != normalized:
            continue
        whale = row.get("whale") if isinstance(row.get("whale"), dict) else {}
        entity = whale.get("entity") or row.get("whale_entity") or ""
        if not entity:
            watched_without_entity.add(row_ticker or normalized or "MERCADO")
            continue
        event = _shape_event(row_ticker, entity, row)
        events.append(event)
        store.save_whale_event(
            row_ticker,
            entity=entity,
            action=event["action"],
            amount=event["amount_usd"] if event["amount_usd"] is not None else event["amount"] or "",
            date=event["date"],
            confidence=event["confidence"],
        )
        learned += 1
    for row_ticker in watched_without_entity:
        store.save_market_observation(row_ticker, "Ballena vigilada sin entidad institucional confirmada.")
    memory = store.get_whale_memory(normalized or None)
    summary = _summary(events)
    if learned == 0:
        scope = f" para {normalized}" if normalized else ""
        answer = f"Sin ballena institucional confirmada{scope} con la fuente activa."
        if watched_without_entity:
            answer += " Deje la observacion en memoria como vigilancia de baja confianza, sin inventar entidad ni monto."
    else:
        answer = f"{learned} eventos de ballenas guardados con entidad, fecha o monto reportado."
    return {
        "learned": learned,
        "memory": memory,
        "events": events[:20],
        "summary": summary,
        "unconfirmed_watch": sorted(watched_without_entity),
        "fallback": learned == 0,
        "answer": answer,
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


def _fmp_rows(normalized: str, existing_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    try:
        settings = load_settings()
    except Exception:
        return []
    if not getattr(settings, "fmp_api_key", "") or not getattr(settings, "fmp_live_enabled", False):
        return []
    tickers: list[str] = [normalized] if normalized else []
    if not tickers:
        for row in existing_rows:
            ticker = normalize_ticker(row.get("ticker") or row.get("symbol"))
            if ticker and ticker not in tickers:
                tickers.append(ticker)
    if not tickers:
        try:
            from services.dashboard.get_radar_snapshot import get_radar_snapshot

            snapshot = get_radar_snapshot()
            for row in _extract_rows(snapshot):
                ticker = normalize_ticker(row.get("ticker") or row.get("symbol"))
                if ticker and ticker not in tickers:
                    tickers.append(ticker)
        except Exception:
            pass
    client = FmpClient(settings.fmp_api_key)
    rows: list[dict[str, Any]] = []
    for ticker in tickers[:8]:
        for item in client.get_smart_money_activity(ticker, limit=5) or []:
            entity = str(item.get("entity") or "").strip()
            if not entity:
                continue
            rows.append(
                {
                    "ticker": ticker,
                    "whale_entity": entity,
                    "direction": item.get("type") or "unknown",
                    "amount_usd": item.get("value"),
                    "amount": item.get("shares"),
                    "timestamp": item.get("date"),
                    "source": item.get("source") or "fmp",
                    "confidence": "medium",
                }
            )
    return rows


def _shape_event(ticker: str, entity: str, row: dict[str, Any]) -> dict[str, Any]:
    whale = row.get("whale") if isinstance(row.get("whale"), dict) else {}
    action = _normalize_action(whale.get("movement_type") or row.get("direction") or row.get("type"))
    amount = row.get("amount") or whale.get("shares") or row.get("shares")
    amount_usd = _num(whale.get("movement_value") or row.get("amount_usd") or row.get("value"))
    date = str(row.get("money_flow_timestamp") or row.get("timestamp") or row.get("date") or "").strip()
    source = str(row.get("source") or whale.get("source") or "fmp").strip()
    confidence = str(whale.get("confidence") or row.get("confidence") or "medium").strip().lower()
    confidence = {"alta": "high", "media": "medium", "baja": "low"}.get(confidence, confidence)
    if confidence not in {"low", "medium", "high"}:
        confidence = "medium"
    impact = "bullish" if action in {"buy", "accumulation"} else "bearish" if action in {"sell", "reduction"} else "neutral"
    return {
        "id": f"whale:{ticker}:{entity}:{date or 'sin_fecha'}",
        "ticker": ticker,
        "entity": entity,
        "action": action,
        "amount": amount,
        "amount_usd": amount_usd,
        "date": date,
        "source": source,
        "confidence": confidence,
        "impact": impact,
        "genesis_reading": _event_reading(ticker, entity, action, confidence),
    }


def _normalize_action(value: object) -> str:
    raw = str(value or "").strip().casefold()
    if any(word in raw for word in ("buy", "compra", "acumula", "acquisition")):
        return "buy"
    if any(word in raw for word in ("sell", "venta", "reduce", "disposition")):
        return "sell"
    if "accum" in raw:
        return "accumulation"
    if "reduct" in raw:
        return "reduction"
    if "transfer" in raw:
        return "transfer"
    return "unknown"


def _event_reading(ticker: str, entity: str, action: str, confidence: str) -> str:
    if action in {"buy", "accumulation"}:
        return f"{entity} aparece acumulando {ticker}; Genesis lo trata como apoyo solo si precio y volumen confirman."
    if action in {"sell", "reduction"}:
        return f"{entity} aparece reduciendo {ticker}; Genesis lo trata como riesgo si coincide con deterioro tecnico."
    return f"Movimiento institucional detectado en {ticker}; falta clasificar direccion con mayor confianza."


def _summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    signed_flow = 0.0
    accumulation: list[str] = []
    distribution: list[str] = []
    for event in events:
        amount = _num(event.get("amount_usd")) or 0.0
        action = event.get("action")
        ticker = normalize_ticker(event.get("ticker"))
        if action in {"buy", "accumulation"}:
            signed_flow += amount
            if ticker and ticker not in accumulation:
                accumulation.append(ticker)
        elif action in {"sell", "reduction"}:
            signed_flow -= amount
            if ticker and ticker not in distribution:
                distribution.append(ticker)
    top_assets = []
    seen = set()
    for event in events:
        ticker = normalize_ticker(event.get("ticker"))
        if ticker and ticker not in seen:
            seen.add(ticker)
            top_assets.append(ticker)
    return {
        "net_flow": round(signed_flow, 2) if signed_flow else None,
        "accumulation": accumulation[:8],
        "distribution": distribution[:8],
        "top_assets": top_assets[:8],
        "confidence": "medium" if events else "low",
    }


def _num(value: object) -> float | None:
    try:
        if value in (None, "", "None"):
            return None
        return float(value)
    except Exception:
        return None

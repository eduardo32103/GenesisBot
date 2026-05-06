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
    confirmed = 0
    watched_without_entity: set[str] = set()
    seen_event_ids: set[str] = set()
    events: list[dict[str, Any]] = []
    for row in rows:
        row_ticker = normalize_ticker(row.get("ticker") or row.get("symbol"))
        if normalized and row_ticker != normalized:
            continue
        whale = row.get("whale") if isinstance(row.get("whale"), dict) else {}
        entity = whale.get("entity") or row.get("whale_entity") or ""
        if not entity:
            watched_without_entity.add(row_ticker or normalized or "MERCADO")
            event = _shape_estimate_event(row_ticker or normalized or "MERCADO", row)
            if event["id"] not in seen_event_ids:
                seen_event_ids.add(event["id"])
                events.append(event)
                store.save_whale_event(
                    event["ticker"],
                    entity="",
                    action=event["action"],
                    amount=event["amount_usd"] if event["amount_usd"] is not None else event.get("estimated_value"),
                    date=event["date"],
                    confidence=event["confidence"],
                    event=event,
                )
                learned += 1
            continue
        event = _shape_event(row_ticker, entity, row)
        if event["id"] in seen_event_ids:
            continue
        seen_event_ids.add(event["id"])
        events.append(event)
        store.save_whale_event(
            row_ticker,
            entity=entity,
            action=event["action"],
            amount=event["amount_usd"] if event["amount_usd"] is not None else event["amount"] or "",
            date=event["date"],
            confidence=event["confidence"],
            event=event,
        )
        learned += 1
        confirmed += 1
    for row_ticker in watched_without_entity:
        store.save_market_observation(row_ticker, "Ballena vigilada sin entidad institucional confirmada.")
    memory = store.get_whale_memory(normalized or None)
    summary = _summary(events)
    if not events:
        scope = f" para {normalized}" if normalized else ""
        answer = f"Sin ballena institucional confirmada{scope} con la fuente activa."
        if watched_without_entity:
            answer += " Genesis vigila flujo institucional, volumen anormal y acumulacion/distribucion sin inventar entidad ni monto."
    elif confirmed == 0:
        focus = ", ".join(summary.get("top_assets") or []) or (normalized or "mercado")
        answer = (
            f"En sencillo: no hay ballena institucional confirmada con nombre y monto, "
            f"pero Genesis detecta flujo en vigilancia en {focus}. No confirma compra directa; sirve para priorizar vigilancia."
        )
    else:
        answer = f"{confirmed} eventos de ballenas guardados con entidad, fecha o monto reportado."
    return {
        "learned": learned,
        "confirmed": confirmed,
        "memory": memory,
        "events": events[:20],
        "summary": summary,
        "unconfirmed_watch": sorted(watched_without_entity),
        "fallback": not events,
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
    units = _num(amount)
    price = _num(row.get("price") or row.get("transaction_price") or whale.get("price"))
    current_price = _num(row.get("current_price") or row.get("reference_price") or row.get("price"))
    estimated_value = amount_usd if amount_usd is not None else (units * (price or current_price) if units is not None and (price or current_price) else None)
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
        "asset_name": str(row.get("name") or row.get("asset_name") or ticker),
        "asset_type": str(row.get("asset_type") or "equity"),
        "event_type": "whale_confirmed" if entity and (amount_usd is not None or amount) else "institutional_flow",
        "entity": entity,
        "entity_name": entity,
        "entity_type": str(row.get("entity_type") or whale.get("entity_type") or "institution"),
        "action": action,
        "amount": amount,
        "units": units,
        "price": price,
        "amount_usd": amount_usd,
        "current_price": current_price,
        "estimated_value": estimated_value,
        "date": date,
        "source": source,
        "confidence": confidence,
        "impact": impact,
        "evidence": _evidence(row),
        "genesis_reading": _event_reading(ticker, entity, action, confidence),
    }


def _shape_estimate_event(ticker: str, row: dict[str, Any]) -> dict[str, Any]:
    action = _estimate_action(row)
    confidence = _estimate_confidence(row)
    date = str(row.get("money_flow_timestamp") or row.get("timestamp") or row.get("date") or "").strip()
    source = str(row.get("source") or "market_flow").strip()
    current_price = _num(row.get("current_price") or row.get("reference_price") or row.get("price"))
    amount_usd = _num(row.get("amount_usd") or row.get("estimated_value") or row.get("value"))
    event_type = "unusual_volume" if _has_unusual_volume(row) else "smart_money_estimate"
    direction = "alcista" if action in {"buy", "accumulation"} else "bajista" if action in {"sell", "distribution", "reduction"} else "neutral"
    return {
        "id": f"flow:{ticker}:{event_type}:{action}:{date or 'sin_fecha'}",
        "ticker": ticker,
        "asset_name": str(row.get("name") or row.get("asset_name") or ticker),
        "asset_type": "crypto" if str(ticker).endswith("-USD") else str(row.get("asset_type") or "market"),
        "event_type": event_type,
        "entity": "",
        "entity_name": "",
        "entity_type": "",
        "action": action,
        "amount": None,
        "units": None,
        "price": current_price,
        "amount_usd": amount_usd,
        "current_price": current_price,
        "estimated_value": amount_usd,
        "date": date,
        "source": source if source not in {"", "runtime"} else "technical",
        "confidence": confidence,
        "impact": "bullish" if direction == "alcista" else "bearish" if direction == "bajista" else "neutral",
        "evidence": _evidence(row),
        "genesis_reading": (
            f"{ticker}: flujo institucional en vigilancia ({direction}). "
            "No hay entidad confirmada; Genesis lo usa como senal secundaria, no como compra directa."
        ),
    }


def _normalize_action(value: object) -> str:
    raw = str(value or "").strip().casefold()
    if any(word in raw for word in ("buy", "compra", "acumula", "acquisition")):
        return "buy"
    if any(word in raw for word in ("sell", "venta", "reduce", "disposition", "distribution")):
        return "sell"
    if "accum" in raw:
        return "accumulation"
    if "reduct" in raw:
        return "reduction"
    if "transfer" in raw:
        return "transfer"
    return "unknown"


def _estimate_action(row: dict[str, Any]) -> str:
    raw = " ".join(
        str(row.get(key) or "")
        for key in (
            "direction",
            "signal",
            "primary_signal",
            "primary_label",
            "probable_cause_label",
            "attention",
            "summary",
            "event",
        )
    ).casefold()
    if any(token in raw for token in ("inflow", "entrada", "acumul", "compra", "buy", "positivo", "alcista")):
        return "accumulation"
    if any(token in raw for token in ("outflow", "salida", "distrib", "venta", "sell", "negativo", "bajista")):
        return "distribution"
    return "unknown"


def _estimate_confidence(row: dict[str, Any]) -> str:
    raw = str(row.get("confidence") or row.get("confidence_label") or row.get("level") or "").casefold()
    if any(token in raw for token in ("high", "alta", "fuerte")):
        return "medium"
    if any(token in raw for token in ("low", "baja", "debil")):
        return "low"
    return "low"


def _has_unusual_volume(row: dict[str, Any]) -> bool:
    raw = " ".join(str(row.get(key) or "") for key in ("signal", "primary_signal", "primary_label", "summary", "event")).casefold()
    return any(token in raw for token in ("volume", "volumen", "unusual", "anomalo", "anormal"))


def _evidence(row: dict[str, Any]) -> dict[str, Any]:
    allowed = (
        "ticker",
        "symbol",
        "direction",
        "signal",
        "primary_signal",
        "primary_label",
        "probable_cause_label",
        "attention",
        "source",
        "timestamp",
        "date",
        "amount_usd",
        "value",
        "shares",
    )
    return {key: row.get(key) for key in allowed if row.get(key) not in (None, "", [])}


def _event_reading(ticker: str, entity: str, action: str, confidence: str) -> str:
    if action in {"buy", "accumulation"}:
        return f"{entity} aparece acumulando {ticker}; Genesis lo trata como apoyo solo si precio y volumen confirman."
    if action in {"sell", "reduction"}:
        return f"{entity} aparece reduciendo {ticker}; Genesis lo trata como riesgo si coincide con deterioro tecnico."
    return f"Movimiento institucional detectado en {ticker}; falta clasificar direccion con mayor confianza."


def _summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    signed_flow = 0.0
    inflow = 0.0
    outflow = 0.0
    total_estimated = 0.0
    accumulation: list[str] = []
    distribution: list[str] = []
    for event in events:
        amount = _num(event.get("amount_usd") or event.get("estimated_value")) or 0.0
        total_estimated += amount
        action = event.get("action")
        ticker = normalize_ticker(event.get("ticker"))
        if action in {"buy", "accumulation"}:
            signed_flow += amount
            inflow += amount
            if ticker and ticker not in accumulation:
                accumulation.append(ticker)
        elif action in {"sell", "reduction", "distribution"}:
            signed_flow -= amount
            outflow += amount
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
        "inflow_value": round(inflow, 2) if inflow else None,
        "outflow_value": round(outflow, 2) if outflow else None,
        "total_estimated_value": round(total_estimated, 2) if total_estimated else None,
        "accumulation": accumulation[:8],
        "distribution": distribution[:8],
        "top_assets": top_assets[:8],
        "confidence": "medium" if events else "low",
        "confirmed_count": len([event for event in events if event.get("entity_name")]),
        "estimated_count": len([event for event in events if not event.get("entity_name")]),
    }


def _num(value: object) -> float | None:
    try:
        if value in (None, "", "None"):
            return None
        return float(value)
    except Exception:
        return None

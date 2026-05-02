from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from app.settings import load_settings
from integrations.fmp.client import FmpClient
from services.dashboard.get_money_flow_causal_snapshot import get_money_flow_causal_snapshot
from services.dashboard.get_money_flow_detection_snapshot import get_money_flow_detection_snapshot

_STOP_WORDS = {
    "QUE",
    "PASA",
    "PASANDO",
    "CON",
    "ESTE",
    "ESTA",
    "ACTIVO",
    "FLUJO",
    "CAPITAL",
    "DINERO",
    "GRANDE",
    "VIENDO",
    "AHORA",
    "MONEY",
    "FLOW",
    "SENAL",
    "CAUSA",
    "PROBABLE",
    "HAY",
    "EN",
    "DE",
    "SEGUN",
}

_HONESTY_NOTE = "Lectura probable y conservadora; no confirma causa final, institucionalidad ni compra/venta."


def get_money_flow_jarvis_answer(question: str = "") -> dict[str, Any]:
    detection = get_money_flow_detection_snapshot()
    causal = get_money_flow_causal_snapshot()
    items = _merge_items(detection, causal)
    requested_ticker = _extract_requested_ticker(question, items)
    premium_activity = _load_premium_smart_money_activity(requested_ticker) if requested_ticker else []
    scoped_items = [item for item in items if item["ticker"] == requested_ticker] if requested_ticker else items[:3]

    if requested_ticker and premium_activity:
        base_answer = _answer_for_item(scoped_items[0]) if scoped_items else f"{requested_ticker}: lectura de flujo local no concluyente."
        answer = f"{base_answer} {_format_premium_activity(premium_activity)}"
    elif requested_ticker and not scoped_items:
        answer = f"No encontre {requested_ticker} en Ballenas local. Lectura: no concluyente."
    elif not scoped_items:
        answer = "No hay activos con lectura Money Flow disponible. Lectura: no concluyente."
    elif requested_ticker:
        answer = _answer_for_item(scoped_items[0])
    else:
        answer = _answer_global(scoped_items)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "phase": "5.5",
        "status": "jarvis_money_flow_ready",
        "question": str(question or "").strip(),
        "matched_ticker": requested_ticker,
        "answer": answer,
        "items": scoped_items,
        "premium_activity": premium_activity,
        "source_status": {
            "detection_status": str(detection.get("status") or "unknown"),
            "causal_status": str(causal.get("status") or "unknown"),
            "fmp_live_queries_enabled": bool(premium_activity),
        },
        "honesty_note": _HONESTY_NOTE,
    }


def _load_premium_smart_money_activity(ticker: str) -> list[dict[str, Any]]:
    settings = load_settings()
    if not settings.fmp_live_enabled or not settings.fmp_api_key or not ticker:
        return []
    client = FmpClient(settings.fmp_api_key)
    return client.get_smart_money_activity(ticker, limit=5)


def _format_premium_activity(activity: list[dict[str, Any]]) -> str:
    if not activity:
        return ""
    first = activity[0]
    entity = str(first.get("entity") or "").strip() or "entidad no confirmada"
    source = str(first.get("source") or "fuente premium").strip()
    movement = str(first.get("type") or "movimiento").strip()
    value = first.get("value") or first.get("shares") or "monto no confirmado"
    date = str(first.get("date") or "").strip() or "fecha no confirmada"
    return (
        f"Dato premium observado: {source}, {entity}, {movement}, valor/cantidad {value}, fecha {date}. "
        "Genesis lo trata como evidencia adicional, no como causalidad garantizada."
    )


def _merge_items(detection: dict[str, Any], causal: dict[str, Any]) -> list[dict[str, Any]]:
    detection_by_ticker = {
        str(item.get("ticker") or "").strip().upper(): item
        for item in detection.get("items") or []
        if isinstance(item, dict) and str(item.get("ticker") or "").strip()
    }
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()

    for causal_item in causal.get("items") or []:
        if not isinstance(causal_item, dict):
            continue
        ticker = str(causal_item.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        merged.append(_normalize_item({**detection_by_ticker.get(ticker, {}), **causal_item}))
        seen.add(ticker)

    for ticker, item in detection_by_ticker.items():
        if ticker not in seen:
            merged.append(_normalize_item(item))

    return sorted(merged, key=_priority_key)


def _normalize_item(item: dict[str, Any]) -> dict[str, Any]:
    signal = str(item.get("money_flow_primary_signal") or item.get("primary_signal") or "insufficient_confirmation").strip()
    cause = str(item.get("probable_cause") or "").strip()
    non_conclusive = signal == "insufficient_confirmation" or cause == "inconclusive"
    whale = item.get("whale") if isinstance(item.get("whale"), dict) else {}
    whale_entity = str(
        whale.get("entity")
        or item.get("entity")
        or item.get("institution")
        or item.get("fund")
        or item.get("holder")
        or item.get("insider")
        or ""
    ).strip()
    whale_identified = bool(whale.get("identified") or whale_entity)
    return {
        "ticker": str(item.get("ticker") or "").strip().upper(),
        "signal": signal,
        "signal_label": str(item.get("money_flow_primary_label") or item.get("primary_label") or signal).strip(),
        "flow_detected": bool(item.get("flow_detected", signal != "insufficient_confirmation")),
        "whale_identified": whale_identified,
        "whale_entity": whale_entity,
        "whale_note": str(whale.get("note") or "Flujo detectado, sin ballena identificada.").strip(),
        "movement_value": str(whale.get("movement_value") or item.get("movement_value") or item.get("amount_usd") or "").strip(),
        "probable_cause": cause or "inconclusive",
        "probable_cause_label": str(item.get("probable_cause_label") or "No concluyente").strip(),
        "confidence": _format_confidence(item.get("confidence")),
        "timestamp": str(item.get("money_flow_timestamp") or item.get("timestamp") or "").strip(),
        "context": str(item.get("reason") or item.get("context_note") or "").strip(),
        "attention": "no concluyente" if non_conclusive else "merece atencion",
    }


def _priority_key(item: dict[str, Any]) -> tuple[int, str]:
    if item["attention"] == "merece atencion" and item["confidence"] in {"Alta", "Media"}:
        return (0, item["ticker"])
    if item["attention"] == "merece atencion":
        return (1, item["ticker"])
    return (2, item["ticker"])


def _extract_requested_ticker(question: str, items: list[dict[str, Any]]) -> str:
    available = {item["ticker"] for item in items if item.get("ticker")}
    tokens = [token.upper() for token in re.findall(r"\b[A-Za-z][A-Za-z0-9=\-]{1,8}\b", str(question or ""))]
    for token in tokens:
        if token in available:
            return token
    for token in tokens:
        if token not in _STOP_WORDS:
            return token
    return ""


def _answer_for_item(item: dict[str, Any]) -> str:
    flow_text = "Flujo detectado" if item.get("flow_detected") else "Flujo no concluyente"
    signal_text = (
        f"{item['signal']} ({item['signal_label']})"
        if item.get("signal_label") and item["signal_label"] != item["signal"]
        else item["signal"]
    )
    whale_text = (
        f"Ballena identificada: {item['whale_entity']}."
        if item.get("whale_identified")
        else "Ballena identificada: no tengo entidad confirmada."
    )
    amount_text = f"Monto: {item['movement_value']}." if item.get("movement_value") else "Monto: no confirmado."
    when_text = f"Fecha util: {item['timestamp']}." if item.get("timestamp") else "Fecha util: no confirmada."
    if item["attention"] == "no concluyente":
        return (
            f"{item['ticker']} queda no concluyente. {flow_text}: {signal_text}. "
            f"{whale_text} {amount_text} {when_text} "
            f"Causa probable: {item['probable_cause_label']}. Confiabilidad: {item['confidence']}. "
            "Faltan datos suficientes para elevar la lectura."
        )
    return (
        f"{item['ticker']} merece atencion. {flow_text}: {signal_text}. "
        f"{whale_text} {amount_text} {when_text} "
        f"Causa probable: {item['probable_cause_label']}. Confiabilidad: {item['confidence']}. "
        "No es recomendacion de compra o venta."
    )


def _answer_global(items: list[dict[str, Any]]) -> str:
    actionable = [item for item in items if item["attention"] == "merece atencion"]
    identified_whales = [item for item in items if item.get("whale_identified")]
    if actionable:
        tickers = ", ".join(item["ticker"] for item in actionable[:3])
        first = actionable[0]
        whale_text = (
            f"Ballenas identificadas: {', '.join(item['whale_entity'] for item in identified_whales[:3])}."
            if identified_whales
            else "Ballenas identificadas: ninguna entidad confirmada."
        )
        return (
            f"Atencion primero en {tickers}. Flujo detectado: {first['ticker']} con {first['signal']} ({first['signal_label']}). "
            f"{whale_text} Causa probable: {first['probable_cause_label']}. Confiabilidad: {first['confidence']}."
        )
    tickers = ", ".join(item["ticker"] for item in items[:3])
    return (
        f"No hay Money Flow concluyente en {tickers}. "
        "Ballenas identificadas: ninguna entidad confirmada. Mantener vigilancia; faltan confirmaciones reales."
    )


def _format_confidence(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "high":
        return "Alta"
    if normalized == "medium":
        return "Media"
    if normalized == "low":
        return "Baja"
    return "No concluyente"

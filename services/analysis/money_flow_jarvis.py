from __future__ import annotations

import html
import re
from typing import Any

from services.dashboard.get_money_flow_causal_snapshot import get_money_flow_causal_snapshot
from services.dashboard.get_money_flow_detection_snapshot import get_money_flow_detection_snapshot

_MONEY_FLOW_TRIGGERS = (
    "money flow",
    "flujo de capital",
    "flujo capital",
    "movimiento relevante",
    "movimientos relevantes",
)

_STOP_WORDS = {
    "MONEY",
    "FLOW",
    "FLUJO",
    "CAPITAL",
    "MOVIMIENTO",
    "MOVIMIENTOS",
    "RELEVANTE",
    "RELEVANTES",
    "DE",
    "DEL",
    "LA",
    "EL",
    "EN",
    "PARA",
    "VER",
}


def should_handle_money_flow_intent(text: str) -> bool:
    normalized = _normalize_text(text)
    return any(trigger in normalized for trigger in _MONEY_FLOW_TRIGGERS) or normalized.startswith("money_flow")


def extract_money_flow_ticker(text: str) -> str:
    cleaned = str(text or "").replace("/", " ")
    for token in re.findall(r"\b[A-Za-z][A-Za-z0-9=\-]{0,8}\b", cleaned):
        candidate = token.strip().upper()
        if candidate not in _STOP_WORDS and len(candidate) >= 2:
            return candidate
    return ""


def build_money_flow_jarvis_briefing(text: str = "", *, limit: int = 4) -> str:
    requested_ticker = extract_money_flow_ticker(text)
    detection = get_money_flow_detection_snapshot()
    causal = get_money_flow_causal_snapshot()
    items = _merge_items(detection, causal, requested_ticker=requested_ticker)

    if not items:
        ticker_note = f" para {_escape(requested_ticker)}" if requested_ticker else ""
        return _lines_to_html(
            "Flujo de Capital",
            [
                f"No encontre lectura Money Flow{ticker_note} en los snapshots actuales.",
                "Lectura: no concluyente.",
                "No se afirma causa confirmada ni recomendacion operativa.",
            ],
        )

    visible = items[: max(1, int(limit or 4))]
    lines = [
        "Lectura basada en Money Flow 5.2 + causa probable 5.3.",
        f"Activos evaluados: {_escape((causal.get('summary') or {}).get('total_assets', len(items)))}.",
    ]
    for item in visible:
        lines.extend(_format_item_lines(item))

    lines.extend(
        [
            "",
            "Regla de honestidad: probable / compatible con / no concluyente.",
            "No confirma causa final, no afirma institucionalidad y no es orden de compra o venta.",
        ]
    )
    return _lines_to_html("Flujo de Capital", lines)


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip().lower())


def _merge_items(detection: dict[str, Any], causal: dict[str, Any], *, requested_ticker: str = "") -> list[dict[str, Any]]:
    detection_by_ticker = {
        str(item.get("ticker") or "").strip().upper(): item
        for item in detection.get("items") or []
        if isinstance(item, dict) and str(item.get("ticker") or "").strip()
    }
    merged: list[dict[str, Any]] = []
    for causal_item in causal.get("items") or []:
        if not isinstance(causal_item, dict):
            continue
        ticker = str(causal_item.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        if requested_ticker and ticker != requested_ticker:
            continue
        merged.append({**detection_by_ticker.get(ticker, {}), **causal_item})

    if requested_ticker and not merged and requested_ticker in detection_by_ticker:
        merged.append(detection_by_ticker[requested_ticker])

    return sorted(merged, key=_item_priority)


def _item_priority(item: dict[str, Any]) -> tuple[int, str]:
    signal = str(item.get("money_flow_primary_signal") or item.get("primary_signal") or "").strip()
    cause = str(item.get("probable_cause") or "").strip()
    if signal != "insufficient_confirmation" and cause and cause != "inconclusive":
        return (0, str(item.get("ticker") or ""))
    if signal != "insufficient_confirmation":
        return (1, str(item.get("ticker") or ""))
    return (2, str(item.get("ticker") or ""))


def _format_item_lines(item: dict[str, Any]) -> list[str]:
    ticker = str(item.get("ticker") or "Sin ticker").strip().upper()
    signal = str(item.get("money_flow_primary_signal") or item.get("primary_signal") or "insufficient_confirmation").strip()
    signal_label = str(item.get("money_flow_primary_label") or item.get("primary_label") or "confirmacion insuficiente").strip()
    cause = _format_cause(item.get("probable_cause_label") or item.get("probable_cause") or "no concluyente")
    confidence = _format_confidence(item.get("confidence"))
    timestamp = str(item.get("money_flow_timestamp") or item.get("timestamp") or "Sin timestamp").strip()
    reason = str(item.get("reason") or item.get("context_note") or "").strip()
    non_conclusive = signal == "insufficient_confirmation" or str(item.get("probable_cause") or "") == "inconclusive"
    attention = "Queda no concluyente; faltan datos suficientes." if non_conclusive else "Merece atencion, sin convertirlo en accion automatica."

    lines = [
        "",
        f"<b>{_escape(ticker)}</b>",
        f"- Senal: <b>{_escape(signal)}</b> | {_escape(signal_label)}",
        f"- Causa probable: {_escape(cause)}",
        f"- Confiabilidad: {_escape(confidence)} | Timestamp: {_escape(timestamp)}",
        f"- Lectura: {_escape(attention)}",
    ]
    if reason:
        lines.append(f"- Contexto: {_escape(_truncate(reason, 160))}")
    return lines


def _format_confidence(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "high":
        return "Alta"
    if normalized == "medium":
        return "Media"
    if normalized == "low":
        return "Baja"
    return "No concluyente"


def _format_cause(value: Any) -> str:
    cause = str(value or "").strip()
    if cause.lower() == "no concluyente":
        return "No concluyente"
    return cause or "No concluyente"


def _lines_to_html(title: str, lines: list[str]) -> str:
    return "\n".join([f"<b>{_escape(title)}</b>", "--------------------", *lines])


def _escape(value: Any) -> str:
    return html.escape(str(value or ""), quote=False)


def _truncate(value: str, limit: int) -> str:
    clean = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(clean) <= limit:
        return clean
    return clean[:limit].rsplit(" ", 1)[0].strip() + "..."

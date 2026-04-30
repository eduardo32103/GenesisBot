from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.settings import load_settings
from services.dashboard.get_macro_activity_snapshot import get_macro_activity_snapshot
from services.dashboard.get_money_flow_detection_snapshot import get_money_flow_detection_snapshot
from services.dashboard.get_radar_ticker_drilldown import _build_context_note, _fetch_related_alerts

_CAUSE_TYPES = (
    "earnings",
    "macro",
    "geopolitics",
    "etf_index_proxy",
    "news",
    "hedging",
    "correlation",
    "inconclusive",
)

_CAUSE_PRIORITY = {
    "earnings": 0,
    "macro": 1,
    "geopolitics": 2,
    "etf_index_proxy": 3,
    "news": 4,
    "hedging": 5,
    "correlation": 6,
    "inconclusive": 99,
}

_KEYWORDS = {
    "earnings": (
        "earnings",
        "resultados",
        "guidance",
        "eps",
        "ingresos",
        "trimestral",
        "reporte",
    ),
    "macro": (
        "macro",
        "fed",
        "tasas",
        "inflacion",
        "inflation",
        "cpi",
        "pce",
        "jobs",
        "empleo",
        "dolar",
        "dollar",
        "yield",
        "yields",
        "recesion",
    ),
    "geopolitics": (
        "geo",
        "geopolit",
        "guerra",
        "conflicto",
        "sancion",
        "sanciones",
        "energia",
        "petroleo",
        "oil",
        "crudo",
        "opep",
        "opec",
    ),
    "news": (
        "news",
        "noticia",
        "titular",
        "headline",
        "sentinel",
    ),
    "hedging": (
        "hedge",
        "hedging",
        "cobertura",
        "proteccion",
        "put",
        "opciones",
        "volatilidad",
        "vix",
    ),
    "correlation": (
        "correlacion",
        "correlation",
        "spread",
        "pares",
        "divergencia",
    ),
}

_HONESTY_RULES = [
    "La lectura es causal probable, no causalidad confirmada.",
    "No se afirma institucionalidad ni compra/venta de grandes jugadores.",
    "No se convierte en recomendacion de compra o venta.",
    "Si falta contexto real asociado, la causa queda como no concluyente.",
    "Las causas se apoyan solo en senales Money Flow, alertas, macro y snapshots persistidos.",
]


def _normalize_ticker(value: Any) -> str:
    return str(value or "").strip().upper()


def _compact_text(*values: Any) -> str:
    return " ".join(str(value or "").strip() for value in values if str(value or "").strip()).lower()


def _matches(text: str, cause_type: str) -> list[str]:
    keywords = _KEYWORDS.get(cause_type, ())
    return [keyword for keyword in keywords if keyword in text]


def _detected_flow_signals(item: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        signal
        for signal in item.get("signals") or []
        if isinstance(signal, dict)
        and bool(signal.get("detected"))
        and str(signal.get("type") or "").strip() != "insufficient_confirmation"
    ]


def _context_text(related_alerts: list[dict[str, Any]], macro: dict[str, Any]) -> str:
    alert_parts: list[str] = []
    for alert in related_alerts:
        if not isinstance(alert, dict):
            continue
        alert_parts.append(
            _compact_text(
                alert.get("alert_type_label"),
                alert.get("title"),
                alert.get("summary"),
                alert.get("source"),
                alert.get("status_label"),
                alert.get("validation"),
                alert.get("result"),
            )
        )

    macro_payload = macro.get("macro") if isinstance(macro.get("macro"), dict) else {}
    headlines = macro_payload.get("headlines") if isinstance(macro_payload.get("headlines"), list) else []
    headline_text = " ".join(
        _compact_text(item.get("title"), item.get("impact_summary"), item.get("source"))
        for item in headlines
        if isinstance(item, dict)
    )
    return _compact_text(
        " ".join(alert_parts),
        macro_payload.get("summary"),
        macro_payload.get("dominant_risk"),
        macro_payload.get("bias_label"),
        headline_text,
    )


def _candidate(
    cause_type: str,
    *,
    label: str,
    reason: str,
    evidence: dict[str, Any],
    confidence: str = "low",
) -> dict[str, Any]:
    return {
        "cause_type": cause_type,
        "label": label,
        "confidence": confidence,
        "language": label,
        "reason": reason,
        "evidence": evidence,
    }


def _inconclusive_reading(item: dict[str, Any], reason: str, *, context_note: str = "") -> dict[str, Any]:
    return {
        "probable_cause": "inconclusive",
        "probable_cause_label": "no concluyente",
        "confidence": "low",
        "language": "no concluyente",
        "reason": reason,
        "evidence": {
            "primary_signal": str(item.get("primary_signal") or "").strip(),
            "detected_signal_count": int(item.get("detected_signal_count") or 0),
        },
        "candidates": [],
        "context_note": context_note or "Sin contexto causal persistido suficiente.",
        "honesty_note": "No hay evidencia real suficiente para asignar una causa probable.",
    }


def _macro_supports_ticker(ticker: str, macro: dict[str, Any]) -> bool:
    macro_payload = macro.get("macro") if isinstance(macro.get("macro"), dict) else {}
    if not bool(macro_payload.get("available", False)):
        return False
    high_risk = {str(value or "").strip().upper() for value in macro_payload.get("high_risk_tickers", [])}
    sensitive = {str(value or "").strip().upper() for value in macro_payload.get("sensitive_tickers", [])}
    return bool(ticker and (ticker in high_risk or ticker in sensitive))


def _append_keyword_candidate(
    candidates: list[dict[str, Any]],
    cause_type: str,
    text: str,
    *,
    label: str,
    reason: str,
    evidence_source: str,
) -> None:
    matched = _matches(text, cause_type)
    if not matched:
        return
    candidates.append(
        _candidate(
            cause_type,
            label=label,
            reason=reason,
            evidence={"source": evidence_source, "matched_keywords": matched[:5]},
            confidence="medium",
        )
    )


def _build_causal_reading(item: dict[str, Any], related_alerts: list[dict[str, Any]], macro: dict[str, Any]) -> dict[str, Any]:
    ticker = _normalize_ticker(item.get("ticker"))
    flow_signals = _detected_flow_signals(item)
    flow_types = {str(signal.get("type") or "").strip() for signal in flow_signals}
    context_note = _build_context_note(related_alerts)

    if not flow_signals:
        return _inconclusive_reading(
            item,
            "La deteccion Money Flow no tiene senal suficiente para explicar causa probable.",
            context_note=context_note,
        )

    text = _context_text(related_alerts, macro)
    candidates: list[dict[str, Any]] = []

    _append_keyword_candidate(
        candidates,
        "earnings",
        text,
        label="compatible con reaccion a earnings",
        reason="El contexto persistido menciona resultados, guidance o reporte financiero.",
        evidence_source="related_alerts_or_macro_snapshot",
    )
    _append_keyword_candidate(
        candidates,
        "geopolitics",
        text,
        label="compatible con riesgo geopolitico",
        reason="El contexto persistido menciona energia, conflicto, sanciones o riesgo geopolitico.",
        evidence_source="related_alerts_or_macro_snapshot",
    )
    _append_keyword_candidate(
        candidates,
        "hedging",
        text,
        label="compatible con cobertura",
        reason="El contexto persistido menciona cobertura, volatilidad u opciones.",
        evidence_source="related_alerts_or_macro_snapshot",
    )
    _append_keyword_candidate(
        candidates,
        "correlation",
        text,
        label="consistente con correlacion",
        reason="El contexto persistido menciona correlacion, spread o divergencia.",
        evidence_source="related_alerts_or_macro_snapshot",
    )

    macro_payload = macro.get("macro") if isinstance(macro.get("macro"), dict) else {}
    macro_available = bool(macro_payload.get("available", False))
    macro_matches = _matches(text, "macro")
    if "risk_on_risk_off" in flow_types or _macro_supports_ticker(ticker, macro) or macro_matches:
        candidates.append(
            _candidate(
                "macro",
                label="consistente con contexto macro",
                reason="La senal Money Flow convive con contexto macro persistido o proxy de riesgo.",
                evidence={
                    "macro_available": macro_available,
                    "macro_source": str((macro.get("meta") or {}).get("macro_source") or "unknown"),
                    "ticker_listed_in_macro_snapshot": _macro_supports_ticker(ticker, macro),
                    "matched_keywords": macro_matches[:5],
                    "flow_signals": sorted(flow_types),
                },
                confidence="medium" if macro_available else "low",
            )
        )

    if flow_types.intersection({"sector_pressure", "risk_on_risk_off", "rotation"}):
        candidates.append(
            _candidate(
                "etf_index_proxy",
                label="compatible con ETF, indice o proxy sectorial",
                reason="La deteccion incluye presion sectorial, risk-on/risk-off o rotacion ya calculada.",
                evidence={"flow_signals": sorted(flow_types.intersection({"sector_pressure", "risk_on_risk_off", "rotation"}))},
                confidence="medium",
            )
        )

    if related_alerts and (_matches(text, "news") or any(str(alert.get("title") or "").strip() for alert in related_alerts)):
        candidates.append(
            _candidate(
                "news",
                label="compatible con noticia relevante",
                reason="Hay alerta relacionada con titulo, resumen o fuente persistida.",
                evidence={
                    "alert_count": len(related_alerts),
                    "latest_alert_id": str(related_alerts[0].get("alert_id") or "").strip(),
                    "latest_alert_created_at": str(related_alerts[0].get("created_at") or "").strip(),
                },
                confidence="low",
            )
        )

    if flow_types.intersection({"price_volume_divergence", "rotation"}):
        candidates.append(
            _candidate(
                "correlation",
                label="consistente con correlacion o divergencia",
                reason="La deteccion incluye divergencia volumen/precio o rotacion entre grupos.",
                evidence={"flow_signals": sorted(flow_types.intersection({"price_volume_divergence", "rotation"}))},
                confidence="medium",
            )
        )

    if not candidates:
        return _inconclusive_reading(
            item,
            "Hay senal Money Flow, pero no hay contexto real suficiente para asignar causa probable.",
            context_note=context_note,
        )

    candidates = sorted(candidates, key=lambda value: _CAUSE_PRIORITY.get(str(value.get("cause_type") or ""), 99))
    primary = candidates[0]
    return {
        "probable_cause": primary["cause_type"],
        "probable_cause_label": primary["label"],
        "confidence": primary["confidence"],
        "language": primary["language"],
        "reason": primary["reason"],
        "evidence": primary["evidence"],
        "candidates": candidates[:4],
        "context_note": context_note,
        "honesty_note": "Lectura causal probable; no confirma causa final ni institucionalidad.",
    }


def _build_item(item: dict[str, Any], database_url: str, macro: dict[str, Any]) -> dict[str, Any]:
    ticker = _normalize_ticker(item.get("ticker"))
    related_alerts = _fetch_related_alerts(database_url, ticker, limit=3)
    reading = _build_causal_reading(item, related_alerts, macro)
    return {
        "ticker": ticker,
        "money_flow_primary_signal": str(item.get("primary_signal") or "").strip(),
        "money_flow_primary_label": str(item.get("primary_label") or "").strip(),
        "money_flow_timestamp": str(item.get("timestamp") or "").strip(),
        "related_alerts_count": len(related_alerts),
        "related_alerts": related_alerts,
        "whale": item.get("whale") if isinstance(item.get("whale"), dict) else {
            "identified": False,
            "entity": "",
            "movement_value": "",
            "movement_type": "",
            "source": "",
            "confidence": "no concluyente",
            "note": "Flujo detectado, sin ballena identificada.",
        },
        **reading,
    }


def get_money_flow_causal_snapshot() -> dict[str, Any]:
    detection = get_money_flow_detection_snapshot()
    macro = get_macro_activity_snapshot()
    settings = load_settings()
    database_url = getattr(settings, "database_url", "") or ""
    items = [
        _build_item(item, database_url, macro)
        for item in detection.get("items") or []
        if isinstance(item, dict)
    ]

    assets_with_probable_cause = sum(1 for item in items if item.get("probable_cause") != "inconclusive")
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "phase": "5.3",
        "name": "Capa causal probable Money Flow",
        "status": "probable_causality_ready",
        "summary": {
            "total_assets": len(items),
            "assets_with_probable_cause": assets_with_probable_cause,
            "assets_inconclusive": len(items) - assets_with_probable_cause,
            "probable_causal_layer_enabled": True,
            "causality_confirmed": False,
            "institutional_claims_enabled": False,
            "recommendation_enabled": False,
            "fmp_live_queries_enabled": False,
            "note": "Cruza deteccion Money Flow con contexto persistido; usa lenguaje probable y conserva no concluyente cuando falta evidencia.",
        },
        "cause_types": list(_CAUSE_TYPES),
        "items": items,
        "source_status": {
            "money_flow_detection_status": str(detection.get("status") or "unknown"),
            "macro_source": str((macro.get("meta") or {}).get("macro_source") or "unknown"),
            "activity_source": str((macro.get("meta") or {}).get("activity_source") or "unknown"),
            "alert_context_source": "alert_events" if database_url else "unavailable",
            "database_configured": bool(database_url),
            "fmp_live_queries_enabled": False,
        },
        "honesty_rules": _HONESTY_RULES,
    }

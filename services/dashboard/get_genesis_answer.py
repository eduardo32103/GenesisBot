from __future__ import annotations

import json
import logging
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any

from services.dashboard.get_alerts_snapshot import get_alerts_snapshot
from services.dashboard.get_executive_queue_snapshot import get_executive_queue_snapshot
from services.dashboard.get_fmp_dependencies_snapshot import get_fmp_dependencies_snapshot
from services.dashboard.get_macro_activity_snapshot import get_macro_activity_snapshot
from services.dashboard.get_money_flow_jarvis_answer import get_money_flow_jarvis_answer
from services.dashboard.get_operational_health import get_operational_health
from services.dashboard.get_operational_reliability_snapshot import get_operational_reliability_snapshot
from services.dashboard.get_radar_snapshot import get_radar_snapshot
from services.dashboard.get_radar_ticker_drilldown import get_dashboard_radar_ticker_drilldown

_LOGGER = logging.getLogger("genesis.dashboard.genesis")
_HONESTY_NOTE = "Respuesta local y conservadora. No confirma causalidad, institucionalidad ni compra/venta."
_SNAPSHOT_FALLBACK_ANSWER = (
    "No pude leer los snapshots activos. Puedo darte una lectura general, "
    "pero no confirmar datos del panel ahora."
)
_DEGRADED_SOURCE_ANSWER = (
    "La fuente esta degradada. Puedo orientar con el contexto disponible, "
    "pero no elevar la lectura a confiable."
)
_TICKER_STOPWORDS = {
    "QUE",
    "CON",
    "COMO",
    "CUAL",
    "CUANDO",
    "DONDE",
    "ESTA",
    "ESTAN",
    "PASA",
    "PASANDO",
    "LEE",
    "LEER",
    "SALUD",
    "SISTEMA",
    "GENESIS",
    "RADAR",
    "ALERTA",
    "ALERTAS",
    "FLUJO",
    "CAPITAL",
    "DINERO",
    "ESTADO",
    "ACTIVO",
    "ACTIVOS",
    "ANALISIS",
    "ANALIZA",
    "ANALIZAR",
    "CONTRA",
    "DATOS",
    "DICE",
    "DICEN",
    "DIRECTO",
    "DIRECTOS",
    "DISPONIBLES",
    "GRANDE",
    "AHORA",
    "VIENDO",
    "OPINA",
    "OPINAS",
    "OPINION",
    "COMPARA",
    "COMPARAR",
    "VERSUS",
    "REVISA",
    "REVISAR",
    "MUNDO",
    "MACRO",
}
_TECHNICAL_TRANSLATIONS = {
    "alerts_origin": "origen de alertas",
    "causal": "causalidad probable",
    "degraded": "datos parciales",
    "detection": "deteccion Money Flow",
    "detection_ready_causality_disabled": "deteccion lista; causalidad no confirmada",
    "Faltan credenciales de Telegram en el entorno.": "Hay una dependencia legacy sin configurar en el entorno.",
    "fallback": "lectura de respaldo",
    "fmp_status": "estado del proveedor",
    "health_status": "salud del sistema",
    "panel_context": "contexto del panel",
    "queue_source": "cola ejecutiva",
    "radar_drilldown_decision_layer": "lectura del radar y cola ejecutiva",
    "snapshot_failure": "snapshots no disponibles",
    "snapshots": "snapshots activos",
    "ticker_not_found": "ticker sin datos suficientes",
    "unavailable": "sin datos disponibles",
    "available": "disponible",
    "unknown": "sin dato",
}


def get_genesis_answer(
    question: str = "",
    context: str = "general",
    ticker: str = "",
    panel_context: Any | None = None,
) -> dict[str, Any]:
    clean_question = str(question or "").strip()
    clean_panel_context = _normalize_panel_context(panel_context)
    detected_tickers = _detect_tickers_from_question(clean_question)
    detected_ticker = detected_tickers[0] if detected_tickers else ""
    requested_context = _normalize_context(context or "general")
    panel_scope = _normalize_context(clean_panel_context.get("scope") or "general")
    clean_context = panel_scope if requested_context == "general" and panel_scope != "general" else requested_context
    if detected_ticker and clean_context == "general":
        clean_context = "ticker"
    clean_ticker = str(detected_ticker or ticker or clean_panel_context.get("ticker") or "").strip().upper()
    intent = _resolve_intent(clean_question, clean_context)
    is_comparison = intent == "asset_priority" and _is_comparison_question(clean_question) and len(detected_tickers) >= 2

    try:
        if intent == "system":
            answer, evidence, source_status = _answer_system()
        elif intent == "asset_priority":
            if is_comparison:
                answer, evidence, source_status = _answer_ticker_comparison(detected_tickers[:2])
            else:
                answer, evidence, source_status = _answer_asset_priority(clean_ticker)
        elif intent == "money_flow":
            answer, evidence, source_status = _answer_money_flow(clean_question, clean_ticker)
        elif intent == "alerts":
            answer, evidence, source_status = _answer_alerts()
        elif intent == "macro":
            answer, evidence, source_status = _answer_macro()
        elif intent == "reliability":
            answer, evidence, source_status = _answer_reliability()
        else:
            answer, evidence, source_status = _answer_overview()
    except Exception:
        _LOGGER.exception("Genesis fallback activated while building answer")
        return get_genesis_fallback_answer(
            clean_question,
            context=clean_context,
            ticker=clean_ticker,
            panel_context=clean_panel_context,
            reason="snapshot_failure",
        )

    evidence = _compact_evidence([*evidence, *_panel_evidence(clean_panel_context, intent)])
    source_status = {
        **source_status,
        "panel_context": "provided" if clean_panel_context else "empty",
    }
    blocks = _build_response_blocks(answer, evidence, source_status, clean_context, intent, clean_panel_context)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "phase": "6.5.E",
        "status": "genesis_assistant_ready",
        "question": clean_question,
        "intent": intent,
        "context": {
            "scope": clean_context,
            "ticker": clean_ticker,
            "tickers": detected_tickers,
            "active_view": clean_panel_context.get("active_view") or "",
            "label": clean_panel_context.get("label") or "",
            "signals": {
                "radar": clean_panel_context.get("radar") or {},
                "alerts": clean_panel_context.get("alerts") or {},
                "money_flow": clean_panel_context.get("money_flow") or {},
                "reliability": clean_panel_context.get("reliability") or {},
                "executive_queue": clean_panel_context.get("executive_queue") or {},
            },
        },
        "answer": answer,
        "blocks": blocks,
        "evidence": evidence[:4],
        "source_status": source_status,
        "honesty_note": _HONESTY_NOTE,
    }


def get_genesis_fallback_answer(
    question: str = "",
    context: str = "general",
    ticker: str = "",
    panel_context: Any | None = None,
    reason: str = "snapshot_failure",
) -> dict[str, Any]:
    clean_question = str(question or "").strip()
    clean_panel_context = _normalize_panel_context(panel_context)
    detected_tickers = _detect_tickers_from_question(clean_question)
    detected_ticker = detected_tickers[0] if detected_tickers else ""
    requested_context = _normalize_context(context or "general")
    panel_scope = _normalize_context(clean_panel_context.get("scope") or "general")
    clean_context = panel_scope if requested_context == "general" and panel_scope != "general" else requested_context
    if detected_ticker and clean_context == "general":
        clean_context = "ticker"
    clean_ticker = str(detected_ticker or ticker or clean_panel_context.get("ticker") or "").strip().upper()
    intent = _resolve_intent(clean_question, clean_context)
    answer = _fallback_answer_for_reason(reason, clean_ticker)
    evidence = _compact_evidence(
        [
            "Snapshots activos no confirmados.",
            "Lectura no concluyente hasta recuperar evidencia del panel.",
            *_panel_evidence(clean_panel_context, intent),
        ]
    )
    source_status = {
        "reliability": "no concluyente",
        "snapshots": "degraded",
        "panel_context": "provided" if clean_panel_context else "empty",
    }
    blocks = _build_response_blocks(answer, evidence, source_status, clean_context, intent, clean_panel_context)
    blocks["reliability"] = "no concluyente"
    blocks["next_step"] = "Reintentar cuando el panel confirme snapshots o revisar solo los datos visibles."

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "phase": "6.5.E",
        "status": "genesis_assistant_ready",
        "question": clean_question,
        "intent": intent,
        "context": {
            "scope": clean_context,
            "ticker": clean_ticker,
            "tickers": detected_tickers,
            "active_view": clean_panel_context.get("active_view") or "",
            "label": clean_panel_context.get("label") or "",
            "signals": {
                "radar": clean_panel_context.get("radar") or {},
                "alerts": clean_panel_context.get("alerts") or {},
                "money_flow": clean_panel_context.get("money_flow") or {},
                "reliability": clean_panel_context.get("reliability") or {},
                "executive_queue": clean_panel_context.get("executive_queue") or {},
            },
        },
        "answer": answer,
        "blocks": blocks,
        "evidence": evidence[:4],
        "source_status": source_status,
        "honesty_note": _HONESTY_NOTE,
    }


def _fallback_answer_for_reason(reason: str, ticker: str = "") -> str:
    normalized = _normalize(reason)
    if ticker and "ticker" in normalized:
        return f"{ticker}: no tengo datos suficientes dentro del panel. La lectura queda no concluyente."
    if "degrad" in normalized:
        return _DEGRADED_SOURCE_ANSWER
    return _SNAPSHOT_FALLBACK_ANSWER


def _detect_ticker_from_question(question: str) -> str:
    tickers = _detect_tickers_from_question(question)
    return tickers[0] if tickers else ""


def _detect_tickers_from_question(question: str) -> list[str]:
    normalized_question = _normalize(question).upper()
    tickers: list[str] = []
    for raw in re.findall(r"\b[A-Z][A-Z0-9.]{1,9}\b", normalized_question):
        token = raw.strip().upper().rstrip(".")
        if token in _TICKER_STOPWORDS:
            continue
        if any(char.isdigit() for char in token) and not any(char.isalpha() for char in token):
            continue
        if 2 <= len(token) <= 10 and token not in tickers:
            tickers.append(token)
    return tickers


def _is_comparison_question(question: str) -> bool:
    text = f" {_normalize(question)} "
    return any(token in text for token in (" compara ", " comparar ", " contra ", " versus ", " vs "))


def _normalize(value: str) -> str:
    normalized = unicodedata.normalize("NFD", str(value or "").casefold())
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn")


def _normalize_context(value: str) -> str:
    normalized = _normalize(value).replace("-", "_").replace(" ", "_")
    if normalized in {"ticker", "radar", "alerts", "money_flow", "general", "reliability", "executive_queue", "macro"}:
        return normalized
    if normalized in {"alertas"}:
        return "alerts"
    if normalized in {"flujo_de_capital", "flujo"}:
        return "money_flow"
    if normalized in {"confiabilidad", "confianza"}:
        return "reliability"
    if normalized in {"cola_ejecutiva", "prioridad_global"}:
        return "executive_queue"
    if normalized in {"mundo", "macro", "noticias", "geopolitica"}:
        return "macro"
    return "general"


def _resolve_intent(question: str, context: str = "general") -> str:
    text = _normalize(question)
    if any(token in text for token in ("flujo", "capital", "money flow", "senal money", "dinero grande", "ballena", "ballenas")):
        return "money_flow"
    if any(token in text for token in ("alerta", "alertas", "evento", "eventos")):
        return "alerts"
    if any(token in text for token in ("macro", "mundo", "noticia", "noticias", "geopolit", "geopolitica")):
        return "macro"
    if any(token in text for token in ("confiable", "confiabilidad", "confianza", "fiable")):
        return "reliability"
    if _is_comparison_question(question):
        return "asset_priority"
    if any(token in text for token in ("sistema", "salud", "estado", "runtime")):
        return "system"
    if any(token in text for token in ("activo", "revisar", "prioridad", "mirar primero", "que mirar")):
        return "asset_priority"
    if context == "money_flow":
        return "money_flow"
    if context == "alerts":
        return "alerts"
    if context == "reliability":
        return "reliability"
    if context == "macro":
        return "macro"
    if context == "executive_queue":
        return "asset_priority"
    if context in {"ticker", "radar"}:
        return "asset_priority"
    return "overview"


def _normalize_panel_context(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = {}
    if not isinstance(value, dict):
        return {}

    return {
        "active_view": _safe_context_text(value.get("active_view"), 40),
        "scope": _normalize_context(str(value.get("scope") or "general")),
        "label": _safe_context_text(value.get("label"), 50),
        "ticker": _safe_context_text(value.get("ticker"), 20).upper(),
        "radar": _compact_context_section(value.get("radar")),
        "alerts": _compact_context_section(value.get("alerts")),
        "money_flow": _compact_context_section(value.get("money_flow")),
        "reliability": _compact_context_section(value.get("reliability")),
        "executive_queue": _compact_context_section(value.get("executive_queue")),
    }


def _compact_context_section(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    compacted: dict[str, str] = {}
    for key, raw in value.items():
        clean_key = _safe_context_text(key, 36)
        clean_value = _safe_context_text(raw, 120)
        if clean_key and clean_value:
            compacted[clean_key] = clean_value
    return compacted


def _safe_context_text(value: Any, max_length: int) -> str:
    return " ".join(str(value or "").split())[:max_length]


def _panel_evidence(panel_context: dict[str, Any], intent: str) -> list[str]:
    if not panel_context:
        return []

    if intent == "money_flow":
        section = panel_context.get("money_flow") or {}
        return [section.get("summary", ""), section.get("detected", "")]
    if intent == "alerts":
        section = panel_context.get("alerts") or {}
        return [section.get("summary", ""), f"Alertas recientes: {section.get('total_recent', '')}"]
    if intent == "reliability":
        section = panel_context.get("reliability") or {}
        return [section.get("level", ""), section.get("decision", "")]
    if intent == "asset_priority":
        radar = panel_context.get("radar") or {}
        queue = panel_context.get("executive_queue") or {}
        return [radar.get("selected_ticker", ""), queue.get("review_now", "")]

    reliability = panel_context.get("reliability") or {}
    queue = panel_context.get("executive_queue") or {}
    alerts = panel_context.get("alerts") or {}
    return [reliability.get("level", ""), queue.get("total", ""), alerts.get("total_recent", "")]


def _build_response_blocks(
    answer: str,
    evidence: list[str],
    source_status: dict[str, str],
    context: str,
    intent: str,
    panel_context: dict[str, Any],
) -> dict[str, Any]:
    reliability = _resolve_reliability_label(source_status, panel_context)
    signals = _resolve_main_signals(evidence, source_status, panel_context, intent)
    risks = _resolve_risks(evidence, source_status, reliability)
    return {
        "summary": _short_sentence(answer, "Lectura no concluyente con los datos actuales."),
        "executive_read": _executive_read(intent, context, reliability),
        "main_signals": signals[:3],
        "risks": risks[:3],
        "money_flow": _money_flow_block(intent, source_status, evidence, panel_context),
        "reliability": reliability,
        "next_step": _next_step(intent, reliability),
    }


def _resolve_reliability_label(source_status: dict[str, str], panel_context: dict[str, Any]) -> str:
    raw = (
        source_status.get("reliability")
        or (panel_context.get("reliability") or {}).get("level")
        or source_status.get("health_status")
        or source_status.get("detection")
        or source_status.get("snapshots")
        or ""
    )
    normalized = _normalize(raw)
    if "no concluyente" in normalized:
        return "no concluyente"
    if "alta" in normalized or normalized in {"ok", "live", "healthy"}:
        return "alta"
    if "media" in normalized or "degrad" in normalized or "partial" in normalized:
        return "media"
    if "baja" in normalized or "unavailable" in normalized or "unknown" in normalized or "error" in normalized:
        return "baja"
    return "no concluyente"


def _resolve_main_signals(
    evidence: list[str],
    source_status: dict[str, str],
    panel_context: dict[str, Any],
    intent: str,
) -> list[str]:
    signals: list[str] = []
    section_by_intent = {
        "money_flow": panel_context.get("money_flow") or {},
        "alerts": panel_context.get("alerts") or {},
        "reliability": panel_context.get("reliability") or {},
        "asset_priority": panel_context.get("radar") or {},
        "system": panel_context.get("reliability") or {},
    }
    for value in (section_by_intent.get(intent) or {}).values():
        if value:
            signals.append(_humanize_text(value))
    for key in ("reliability", "alerts_origin", "queue_source", "detection", "causal", "health_status", "fmp_status", "snapshots"):
        signal = _human_source_status(key, source_status.get(key, ""))
        if signal:
            signals.append(signal)
    signals.extend(evidence)
    compacted = _compact_evidence(signals)
    return compacted or ["Sin senal suficiente en snapshots actuales."]


def _resolve_risks(evidence: list[str], source_status: dict[str, str], reliability: str) -> list[str]:
    risks: list[str] = []
    joined = _normalize(" ".join([*evidence, *(_humanize_text(value) for value in source_status.values()), reliability]))
    if reliability in {"baja", "no concluyente"}:
        risks.append("Confiabilidad limitada; conviene esperar confirmacion.")
    if any(token in joined for token in ("fallback", "unavailable", "unknown", "degrad", "insuficiente", "no concluyente")):
        risks.append("Hay datos incompletos o degradados en la lectura.")
    if source_status.get("panel_context") == "empty":
        risks.append("Falta contexto activo del panel para afinar la respuesta.")
    return _compact_evidence(risks) or ["Sin freno dominante visible, pero la lectura sigue siendo conservadora."]


def _money_flow_block(
    intent: str,
    source_status: dict[str, str],
    evidence: list[str],
    panel_context: dict[str, Any],
) -> str:
    if intent == "money_flow":
        clean = _compact_evidence(evidence)
        return clean[0] if clean else "Flujo detectado y ballenas separadas; sin entidad confirmada si no aparece en fuentes reales."
    section = panel_context.get("money_flow") or {}
    summary = _humanize_text(section.get("summary", ""))
    detected = _humanize_text(section.get("detected", ""))
    if summary:
        return summary
    if detected:
        return f"Dinero Grande reporta {detected}; confirmar entidad antes de hablar de ballena."
    if source_status.get("market_data") == "available":
        return "Datos directos disponibles para precio; sin ballena identificada en esta lectura."
    return "Sin lectura de ballena identificada; cualquier flujo queda no concluyente hasta confirmar entidad y monto."


def _executive_read(intent: str, context: str, reliability: str) -> str:
    if reliability in {"baja", "no concluyente"}:
        base = "Lectura util como orientacion, no como decision fuerte."
    else:
        base = "Lectura usable para priorizar revision."
    if intent == "money_flow":
        return f"{base} Flujo de Capital requiere confirmacion antes de concluir."
    if intent == "alerts":
        return f"{base} La prioridad sale de alertas visibles y su estado actual."
    if intent == "asset_priority":
        return f"{base} El activo se evalua contra radar y cola ejecutiva."
    if intent == "reliability":
        return f"{base} La confiabilidad manda el grado de cautela."
    if intent == "system":
        return f"{base} Primero valida salud operativa y dependencias."
    return f"{base} Contexto activo: {context}."


def _next_step(intent: str, reliability: str) -> str:
    if reliability in {"baja", "no concluyente"}:
        return "Esperar confirmacion o revisar la fuente limitada antes de actuar."
    if intent == "alerts":
        return "Revisar la alerta principal y su validacion."
    if intent == "money_flow":
        return "Revisar Flujo de Capital y buscar confirmacion adicional."
    if intent == "asset_priority":
        return "Validar el ticker en Radar antes de decidir."
    if intent == "reliability":
        return "Revisar componentes live, fallback y degradados."
    return "Usar la cola ejecutiva para decidir que revisar primero."


def _short_sentence(value: str, fallback: str) -> str:
    text = _humanize_text(_clean_sentence(value))
    if not text:
        return fallback
    parts = text.split(". ")
    return parts[0][:150].strip() or fallback


def _answer_overview() -> tuple[str, list[str], dict[str, str]]:
    reliability = _safe_dashboard_snapshot(
        get_operational_reliability_snapshot,
        _fallback_reliability_snapshot,
        "reliability",
    )
    queue = _safe_dashboard_snapshot(get_executive_queue_snapshot, _fallback_queue_snapshot, "executive_queue")
    alerts = _safe_dashboard_snapshot(get_alerts_snapshot, _fallback_alerts_snapshot, "alerts")
    rel = reliability.get("reliability") or {}
    summary = queue.get("summary") or {}
    review = ((queue.get("buckets") or {}).get("revisar ahora") or [])
    watch = ((queue.get("buckets") or {}).get("vigilar") or [])
    alert_summary = alerts.get("summary") or {}

    candidate = (review or watch or [None])[0]
    if candidate:
        answer = (
            f"Genesis esta {str(rel.get('level') or 'MEDIA').lower()} para decidir. "
            f"Primero miraria {candidate.get('ticker')}: {candidate.get('decision')}. "
            f"Alertas recientes: {alert_summary.get('total_recent', 0)}."
        )
        evidence = [str(candidate.get("main_reason") or ""), str(rel.get("summary") or "")]
    else:
        note = _clean_sentence(summary.get("note") or "sin prioridad clara")
        answer = (
            f"Panorama no concluyente: {note}. "
            f"Confiabilidad: {rel.get('level') or 'sin dato'}."
        )
        evidence = [str(rel.get("summary") or ""), str(alert_summary.get("engine_summary") or "")]

    return answer, _compact_evidence(evidence), {
        "reliability": str(rel.get("level") or "unknown"),
        "queue_source": str((queue.get("meta") or {}).get("source") or "unknown"),
        "alerts_origin": str(alert_summary.get("data_origin") or "unknown"),
        "snapshots": _snapshot_status(reliability, queue, alerts),
    }


def _answer_asset_priority(ticker: str = "") -> tuple[str, list[str], dict[str, str]]:
    if ticker:
        detail = _safe_dashboard_snapshot(
            lambda: get_dashboard_radar_ticker_drilldown(ticker),
            lambda: {"found": False, "ticker": ticker, "error": "ticker_not_found"},
            "radar_drilldown",
        )
        if detail.get("found"):
            return _answer_asset_drilldown(detail)

    queue = _safe_dashboard_snapshot(get_executive_queue_snapshot, _fallback_queue_snapshot, "executive_queue")
    buckets = queue.get("buckets") or {}
    all_items = [
        item
        for bucket in ("revisar ahora", "vigilar", "esperar", "no concluyente")
        for item in buckets.get(bucket, [])
        if isinstance(item, dict)
    ]
    candidate = next((item for item in all_items if str(item.get("ticker") or "").strip().upper() == ticker), None)
    if ticker and candidate is None:
        return (
            f"{ticker}: lectura no concluyente. No encuentro datos suficientes de ese ticker en los snapshots actuales.",
            ["El ticker no aparece en la cola ejecutiva actual.", "Conviene revisar Radar o Flujo de Capital y esperar confirmacion."],
            {"queue_source": "ticker_not_found", "snapshots": _snapshot_status(queue)},
        )
    if candidate is None:
        candidate = (buckets.get("revisar ahora") or buckets.get("vigilar") or buckets.get("esperar") or buckets.get("no concluyente") or [None])[0]

    if not candidate:
        return (
            "No hay un activo claro para priorizar. Lectura no concluyente.",
            ["La cola ejecutiva no devolvio activos visibles."],
            {"queue_source": str((queue.get("meta") or {}).get("source") or "unknown"), "snapshots": _snapshot_status(queue)},
        )

    answer = (
        f"Revisaria {candidate.get('ticker')}. "
        f"Decision: {candidate.get('decision')}; prioridad {candidate.get('priority')}. "
        f"Motivo: {_clean_sentence(candidate.get('main_reason') or 'sin motivo dominante')}."
    )
    evidence = [str(candidate.get("dominant_signal") or ""), str(candidate.get("current_reliability") or "")]
    return answer, _compact_evidence(evidence), {
        "queue_source": str((queue.get("meta") or {}).get("source") or "unknown"),
        "snapshots": _snapshot_status(queue),
    }


def _answer_ticker_comparison(tickers: list[str]) -> tuple[str, list[str], dict[str, str]]:
    details = []
    for ticker in tickers[:2]:
        normalized = str(ticker or "").strip().upper()
        if not normalized:
            continue
        detail = _safe_dashboard_snapshot(
            lambda normalized=normalized: get_dashboard_radar_ticker_drilldown(normalized),
            lambda normalized=normalized: {"found": False, "ticker": normalized, "error": "ticker_not_found"},
            f"radar_drilldown_{normalized}",
        )
        details.append(detail if isinstance(detail, dict) else {"found": False, "ticker": normalized})

    labels = [str(detail.get("ticker") or detail.get("symbol") or "").strip().upper() for detail in details]
    labels = [label for label in labels if label]
    summaries = []
    evidence = []
    any_price = False
    for detail in details:
        ticker = str(detail.get("ticker") or detail.get("symbol") or "").strip().upper()
        if not ticker:
            continue
        price = detail.get("current_price")
        decision = str(detail.get("decision") or "no concluyente").strip()
        reason = _clean_sentence(detail.get("main_reason") or detail.get("error") or "sin evidencia suficiente")
        if detail.get("found") and price is not None:
            summaries.append(f"{ticker}: precio actual {price} con datos directos; lectura {decision}")
            evidence.append(f"{ticker}: {reason}")
            any_price = True
        elif detail.get("found"):
            summaries.append(f"{ticker}: sin precio directo confirmado; lectura {decision}")
            evidence.append(f"{ticker}: {reason}")
        else:
            summaries.append(f"{ticker}: sin datos suficientes dentro del panel")
            evidence.append(f"{ticker}: no concluyente por falta de evidencia")

    title = " contra ".join(labels[:2]) if labels else "comparacion solicitada"
    answer = (
        f"Comparacion {title}: " + "; ".join(summaries) + ". "
        "No mezclo el contexto anterior; cada ticker queda evaluado con su propia ficha disponible."
    )
    return answer, _compact_evidence(evidence), {
        "reliability": "media" if any_price else "no concluyente",
        "queue_source": "comparacion de fichas tacticas",
        "market_data": "available" if any_price else "unavailable",
        "snapshots": _snapshot_status(*details),
    }


def _answer_asset_drilldown(detail: dict[str, Any]) -> tuple[str, list[str], dict[str, str]]:
    ticker = str(detail.get("ticker") or detail.get("symbol") or "").strip().upper()
    current_price = detail.get("current_price")
    decision = str(detail.get("decision") or "no concluyente").strip()
    reason = _clean_sentence(detail.get("main_reason") or "sin motivo dominante")
    profile = detail.get("profile") if isinstance(detail.get("profile"), dict) else {}
    market_data = detail.get("market_data") if isinstance(detail.get("market_data"), dict) else {}

    direct_data_available = bool(current_price is not None and (market_data.get("live_ready") or market_data.get("source") == "datos_directos"))
    if current_price is None:
        price_text = "sin precio actual confirmado"
    elif direct_data_available:
        price_text = f"precio actual {current_price} con datos directos"
    else:
        price_text = f"precio actual {current_price}"

    name = str(profile.get("name") or ticker).strip()
    profile_text = ""
    if profile.get("sector") or profile.get("industry"):
        profile_text = f" Perfil: {profile.get('sector') or 'sector sin dato'} / {profile.get('industry') or 'industria sin dato'}."

    answer = (
        f"{ticker}: {price_text}. "
        f"Lectura: {decision}. Motivo: {reason}."
        f"{profile_text}"
    )
    if name and name != ticker:
        answer = f"{name} ({ticker}): {price_text}. Lectura: {decision}. Motivo: {reason}.{profile_text}"

    evidence = [
        str(detail.get("dominant_signal") or ""),
        str(detail.get("reliability_note") or ""),
        str(detail.get("quote_timestamp") or ""),
        str(detail.get("alert_state_summary") or ""),
    ]
    if profile.get("sector"):
        evidence.append(f"Sector: {profile.get('sector')}")
    return answer, _compact_evidence(evidence), {
        "reliability": str(detail.get("current_reliability") or "no concluyente"),
        "queue_source": "radar_drilldown_decision_layer",
        "market_data": "available" if current_price is not None else "unavailable",
        "live_enabled": "available" if market_data.get("live_ready") else "unavailable",
        "snapshots": _snapshot_status(detail),
    }


def _answer_system() -> tuple[str, list[str], dict[str, str]]:
    health = _safe_dashboard_snapshot(get_operational_health, _fallback_health_snapshot, "health")
    reliability = _safe_dashboard_snapshot(
        get_operational_reliability_snapshot,
        _fallback_reliability_snapshot,
        "reliability",
    )
    fmp = _safe_dashboard_snapshot(get_fmp_dependencies_snapshot, _fallback_fmp_snapshot, "fmp")
    system = health.get("system") or {}
    bot = health.get("bot") or {}
    rel = reliability.get("reliability") or {}
    provider = fmp.get("provider") or {}

    answer = (
        f"Sistema {_human_status_label(system.get('status'))}. "
        f"Confiabilidad {rel.get('level') or 'sin dato'}: {rel.get('decision_note') or 'no concluyente'}. "
        f"Proveedor {_human_status_label(provider.get('status'))}."
    )
    evidence = [str(system.get("summary") or ""), str(bot.get("runtime_note") or ""), str(rel.get("summary") or "")]
    return answer, _compact_evidence(evidence), {
        "health_status": str(system.get("status") or "unknown"),
        "fmp_status": str(provider.get("status") or "unknown"),
        "snapshots": _snapshot_status(health, reliability, fmp),
    }


def _answer_reliability() -> tuple[str, list[str], dict[str, str]]:
    reliability = _safe_dashboard_snapshot(
        get_operational_reliability_snapshot,
        _fallback_reliability_snapshot,
        "reliability",
    )
    rel = reliability.get("reliability") or {}
    answer = (
        f"Confiabilidad {rel.get('level') or 'sin dato'}: {rel.get('decision_note') or 'no concluyente'}. "
        f"Datos directos {rel.get('live_count', 0)}, datos locales {rel.get('fallback_count', 0)}, datos parciales {rel.get('degraded_count', 0)}."
    )
    evidence = [str(rel.get("summary") or ""), ", ".join(rel.get("degraded_parts") or [])]
    return answer, _compact_evidence(evidence), {
        "reliability": str(rel.get("level") or "unknown"),
        "fmp": str(rel.get("fmp_status_label") or "unknown"),
        "snapshots": _snapshot_status(reliability),
    }


def _answer_alerts() -> tuple[str, list[str], dict[str, str]]:
    alerts = _safe_dashboard_snapshot(get_alerts_snapshot, _fallback_alerts_snapshot, "alerts")
    summary = alerts.get("summary") or {}
    recent = alerts.get("recent_alerts") or []

    if not recent:
        answer = f"Alertas no concluyentes: {summary.get('engine_summary') or 'sin alertas recientes'}"
        evidence = [str(summary.get("data_origin") or "unknown")]
    else:
        first = recent[0]
        ticker = first.get("ticker") or "sin ticker"
        answer = (
            f"Alerta a mirar: {ticker} / {first.get('alert_type_label') or first.get('alert_type')}. "
            f"Estado: {first.get('state_label') or first.get('status')}. Total reciente: {summary.get('total_recent', 0)}."
        )
        evidence = [str(first.get("summary") or first.get("title") or ""), str(summary.get("engine_summary") or "")]

    return answer, _compact_evidence(evidence), {
        "alerts_origin": str(summary.get("data_origin") or "unknown"),
        "total_recent": str(summary.get("total_recent", 0)),
        "snapshots": _snapshot_status(alerts),
    }


def _answer_macro() -> tuple[str, list[str], dict[str, str]]:
    payload = _safe_dashboard_snapshot(
        get_macro_activity_snapshot,
        lambda: {"macro": {"available": False}, "meta": {"macro_source": "unavailable"}},
        "macro",
    )
    macro = payload.get("macro") if isinstance(payload.get("macro"), dict) else {}
    if not bool(macro.get("available", False)):
        answer = (
            "Sin contexto macro activo. Genesis puede operar con datos del panel, "
            "pero no confirmar entorno macro, noticias o geopolitica ahora."
        )
        evidence = [
            "No hay snapshot macro/noticias activo.",
            "La lectura queda no concluyente para entorno externo.",
        ]
        return answer, evidence, {
            "reliability": "no concluyente",
            "macro": "unavailable",
            "snapshots": _snapshot_status(payload),
        }

    headlines = macro.get("headlines") if isinstance(macro.get("headlines"), list) else []
    risk = _clean_sentence(macro.get("dominant_risk") or macro.get("summary") or "sin riesgo dominante")
    answer = (
        f"Mundo/Macro: {macro.get('bias_label') or 'sesgo sin confirmar'}. "
        f"Riesgo principal: {risk}. Titulares utiles: {len(headlines)}. "
        f"Confiabilidad: {macro.get('confidence') or 0}%."
    )
    evidence = [
        str(macro.get("summary") or ""),
        *(str((item or {}).get("impact_summary") or (item or {}).get("title") or "") for item in headlines[:2]),
    ]
    return answer, _compact_evidence(evidence), {
        "reliability": "media" if int(macro.get("confidence") or 0) >= 50 else "no concluyente",
        "macro": "available",
        "snapshots": _snapshot_status(payload),
    }


def _answer_money_flow(question: str, ticker: str = "") -> tuple[str, list[str], dict[str, str]]:
    scoped_question = question or "flujo de capital"
    if ticker and ticker not in scoped_question.upper():
        scoped_question = f"{ticker} flujo de capital"
    try:
        payload = get_money_flow_jarvis_answer(scoped_question)
    except Exception:
        _LOGGER.warning("Genesis money flow source unavailable", exc_info=True)
        payload = {}
    if not isinstance(payload, dict) or not payload:
        return _DEGRADED_SOURCE_ANSWER, ["Flujo de Capital no devolvio contexto suficiente."], {
            "detection": "degraded",
            "causal": "no concluyente",
            "reliability": "no concluyente",
            "snapshots": "degraded",
        }
    source = payload.get("source_status") or {}
    evidence = [str(payload.get("honesty_note") or ""), *(str((item or {}).get("context") or "") for item in payload.get("items") or [])]
    return str(payload.get("answer") or "Flujo de Capital no concluyente."), _compact_evidence(evidence), {
        "detection": str(source.get("detection_status") or "unknown"),
        "causal": str(source.get("causal_status") or "unknown"),
    }


def _compact_evidence(values: list[str]) -> list[str]:
    compacted: list[str] = []
    for value in values:
        text = _humanize_text(" ".join(str(value or "").split()))
        if _is_low_signal_text(text):
            continue
        if text and text not in compacted:
            compacted.append(text[:160])
    return compacted


def _clean_sentence(value: Any) -> str:
    return " ".join(str(value or "").strip().rstrip(".").split())


def _human_source_status(key: str, value: Any) -> str:
    if key == "snapshots" and _normalize(value) == "available":
        return ""
    clean_value = _humanize_text(value)
    if not clean_value:
        return ""
    labels = {
        "alerts_origin": "Alertas",
        "causal": "Causalidad probable",
        "detection": "Money Flow",
        "fmp_status": "Proveedor",
        "health_status": "Sistema",
        "queue_source": "Cola ejecutiva",
        "reliability": "Confiabilidad",
        "snapshots": "Snapshots",
    }
    return f"{labels.get(key, 'Senal')}: {clean_value}"


def _humanize_text(value: Any) -> str:
    text = " ".join(str(value or "").split())
    if not text:
        return ""
    for raw, replacement in _TECHNICAL_TRANSLATIONS.items():
        text = re.sub(re.escape(raw), replacement, text, flags=re.IGNORECASE)
    text = text.replace("_", " ")
    return text


def _human_status_label(value: Any) -> str:
    normalized = _normalize(value)
    if any(token in normalized for token in ("degrad", "partial", "warning")):
        return "degradado"
    if normalized in {"ok", "live", "healthy", "ready", "online"}:
        return "estable"
    if any(token in normalized for token in ("unavailable", "unknown", "none", "missing")) or not normalized:
        return "sin dato suficiente"
    if "error" in normalized or "fail" in normalized:
        return "con error"
    return _humanize_text(value).lower()


def _is_low_signal_text(value: str) -> bool:
    normalized = _normalize(value)
    return (
        not normalized
        or normalized.isdigit()
        or normalized.startswith("cargando")
        or normalized in {"...", "sin dato", "pendiente"}
    )


def _safe_dashboard_snapshot(loader: Any, fallback_factory: Any, name: str) -> dict[str, Any]:
    try:
        payload = loader()
    except Exception:
        _LOGGER.warning("Genesis snapshot unavailable: %s", name, exc_info=True)
        payload = None
    if not isinstance(payload, dict):
        fallback = fallback_factory()
        fallback["_genesis_snapshot_status"] = "degraded"
        return fallback
    return payload


def _snapshot_status(*payloads: dict[str, Any]) -> str:
    for payload in payloads:
        if isinstance(payload, dict) and payload.get("_genesis_snapshot_status") == "degraded":
            return "degraded"
    return "available"


def _fallback_reliability_snapshot() -> dict[str, Any]:
    return {
        "reliability": {
            "level": "no concluyente",
            "decision_note": _DEGRADED_SOURCE_ANSWER,
            "summary": _SNAPSHOT_FALLBACK_ANSWER,
            "live_count": 0,
            "fallback_count": 0,
            "degraded_count": 1,
            "degraded_parts": ["snapshots activos"],
            "fmp_status_label": "sin dato suficiente",
        }
    }


def _fallback_queue_snapshot() -> dict[str, Any]:
    return {
        "summary": {"note": "No pude leer la cola ejecutiva activa."},
        "buckets": {"revisar ahora": [], "vigilar": [], "esperar": [], "no concluyente": []},
        "meta": {"source": "degraded"},
    }


def _fallback_alerts_snapshot() -> dict[str, Any]:
    return {
        "summary": {
            "total_recent": 0,
            "engine_summary": "No pude leer alertas activas desde el snapshot.",
            "data_origin": "degraded",
        },
        "recent_alerts": [],
    }


def _fallback_health_snapshot() -> dict[str, Any]:
    return {
        "system": {"status": "degraded", "summary": _SNAPSHOT_FALLBACK_ANSWER},
        "bot": {"runtime_note": "El panel sigue disponible con contexto limitado."},
    }


def _fallback_fmp_snapshot() -> dict[str, Any]:
    return {
        "provider": {
            "status": "degraded",
            "note": _DEGRADED_SOURCE_ANSWER,
        }
    }

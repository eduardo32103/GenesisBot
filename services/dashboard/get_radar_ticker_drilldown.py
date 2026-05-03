from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
from typing import Any

from app.settings import load_settings
from api.routes.portfolio import get_portfolio, get_portfolio_ticker_drilldown
from integrations.fmp.client import FmpClient
from services.dashboard.get_alert_drilldown import _derive_validation_label, _humanize_outcome
from services.dashboard.get_alerts_snapshot import _DEFAULT_ALERT_WINDOW_DAYS, _format_alert_state, _normalize_label
from services.dashboard.get_operational_health import _connect_database, _safe_iso
from services.dashboard.get_operational_reliability_snapshot import get_operational_reliability_snapshot
from services.dashboard.get_radar_snapshot import get_radar_snapshot

_RELATED_ALERT_LIMIT = 3


def _normalize_ticker(value: object) -> str:
    return str(value or "").strip().upper()


def _find_snapshot_item(snapshot: dict[str, Any], ticker: str) -> dict[str, Any]:
    normalized = _normalize_ticker(ticker)
    for item in snapshot.get("items") or []:
        if _normalize_ticker(item.get("ticker")) == normalized:
            return item
    return {}


def _fmp_live_ready(settings: Any) -> bool:
    return bool(getattr(settings, "fmp_api_key", "") and getattr(settings, "fmp_live_enabled", False))


def _safe_profile_text(value: Any) -> str:
    return " ".join(str(value or "").split())[:120]


def _build_dashboard_portfolio_payload(*, quote_tickers: list[str] | None = None, snapshot: dict[str, Any] | None = None) -> dict:
    snapshot = snapshot or get_radar_snapshot()
    payload: dict[str, dict] = {
        "owner_id": "dashboard_web",
        "positions": {},
        "quotes": {},
    }
    requested_quotes = {
        str(ticker or "").strip().upper()
        for ticker in (quote_tickers or [])
        if str(ticker or "").strip()
    }

    for item in snapshot.get("items") or []:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker") or "").strip().upper()
        if not ticker:
            continue

        is_investment = bool(item.get("is_investment"))
        payload["positions"][ticker] = {
            "display_name": item.get("display_name") or item.get("name") or ticker,
            "is_investment": is_investment,
            "amount_usd": float(item.get("amount_usd") or 0.0) if is_investment else 0.0,
            "units": float(item.get("units") or 0.0) if is_investment else 0.0,
            "entry_price": float(item.get("entry_price") or item.get("reference_price") or 0.0) if is_investment else 0.0,
            "reference_price": float(item.get("reference_price") or 0.0),
            "mode": str(item.get("mode") or "").strip(),
            "watchlist": bool(item.get("watchlist")),
            "timestamp": item.get("updated_at") or "",
        }
        if ticker in requested_quotes and float(item.get("current_price") or 0.0) > 0:
            payload["quotes"][ticker] = {
                "price": item.get("current_price"),
                "change": item.get("daily_change"),
                "changesPercentage": item.get("daily_change_pct"),
                "previousClose": item.get("previous_close"),
                "dayHigh": item.get("day_high"),
                "dayLow": item.get("day_low"),
                "extendedHoursPrice": item.get("extended_hours_price"),
                "extendedHoursChange": item.get("extended_hours_change"),
                "extendedHoursChangePct": item.get("extended_hours_change_pct"),
                "marketSession": item.get("market_session"),
                "volume": item.get("volume"),
                "timestamp": item.get("quote_timestamp") or item.get("updated_at") or "",
            }

    if not requested_quotes:
        return payload

    settings = load_settings()
    if not _fmp_live_ready(settings):
        return payload

    client = FmpClient(settings.fmp_api_key, logger=logging.getLogger("genesis.dashboard"))
    for ticker in requested_quotes:
        if ticker in payload["quotes"]:
            continue
        quote = client.get_quote(ticker) or {}
        if not isinstance(quote, dict) or not quote:
            continue
        payload["quotes"][ticker] = {
            "price": quote.get("price"),
            "change": quote.get("change"),
            "changesPercentage": quote.get("changesPercentage"),
            "previousClose": quote.get("previousClose"),
            "dayHigh": quote.get("dayHigh"),
            "dayLow": quote.get("dayLow"),
            "extendedHoursPrice": quote.get("extendedHoursPrice"),
            "extendedHoursChange": quote.get("extendedHoursChange"),
            "extendedHoursChangePct": quote.get("extendedHoursChangePct"),
            "marketSession": quote.get("marketSession"),
            "volume": quote.get("volume") or quote.get("vol"),
            "timestamp": quote.get("timestamp") or quote.get("updated_at") or "",
        }

    return payload


def _build_live_profile(ticker: str) -> dict[str, str]:
    settings = load_settings()
    if not _fmp_live_ready(settings):
        return {}

    client = FmpClient(settings.fmp_api_key, logger=logging.getLogger("genesis.dashboard"))
    profile = client.get_profile(ticker) or {}
    if not isinstance(profile, dict):
        return {}

    shaped = {
        "name": _safe_profile_text(profile.get("companyName") or profile.get("companyNameUSD") or profile.get("name")),
        "sector": _safe_profile_text(profile.get("sector")),
        "industry": _safe_profile_text(profile.get("industry")),
        "exchange": _safe_profile_text(profile.get("exchangeShortName") or profile.get("exchange")),
        "country": _safe_profile_text(profile.get("country")),
        "source": "datos_directos",
    }
    return {key: value for key, value in shaped.items() if value}


def _fetch_related_alerts(database_url: str, ticker: str, *, limit: int = _RELATED_ALERT_LIMIT) -> list[dict[str, Any]]:
    normalized_ticker = _normalize_ticker(ticker)
    if not database_url or not normalized_ticker:
        return []

    cutoff = (datetime.now(timezone.utc) - timedelta(days=max(7, int(_DEFAULT_ALERT_WINDOW_DAYS)))).isoformat()
    conn = None
    try:
        conn = _connect_database(database_url)
        if not conn:
            return []

        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT alert_id, alert_type, title, summary, source, signal_strength, status, created_at
            FROM alert_events
            WHERE UPPER(COALESCE(ticker, '')) = %s AND created_at >= %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (normalized_ticker, cutoff, int(limit)),
        )
        rows = cursor.fetchall() or []
        alert_ids = [str(row[0] or "").strip() for row in rows if row and row[0]]
        if not alert_ids:
            conn.commit()
            return []

        placeholders = ", ".join(["%s"] * len(alert_ids))
        latest_validation_map: dict[str, dict[str, Any]] = {}
        pending_validation_map: dict[str, dict[str, Any]] = {}

        cursor.execute(
            f"""
            SELECT DISTINCT ON (alert_id)
                   alert_id, horizon_key, evaluated_at, score_value, signed_return_pct, outcome_label
            FROM alert_validations
            WHERE alert_id IN ({placeholders}) AND evaluated_at IS NOT NULL
            ORDER BY alert_id, evaluated_at DESC
            """,
            tuple(alert_ids),
        )
        for row in cursor.fetchall() or []:
            latest_validation_map[str(row[0] or "").strip()] = {
                "horizon_key": str(row[1] or "").strip().upper(),
                "evaluated_at": _safe_iso(row[2]),
                "score_value": float(row[3]) if row[3] is not None else None,
                "signed_return_pct": float(row[4]) if row[4] is not None else None,
                "outcome_label": str(row[5] or "").strip(),
            }

        cursor.execute(
            f"""
            SELECT DISTINCT ON (alert_id)
                   alert_id, horizon_key, scheduled_at
            FROM alert_validations
            WHERE alert_id IN ({placeholders}) AND evaluated_at IS NULL
            ORDER BY alert_id, scheduled_at ASC
            """,
            tuple(alert_ids),
        )
        for row in cursor.fetchall() or []:
            pending_validation_map[str(row[0] or "").strip()] = {
                "horizon_key": str(row[1] or "").strip().upper(),
                "scheduled_at": _safe_iso(row[2]),
            }

        conn.commit()

        related_alerts: list[dict[str, Any]] = []
        for row in rows:
            alert_id = str(row[0] or "").strip()
            latest_validation = latest_validation_map.get(alert_id, {})
            pending_validation = pending_validation_map.get(alert_id, {})
            related_alerts.append(
                {
                    "alert_id": alert_id,
                    "ticker": normalized_ticker,
                    "alert_type_label": _normalize_label(row[1]),
                    "title": str(row[2] or "").strip(),
                    "summary": str(row[3] or "").strip(),
                    "source": str(row[4] or "").strip() or "runtime",
                    "signal_strength": float(row[5]) if row[5] is not None else None,
                    "status": str(row[6] or "tracking").strip().lower() or "tracking",
                    "status_label": _format_alert_state(row[6], latest_validation.get("outcome_label", "")),
                    "created_at": _safe_iso(row[7]),
                    "evaluated_at": latest_validation.get("evaluated_at", ""),
                    "score": latest_validation.get("score_value"),
                    "validation": _derive_validation_label(latest_validation, pending_validation),
                    "result": _humanize_outcome(latest_validation.get("outcome_label", "")),
                }
            )
        return related_alerts
    except Exception:
        return []
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _build_alert_state_summary(related_alerts: list[dict[str, Any]]) -> str:
    if not related_alerts:
        return "Sin alertas recientes asociadas"

    latest_status = str(related_alerts[0].get("status_label") or "Sin estado").strip()
    count = len(related_alerts)
    if count == 1:
        return f"1 alerta reciente | {latest_status}"
    return f"{count} alertas recientes | Ultima {latest_status.lower()}"


def _build_context_note(related_alerts: list[dict[str, Any]]) -> str:
    for alert in related_alerts:
        summary = str(alert.get("summary") or "").strip()
        title = str(alert.get("title") or "").strip()
        if summary:
            return summary
        if title:
            return title
    return "Sin contexto corto persistido para este activo."


def _build_reliability_note(snapshot_item: dict[str, Any], detail: dict[str, Any], related_alerts: list[dict[str, Any]]) -> str:
    origin = str(snapshot_item.get("origin") or "").strip().lower()
    source = str(snapshot_item.get("source") or "").strip().lower()
    parts: list[str] = []

    if origin == "database":
        parts.append("Radar y cartera leidos desde la base principal.")
    elif origin == "portfolio_fallback":
        parts.append("El radar vino desde portfolio.json como contingencia local.")
    else:
        parts.append("El origen del radar no quedo persistido de forma explicita.")

    if detail.get("current_price") is not None:
        parts.append("La cotizacion live estuvo disponible en este corte.")
    elif source and source != "unavailable":
        parts.append("Existe referencia persistida, pero no hubo cotizacion live confirmada en este corte.")
    else:
        parts.append("No hubo referencia suficiente para validar precio actual en este corte.")

    if related_alerts:
        parts.append("Las alertas relacionadas salen de alert_events y alert_validations persistidas; no recalcula el motor.")
    else:
        parts.append("No hay alertas recientes asociadas dentro de la ventana actual.")

    return " ".join(parts)


def _has_live_price(detail: dict[str, Any]) -> bool:
    return detail.get("current_price") is not None


def _build_dominant_signal(detail: dict[str, Any], snapshot_item: dict[str, Any], related_alerts: list[dict[str, Any]]) -> str:
    latest_alert = related_alerts[0] if related_alerts else {}
    if latest_alert:
        alert_type = str(latest_alert.get("alert_type_label") or "Alerta").strip()
        status = str(latest_alert.get("status_label") or "Sin estado").strip()
        return f"{alert_type} | {status}"

    if _has_live_price(detail):
        return "Cotizacion live disponible"
    if bool(detail.get("is_investment")):
        return "Posicion abierta"

    signal = str(snapshot_item.get("signal") or "").strip()
    if signal:
        return signal

    source_label = str(snapshot_item.get("source_label") or "").strip()
    if source_label:
        return f"Radar | {source_label}"

    return "Sin senal dominante persistida"


def _build_main_risk(detail: dict[str, Any], snapshot_item: dict[str, Any], related_alerts: list[dict[str, Any]], reliability_level: str) -> str:
    source = str(snapshot_item.get("source") or "").strip().lower()
    origin = str(snapshot_item.get("origin") or "").strip().lower()

    if reliability_level == "BAJA":
        return "Confiabilidad operativa baja"
    if source in {"unavailable", "contingency", "cache"} or origin == "portfolio_fallback":
        return "Dato apoyado en fallback o contingencia"
    if bool(detail.get("is_investment")) and not _has_live_price(detail):
        return "Posicion abierta sin precio live confirmado"
    if not related_alerts:
        return "Sin alertas recientes asociadas"
    return "Sin riesgo adicional persistido"


def _build_decision_timestamp(detail: dict[str, Any], snapshot_item: dict[str, Any], related_alerts: list[dict[str, Any]]) -> str:
    latest_alert = related_alerts[0] if related_alerts else {}
    for value in (
        latest_alert.get("evaluated_at"),
        latest_alert.get("created_at"),
        detail.get("quote_timestamp"),
        detail.get("opened_at"),
        snapshot_item.get("updated_at"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _build_asset_decision_layer(
    detail: dict[str, Any],
    snapshot_item: dict[str, Any],
    related_alerts: list[dict[str, Any]],
    operational_reliability: dict[str, Any],
) -> dict[str, str]:
    reliability = operational_reliability.get("reliability") or {}
    reliability_level = str(reliability.get("level") or "BAJA").strip().upper() or "BAJA"
    latest_alert = related_alerts[0] if related_alerts else {}
    has_alerts = bool(related_alerts)
    has_live_price = _has_live_price(detail)
    is_investment = bool(detail.get("is_investment"))
    source = str(snapshot_item.get("source") or "").strip().lower()
    origin = str(snapshot_item.get("origin") or "").strip().lower()
    fallback_visible = source in {"unavailable", "contingency", "cache"} or origin == "portfolio_fallback"

    if reliability_level == "BAJA" and has_alerts:
        priority = "media"
        decision = "no concluyente"
        reason = "Hay alerta relacionada, pero la confiabilidad operativa esta baja."
    elif reliability_level == "BAJA":
        priority = "baja"
        decision = "no concluyente"
        reason = "La confiabilidad operativa esta baja para sostener una lectura firme."
    elif has_alerts:
        priority = "alta"
        decision = "revisar ahora"
        reason = str(latest_alert.get("status_label") or latest_alert.get("validation") or "Alerta reciente relacionada").strip()
    elif is_investment and not has_live_price:
        priority = "media"
        decision = "vigilar"
        reason = "Existe posicion abierta, pero no hay precio live confirmado."
    elif has_live_price and not fallback_visible:
        priority = "media"
        decision = "vigilar"
        reason = "Hay referencia live disponible, sin alerta reciente asociada."
    elif fallback_visible:
        priority = "baja"
        decision = "esperar"
        reason = "El activo depende de fallback o contingencia y no tiene alerta reciente."
    else:
        priority = "baja"
        decision = "esperar"
        reason = "No hay alerta reciente ni riesgo operativo persistido para priorizarlo."

    dominant_signal = _build_dominant_signal(detail, snapshot_item, related_alerts)
    main_risk = _build_main_risk(detail, snapshot_item, related_alerts, reliability_level)
    timestamp = _build_decision_timestamp(detail, snapshot_item, related_alerts)

    if decision == "revisar ahora":
        note = "Merece atencion primero porque existe una senal reciente asociada al activo."
    elif decision == "vigilar":
        note = "Conviene mantenerlo visible, pero sin convertirlo en accion automatica."
    elif decision == "esperar":
        note = "No hay evidencia reciente suficiente para elevarlo en la cola de decision."
    else:
        note = "La lectura no alcanza base suficiente; valida fuente y contexto antes de decidir."

    return {
        "priority": priority,
        "decision": decision,
        "main_reason": reason,
        "dominant_signal": dominant_signal,
        "main_risk": main_risk,
        "current_reliability": reliability_level.lower(),
        "decision_timestamp": timestamp,
        "executive_note": note,
    }


def _append_unique(values: list[str], text: str) -> None:
    normalized = str(text or "").strip()
    if normalized and normalized not in values:
        values.append(normalized)


def _build_explainability_layer(
    detail: dict[str, Any],
    snapshot_item: dict[str, Any],
    related_alerts: list[dict[str, Any]],
    decision_layer: dict[str, str],
) -> dict[str, Any]:
    decision = str(decision_layer.get("decision") or "").strip().lower()
    reliability = str(decision_layer.get("current_reliability") or "").strip().lower()
    dominant_signal = str(decision_layer.get("dominant_signal") or "").strip()
    main_risk = str(decision_layer.get("main_risk") or "").strip()
    context_note = _build_context_note(related_alerts)
    source = str(snapshot_item.get("source") or "").strip().lower()
    origin = str(snapshot_item.get("origin") or "").strip().lower()
    has_live_price = _has_live_price(detail)
    is_investment = bool(detail.get("is_investment"))
    has_alerts = bool(related_alerts)
    latest_alert = related_alerts[0] if related_alerts else {}
    fallback_visible = source in {"unavailable", "contingency", "cache"} or origin == "portfolio_fallback"

    if reliability == "baja":
        dominant_factor = "Confiabilidad operativa baja"
    elif has_alerts:
        dominant_factor = "Alerta relacionada reciente"
    elif is_investment and not has_live_price:
        dominant_factor = "Posicion abierta sin precio live"
    elif has_live_price:
        dominant_factor = "Precio live disponible"
    elif fallback_visible:
        dominant_factor = "Dato en fallback o contingencia"
    else:
        dominant_factor = dominant_signal or "Sin factor dominante persistido"

    supporting_signals: list[str] = []
    blocking_signals: list[str] = []
    upgrade_requirements: list[str] = []

    if has_alerts:
        _append_unique(supporting_signals, _build_alert_state_summary(related_alerts))
        _append_unique(supporting_signals, str(latest_alert.get("validation") or "").strip())
        _append_unique(supporting_signals, str(latest_alert.get("result") or "").strip())
    else:
        _append_unique(blocking_signals, "Sin alertas recientes asociadas")
        _append_unique(upgrade_requirements, "Una alerta reciente asociada al activo")

    if context_note != "Sin contexto corto persistido para este activo.":
        _append_unique(supporting_signals, context_note)
    else:
        _append_unique(blocking_signals, "Sin contexto corto persistido")
        _append_unique(upgrade_requirements, "Contexto corto persistido")

    if has_live_price:
        _append_unique(supporting_signals, "Precio live confirmado")
    else:
        _append_unique(blocking_signals, "Sin precio live confirmado")
        _append_unique(upgrade_requirements, "Precio live confirmado")

    if is_investment:
        _append_unique(supporting_signals, "Posicion abierta detectada")

    if origin == "database":
        _append_unique(supporting_signals, "Radar leido desde DB")
    elif origin == "portfolio_fallback":
        _append_unique(blocking_signals, "Radar apoyado en portfolio.json")
        _append_unique(upgrade_requirements, "Radar leido desde DB")

    if fallback_visible:
        _append_unique(blocking_signals, main_risk or "Dato apoyado en fallback o contingencia")
        _append_unique(upgrade_requirements, "Fuente live o DB sin contingencia")

    if reliability == "baja":
        _append_unique(blocking_signals, "Confiabilidad operativa baja")
        _append_unique(upgrade_requirements, "Confiabilidad operativa media o alta")
    elif reliability in {"media", "alta"}:
        _append_unique(supporting_signals, f"Confiabilidad operativa {reliability}")
    else:
        _append_unique(blocking_signals, "Confiabilidad operativa sin dato")
        _append_unique(upgrade_requirements, "Confiabilidad operativa calculada")

    if not supporting_signals:
        supporting_signals.append("Sin senales de apoyo suficientes")
    if not blocking_signals:
        blocking_signals.append("Sin frenos relevantes persistidos")
    if not upgrade_requirements:
        upgrade_requirements.append("Mantener evidencia actual y revisar la siguiente alerta")

    if decision == "revisar ahora":
        decision_explanation = "Quedo en revisar ahora porque hay una senal reciente asociada y la confiabilidad no bloquea la lectura."
    elif decision == "vigilar":
        decision_explanation = "Quedo en vigilar porque hay informacion util, pero falta una senal mas fuerte para priorizar accion inmediata."
    elif decision == "esperar":
        decision_explanation = "Quedo en esperar porque no hay evidencia reciente suficiente para subirlo en la cola."
    else:
        decision_explanation = "Quedo como no concluyente porque la confiabilidad o los datos disponibles no sostienen una lectura firme."

    return {
        "dominant_factor": dominant_factor,
        "supporting_signals": supporting_signals[:4],
        "blocking_signals": blocking_signals[:4],
        "upgrade_requirements": upgrade_requirements[:5],
        "decision_explanation": decision_explanation,
    }


def get_dashboard_portfolio() -> dict:
    return get_portfolio(_build_dashboard_portfolio_payload())


def get_dashboard_radar_ticker_drilldown(ticker: str) -> dict:
    normalized_ticker = _normalize_ticker(ticker)
    if not normalized_ticker:
        return {
            "symbol": "",
            "ticker": "",
            "found": False,
            "error": "ticker_required",
            "source": "",
            "source_label": "Sin dato",
            "source_note": "",
            "related_alerts": [],
            "related_alerts_count": 0,
            "alert_state_summary": "Sin alertas recientes asociadas",
            "context_note": "Sin contexto corto persistido para este activo.",
            "reliability_note": "Necesito un ticker valido para abrir la ficha tactica unificada.",
            "priority": "baja",
            "decision": "no concluyente",
            "main_reason": "No hay ticker valido para evaluar.",
            "dominant_signal": "Sin senal dominante persistida",
            "main_risk": "Ticker no informado",
            "current_reliability": "baja",
            "decision_timestamp": "",
            "executive_note": "La lectura no alcanza base suficiente; valida fuente y contexto antes de decidir.",
            "dominant_factor": "Ticker no informado",
            "supporting_signals": ["Sin senales de apoyo suficientes"],
            "blocking_signals": ["Ticker no informado"],
            "upgrade_requirements": ["Informar un ticker valido"],
            "decision_explanation": "Quedo como no concluyente porque no hay ticker valido para evaluar.",
            "latest_alert_created_at": "",
            "latest_alert_evaluated_at": "",
        }

    snapshot = get_radar_snapshot()
    snapshot_item = _find_snapshot_item(snapshot, normalized_ticker)
    detail = get_portfolio_ticker_drilldown(
        normalized_ticker,
        _build_dashboard_portfolio_payload(snapshot=snapshot, quote_tickers=[normalized_ticker]),
    )
    live_profile = _build_live_profile(normalized_ticker) if detail.get("found") else {}

    settings = load_settings()
    related_alerts = _fetch_related_alerts(getattr(settings, "database_url", "") or "", normalized_ticker)
    latest_alert = related_alerts[0] if related_alerts else {}
    decision_layer = _build_asset_decision_layer(
        detail,
        snapshot_item,
        related_alerts,
        get_operational_reliability_snapshot(),
    )
    explainability_layer = _build_explainability_layer(detail, snapshot_item, related_alerts, decision_layer)

    return {
        **detail,
        "source": str(snapshot_item.get("source") or "").strip(),
        "source_label": str(snapshot_item.get("source_label") or "Sin dato").strip() or "Sin dato",
        "source_note": str(snapshot_item.get("source_note") or "").strip(),
        "origin": str(snapshot_item.get("origin") or "").strip(),
        "signal": str(snapshot_item.get("signal") or "").strip(),
        "profile": live_profile,
        "market_data": {
            "key_configured": bool(getattr(settings, "fmp_api_key", "")),
            "live_enabled": bool(getattr(settings, "fmp_live_enabled", False)),
            "live_ready": _fmp_live_ready(settings),
            "quote_available": detail.get("current_price") is not None,
            "profile_available": bool(live_profile),
            "source": "datos_directos" if detail.get("current_price") is not None else "datos_guardados",
        },
        "related_alerts": related_alerts,
        "related_alerts_count": len(related_alerts),
        "alert_state_summary": _build_alert_state_summary(related_alerts),
        "context_note": _build_context_note(related_alerts),
        "reliability_note": _build_reliability_note(snapshot_item, detail, related_alerts),
        **decision_layer,
        **explainability_layer,
        "latest_alert_created_at": str(latest_alert.get("created_at") or "").strip(),
        "latest_alert_evaluated_at": str(latest_alert.get("evaluated_at") or "").strip(),
    }

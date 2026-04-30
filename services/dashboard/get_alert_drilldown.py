from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.settings import load_settings
from services.dashboard.get_alerts_snapshot import _format_alert_state, _normalize_label
from services.dashboard.get_operational_health import _connect_database, _safe_iso


def _empty_detail(alert_id: str, note: str, *, error: str) -> dict[str, Any]:
    return {
        "alert_id": str(alert_id or "").strip(),
        "found": False,
        "error": error,
        "ticker": "",
        "alert_type": "",
        "alert_type_label": "Alerta",
        "title": "",
        "summary": "",
        "horizon": "",
        "status": "",
        "status_label": "Sin dato",
        "score": None,
        "validation": "Sin validacion",
        "result": "Sin resultado",
        "created_at": "",
        "evaluated_at": "",
        "context_note": "",
        "reliability_note": note,
        "source": "",
        "signal_strength": None,
    }


def _humanize_outcome(outcome_label: str) -> str:
    mapping = {
        "ganadora_fuerte": "Ganadora fuerte",
        "ganadora": "Ganadora",
        "mixta": "Mixta",
        "fallida": "Fallida",
        "fallida_fuerte": "Fallida fuerte",
    }
    normalized = str(outcome_label or "").strip().lower()
    if not normalized:
        return "Sin resultado"
    return mapping.get(normalized, normalized.replace("_", " ").title())


def _derive_validation_label(latest_validation: dict[str, Any], next_pending: dict[str, Any]) -> str:
    latest_horizon = str(latest_validation.get("horizon_key") or "").strip().upper()
    pending_horizon = str(next_pending.get("horizon_key") or "").strip().upper()
    if latest_validation.get("evaluated_at"):
        return f"Validada | {latest_horizon}" if latest_horizon else "Validada"
    if next_pending.get("scheduled_at"):
        return f"Pendiente | {pending_horizon}" if pending_horizon else "Pendiente"
    return "Sin validacion"


def _derive_status_label(event_status: str, latest_validation: dict[str, Any]) -> str:
    return _format_alert_state(event_status, latest_validation.get("outcome_label", ""))


def _build_context_note(event_row: dict[str, Any], latest_validation: dict[str, Any], next_pending: dict[str, Any]) -> str:
    summary = str(event_row.get("summary") or "").strip()
    title = str(event_row.get("title") or "").strip()
    source = str(event_row.get("source") or "").strip() or "runtime"
    signal_strength = event_row.get("signal_strength")

    if summary:
        return summary
    if title:
        return title

    parts: list[str] = [f"Origen: {source}."]
    if signal_strength is not None:
        parts.append(f"Intensidad registrada: {float(signal_strength):+.2f}.")
    if latest_validation.get("signed_return_pct") is not None:
        parts.append(f"Retorno validado: {float(latest_validation['signed_return_pct']):+.2f}%.")
    elif next_pending.get("horizon_key"):
        parts.append(f"Proxima validacion visible: {str(next_pending['horizon_key']).upper()}.")
    return " ".join(parts) if parts else "Sin contexto corto persistido."


def _build_reliability_note(event_status: str, latest_validation: dict[str, Any], next_pending: dict[str, Any]) -> str:
    if latest_validation.get("evaluated_at"):
        return "Detalle construido desde alert_events y la ultima validacion persistida. No recalcula el motor."
    if str(event_status or "").strip().lower() == "tracking" and next_pending.get("scheduled_at"):
        return "La alerta sigue en seguimiento y todavia no tiene una validacion cerrada."
    return "La alerta existe en el runtime, pero sin validaciones persistidas suficientes para ampliar la lectura."


def _fetch_alert_event(cursor: Any, alert_id: str) -> dict[str, Any]:
    cursor.execute(
        """
        SELECT alert_id, alert_type, ticker, title, summary, source, signal_strength, status, created_at
        FROM alert_events
        WHERE alert_id = %s
        LIMIT 1
        """,
        (alert_id,),
    )
    row = cursor.fetchone()
    if not row:
        return {}
    return {
        "alert_id": str(row[0] or "").strip(),
        "alert_type": str(row[1] or "").strip(),
        "ticker": str(row[2] or "").strip().upper(),
        "title": str(row[3] or "").strip(),
        "summary": str(row[4] or "").strip(),
        "source": str(row[5] or "").strip() or "runtime",
        "signal_strength": float(row[6]) if row[6] is not None else None,
        "status": str(row[7] or "tracking").strip().lower() or "tracking",
        "created_at": _safe_iso(row[8]),
    }


def _fetch_alert_validations(cursor: Any, alert_id: str) -> list[dict[str, Any]]:
    cursor.execute(
        """
        SELECT horizon_key, scheduled_at, evaluated_at, score_value, signed_return_pct, outcome_label
        FROM alert_validations
        WHERE alert_id = %s
        ORDER BY COALESCE(evaluated_at, scheduled_at) DESC
        """,
        (alert_id,),
    )
    rows = cursor.fetchall() or []
    validations: list[dict[str, Any]] = []
    for row in rows:
        validations.append(
            {
                "horizon_key": str(row[0] or "").strip().upper(),
                "scheduled_at": _safe_iso(row[1]),
                "evaluated_at": _safe_iso(row[2]),
                "score_value": float(row[3]) if row[3] is not None else None,
                "signed_return_pct": float(row[4]) if row[4] is not None else None,
                "outcome_label": str(row[5] or "").strip(),
            }
        )
    return validations


def _pick_latest_validation(validations: list[dict[str, Any]]) -> dict[str, Any]:
    for validation in validations:
        if validation.get("evaluated_at"):
            return validation
    return {}


def _pick_next_pending_validation(validations: list[dict[str, Any]]) -> dict[str, Any]:
    pending = [validation for validation in validations if validation.get("scheduled_at") and not validation.get("evaluated_at")]
    if not pending:
        return {}
    pending.sort(key=lambda item: item.get("scheduled_at") or "")
    return pending[0]


def _build_alert_detail(event_row: dict[str, Any], validations: list[dict[str, Any]]) -> dict[str, Any]:
    latest_validation = _pick_latest_validation(validations)
    next_pending = _pick_next_pending_validation(validations)

    return {
        "alert_id": event_row["alert_id"],
        "found": True,
        "ticker": event_row["ticker"],
        "alert_type": event_row["alert_type"],
        "alert_type_label": _normalize_label(event_row["alert_type"]),
        "title": event_row["title"],
        "summary": event_row["summary"],
        "horizon": str(latest_validation.get("horizon_key") or next_pending.get("horizon_key") or "").strip().upper(),
        "status": event_row["status"],
        "status_label": _derive_status_label(event_row["status"], latest_validation),
        "score": latest_validation.get("score_value"),
        "validation": _derive_validation_label(latest_validation, next_pending),
        "result": _humanize_outcome(latest_validation.get("outcome_label", "")),
        "created_at": event_row["created_at"],
        "evaluated_at": latest_validation.get("evaluated_at", ""),
        "context_note": _build_context_note(event_row, latest_validation, next_pending),
        "reliability_note": _build_reliability_note(event_row["status"], latest_validation, next_pending),
        "source": event_row["source"],
        "signal_strength": event_row["signal_strength"],
    }


def _fetch_alert_drilldown(database_url: str, alert_id: str) -> dict[str, Any]:
    normalized_alert_id = str(alert_id or "").strip()
    if not normalized_alert_id:
        return _empty_detail("", "Necesito un alert_id valido para abrir el detalle.", error="alert_id_required")

    conn = None
    try:
        conn = _connect_database(database_url)
        if not conn:
            return _empty_detail(
                normalized_alert_id,
                "No pude conectarme a la base de datos para leer el detalle de la alerta.",
                error="database_unavailable",
            )

        cursor = conn.cursor()
        event_row = _fetch_alert_event(cursor, normalized_alert_id)
        if not event_row:
            conn.commit()
            return _empty_detail(
                normalized_alert_id,
                "No encontre una alerta persistida con ese identificador dentro de la ventana actual.",
                error="alert_not_found",
            )

        validations = _fetch_alert_validations(cursor, normalized_alert_id)
        conn.commit()
        return _build_alert_detail(event_row, validations)
    except Exception as exc:
        return _empty_detail(
            normalized_alert_id,
            f"No pude construir el detalle de la alerta: {exc}",
            error="detail_build_failed",
        )
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def get_alert_drilldown(alert_id: str) -> dict[str, Any]:
    settings = load_settings()
    return _fetch_alert_drilldown(settings.database_url, alert_id)

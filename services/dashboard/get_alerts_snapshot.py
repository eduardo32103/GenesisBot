from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.settings import load_settings
from services.dashboard.get_operational_health import _connect_database, _safe_iso

_DEFAULT_ALERT_WINDOW_DAYS = 45
_DEFAULT_RECENT_LIMIT = 6

_ALERT_TYPE_LABELS = {
    "geo_macro": "Geo / Macro",
    "sentinel_news": "Sentinela",
    "protection": "Proteccion",
    "divergence": "Divergencia",
    "market": "Market",
}


def _empty_snapshot(note: str) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "window_days": _DEFAULT_ALERT_WINDOW_DAYS,
            "total_recent": 0,
            "active_alerts": 0,
            "validated_alerts": 0,
            "avg_score": None,
            "win_rate": None,
            "pass_rate": None,
            "engine_summary": note,
            "data_origin": "unavailable",
            "last_update": "",
        },
        "recent_alerts": [],
    }


def _normalize_label(alert_type: str) -> str:
    raw = str(alert_type or "").strip().lower()
    return _ALERT_TYPE_LABELS.get(raw, raw.replace("_", " ").title() or "Alerta")


def _build_engine_summary(total_recent: int, validated_alerts: int, avg_score: float | None, win_rate: float | None, pass_rate: float | None) -> str:
    if total_recent <= 0:
        return "Todavia no hay alertas recientes registradas."
    if validated_alerts <= 0:
        if pass_rate is not None:
            return f"Hay alertas recientes y filtro activo ({pass_rate:.1f}% de paso), pero aun sin validaciones suficientes."
        return "Hay alertas recientes, pero todavia no existen validaciones suficientes para puntuar el motor."
    if avg_score is None or win_rate is None:
        return "Existen validaciones recientes, pero el score agregado aun no esta disponible de forma consistente."
    if avg_score >= 1.0 and win_rate >= 55:
        return f"El motor viene leyendo bien en la ventana reciente (score {avg_score:+.2f}, acierto {win_rate:.1f}%)."
    if avg_score <= -0.35 or win_rate < 45:
        return f"El motor esta en zona fragil en la ventana reciente (score {avg_score:+.2f}, acierto {win_rate:.1f}%)."
    return f"El motor esta mixto pero utilizable (score {avg_score:+.2f}, acierto {win_rate:.1f}%)."


def _format_alert_state(event_status: str, validation_outcome: str) -> str:
    outcome = str(validation_outcome or "").strip().lower()
    if outcome.startswith("ganadora_fuerte"):
        return "Validada fuerte"
    if outcome.startswith("ganadora"):
        return "Validada positiva"
    if outcome.startswith("fallida_fuerte"):
        return "Validada negativa fuerte"
    if outcome.startswith("fallida"):
        return "Validada negativa"
    if outcome.startswith("mixta"):
        return "Validada mixta"

    status = str(event_status or "tracking").strip().lower()
    if status == "completed":
        return "Completada"
    return "Seguimiento"


def _fetch_alerts_snapshot(database_url: str, *, window_days: int = _DEFAULT_ALERT_WINDOW_DAYS, recent_limit: int = _DEFAULT_RECENT_LIMIT) -> dict[str, Any]:
    conn = None
    cutoff = (datetime.now(timezone.utc) - timedelta(days=max(7, int(window_days)))).isoformat()
    try:
        conn = _connect_database(database_url)
        if not conn:
            return _empty_snapshot("No pude conectarme a la base de datos para leer alertas.")

        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT COUNT(*) AS total_recent,
                   SUM(CASE WHEN status = 'completed' THEN 0 ELSE 1 END) AS active_alerts,
                   MAX(created_at) AS last_created_at
            FROM alert_events
            WHERE created_at >= %s
            """,
            (cutoff,),
        )
        totals_row = cursor.fetchone() or (0, 0, "")
        total_recent = int(totals_row[0] or 0)
        active_alerts = int(totals_row[1] or 0)
        last_created_at = _safe_iso(totals_row[2])

        cursor.execute(
            """
            SELECT COUNT(DISTINCT e.alert_id) AS validated_alerts,
                   AVG(v.score_value) AS avg_score,
                   SUM(CASE WHEN LOWER(COALESCE(v.outcome_label, '')) LIKE 'ganadora%%' THEN 1 ELSE 0 END) AS wins,
                   COUNT(*) AS validation_count,
                   MAX(v.evaluated_at) AS last_evaluated_at
            FROM alert_validations v
            JOIN alert_events e ON e.alert_id = v.alert_id
            WHERE e.created_at >= %s AND v.evaluated_at IS NOT NULL
            """,
            (cutoff,),
        )
        validation_row = cursor.fetchone() or (0, None, 0, 0, "")
        validated_alerts = int(validation_row[0] or 0)
        avg_score = float(validation_row[1]) if validation_row[1] is not None else None
        wins = int(validation_row[2] or 0)
        validation_count = int(validation_row[3] or 0)
        win_rate = round((wins / validation_count * 100.0), 2) if validation_count > 0 else None
        last_evaluated_at = _safe_iso(validation_row[4])

        cursor.execute(
            """
            SELECT COUNT(*) AS audit_count,
                   SUM(CASE WHEN was_allowed = 1 THEN 1 ELSE 0 END) AS allowed_count
            FROM alert_policy_audit
            WHERE created_at >= %s
            """,
            (cutoff,),
        )
        policy_row = cursor.fetchone() or (0, 0)
        audit_count = int(policy_row[0] or 0)
        allowed_count = int(policy_row[1] or 0)
        pass_rate = round((allowed_count / audit_count * 100.0), 2) if audit_count > 0 else None

        cursor.execute(
            """
            SELECT alert_id, alert_type, ticker, title, summary, source, signal_strength, status, created_at
            FROM alert_events
            WHERE created_at >= %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (cutoff, int(recent_limit)),
        )
        recent_rows = cursor.fetchall() or []
        alert_ids = [str(row[0]) for row in recent_rows if row and row[0]]

        latest_validation_map: dict[str, dict[str, Any]] = {}
        if alert_ids:
            placeholders = ", ".join(["%s"] * len(alert_ids))
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
                latest_validation_map[str(row[0])] = {
                    "horizon_key": str(row[1] or "").upper(),
                    "evaluated_at": _safe_iso(row[2]),
                    "score_value": float(row[3]) if row[3] is not None else None,
                    "signed_return_pct": float(row[4]) if row[4] is not None else None,
                    "outcome_label": str(row[5] or "").strip(),
                }

        conn.commit()

        recent_alerts: list[dict[str, Any]] = []
        for row in recent_rows:
            alert_id = str(row[0] or "")
            latest_validation = latest_validation_map.get(alert_id, {})
            recent_alerts.append(
                {
                    "alert_id": alert_id,
                    "alert_type": str(row[1] or "").strip(),
                    "alert_type_label": _normalize_label(row[1]),
                    "ticker": str(row[2] or "").strip().upper(),
                    "title": str(row[3] or "").strip(),
                    "summary": str(row[4] or "").strip(),
                    "source": str(row[5] or "").strip() or "runtime",
                    "signal_strength": float(row[6] or 0.0),
                    "status": str(row[7] or "tracking").strip().lower() or "tracking",
                    "created_at": _safe_iso(row[8]),
                    "state_label": _format_alert_state(row[7], latest_validation.get("outcome_label", "")),
                    "latest_validation": latest_validation,
                }
            )

        last_update = max([value for value in [last_created_at, last_evaluated_at] if value], default="")

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "window_days": max(7, int(window_days)),
                "total_recent": total_recent,
                "active_alerts": active_alerts,
                "validated_alerts": validated_alerts,
                "avg_score": round(avg_score, 3) if avg_score is not None else None,
                "win_rate": win_rate,
                "pass_rate": pass_rate,
                "engine_summary": _build_engine_summary(total_recent, validated_alerts, avg_score, win_rate, pass_rate),
                "data_origin": "database",
                "last_update": last_update,
            },
            "recent_alerts": recent_alerts,
        }
    except Exception as exc:
        return _empty_snapshot(f"No pude construir el snapshot de alertas: {exc}")
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def get_alerts_snapshot() -> dict[str, Any]:
    settings = load_settings()
    return _fetch_alerts_snapshot(settings.database_url)

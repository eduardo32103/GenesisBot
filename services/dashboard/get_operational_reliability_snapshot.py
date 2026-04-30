from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.dashboard.get_alerts_snapshot import get_alerts_snapshot
from services.dashboard.get_fmp_dependencies_snapshot import get_fmp_dependencies_snapshot
from services.dashboard.get_operational_health import get_operational_health
from services.dashboard.get_radar_snapshot import get_radar_snapshot


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _usage_has_activity(bucket: dict[str, Any]) -> bool:
    if not isinstance(bucket, dict):
        return False
    return any(_safe_int(bucket.get(field)) > 0 for field in ("fetch", "ok", "cache_hit", "throttle", "quota", "access"))


def _build_live_parts(health: dict[str, Any], radar: dict[str, Any], alerts: dict[str, Any]) -> list[str]:
    system = health.get("system") or {}
    summary = radar.get("summary") or {}
    items = radar.get("items") or []
    parts: list[str] = []

    if str(system.get("status") or "").strip().lower() == "online":
        parts.append("Salud operativa online")
    if str(summary.get("data_origin") or "").strip().lower() == "database":
        parts.append("Radar / cartera desde DB")
    if str((alerts.get("summary") or {}).get("data_origin") or "").strip().lower() == "database":
        parts.append("Alertas desde DB")

    live_refs = sum(1 for item in items if str((item or {}).get("source") or "").strip().lower() == "live")
    if live_refs > 0:
        parts.append(f"{live_refs} referencias live")

    return parts


def _build_fallback_parts(radar: dict[str, Any]) -> list[str]:
    summary = radar.get("summary") or {}
    items = radar.get("items") or []
    parts: list[str] = []

    origin = str(summary.get("data_origin") or "").strip().lower()
    if origin == "portfolio_fallback":
        parts.append("Radar / cartera desde portfolio.json")

    cache_refs = sum(1 for item in items if str((item or {}).get("source") or "").strip().lower() == "cache")
    contingency_refs = sum(1 for item in items if str((item or {}).get("source") or "").strip().lower() == "contingency")

    if cache_refs > 0:
        parts.append(f"{cache_refs} referencias desde cache")
    if contingency_refs > 0:
        parts.append(f"{contingency_refs} referencias en contingencia")

    return parts


def _build_fmp_dependent_parts(fmp: dict[str, Any]) -> list[str]:
    usage = fmp.get("usage") or {}
    labels = {
        "quote": "Cotizaciones quote",
        "intraday": "Intraday",
        "eod": "EOD",
        "news": "News",
    }
    parts = [label for kind, label in labels.items() if _usage_has_activity(usage.get(kind))]
    if parts:
        return parts

    meta_source = str((fmp.get("meta") or {}).get("source") or "").strip().lower()
    if meta_source == "runtime_snapshot":
        return ["Sin consumo FMP reciente en la ventana visible"]
    return ["Sin telemetria FMP persistida"]


def _build_degraded_parts(health: dict[str, Any], radar: dict[str, Any], alerts: dict[str, Any], fmp: dict[str, Any]) -> list[str]:
    system = health.get("system") or {}
    summary = radar.get("summary") or {}
    items = radar.get("items") or []
    alert_summary = alerts.get("summary") or {}
    provider = fmp.get("provider") or {}
    signals = fmp.get("signals") or {}
    parts: list[str] = []

    system_status = str(system.get("status") or "").strip().lower()
    radar_origin = str(summary.get("data_origin") or "").strip().lower()
    alerts_origin = str(alert_summary.get("data_origin") or "").strip().lower()

    if system_status == "degraded":
        parts.append("Salud operativa degradada")
    elif system_status == "limited":
        parts.append("Salud operativa limitada")
    elif system_status == "booting":
        parts.append("Salud operativa en warmup")
    elif system_status and system_status != "online":
        parts.append("Salud operativa sin lectura completa")

    if radar_origin not in {"database", "portfolio_fallback"}:
        parts.append("Radar / cartera sin fuente persistida")
    if alerts_origin != "database":
        parts.append("Alertas sin lectura real de DB")

    unavailable_refs = sum(1 for item in items if str((item or {}).get("source") or "").strip().lower() == "unavailable")
    if unavailable_refs > 0:
        parts.append(f"{unavailable_refs} referencias sin dato suficiente")

    if bool(provider.get("degraded", False)):
        parts.append("Proveedor FMP degradado")

    cooldown_active = _safe_int(signals.get("cooldown_active"))
    quota = _safe_int(signals.get("quota"))
    access = _safe_int(signals.get("access"))
    if cooldown_active > 0:
        parts.append(f"Cooldown FMP activo ({cooldown_active})")
    if quota > 0:
        parts.append(f"Senales de quota ({quota})")
    if access > 0:
        parts.append(f"Senales de access ({access})")

    return parts


def _resolve_reliability_level(health: dict[str, Any], radar: dict[str, Any], alerts: dict[str, Any], fmp: dict[str, Any], degraded_parts: list[str], fallback_parts: list[str]) -> str:
    system = health.get("system") or {}
    radar_summary = radar.get("summary") or {}
    alert_summary = alerts.get("summary") or {}
    provider = fmp.get("provider") or {}
    signals = fmp.get("signals") or {}

    system_status = str(system.get("status") or "").strip().lower()
    radar_origin = str(radar_summary.get("data_origin") or "").strip().lower()
    alerts_origin = str(alert_summary.get("data_origin") or "").strip().lower()
    tracked_count = _safe_int(radar_summary.get("tracked_count"))
    fallback_count = len(fallback_parts)
    degraded_count = len(degraded_parts)

    provider_degraded = bool(provider.get("degraded", False))
    cooldown_active = _safe_int(signals.get("cooldown_active"))
    quota = _safe_int(signals.get("quota"))
    access = _safe_int(signals.get("access"))

    if system_status == "degraded":
        return "BAJA"
    if tracked_count <= 0 and alerts_origin != "database":
        return "BAJA"
    if radar_origin != "database" and alerts_origin != "database":
        return "BAJA"
    if degraded_count >= 3 or ((provider_degraded or cooldown_active > 0 or quota > 0 or access > 0) and alerts_origin != "database"):
        return "BAJA"
    if system_status == "online" and radar_origin == "database" and alerts_origin == "database" and fallback_count == 0 and not provider_degraded and cooldown_active == 0 and quota == 0 and access == 0:
        return "ALTA"
    return "MEDIA"


def _build_decision_note(level: str) -> str:
    if level == "ALTA":
        return "Usable para decidir"
    if level == "MEDIA":
        return "Usable con cautela"
    return "No concluyente"


def _build_summary(level: str, *, live_parts: list[str], fallback_parts: list[str], degraded_parts: list[str]) -> str:
    if level == "ALTA":
        return "La lectura actual se apoya en fuentes reales y no muestra degradacion relevante en las capas criticas."
    if level == "MEDIA":
        return "La lectura actual es utilizable, pero mezcla partes reales con contingencia o degradacion acotada."
    if degraded_parts:
        return "La lectura actual tiene demasiadas capas degradadas o sin soporte suficiente para sostener una decision confiable."
    if fallback_parts and not live_parts:
        return "La lectura actual depende demasiado de contingencia y no alcanza una base firme para decidir."
    return "La lectura actual no alcanza una base firme para decidir sin apoyo adicional."


def _build_fmp_status_label(fmp: dict[str, Any]) -> str:
    provider = fmp.get("provider") or {}
    status = str(provider.get("status") or "DEGRADED").strip().upper() or "DEGRADED"
    if bool(provider.get("degraded", False)):
        return "Degradado"
    if status == "OK":
        return "Estable"
    return status.title()


def get_operational_reliability_snapshot() -> dict[str, Any]:
    health = get_operational_health()
    radar = get_radar_snapshot()
    alerts = get_alerts_snapshot()
    fmp = get_fmp_dependencies_snapshot()

    live_parts = _build_live_parts(health, radar, alerts)
    fallback_parts = _build_fallback_parts(radar)
    fmp_dependent_parts = _build_fmp_dependent_parts(fmp)
    degraded_parts = _build_degraded_parts(health, radar, alerts, fmp)
    level = _resolve_reliability_level(health, radar, alerts, fmp, degraded_parts, fallback_parts)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "reliability": {
            "level": level,
            "decision_note": _build_decision_note(level),
            "summary": _build_summary(level, live_parts=live_parts, fallback_parts=fallback_parts, degraded_parts=degraded_parts),
            "fmp_status_label": _build_fmp_status_label(fmp),
            "live_count": len(live_parts),
            "fallback_count": len(fallback_parts),
            "degraded_count": len(degraded_parts),
            "live_parts": live_parts,
            "fallback_parts": fallback_parts,
            "fmp_dependent_parts": fmp_dependent_parts,
            "degraded_parts": degraded_parts,
        },
        "signals": {
            "health_status": str((health.get("system") or {}).get("status") or "").strip(),
            "health_summary": str((health.get("system") or {}).get("summary") or "").strip(),
            "heartbeat_age_seconds": _safe_int((health.get("bot") or {}).get("heartbeat_age_seconds")),
            "radar_data_origin": str((radar.get("summary") or {}).get("data_origin") or "").strip(),
            "alerts_data_origin": str((alerts.get("summary") or {}).get("data_origin") or "").strip(),
            "tracked_count": _safe_int((radar.get("summary") or {}).get("tracked_count")),
            "reference_count": _safe_int((radar.get("summary") or {}).get("reference_count")),
            "total_recent_alerts": _safe_int((alerts.get("summary") or {}).get("total_recent")),
            "validated_alerts": _safe_int((alerts.get("summary") or {}).get("validated_alerts")),
            "fmp_provider_status": str((fmp.get("provider") or {}).get("status") or "").strip().upper(),
            "fmp_cooldown_active": _safe_int((fmp.get("signals") or {}).get("cooldown_active")),
            "fmp_quota": _safe_int((fmp.get("signals") or {}).get("quota")),
            "fmp_access": _safe_int((fmp.get("signals") or {}).get("access")),
        },
        "meta": {
            "health_source": "operational_health",
            "radar_source": str((radar.get("summary") or {}).get("data_origin") or "").strip() or "unknown",
            "alerts_source": str((alerts.get("summary") or {}).get("data_origin") or "").strip() or "unknown",
            "fmp_source": str((fmp.get("meta") or {}).get("source") or "").strip() or "unknown",
        },
    }

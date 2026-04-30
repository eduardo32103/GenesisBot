from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.settings import load_settings
from services.dashboard.get_operational_reliability_snapshot import get_operational_reliability_snapshot
from services.dashboard.get_radar_snapshot import get_radar_snapshot
from services.dashboard.get_radar_ticker_drilldown import (
    _build_alert_state_summary,
    _build_asset_decision_layer,
    _build_context_note,
    _fetch_related_alerts,
)

_DECISION_BUCKETS = ("revisar ahora", "vigilar", "esperar", "no concluyente")
_PRIORITY_WEIGHT = {
    "alta": 0,
    "media": 1,
    "baja": 2,
}
_DECISION_WEIGHT = {
    "revisar ahora": 0,
    "vigilar": 1,
    "esperar": 2,
    "no concluyente": 3,
}


def _normalize_ticker(value: Any) -> str:
    return str(value or "").strip().upper()


def _as_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric if numeric > 0 else None


def _build_detail_from_radar_item(item: dict[str, Any]) -> dict[str, Any]:
    source = str(item.get("source") or "").strip().lower()
    reference_price = _as_float(item.get("reference_price"))
    current_price = reference_price if source == "live" else None

    return {
        "found": True,
        "symbol": _normalize_ticker(item.get("ticker")),
        "ticker": _normalize_ticker(item.get("ticker")),
        "is_investment": bool(item.get("is_investment")),
        "amount_usd": item.get("amount_usd"),
        "entry_price": item.get("reference_price"),
        "current_price": current_price,
        "opened_at": str(item.get("updated_at") or "").strip(),
        "quote_timestamp": str(item.get("updated_at") or "").strip() if current_price is not None else "",
    }


def _sort_key(item: dict[str, Any]) -> tuple[int, int, str, str]:
    priority = str(item.get("priority") or "").strip().lower()
    decision = str(item.get("decision") or "").strip().lower()
    timestamp = str(item.get("timestamp") or "").strip()
    ticker = str(item.get("ticker") or "").strip()
    return (
        _PRIORITY_WEIGHT.get(priority, 9),
        _DECISION_WEIGHT.get(decision, 9),
        "" if timestamp else "z",
        ticker,
    )


def _bucket_items(items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    buckets = {bucket: [] for bucket in _DECISION_BUCKETS}
    for item in items:
        decision = str(item.get("decision") or "").strip().lower()
        bucket = decision if decision in buckets else "no concluyente"
        buckets[bucket].append(item)
    return buckets


def _build_summary(items: list[dict[str, Any]], radar: dict[str, Any], reliability: dict[str, Any]) -> dict[str, Any]:
    buckets = _bucket_items(items)
    decision_note = str((reliability.get("reliability") or {}).get("decision_note") or "").strip()
    reliability_level = str((reliability.get("reliability") or {}).get("level") or "").strip().lower()

    if not items:
        note = "No hay activos visibles para construir cola ejecutiva."
    elif buckets["revisar ahora"]:
        note = "Hay activos con senales recientes que merecen revision primero."
    elif buckets["vigilar"]:
        note = "La cola tiene activos para mantener visibles sin forzar accion."
    else:
        note = "No hay evidencia reciente suficiente para elevar activos en prioridad."

    return {
        "total_assets": len(items),
        "review_now_count": len(buckets["revisar ahora"]),
        "watch_count": len(buckets["vigilar"]),
        "wait_count": len(buckets["esperar"]) + len(buckets["no concluyente"]),
        "reliability_level": reliability_level or "sin dato",
        "decision_note": decision_note or "Sin lectura",
        "radar_origin": str((radar.get("summary") or {}).get("data_origin") or "").strip() or "unknown",
        "note": note,
    }


def get_executive_queue_snapshot() -> dict[str, Any]:
    radar = get_radar_snapshot()
    reliability = get_operational_reliability_snapshot()
    settings = load_settings()
    database_url = getattr(settings, "database_url", "") or ""
    items: list[dict[str, Any]] = []

    for radar_item in radar.get("items") or []:
        if not isinstance(radar_item, dict):
            continue
        ticker = _normalize_ticker(radar_item.get("ticker"))
        if not ticker:
            continue

        related_alerts = _fetch_related_alerts(database_url, ticker, limit=1)
        detail = _build_detail_from_radar_item(radar_item)
        decision_layer = _build_asset_decision_layer(detail, radar_item, related_alerts, reliability)
        context_note = _build_context_note(related_alerts)
        signal_or_context = decision_layer.get("dominant_signal") or context_note

        items.append(
            {
                "ticker": ticker,
                "priority": decision_layer["priority"],
                "decision": decision_layer["decision"],
                "main_reason": decision_layer["main_reason"],
                "current_reliability": decision_layer["current_reliability"],
                "timestamp": decision_layer["decision_timestamp"],
                "dominant_signal": decision_layer["dominant_signal"],
                "context_note": context_note,
                "signal_or_context": signal_or_context,
                "main_risk": decision_layer["main_risk"],
                "alert_state_summary": _build_alert_state_summary(related_alerts),
                "source": str(radar_item.get("source") or "").strip(),
                "origin": str(radar_item.get("origin") or "").strip(),
            }
        )

    items = sorted(items, key=_sort_key)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": _build_summary(items, radar, reliability),
        "buckets": _bucket_items(items),
        "items": items,
        "meta": {
            "source": "radar_drilldown_decision_layer",
            "radar_origin": str((radar.get("summary") or {}).get("data_origin") or "").strip() or "unknown",
            "uses_live_quotes": False,
            "note": "La cola reutiliza radar, alertas relacionadas y confiabilidad operativa; no dispara cotizaciones nuevas.",
        },
    }

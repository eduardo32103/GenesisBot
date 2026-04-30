from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from services.dashboard.get_fmp_dependencies_snapshot import _FMP_SNAPSHOT_PATH, get_fmp_dependencies_snapshot
from services.dashboard.get_macro_activity_snapshot import get_macro_activity_snapshot
from services.dashboard.get_money_flow_signal_model import get_money_flow_signal_model
from services.dashboard.get_radar_snapshot import get_radar_snapshot

_SIGNAL_TYPES = (
    "strong_inflow",
    "strong_outflow",
    "volume_breakout",
    "price_volume_divergence",
    "sector_pressure",
    "risk_on_risk_off",
    "rotation",
    "insufficient_confirmation",
)


def _safe_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except Exception:
        return None
    return numeric


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _normalize_ticker(value: Any) -> str:
    return str(value or "").strip().upper()


def _load_persisted_market_metrics() -> dict[str, dict[str, Any]]:
    if not _FMP_SNAPSHOT_PATH.exists():
        return {}
    try:
        raw = json.loads(_FMP_SNAPSHOT_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

    candidates = []
    for key in ("money_flow", "market_metrics", "market_data", "quotes"):
        value = raw.get(key)
        if isinstance(value, dict):
            candidates.append(value)

    metrics_by_ticker: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        for ticker, metrics in candidate.items():
            normalized = _normalize_ticker(ticker)
            if normalized and isinstance(metrics, dict):
                metrics_by_ticker[normalized] = metrics
    return metrics_by_ticker


def _empty_signal(signal_type: str, *, reason: str, missing: list[str] | None = None) -> dict[str, Any]:
    return {
        "type": signal_type,
        "detected": False,
        "confidence": "low",
        "label": "no concluyente" if signal_type == "insufficient_confirmation" else "sin deteccion",
        "reason": reason,
        "evidence": {},
        "missing_inputs": missing or [],
        "language": "no concluyente",
    }


def _detected_signal(signal_type: str, *, label: str, reason: str, evidence: dict[str, Any], confidence: str = "medium") -> dict[str, Any]:
    return {
        "type": signal_type,
        "detected": True,
        "confidence": confidence,
        "label": label,
        "reason": reason,
        "evidence": evidence,
        "missing_inputs": [],
        "language": label,
    }


def _empty_whale_state() -> dict[str, Any]:
    return {
        "identified": False,
        "entity": "",
        "movement_value": "",
        "movement_type": "",
        "source": "",
        "confidence": "no concluyente",
        "note": "Flujo detectado, sin ballena identificada.",
    }


def _extract_market_metrics(item: dict[str, Any], persisted_metrics: dict[str, dict[str, Any]]) -> dict[str, Any]:
    ticker = _normalize_ticker(item.get("ticker"))
    metrics = persisted_metrics.get(ticker) or {}
    if not metrics:
        metrics = item.get("money_flow") if isinstance(item.get("money_flow"), dict) else {}
    reference_price = _safe_float(item.get("reference_price"))
    price_change_pct = _safe_float(metrics.get("price_change_pct"))
    relative_volume = _safe_float(metrics.get("relative_volume"))
    volume_baseline = _safe_float(metrics.get("volume_baseline"))
    breakout_reference = _safe_float(metrics.get("breakout_reference"))
    sector_move_pct = _safe_float(metrics.get("sector_move_pct"))
    risk_proxy_move_pct = _safe_float(metrics.get("risk_proxy_move_pct"))
    source_group_move_pct = _safe_float(metrics.get("source_group_move_pct"))
    target_group_move_pct = _safe_float(metrics.get("target_group_move_pct"))
    timestamp = str(metrics.get("timestamp") or item.get("updated_at") or "").strip()

    return {
        "reference_price": reference_price,
        "price_change_pct": price_change_pct,
        "relative_volume": relative_volume,
        "volume_baseline": volume_baseline,
        "breakout_reference": breakout_reference,
        "sector_move_pct": sector_move_pct,
        "risk_proxy_move_pct": risk_proxy_move_pct,
        "source_group_move_pct": source_group_move_pct,
        "target_group_move_pct": target_group_move_pct,
        "timestamp": timestamp,
        "source": str(item.get("source") or "").strip(),
        "origin": str(item.get("origin") or "").strip(),
    }


def _missing(metrics: dict[str, Any], keys: list[str]) -> list[str]:
    return [key for key in keys if metrics.get(key) is None or metrics.get(key) == ""]


def _detect_strong_inflow(metrics: dict[str, Any]) -> dict[str, Any]:
    missing = _missing(metrics, ["price_change_pct", "relative_volume", "volume_baseline", "timestamp"])
    if missing:
        return _empty_signal("strong_inflow", reason="Faltan precio, volumen relativo o baseline para confirmar entrada fuerte.", missing=missing)
    if metrics["price_change_pct"] >= 2.5 and metrics["relative_volume"] >= 1.8:
        return _detected_signal(
            "strong_inflow",
            label="compatible con entrada fuerte",
            reason="Precio positivo con volumen relativo elevado.",
            evidence={"price_change_pct": metrics["price_change_pct"], "relative_volume": metrics["relative_volume"]},
        )
    return _empty_signal("strong_inflow", reason="El precio y volumen no superan el umbral conservador de entrada fuerte.")


def _detect_strong_outflow(metrics: dict[str, Any]) -> dict[str, Any]:
    missing = _missing(metrics, ["price_change_pct", "relative_volume", "volume_baseline", "timestamp"])
    if missing:
        return _empty_signal("strong_outflow", reason="Faltan precio, volumen relativo o baseline para confirmar salida fuerte.", missing=missing)
    if metrics["price_change_pct"] <= -2.5 and metrics["relative_volume"] >= 1.8:
        return _detected_signal(
            "strong_outflow",
            label="compatible con salida fuerte",
            reason="Precio negativo con volumen relativo elevado.",
            evidence={"price_change_pct": metrics["price_change_pct"], "relative_volume": metrics["relative_volume"]},
        )
    return _empty_signal("strong_outflow", reason="El precio y volumen no superan el umbral conservador de salida fuerte.")


def _detect_volume_breakout(metrics: dict[str, Any]) -> dict[str, Any]:
    missing = _missing(metrics, ["price_change_pct", "relative_volume", "breakout_reference", "timestamp"])
    if missing:
        return _empty_signal("volume_breakout", reason="Faltan referencia de ruptura, precio o volumen relativo.", missing=missing)
    if metrics["reference_price"] is not None and metrics["reference_price"] >= metrics["breakout_reference"] and metrics["relative_volume"] >= 1.5:
        return _detected_signal(
            "volume_breakout",
            label="ruptura compatible con volumen",
            reason="La referencia actual supera el nivel de ruptura con volumen relativo elevado.",
            evidence={"reference_price": metrics["reference_price"], "breakout_reference": metrics["breakout_reference"], "relative_volume": metrics["relative_volume"]},
        )
    return _empty_signal("volume_breakout", reason="No hay ruptura confirmable con volumen bajo las reglas actuales.")


def _detect_price_volume_divergence(metrics: dict[str, Any]) -> dict[str, Any]:
    missing = _missing(metrics, ["price_change_pct", "relative_volume", "timestamp"])
    if missing:
        return _empty_signal("price_volume_divergence", reason="Faltan precio o volumen relativo para evaluar divergencia.", missing=missing)
    if abs(metrics["price_change_pct"]) < 0.5 and metrics["relative_volume"] >= 2.0:
        return _detected_signal(
            "price_volume_divergence",
            label="divergencia compatible",
            reason="Volumen anomalo con precio casi plano.",
            evidence={"price_change_pct": metrics["price_change_pct"], "relative_volume": metrics["relative_volume"]},
        )
    return _empty_signal("price_volume_divergence", reason="No aparece divergencia conservadora entre precio y volumen.")


def _detect_sector_pressure(metrics: dict[str, Any]) -> dict[str, Any]:
    missing = _missing(metrics, ["price_change_pct", "sector_move_pct", "timestamp"])
    if missing:
        return _empty_signal("sector_pressure", reason="Falta proxy sectorial persistido para evaluar presion sectorial.", missing=missing)
    if abs(metrics["sector_move_pct"]) >= 1.5 and abs(metrics["price_change_pct"]) >= 1.0:
        return _detected_signal(
            "sector_pressure",
            label="compatible con presion sectorial",
            reason="Activo y proxy sectorial se movieron con magnitud relevante.",
            evidence={"price_change_pct": metrics["price_change_pct"], "sector_move_pct": metrics["sector_move_pct"]},
        )
    return _empty_signal("sector_pressure", reason="El proxy sectorial no alcanza umbral conservador.")


def _detect_risk_on_off(metrics: dict[str, Any], macro: dict[str, Any]) -> dict[str, Any]:
    missing = _missing(metrics, ["price_change_pct", "risk_proxy_move_pct", "timestamp"])
    macro_available = bool((macro.get("macro") or {}).get("available", False))
    if missing or not macro_available:
        extra = [] if macro_available else ["macro_context"]
        return _empty_signal("risk_on_risk_off", reason="Falta proxy de riesgo o contexto macro persistido.", missing=missing + extra)
    if abs(metrics["risk_proxy_move_pct"]) >= 1.0:
        label = "compatible con risk-on" if metrics["risk_proxy_move_pct"] > 0 else "compatible con risk-off"
        return _detected_signal(
            "risk_on_risk_off",
            label=label,
            reason="Proxy de riesgo persistido se movio con magnitud relevante.",
            evidence={"risk_proxy_move_pct": metrics["risk_proxy_move_pct"], "price_change_pct": metrics["price_change_pct"]},
        )
    return _empty_signal("risk_on_risk_off", reason="El proxy de riesgo no alcanza umbral conservador.")


def _detect_rotation(metrics: dict[str, Any]) -> dict[str, Any]:
    missing = _missing(metrics, ["source_group_move_pct", "target_group_move_pct", "timestamp"])
    if missing:
        return _empty_signal("rotation", reason="Faltan comparables persistidos para evaluar rotacion.", missing=missing)
    spread = metrics["target_group_move_pct"] - metrics["source_group_move_pct"]
    if abs(spread) >= 2.0:
        return _detected_signal(
            "rotation",
            label="compatible con rotacion",
            reason="El diferencial entre grupos supera el umbral conservador.",
            evidence={"source_group_move_pct": metrics["source_group_move_pct"], "target_group_move_pct": metrics["target_group_move_pct"], "spread_pct": spread},
        )
    return _empty_signal("rotation", reason="El diferencial entre grupos no alcanza umbral conservador.")


def _build_insufficient_confirmation(signals: list[dict[str, Any]]) -> dict[str, Any]:
    missing_inputs = sorted({item for signal in signals for item in signal.get("missing_inputs", [])})
    detected_count = sum(1 for signal in signals if signal.get("detected"))
    if missing_inputs or detected_count == 0:
        return {
            "type": "insufficient_confirmation",
            "detected": True,
            "confidence": "high" if missing_inputs else "medium",
            "label": "confirmacion insuficiente",
            "reason": "Faltan insumos reales suficientes para elevar una lectura Money Flow." if missing_inputs else "Ninguna senal supero umbrales conservadores.",
            "evidence": {"detected_signal_count": detected_count},
            "missing_inputs": missing_inputs,
            "language": "confirmacion insuficiente",
        }
    return _empty_signal("insufficient_confirmation", reason="Hay al menos una senal con evidencia suficiente.")


def _detect_for_item(item: dict[str, Any], macro: dict[str, Any], persisted_metrics: dict[str, dict[str, Any]]) -> dict[str, Any]:
    ticker = _normalize_ticker(item.get("ticker"))
    metrics = _extract_market_metrics(item, persisted_metrics)
    signals = [
        _detect_strong_inflow(metrics),
        _detect_strong_outflow(metrics),
        _detect_volume_breakout(metrics),
        _detect_price_volume_divergence(metrics),
        _detect_sector_pressure(metrics),
        _detect_risk_on_off(metrics, macro),
        _detect_rotation(metrics),
    ]
    signals.append(_build_insufficient_confirmation(signals))
    detected = [signal for signal in signals if signal.get("detected")]
    primary = next((signal for signal in detected if signal["type"] != "insufficient_confirmation"), detected[0] if detected else signals[-1])

    return {
        "ticker": ticker,
        "primary_signal": primary["type"],
        "primary_label": primary["label"],
        "flow_detected": primary["type"] != "insufficient_confirmation",
        "detected_signal_count": len(detected),
        "signals": signals,
        "timestamp": metrics.get("timestamp") or "",
        "source": metrics.get("source") or "",
        "origin": metrics.get("origin") or "",
        "whale": _empty_whale_state(),
        "language_guardrail": "No se afirma institucionalidad ni causalidad.",
    }


def get_money_flow_detection_snapshot() -> dict[str, Any]:
    model = get_money_flow_signal_model()
    radar = get_radar_snapshot()
    fmp = get_fmp_dependencies_snapshot()
    macro = get_macro_activity_snapshot()
    persisted_metrics = _load_persisted_market_metrics()
    items = [_detect_for_item(item, macro, persisted_metrics) for item in radar.get("items") or [] if isinstance(item, dict)]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "phase": "5.2",
        "status": "detection_ready_causality_disabled",
        "summary": {
            "total_assets": len(items),
            "assets_with_detected_flow": sum(1 for item in items if item.get("primary_signal") != "insufficient_confirmation"),
            "assets_insufficient_confirmation": sum(1 for item in items if item.get("primary_signal") == "insufficient_confirmation"),
            "causality_enabled": False,
            "institutional_claims_enabled": False,
            "fmp_live_queries_enabled": False,
            "note": "Deteccion conservadora basada en snapshots existentes; sin causalidad ni institucionalidad.",
        },
        "items": items,
        "signal_types": [signal["id"] for signal in model.get("signal_types", [])],
        "source_status": {
            "radar_origin": str((radar.get("summary") or {}).get("data_origin") or "unknown"),
            "fmp_source": str((fmp.get("meta") or {}).get("source") or "unknown"),
            "macro_source": str((macro.get("meta") or {}).get("macro_source") or "unknown"),
            "persisted_market_metrics_count": len(persisted_metrics),
        },
        "honesty_rules": model.get("honesty_rules", []),
    }

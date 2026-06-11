from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.mt5.mt5_runtime_snapshot import get_snapshot


RUNTIME_CONTEXT_DIAGNOSTICS_VERSION = "2026-06-11.mt5_runtime_context_diagnostics.v1"


def run_runtime_context_diagnostics(
    *,
    symbol: str = "BTCUSD",
    timeframe: str = "M30",
    snapshot: dict[str, Any] | None = None,
    generic_snapshot: dict[str, Any] | None = None,
    max_age_minutes: float = 90.0,
) -> dict[str, Any]:
    clean_symbol = _symbol(symbol)
    clean_timeframe = _timeframe(timeframe)
    tf_snapshot = snapshot if snapshot is not None else get_snapshot(clean_symbol, clean_timeframe)
    generic = generic_snapshot if generic_snapshot is not None else get_snapshot(clean_symbol, "")
    active = tf_snapshot if isinstance(tf_snapshot, dict) and tf_snapshot else generic if isinstance(generic, dict) else {}
    missing = _missing_fields(active, clean_timeframe, max_age_minutes=max_age_minutes)
    available = bool(active)
    recent = _is_recent(active.get("last_tick_at") or active.get("updated_at"), max_age_minutes=max_age_minutes)
    complete = bool(active.get("runtime_snapshot_complete"))
    context = str(active.get("runtime_snapshot_context") or "")
    status = "runtime_context_ready" if available and recent and complete and context == "bar_context" and not missing else "runtime_context_incomplete"
    return {
        "ok": True,
        "status": status,
        "diagnostics_version": RUNTIME_CONTEXT_DIAGNOSTICS_VERSION,
        "symbol": clean_symbol,
        "timeframe": clean_timeframe,
        "runtime_snapshot_available": available,
        "runtime_snapshot_recent": recent,
        "runtime_snapshot_complete": complete,
        "runtime_snapshot_context": context,
        "runtime_context_status": status,
        "runtime_context_missing_fields": missing,
        "latest_tick": active.get("last_tick") if isinstance(active.get("last_tick"), dict) else {},
        "latest_bars_available": bool(active.get("ohlc_recent")),
        "last_tick_at": str(active.get("last_tick_at") or ""),
        "bars_last_at": str(active.get("bars_last_at") or active.get("last_bars_at") or ""),
        "bars_count": int(_number(active.get("bars_count")) or 0),
        "min_bars_required": int(_number(active.get("min_bars_required")) or 0),
        "tick_merged_into_bar_context": bool(active.get("tick_merged_into_bar_context")),
        "snapshot_freshness_seconds": _age_seconds(active.get("last_tick_at") or active.get("updated_at")),
        "snapshot_source": "timeframe_snapshot" if tf_snapshot else "generic_snapshot" if generic else "none",
        "data_invented": False,
        "forced_context": False,
        "paper_rotation_applied": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _missing_fields(snapshot: dict[str, Any], timeframe: str, *, max_age_minutes: float) -> list[str]:
    if not snapshot:
        return ["runtime_snapshot"]
    missing: list[str] = []
    if timeframe and _timeframe(snapshot.get("timeframe")) not in {"", timeframe}:
        missing.append("requested_timeframe_snapshot")
    if not snapshot.get("last_tick_at"):
        missing.append("last_tick_at")
    if not _is_recent(snapshot.get("last_tick_at") or snapshot.get("updated_at"), max_age_minutes=max_age_minutes):
        missing.append("runtime_snapshot_recent")
    if not snapshot.get("runtime_snapshot_complete"):
        missing.append("runtime_snapshot_complete")
    if str(snapshot.get("runtime_snapshot_context") or "") != "bar_context":
        missing.append("bar_context")
    if int(_number(snapshot.get("bars_count")) or 0) < max(1, int(_number(snapshot.get("min_bars_required")) or 0)):
        missing.append("bars_count")
    if not snapshot.get("bars_last_at") and not snapshot.get("last_bars_at"):
        missing.append("bars_last_at")
    if not snapshot.get("tick_merged_into_bar_context"):
        missing.append("tick_merged_into_bar_context")
    return missing


def _is_recent(value: object, *, max_age_minutes: float) -> bool:
    age = _age_seconds(value)
    return age is not None and 0 <= age <= max(0.0, float(max_age_minutes or 0.0)) * 60


def _age_seconds(value: object) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return round((datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds(), 3)


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _symbol(value: object) -> str:
    return str(value or "").upper().strip().replace(".B", "")


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

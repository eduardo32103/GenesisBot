from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from threading import Lock
from typing import Any

from services.genesis.memory_store import MemoryStore
from services.mt5.instrument_resolver import normalize_mt5_symbol
from services.mt5.mt5_runtime_snapshot import get_snapshot, update_snapshot


_LOCK = Lock()
_DEFAULT_PROFILE = {
    "symbol": "BTCUSD",
    "normalized_symbol": "BTCUSD",
    "timeframe": "M30",
    "profile": "quality_loose",
    "mode": "paper_forward_candidate",
    "promoted_by": "walk_forward_optimizer",
    "applies_to": "paper_shadow_only",
    "forward_baseline_closed": 0,
    "promotion_metrics": {
        "trades": 190,
        "profit_factor": 1.1585,
        "test_profit_factor": 1.4007,
        "test_expectancy": 0.0612,
    },
    "guardrails": {
        "min_forward_profit_factor": 1.1,
        "min_forward_expectancy": 0.0,
        "min_new_trades_before_degrade": 50,
        "degrade_to": "observation_only",
    },
    "created_at": "",
    "updated_at": "",
}
_PROMOTED_PROFILES: dict[tuple[str, str], dict[str, Any]] = {}


def get_promoted_profile(
    *,
    symbol: str = "BTCUSD",
    timeframe: str = "M30",
    memory: MemoryStore | None = None,
    forward_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    clean_symbol = _symbol(symbol or "BTCUSD")
    clean_timeframe = _timeframe(timeframe or "M30")
    normalized = normalize_mt5_symbol(clean_symbol) or clean_symbol
    _ensure_defaults()
    key = (normalized, clean_timeframe)
    with _LOCK:
        state = deepcopy(_PROMOTED_PROFILES.get(key) or {})
    if not state:
        return {
            "ok": True,
            "status": "mt5_promoted_profile_empty",
            "symbol": clean_symbol,
            "normalized_symbol": normalized,
            "timeframe": clean_timeframe,
            "profile": "",
            "mode": "observation_only",
            "active": False,
            "reason": "no_candidate_for_symbol_timeframe",
            **_safety(),
            "updated_at": _now(),
        }
    stats = forward_stats if isinstance(forward_stats, dict) else _snapshot_forward_stats(normalized)
    degraded, reason = _degrade_if_needed(key, state, stats)
    if degraded:
        with _LOCK:
            state = deepcopy(_PROMOTED_PROFILES.get(key) or state)
        if memory is not None:
            _save_profile_event(memory, state, event_type="mt5_promoted_profile_degraded")
    active = state.get("mode") == "paper_forward_candidate"
    payload = {
        "ok": True,
        "status": "mt5_promoted_profile_ready",
        "symbol": clean_symbol,
        "normalized_symbol": normalized,
        "timeframe": clean_timeframe,
        "profile": state.get("profile") or "",
        "mode": state.get("mode") or "observation_only",
        "active": active,
        "applies_to_paper_shadow": active,
        "applies_to_real_trading": False,
        "promoted_by": state.get("promoted_by") or "",
        "promotion_metrics": deepcopy(state.get("promotion_metrics") or {}),
        "guardrails": deepcopy(state.get("guardrails") or {}),
        "forward_stats": stats,
        "degraded": degraded or state.get("mode") == "observation_only",
        "degrade_reason": reason or state.get("degrade_reason") or "",
        "created_at": state.get("created_at") or "",
        "updated_at": state.get("updated_at") or "",
        **_safety(),
    }
    update_snapshot(normalized, {"promoted_profile": payload})
    return payload


def active_promoted_profile(symbol: str, timeframe: str, *, snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized = normalize_mt5_symbol(_symbol(symbol)) or _symbol(symbol)
    clean_timeframe = _timeframe(timeframe)
    stats = _summary_from_snapshot(snapshot or {}) if isinstance(snapshot, dict) else None
    payload = get_promoted_profile(symbol=normalized, timeframe=clean_timeframe, forward_stats=stats)
    if payload.get("active"):
        return payload
    return {}


def record_promoted_profile(
    *,
    symbol: str = "BTCUSD",
    timeframe: str = "M30",
    profile: str = "quality_loose",
    mode: str = "paper_forward_candidate",
    promoted_by: str = "walk_forward_optimizer",
    promotion_metrics: dict[str, Any] | None = None,
    memory: MemoryStore | None = None,
) -> dict[str, Any]:
    normalized = normalize_mt5_symbol(_symbol(symbol)) or _symbol(symbol)
    clean_timeframe = _timeframe(timeframe)
    now = _now()
    state = {
        **deepcopy(_DEFAULT_PROFILE),
        "symbol": _symbol(symbol),
        "normalized_symbol": normalized,
        "timeframe": clean_timeframe,
        "profile": str(profile or "quality_loose").strip(),
        "mode": str(mode or "paper_forward_candidate").strip(),
        "promoted_by": str(promoted_by or "walk_forward_optimizer").strip(),
        "promotion_metrics": dict(promotion_metrics or _DEFAULT_PROFILE["promotion_metrics"]),
        "created_at": now,
        "updated_at": now,
    }
    with _LOCK:
        _PROMOTED_PROFILES[(normalized, clean_timeframe)] = deepcopy(state)
    if memory is not None:
        _save_profile_event(memory, state, event_type="mt5_promoted_profile")
    update_snapshot(normalized, {"promoted_profile": {**state, **_safety()}})
    return {**state, "ok": True, "active": state["mode"] == "paper_forward_candidate", **_safety()}


def reset_promoted_profiles_for_tests() -> None:
    with _LOCK:
        _PROMOTED_PROFILES.clear()
    _ensure_defaults()


def _ensure_defaults() -> None:
    with _LOCK:
        if ("BTCUSD", "M30") not in _PROMOTED_PROFILES:
            now = _now()
            _PROMOTED_PROFILES[("BTCUSD", "M30")] = {
                **deepcopy(_DEFAULT_PROFILE),
                "created_at": now,
                "updated_at": now,
            }


def _degrade_if_needed(key: tuple[str, str], state: dict[str, Any], stats: dict[str, Any]) -> tuple[bool, str]:
    if state.get("mode") != "paper_forward_candidate":
        return False, str(state.get("degrade_reason") or "")
    guardrails = state.get("guardrails") if isinstance(state.get("guardrails"), dict) else {}
    baseline = int(_number(state.get("forward_baseline_closed")) or 0)
    closed = int(_number(stats.get("closed") or stats.get("closed_trades")) or 0)
    new_trades = max(0, closed - baseline)
    min_trades = int(_number(guardrails.get("min_new_trades_before_degrade")) or 50)
    if new_trades < min_trades:
        return False, ""
    pf = float(_number(stats.get("profit_factor")) or 0.0)
    expectancy = float(_number(stats.get("expectancy")) or 0.0)
    min_pf = float(_number(guardrails.get("min_forward_profit_factor")) or 1.1)
    min_exp = float(_number(guardrails.get("min_forward_expectancy")) or 0.0)
    reason = ""
    if pf < min_pf:
        reason = "forward_pf_below_1_1"
    elif expectancy <= min_exp:
        reason = "forward_expectancy_not_positive"
    if not reason:
        return False, ""
    updated = {
        **state,
        "mode": str(guardrails.get("degrade_to") or "observation_only"),
        "degraded_at": _now(),
        "degrade_reason": reason,
        "updated_at": _now(),
    }
    with _LOCK:
        _PROMOTED_PROFILES[key] = deepcopy(updated)
    update_snapshot(key[0], {"promoted_profile": {**updated, **_safety()}})
    return True, reason


def _snapshot_forward_stats(symbol: str) -> dict[str, Any]:
    snapshot = get_snapshot(symbol) or {}
    return _summary_from_snapshot(snapshot)


def _summary_from_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    summary = snapshot.get("latest_performance_summary") if isinstance(snapshot.get("latest_performance_summary"), dict) else {}
    if summary:
        return dict(summary)
    payload = snapshot.get("latest_performance_payload") if isinstance(snapshot.get("latest_performance_payload"), dict) else {}
    if isinstance(payload.get("summary_exploration"), dict):
        return dict(payload["summary_exploration"])
    if isinstance(payload.get("summary"), dict):
        return dict(payload["summary"])
    return {}


def _save_profile_event(memory: MemoryStore, state: dict[str, Any], *, event_type: str) -> None:
    try:
        memory.save_mt5_event(event_type, str(state.get("symbol") or "BTCUSD"), {**state, **_safety()}, event_type, "low")
    except Exception:
        return


def _symbol(value: object) -> str:
    return str(value or "").upper().strip()


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip() or "M30"


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from threading import Lock
from typing import Any

from services.mt5.instrument_resolver import normalize_mt5_symbol


_LOCK = Lock()
_SNAPSHOTS: dict[str, dict[str, Any]] = {}


def update_tick(symbol: str, tick: dict[str, Any]) -> dict[str, Any]:
    clean = _symbol(symbol)
    normalized = normalize_mt5_symbol(clean)
    timestamp = _now()
    timeframe = _timeframe((tick or {}).get("timeframe"))
    with _LOCK:
        current = _update_tick_locked(clean, normalized, tick, timestamp, timeframe="")
        if timeframe:
            _update_tick_locked(clean, normalized, tick, timestamp, timeframe=timeframe)
        return deepcopy(current)


def update_account_sync(symbol: str, account: dict[str, Any]) -> dict[str, Any]:
    return update_snapshot(symbol, {"last_account_sync": dict(account), "last_account_sync_at": _now()})


def update_decision(symbol: str, decision: dict[str, Any]) -> dict[str, Any]:
    return update_snapshot(symbol, {"last_decision": dict(decision), "latest_decision": dict(decision), "updated_at": _now()})


def update_performance(symbol: str, summary: dict[str, Any], payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return update_snapshot(
        symbol,
        {
            "latest_performance_summary": dict(summary),
            "latest_performance_payload": dict(payload or {}),
            "latest_performance_at": _now(),
        },
    )


def update_adaptive_state(symbol: str, state: dict[str, Any]) -> dict[str, Any]:
    return update_snapshot(symbol, {"latest_adaptive_state": dict(state), "latest_adaptive_state_at": _now()})


def update_recommendations(symbol: str, recommendations: dict[str, Any]) -> dict[str, Any]:
    return update_snapshot(symbol, {"latest_recommendations": dict(recommendations), "latest_recommendations_at": _now()})


def update_open_shadow_trade(symbol: str, trade: dict[str, Any] | None, timeframe: str = "") -> dict[str, Any]:
    patch = {"open_shadow_trade": dict(trade or {}), "updated_at": _now()}
    current = update_snapshot(symbol, patch)
    if _timeframe(timeframe):
        update_snapshot(symbol, patch, timeframe=timeframe)
    return current


def append_closed_shadow_trade(symbol: str, trade: dict[str, Any], *, limit: int = 50, timeframe: str = "") -> dict[str, Any]:
    clean = _symbol(symbol)
    normalized = normalize_mt5_symbol(clean)
    with _LOCK:
        current = _append_closed_locked(clean, normalized, trade, limit=limit, timeframe="")
        if _timeframe(timeframe):
            _append_closed_locked(clean, normalized, trade, limit=limit, timeframe=timeframe)
        return deepcopy(current)


def reset_runtime_snapshots_for_tests() -> None:
    with _LOCK:
        _SNAPSHOTS.clear()


def update_snapshot(symbol: str, patch: dict[str, Any], timeframe: str = "") -> dict[str, Any]:
    clean = _symbol(symbol)
    normalized = normalize_mt5_symbol(clean)
    with _LOCK:
        key = _snapshot_key(normalized or clean or "MT5", timeframe)
        current = _SNAPSHOTS.setdefault(
            key,
            {
                "symbol": clean,
                "normalized_symbol": normalized or clean,
                "timeframe": _timeframe(timeframe),
                "created_at": _now(),
            },
        )
        current.update({key: deepcopy(value) for key, value in patch.items()})
        current["symbol"] = clean or current.get("symbol") or normalized
        current["normalized_symbol"] = normalized or current.get("normalized_symbol") or clean
        current["timeframe"] = _timeframe(timeframe) or current.get("timeframe") or ""
        current["updated_at"] = _now()
        return deepcopy(current)


def get_snapshot(symbol: str, timeframe: str = "") -> dict[str, Any] | None:
    clean = _symbol(symbol)
    normalized = normalize_mt5_symbol(clean)
    key = _snapshot_key(normalized or clean, timeframe)
    with _LOCK:
        payload = _SNAPSHOTS.get(key)
        return deepcopy(payload) if payload else None


def snapshot_status(symbol: str = "") -> dict[str, Any]:
    clean = _symbol(symbol)
    with _LOCK:
        if clean:
            item = _SNAPSHOTS.get(normalize_mt5_symbol(clean) or clean)
            return {
                "snapshot_symbols": [clean] if item else [],
                "snapshot_count": 1 if item else 0,
                "latest_snapshot": deepcopy(item) if item else None,
            }
        return {
            "snapshot_symbols": sorted(_SNAPSHOTS.keys()),
            "snapshot_count": len(_SNAPSHOTS),
            "latest_snapshot": deepcopy(next(iter(_SNAPSHOTS.values()), None)),
        }


def _symbol(value: object) -> str:
    return str(value or "").upper().strip()


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _snapshot_key(symbol: str, timeframe: str = "") -> str:
    clean_symbol = _symbol(symbol) or "MT5"
    clean_timeframe = _timeframe(timeframe)
    return f"{clean_symbol}:{clean_timeframe}" if clean_timeframe else clean_symbol


def _update_tick_locked(clean: str, normalized: str, tick: dict[str, Any], timestamp: str, *, timeframe: str = "") -> dict[str, Any]:
    key = _snapshot_key(normalized or clean or "MT5", timeframe)
    current = _SNAPSHOTS.setdefault(
        key,
        {
            "symbol": clean,
            "normalized_symbol": normalized or clean,
            "timeframe": timeframe,
            "created_at": timestamp,
        },
    )
    previous_tick = current.get("last_tick") if isinstance(current.get("last_tick"), dict) else None
    if previous_tick:
        current["previous_tick"] = deepcopy(previous_tick)
    current["last_tick"] = deepcopy(dict(tick))
    current["last_tick_at"] = timestamp
    current["symbol"] = clean or current.get("symbol") or normalized
    current["normalized_symbol"] = normalized or current.get("normalized_symbol") or clean
    current["timeframe"] = timeframe or _timeframe((tick or {}).get("timeframe"))
    current["updated_at"] = timestamp
    return current


def _append_closed_locked(clean: str, normalized: str, trade: dict[str, Any], *, limit: int, timeframe: str = "") -> dict[str, Any]:
    key = _snapshot_key(normalized or clean or "MT5", timeframe)
    current = _SNAPSHOTS.setdefault(
        key,
        {
            "symbol": clean,
            "normalized_symbol": normalized or clean,
            "timeframe": _timeframe(timeframe),
            "created_at": _now(),
        },
    )
    items = current.get("recent_closed_shadow_trades") if isinstance(current.get("recent_closed_shadow_trades"), list) else []
    items = [deepcopy(dict(trade))] + [deepcopy(item) for item in items if isinstance(item, dict)]
    current["recent_closed_shadow_trades"] = items[: max(1, int(limit or 50))]
    current["last_shadow_trade_closed_at"] = trade.get("closed_at") or _now()
    current["last_shadow_trade_reason"] = trade.get("exit_reason") or trade.get("reason") or ""
    current["symbol"] = clean or current.get("symbol") or normalized
    current["normalized_symbol"] = normalized or current.get("normalized_symbol") or clean
    current["timeframe"] = _timeframe(timeframe) or current.get("timeframe") or ""
    current["updated_at"] = _now()
    return current


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

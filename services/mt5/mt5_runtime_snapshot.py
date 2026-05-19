from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from threading import Lock
from typing import Any

from services.mt5.instrument_resolver import normalize_mt5_symbol


_LOCK = Lock()
_SNAPSHOTS: dict[str, dict[str, Any]] = {}


def update_tick(symbol: str, tick: dict[str, Any]) -> dict[str, Any]:
    return update_snapshot(symbol, {"last_tick": dict(tick), "last_tick_at": _now()})


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


def update_open_shadow_trade(symbol: str, trade: dict[str, Any] | None) -> dict[str, Any]:
    return update_snapshot(symbol, {"open_shadow_trade": dict(trade or {}), "updated_at": _now()})


def update_snapshot(symbol: str, patch: dict[str, Any]) -> dict[str, Any]:
    clean = _symbol(symbol)
    normalized = normalize_mt5_symbol(clean)
    with _LOCK:
        current = _SNAPSHOTS.setdefault(
            normalized or clean or "MT5",
            {
                "symbol": clean,
                "normalized_symbol": normalized or clean,
                "created_at": _now(),
            },
        )
        current.update({key: deepcopy(value) for key, value in patch.items()})
        current["symbol"] = clean or current.get("symbol") or normalized
        current["normalized_symbol"] = normalized or current.get("normalized_symbol") or clean
        current["updated_at"] = _now()
        return deepcopy(current)


def get_snapshot(symbol: str) -> dict[str, Any] | None:
    clean = _symbol(symbol)
    normalized = normalize_mt5_symbol(clean)
    with _LOCK:
        payload = _SNAPSHOTS.get(normalized or clean or "MT5")
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


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

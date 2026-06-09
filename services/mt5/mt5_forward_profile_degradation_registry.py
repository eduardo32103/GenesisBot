from __future__ import annotations

from copy import deepcopy
from typing import Any


DEGRADATION_REGISTRY_VERSION = "2026-06-09.eth_m30_degraded_profiles.v1"

_DEGRADED_FORWARD_PROFILES: tuple[dict[str, Any], ...] = (
    {
        "symbol": "ETHUSD",
        "timeframe": "M30",
        "profile": "eth_m30_vol_breakout_chop_guard_v1",
        "status": "observation_only",
        "degradation_reason": "early_forward_edge_failed",
        "degradation_source": "forward_profile_degradation_registry",
        "registry_version": DEGRADATION_REGISTRY_VERSION,
        "applies_to_paper_shadow": False,
        "applies_to_real_trading": False,
        "active": False,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "evidence": {
            "trades_forward": 5,
            "wins": 1,
            "losses": 4,
            "win_rate": 20.0,
            "profit_factor": 0.0144,
            "expectancy": -0.1025,
        },
    },
)


def forward_profile_degradation(symbol: str, timeframe: str, profile: str = "") -> dict[str, Any]:
    clean_symbol = _symbol(symbol)
    clean_timeframe = _timeframe(timeframe)
    clean_profile = str(profile or "").strip()
    for item in _DEGRADED_FORWARD_PROFILES:
        if _symbol(item.get("symbol")) != clean_symbol:
            continue
        if _timeframe(item.get("timeframe")) != clean_timeframe:
            continue
        if clean_profile and str(item.get("profile") or "").strip() != clean_profile:
            continue
        return {**deepcopy(item), **_safety()}
    return {}


def forward_profile_degradation_registry_status() -> dict[str, Any]:
    return {
        "ok": True,
        "status": "forward_profile_degradation_registry_ready",
        "registry_version": DEGRADATION_REGISTRY_VERSION,
        "degraded_profiles": [deepcopy(item) for item in _DEGRADED_FORWARD_PROFILES],
        "count": len(_DEGRADED_FORWARD_PROFILES),
        **_safety(),
    }


def _symbol(value: object) -> str:
    return str(value or "").upper().strip()


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

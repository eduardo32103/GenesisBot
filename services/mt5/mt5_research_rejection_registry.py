from __future__ import annotations

from copy import deepcopy
from fnmatch import fnmatchcase
from typing import Any


RESEARCH_REJECTION_REGISTRY_VERSION = "2026-06-09.mt5_research_rejection_registry.v2"

_RESEARCH_REJECTIONS: tuple[dict[str, Any], ...] = (
    {
        "symbol": "ETHUSD",
        "timeframe": "M30",
        "family_profile_patterns": (
            "*volatility_breakout*",
            "*vol_breakout*",
            "eth_m30_vol_breakout*",
        ),
        "rejection_status": "rejected_after_forward_degradation",
        "rejection_reason": "eth_m30_volatility_breakout_cluster_degraded_or_sibling_risk",
        "applies_to_paper_forward_candidate": True,
        "applies_to_real_trading": False,
        "reviewed_at_version": RESEARCH_REJECTION_REGISTRY_VERSION,
        "allow_future_research": False,
        "allow_manual_override": True,
    },
    {
        "symbol": "XAUUSD",
        "timeframe": "M15",
        "family_profile_patterns": (
            "*recent_session_open_continuation*",
            "*session_open_continuation*",
            "*xau*m15*session*",
        ),
        "rejection_status": "rejected_after_hardening",
        "rejection_reason": "xau_m15_session_open_continuation_failed_mc_and_remove_best_5",
        "applies_to_paper_forward_candidate": True,
        "applies_to_real_trading": False,
        "reviewed_at_version": RESEARCH_REJECTION_REGISTRY_VERSION,
        "allow_future_research": False,
        "allow_manual_override": True,
    },
    {
        "symbol": "BTCUSD",
        "timeframe": "H1",
        "family_profile_patterns": (
            "*recent_ema_reclaim*",
            "*ema_reclaim*",
        ),
        "rejection_status": "rejected_after_hardening",
        "rejection_reason": "btc_h1_ema_reclaim_failed_pf_mc_remove_best_and_dependency_gates",
        "applies_to_paper_forward_candidate": True,
        "applies_to_real_trading": False,
        "reviewed_at_version": RESEARCH_REJECTION_REGISTRY_VERSION,
        "allow_future_research": False,
        "allow_manual_override": True,
    },
    {
        "symbol": "BTCUSD",
        "timeframe": "M30",
        "family_profile_patterns": (
            "*recent_london_us_breakout*",
            "*london_us_breakout*",
        ),
        "rejection_status": "rejected_after_deep_validation",
        "rejection_reason": "btc_m30_london_us_breakout_failed_deep_sample_validation",
        "applies_to_paper_forward_candidate": True,
        "applies_to_real_trading": False,
        "reviewed_at_version": RESEARCH_REJECTION_REGISTRY_VERSION,
        "allow_future_research": False,
        "allow_manual_override": True,
    },
    {
        "symbol": "BTCUSD",
        "timeframe": "M30",
        "family_profile_patterns": (
            "*opening_range_fakeout*",
            "*london_us_breakout*",
        ),
        "rejection_status": "rejected_as_correlated_family",
        "rejection_reason": "btc_m30_opening_range_fakeout_correlated_with_failed_london_us_breakout",
        "applies_to_paper_forward_candidate": True,
        "applies_to_real_trading": False,
        "reviewed_at_version": RESEARCH_REJECTION_REGISTRY_VERSION,
        "allow_future_research": False,
        "allow_manual_override": True,
    },
    {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "family_profile_patterns": (
            "*session_vwap_reclaim*",
        ),
        "rejection_status": "rejected_after_real_hardening",
        "rejection_reason": "proxy_false_positive_after_costs_and_mc_failure",
        "applies_to_paper_forward_candidate": True,
        "applies_to_real_trading": False,
        "reviewed_at_version": RESEARCH_REJECTION_REGISTRY_VERSION,
        "allow_future_research": False,
        "allow_manual_override": True,
    },
)


def research_rejection(
    symbol: str,
    timeframe: str,
    profile: str = "",
    family: str = "",
    conceptual_family: str = "",
) -> dict[str, Any]:
    clean_symbol = _symbol(symbol)
    clean_timeframe = _timeframe(timeframe)
    blob = _blob(profile, family, conceptual_family)
    for item in _RESEARCH_REJECTIONS:
        if _symbol(item.get("symbol")) != clean_symbol:
            continue
        if _timeframe(item.get("timeframe")) != clean_timeframe:
            continue
        if _matches_patterns(blob, item.get("family_profile_patterns") or ()):
            return {**deepcopy(item), **_safety()}
    return {}


def research_rejection_registry_status() -> dict[str, Any]:
    return {
        "ok": True,
        "status": "research_rejection_registry_ready",
        "registry_version": RESEARCH_REJECTION_REGISTRY_VERSION,
        "research_rejections": [deepcopy(item) for item in _RESEARCH_REJECTIONS],
        "count": len(_RESEARCH_REJECTIONS),
        **_safety(),
    }


def _matches_patterns(blob: str, patterns: tuple[str, ...]) -> bool:
    if not blob:
        return False
    return any(fnmatchcase(blob, str(pattern or "").casefold()) for pattern in patterns)


def _blob(*values: object) -> str:
    return " ".join(str(value or "").casefold().strip() for value in values if str(value or "").strip())


def _symbol(value: object) -> str:
    symbol = str(value or "").upper().strip().replace(".B", "")
    if symbol == "XAUUSDB":
        return "XAUUSD"
    if symbol == "USTECB":
        return "USTEC"
    return symbol


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

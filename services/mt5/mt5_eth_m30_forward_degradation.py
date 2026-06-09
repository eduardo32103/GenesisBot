from __future__ import annotations

from typing import Any

from services.mt5.mt5_runtime_snapshot import get_snapshot


ETH_M30_DEGRADATION_SYMBOL = "ETHUSD"
ETH_M30_DEGRADATION_TIMEFRAME = "M30"
ETH_M30_DEGRADATION_PROFILE = "eth_m30_vol_breakout_chop_guard_v1"


def eth_m30_forward_degradation_status(symbol: str = "ETHUSD", *, timeframe: str = "M30") -> dict[str, Any]:
    clean_symbol = _symbol(symbol or ETH_M30_DEGRADATION_SYMBOL)
    clean_timeframe = _timeframe(timeframe or ETH_M30_DEGRADATION_TIMEFRAME)
    snapshot = get_snapshot(clean_symbol, clean_timeframe) or {}
    generic_snapshot = get_snapshot(clean_symbol) or {}
    stats = _forward_stats(snapshot, generic_snapshot)
    open_trade = snapshot.get("open_shadow_trade") if isinstance(snapshot.get("open_shadow_trade"), dict) else {}
    decision = snapshot.get("last_decision") if isinstance(snapshot.get("last_decision"), dict) else {}
    profile = str(
        decision.get("paper_forward_candidate_profile")
        or decision.get("strategy_profile")
        or ETH_M30_DEGRADATION_PROFILE
    )
    return evaluate_eth_m30_forward_degradation(
        symbol=clean_symbol,
        timeframe=clean_timeframe,
        profile=profile,
        stats=stats,
        open_shadow_count=1 if open_trade else 0,
        current_status="paper_forward_candidate",
        risk_governor_reason=str(decision.get("risk_governor_reason") or ""),
    )


def evaluate_eth_m30_forward_degradation(
    *,
    symbol: str,
    timeframe: str,
    profile: str,
    stats: dict[str, Any],
    open_shadow_count: int = 0,
    current_status: str = "paper_forward_candidate",
    risk_governor_reason: str = "",
) -> dict[str, Any]:
    clean_symbol = _symbol(symbol)
    clean_timeframe = _timeframe(timeframe)
    clean_profile = str(profile or "").strip()
    metrics = _metrics(stats)
    safe_to_degrade = int(open_shadow_count or 0) == 0
    scope_matches = (
        clean_symbol == ETH_M30_DEGRADATION_SYMBOL
        and clean_timeframe == ETH_M30_DEGRADATION_TIMEFRAME
        and clean_profile == ETH_M30_DEGRADATION_PROFILE
    )
    edge_failed = (
        metrics["trades_forward"] >= 5
        and metrics["wins"] <= 1
        and metrics["losses"] >= 4
        and metrics["profit_factor"] < 0.9
        and metrics["expectancy"] <= 0
    )
    should_degrade = bool(scope_matches and safe_to_degrade and edge_failed)
    recommendation = "degrade_to_observation_only" if should_degrade else "continue_observation"
    degradation_reason = "early_forward_edge_failed" if should_degrade else ""
    new_status = "observation_only" if should_degrade else str(current_status or "paper_forward_candidate")
    return {
        "ok": True,
        "status": "eth_m30_forward_degradation_guardrail_ready",
        "symbol": clean_symbol,
        "timeframe": clean_timeframe,
        "profile": clean_profile,
        "current_status": str(current_status or ""),
        "new_status": new_status,
        "active_after_guardrail": False if should_degrade else None,
        "applies_to_paper_shadow_after_guardrail": False if should_degrade else None,
        "open_shadow_count": int(open_shadow_count or 0),
        **metrics,
        "risk_governor_reason": risk_governor_reason,
        "scope_matches": scope_matches,
        "edge_failed": edge_failed,
        "whether_degradation_is_safe": safe_to_degrade,
        "should_degrade": should_degrade,
        "recommendation": recommendation,
        "degradation_reason": degradation_reason,
        "applies_to_real_trading": False,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        **_safety(),
    }


def _metrics(stats: dict[str, Any]) -> dict[str, Any]:
    trades_forward = int(_number(stats.get("trades_forward") or stats.get("forward_closed") or stats.get("closed") or stats.get("closed_trades")) or 0)
    wins = int(_number(stats.get("wins")) or 0)
    losses = int(_number(stats.get("losses")) or 0)
    return {
        "trades_forward": trades_forward,
        "wins": wins,
        "losses": losses,
        "win_rate": float(_number(stats.get("win_rate")) or 0.0),
        "profit_factor": float(_number(stats.get("profit_factor")) or 0.0),
        "expectancy": float(_number(stats.get("expectancy")) or 0.0),
    }


def _forward_stats(snapshot: dict[str, Any], generic_snapshot: dict[str, Any]) -> dict[str, Any]:
    for source in [snapshot, generic_snapshot]:
        summary = source.get("latest_performance_summary") if isinstance(source.get("latest_performance_summary"), dict) else {}
        if summary:
            return dict(summary)
        payload = source.get("latest_performance_payload") if isinstance(source.get("latest_performance_payload"), dict) else {}
        if isinstance(payload.get("summary_forward_auto"), dict):
            return dict(payload["summary_forward_auto"])
        if isinstance(payload.get("summary"), dict):
            return dict(payload["summary"])
    return {}


def _symbol(value: object) -> str:
    return str(value or "").upper().strip()


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

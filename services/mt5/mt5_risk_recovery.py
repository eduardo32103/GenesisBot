from __future__ import annotations

from typing import Any

from services.mt5.instrument_resolver import normalize_mt5_symbol
from services.mt5.mt5_risk_governor import RiskGovernorLimits, assess_runtime_risk
from services.mt5.mt5_runtime_snapshot import get_snapshot


def mt5_risk_recovery_status(symbol: str = "ETHUSD", *, timeframe: str = "M30") -> dict[str, Any]:
    limits = RiskGovernorLimits()
    clean_symbol = str(symbol or "ETHUSD").upper().strip()
    clean_timeframe = str(timeframe or "M30").upper().strip()
    normalized = normalize_mt5_symbol(clean_symbol) or clean_symbol
    snapshot = get_snapshot(normalized, clean_timeframe) or {}
    generic_snapshot = get_snapshot(normalized) or {}
    account_snapshot = get_snapshot("MT5") or {}
    risk = assess_runtime_risk(clean_symbol, timeframe=clean_timeframe, limits=limits)
    summary, summary_source = _summary_source(snapshot, generic_snapshot)
    adaptive, adaptive_source = _adaptive_source(snapshot, generic_snapshot)
    account_state = _merge_dicts(
        account_snapshot.get("last_account_sync") if isinstance(account_snapshot.get("last_account_sync"), dict) else {},
        generic_snapshot.get("last_account_sync") if isinstance(generic_snapshot.get("last_account_sync"), dict) else {},
        snapshot.get("last_account_sync") if isinstance(snapshot.get("last_account_sync"), dict) else {},
    )
    active_tick = snapshot.get("last_tick") if isinstance(snapshot.get("last_tick"), dict) else {}
    if not active_tick:
        active_tick = generic_snapshot.get("last_tick") if isinstance(generic_snapshot.get("last_tick"), dict) else {}
    open_trade = snapshot.get("open_shadow_trade") if isinstance(snapshot.get("open_shadow_trade"), dict) else {}
    spread = _spread(active_tick)

    recent_closed = int(_number(summary.get("closed") or summary.get("closed_trades")) or 0)
    recent_profit_factor = float(_number(summary.get("profit_factor")) or 0.0)
    recent_expectancy = float(_number(summary.get("expectancy")) or 0.0)
    consecutive_losses = int(
        _number(adaptive.get("current_loss_streak") or summary.get("current_loss_streak") or summary.get("consecutive_losses"))
        or 0
    )
    summary_negative = _flag_active(summary.get("negative_recent_edge"))
    adaptive_negative = _flag_active(adaptive.get("negative_edge"))
    explicit_negative_edge = summary_negative or adaptive_negative
    computed_recent_pf_rule = (
        recent_closed >= 10
        and recent_profit_factor < limits.forward_pf_threshold
        and recent_expectancy <= limits.min_recent_expectancy
    )
    current_drawdown = _float_value(account_state, "total_drawdown_pct", "drawdown_pct")
    open_shadow_trades = 1 if open_trade else 0
    blocker_source = {
        "latest_performance_summary.negative_recent_edge": summary_negative,
        "latest_adaptive_state.negative_edge": adaptive_negative,
        "computed_recent_pf_rule": computed_recent_pf_rule,
        "consecutive_losses": consecutive_losses >= limits.max_consecutive_losses,
        "drawdown": current_drawdown >= limits.max_total_drawdown_pct,
        "spread": spread is not None and spread > limits.max_spread_points,
        "open_shadow_trades": open_shadow_trades >= limits.max_open_trades,
    }
    current_metrics = {
        "trades_forward": int(
            _number(summary.get("trades_forward") or summary.get("forward_closed") or summary.get("closed") or summary.get("closed_trades"))
            or 0
        ),
        "wins": int(_number(summary.get("wins")) or 0),
        "losses": int(_number(summary.get("losses")) or 0),
        "consecutive_losses": consecutive_losses,
        "win_rate": float(_number(summary.get("win_rate")) or 0.0),
        "profit_factor": recent_profit_factor,
        "expectancy": recent_expectancy,
        "recent_closed": recent_closed,
        "recent_profit_factor": recent_profit_factor,
        "recent_expectancy": recent_expectancy,
        "daily_loss_pct": _float_value(account_state, "daily_loss_pct"),
        "weekly_loss_pct": _float_value(account_state, "weekly_loss_pct"),
        "current_drawdown_pct": current_drawdown,
    }
    recovery_requirements = _recovery_requirements(
        explicit_negative_edge=explicit_negative_edge,
        summary_negative=summary_negative,
        adaptive_negative=adaptive_negative,
        computed_recent_pf_rule=computed_recent_pf_rule,
        recent_closed=recent_closed,
        recent_profit_factor=recent_profit_factor,
        recent_expectancy=recent_expectancy,
        limits=limits,
    )
    recovery_status = _recovery_status(
        risk_allowed=bool(risk.get("allowed")),
        risk_reason=str(risk.get("reason") or ""),
        explicit_negative_edge=explicit_negative_edge,
        computed_recent_pf_rule=computed_recent_pf_rule,
        snapshot_available=bool(snapshot or generic_snapshot),
    )
    indefinite_block_risk = bool(explicit_negative_edge)
    recommended_action = _recommended_actions(
        risk_allowed=bool(risk.get("allowed")),
        risk_reason=str(risk.get("reason") or ""),
        explicit_negative_edge=explicit_negative_edge,
        computed_recent_pf_rule=computed_recent_pf_rule,
        indefinite_block_risk=indefinite_block_risk,
    )
    return {
        "ok": True,
        "status": "mt5_risk_recovery_ready",
        "symbol": clean_symbol,
        "normalized_symbol": normalized,
        "timeframe": clean_timeframe,
        "risk_governor_allowed": bool(risk.get("allowed")),
        "risk_governor_reason": risk.get("reason") or "",
        "risk_state": risk.get("risk_state") or "normal",
        "recovery_status": recovery_status,
        "blocker_source": blocker_source,
        "blocker_source_details": {
            "performance_summary_source": summary_source,
            "adaptive_state_source": adaptive_source,
            "performance_summary_flag_raw": summary.get("negative_recent_edge"),
            "adaptive_state_flag_raw": adaptive.get("negative_edge"),
            "spread_points": spread,
            "open_shadow_trades": open_shadow_trades,
            "max_open_trades": limits.max_open_trades,
            "max_consecutive_losses": limits.max_consecutive_losses,
            "max_drawdown_pct": limits.max_total_drawdown_pct,
            "max_spread_points": limits.max_spread_points,
        },
        "current_metrics": current_metrics,
        "recovery_requirements": recovery_requirements,
        "indefinite_block_risk": indefinite_block_risk,
        "recommended_action": recommended_action,
        "applies_to_real_trading": False,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        **_safety(),
    }


def _summary_source(snapshot: dict[str, Any], generic_snapshot: dict[str, Any]) -> tuple[dict[str, Any], str]:
    summary = snapshot.get("latest_performance_summary") if isinstance(snapshot.get("latest_performance_summary"), dict) else {}
    if summary:
        return summary, "timeframe_snapshot.latest_performance_summary"
    summary = generic_snapshot.get("latest_performance_summary") if isinstance(generic_snapshot.get("latest_performance_summary"), dict) else {}
    if summary:
        return summary, "symbol_snapshot.latest_performance_summary"
    return {}, "missing"


def _adaptive_source(snapshot: dict[str, Any], generic_snapshot: dict[str, Any]) -> tuple[dict[str, Any], str]:
    adaptive = snapshot.get("latest_adaptive_state") if isinstance(snapshot.get("latest_adaptive_state"), dict) else {}
    if adaptive:
        return adaptive, "timeframe_snapshot.latest_adaptive_state"
    adaptive = generic_snapshot.get("latest_adaptive_state") if isinstance(generic_snapshot.get("latest_adaptive_state"), dict) else {}
    if adaptive:
        return adaptive, "symbol_snapshot.latest_adaptive_state"
    return {}, "missing"


def _recovery_requirements(
    *,
    explicit_negative_edge: bool,
    summary_negative: bool,
    adaptive_negative: bool,
    computed_recent_pf_rule: bool,
    recent_closed: int,
    recent_profit_factor: float,
    recent_expectancy: float,
    limits: RiskGovernorLimits,
) -> dict[str, Any]:
    return {
        "clear_explicit_negative_edge_flag": {
            "required": explicit_negative_edge,
            "satisfied": not explicit_negative_edge,
            "sources": [
                source
                for source, active in (
                    ("latest_performance_summary.negative_recent_edge", summary_negative),
                    ("latest_adaptive_state.negative_edge", adaptive_negative),
                )
                if active
            ],
        },
        "recent_closed_less_than_10": {
            "required": computed_recent_pf_rule and recent_closed >= 10,
            "satisfied": recent_closed < 10,
            "current": recent_closed,
            "target": "<10",
        },
        "recent_profit_factor_at_least_1": {
            "required": computed_recent_pf_rule and recent_profit_factor < limits.forward_pf_threshold,
            "satisfied": recent_profit_factor >= limits.forward_pf_threshold,
            "current": recent_profit_factor,
            "target": f">={limits.forward_pf_threshold:g}",
        },
        "recent_expectancy_above_0": {
            "required": computed_recent_pf_rule and recent_expectancy <= limits.min_recent_expectancy,
            "satisfied": recent_expectancy > limits.min_recent_expectancy,
            "current": recent_expectancy,
            "target": f">{limits.min_recent_expectancy:g}",
        },
        "cooldown_if_any": {
            "required": False,
            "detected": False,
            "value": "none_detected_in_risk_governor",
        },
        "next_review_if_any": {
            "required": explicit_negative_edge or computed_recent_pf_rule,
            "value": "next_runtime_performance_or_adaptive_state_update" if explicit_negative_edge or computed_recent_pf_rule else "",
        },
    }


def _recovery_status(
    *,
    risk_allowed: bool,
    risk_reason: str,
    explicit_negative_edge: bool,
    computed_recent_pf_rule: bool,
    snapshot_available: bool,
) -> str:
    if not snapshot_available:
        return "no_runtime_snapshot_available"
    if risk_allowed:
        return "risk_governor_pass"
    if risk_reason == "recent_edge_negative":
        if explicit_negative_edge and not computed_recent_pf_rule:
            return "blocked_by_explicit_recent_edge_flag"
        if computed_recent_pf_rule and not explicit_negative_edge:
            return "blocked_by_computed_recent_pf_rule"
        return "blocked_by_recent_edge_negative"
    return f"blocked_by_{risk_reason or 'risk_governor'}"


def _recommended_actions(
    *,
    risk_allowed: bool,
    risk_reason: str,
    explicit_negative_edge: bool,
    computed_recent_pf_rule: bool,
    indefinite_block_risk: bool,
) -> list[str]:
    actions: list[str] = ["continue_observation" if risk_allowed else "keep_blocked"]
    if explicit_negative_edge:
        actions.append("review_flag_source")
    if indefinite_block_risk:
        actions.append("consider_paper_only_recovery_policy")
    if risk_reason == "recent_edge_negative" or computed_recent_pf_rule:
        actions.append("review_flag_source")
    actions.extend(["do_not_relax_risk_governor", "no_real_trading"])
    return list(dict.fromkeys(actions))


def _merge_dicts(*items: Any) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for item in items:
        if isinstance(item, dict):
            merged.update(item)
    return merged


def _spread(tick: dict[str, Any]) -> float | None:
    spread = _number((tick or {}).get("spread"))
    if spread is not None:
        return spread
    bid = _number((tick or {}).get("bid"))
    ask = _number((tick or {}).get("ask"))
    if bid is not None and ask is not None:
        return abs(ask - bid)
    return None


def _float_value(data: dict[str, Any], *keys: str) -> float:
    for key in keys:
        parsed = _number(data.get(key))
        if parsed is not None:
            return float(parsed)
    return 0.0


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _flag_active(value: object) -> bool:
    return bool(value)


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

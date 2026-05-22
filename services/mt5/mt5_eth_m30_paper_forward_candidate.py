from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from services.mt5.instrument_resolver import normalize_mt5_symbol
from services.mt5.mt5_risk_governor import assess_runtime_risk
from services.mt5.mt5_runtime_snapshot import get_snapshot


ETH_M30_CANDIDATE_PROFILE = "eth_m30_vol_breakout_chop_guard_v1"
ETH_M30_ALTERNATE_PROFILE = "eth_m30_vol_breakout_regime_filtered_v1"
ETH_M30_TIMEFRAME = "M30"
ETH_M30_SYMBOL = "ETHUSD"

ETH_M30_CANDIDATE_METADATA: dict[str, Any] = {
    "recent_closed": 21,
    "total_closed": 82,
    "recent_pf": 1.4045,
    "total_pf": 1.8823,
    "expectancy": 0.2349,
    "max_drawdown": 95.589964,
    "monte_carlo_stressed_pf": 1.1472,
    "monte_carlo_stressed_expectancy": 1.265222,
    "monte_carlo_p95_drawdown": 206.379464,
    "spread_x2_pf": 1.9349,
    "remove_best_5_pf": 1.4846,
    "capital_preservation_passed": True,
    "automatic_promotion": False,
    "source": "eth_m30_capital_preservation",
}

ETH_M30_PROFILE_RULES: dict[str, Any] = {
    "max_open_trades": 1,
    "mode": "paper_only_forward",
    "allowed_sessions": ["asia", "london_us", "ny_core"],
    "blocked_sessions": ["off_session"],
    "blocked_regimes": ["chop", "range", "sideways"],
    "side_mode": "both",
    "min_score": 58.0,
    "min_momentum_score": 50.0,
    "min_trend_score": 50.0,
    "min_volatility_score": 35.0,
    "risk_reward": 1.05,
    "time_stop_bars": 2,
    "mae_exit_r": 0.70,
    "fast_loss_cut_r": 0.34,
    "risk_pct": 0.1,
    "max_spread_points": 60.0,
}

ETH_M30_GUARDRAILS: dict[str, Any] = {
    "early_guardrail_active": True,
    "early_guardrail_min_trades": 10,
    "early_pf_min": 0.9,
    "early_expectancy_min": 0.0,
    "early_win_rate_min": 40.0,
    "main_guardrail_min_trades": 50,
    "main_pf_min": 1.15,
    "main_expectancy_min": 0.0,
    "max_forward_drawdown": 5000.0,
    "degrade_to": "observation_only",
}

SNAPSHOT_FRESHNESS_MINUTES = 90


def is_eth_m30_candidate_scope(symbol: str = "", timeframe: str = "") -> bool:
    return _symbol(symbol) == ETH_M30_SYMBOL and _timeframe(timeframe) == ETH_M30_TIMEFRAME


def eth_m30_candidate_profile() -> dict[str, Any]:
    return {
        "symbol": ETH_M30_SYMBOL,
        "normalized_symbol": ETH_M30_SYMBOL,
        "timeframe": ETH_M30_TIMEFRAME,
        "profile": ETH_M30_CANDIDATE_PROFILE,
        "alternate_profile": ETH_M30_ALTERNATE_PROFILE,
        "status": "paper_forward_candidate",
        "mode": "paper_only_forward",
        "promoted_by": "capital_preservation_validation",
        "metadata": deepcopy(ETH_M30_CANDIDATE_METADATA),
        "profile_rules": deepcopy(ETH_M30_PROFILE_RULES),
        "guardrails": deepcopy(ETH_M30_GUARDRAILS),
        "applies_to_real_trading": False,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        **_safety(),
    }


def eth_m30_forward_profile_state(*, symbol: str = ETH_M30_SYMBOL, timeframe: str = ETH_M30_TIMEFRAME) -> dict[str, Any]:
    clean_symbol = _symbol(symbol or ETH_M30_SYMBOL)
    clean_timeframe = _timeframe(timeframe or ETH_M30_TIMEFRAME)
    if not is_eth_m30_candidate_scope(clean_symbol, clean_timeframe):
        return _observation_only(clean_symbol, clean_timeframe, reason="no_eth_m30_candidate_for_symbol_timeframe")

    snapshot = get_snapshot(clean_symbol, clean_timeframe) or {}
    generic_snapshot = get_snapshot(clean_symbol) or {}
    active_tick = snapshot.get("last_tick") if isinstance(snapshot.get("last_tick"), dict) else {}
    last_tick_at = str(snapshot.get("last_tick_at") or "")
    if not active_tick:
        generic_tick = generic_snapshot.get("last_tick") if isinstance(generic_snapshot.get("last_tick"), dict) else {}
        generic_timeframe = _timeframe(generic_tick.get("timeframe") or generic_snapshot.get("timeframe"))
        generic_recent = _snapshot_recent(str(generic_snapshot.get("last_tick_at") or ""))
        if generic_tick and generic_recent and (not generic_timeframe or generic_timeframe == clean_timeframe):
            active_tick = {**generic_tick, "timeframe": clean_timeframe}
            last_tick_at = str(generic_snapshot.get("last_tick_at") or "")
    snapshot_recent = _snapshot_recent(last_tick_at)
    open_trade = snapshot.get("open_shadow_trade") if isinstance(snapshot.get("open_shadow_trade"), dict) else {}
    risk = assess_runtime_risk(clean_symbol, timeframe=clean_timeframe, tick=active_tick, open_trade=open_trade)
    stats = _forward_stats(snapshot, generic_snapshot)
    degradation_reason = _degradation_reason(stats)
    degraded = bool(degradation_reason)
    risk_allowed = bool(risk.get("allowed"))
    context_ready = _context_ready(active_tick, snapshot)
    active = bool(active_tick and snapshot_recent and context_ready and risk_allowed and not degraded)
    applies_to_paper_shadow = bool(active and risk_allowed)
    payload = {
        **eth_m30_candidate_profile(),
        "ok": True,
        "api_status": "mt5_forward_profile_state_ready",
        "symbol": clean_symbol,
        "normalized_symbol": normalize_mt5_symbol(clean_symbol) or clean_symbol,
        "timeframe": clean_timeframe,
        "active": active,
        "applies_to_paper_shadow": applies_to_paper_shadow,
        "applies_to_real_trading": False,
        "reason": _state_reason(bool(active_tick), snapshot_recent, context_ready, risk, degraded, degradation_reason),
        "risk_governor_allowed": risk_allowed,
        "risk_governor_reason": risk.get("reason") or "",
        "risk_state": risk.get("risk_state") or "normal",
        "suggested_lot_multiplier": risk.get("suggested_lot_multiplier", 0.0),
        "risk_governor": risk,
        "runtime_snapshot_available": bool(active_tick),
        "runtime_snapshot_recent": snapshot_recent,
        "runtime_snapshot_complete": context_ready,
        "runtime_snapshot_context": snapshot.get("runtime_snapshot_context") or active_tick.get("runtime_snapshot_context") or ("indicator_context" if context_ready else "tick_only" if active_tick else ""),
        "last_tick_at": last_tick_at,
        "bars_last_at": snapshot.get("bars_last_at") or snapshot.get("last_bars_at") or "",
        "bars_count": int(_number(snapshot.get("bars_count") or active_tick.get("bars_count")) or 0),
        "snapshot_context_source": snapshot.get("snapshot_context_source") or "",
        "tick_merged_into_bar_context": bool(snapshot.get("tick_merged_into_bar_context") or active_tick.get("tick_merged_into_bar_context")),
        "snapshot_freshness_minutes": SNAPSHOT_FRESHNESS_MINUTES,
        "open_shadow_trades": 1 if open_trade else 0,
        "blocking_shadow_trade_id": open_trade.get("shadow_trade_id") or "",
        "trades_forward": int(_number(stats.get("closed") or stats.get("closed_trades")) or 0),
        "wins": int(_number(stats.get("wins")) or 0),
        "losses": int(_number(stats.get("losses")) or 0),
        "win_rate": float(_number(stats.get("win_rate")) or 0.0),
        "profit_factor": float(_number(stats.get("profit_factor")) or 0.0),
        "expectancy": float(_number(stats.get("expectancy")) or 0.0),
        "max_drawdown": float(_number(stats.get("max_drawdown") or stats.get("drawdown")) or 0.0),
        "degraded": degraded,
        "degradation_reason": degradation_reason,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "updated_at": _now(),
        **_safety(),
    }
    return payload


def active_eth_m30_paper_forward_candidate(
    symbol: str,
    timeframe: str,
    *,
    snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not is_eth_m30_candidate_scope(symbol, timeframe):
        return {}
    state = eth_m30_forward_profile_state(symbol=symbol, timeframe=timeframe)
    if not state.get("active") or not state.get("applies_to_paper_shadow"):
        return {}
    payload = {
        **state,
        "mode": "paper_forward_candidate",
        "profile_mode": "paper_forward_candidate",
        "applies_to": "paper_shadow_only",
    }
    return payload


def _observation_only(symbol: str, timeframe: str, *, reason: str) -> dict[str, Any]:
    return {
        "ok": True,
        "api_status": "mt5_forward_profile_state_ready",
        "symbol": _symbol(symbol),
        "normalized_symbol": normalize_mt5_symbol(symbol) or _symbol(symbol),
        "timeframe": _timeframe(timeframe),
        "profile": "",
        "status": "observation_only",
        "mode": "observation_only",
        "active": False,
        "applies_to_paper_shadow": False,
        "applies_to_real_trading": False,
        "reason": reason,
        "degraded": False,
        "degradation_reason": "",
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "updated_at": _now(),
        **_safety(),
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


def _degradation_reason(stats: dict[str, Any]) -> str:
    closed = int(_number(stats.get("closed") or stats.get("closed_trades")) or 0)
    pf = _number(stats.get("profit_factor"))
    expectancy = _number(stats.get("expectancy"))
    win_rate = _number(stats.get("win_rate"))
    drawdown = float(_number(stats.get("max_drawdown") or stats.get("drawdown")) or 0.0)
    if closed >= int(ETH_M30_GUARDRAILS["early_guardrail_min_trades"]):
        if pf is not None and pf < float(ETH_M30_GUARDRAILS["early_pf_min"]):
            return "early_forward_underperformance"
        if expectancy is not None and expectancy < float(ETH_M30_GUARDRAILS["early_expectancy_min"]):
            return "early_forward_underperformance"
        if win_rate is not None and win_rate < float(ETH_M30_GUARDRAILS["early_win_rate_min"]):
            return "early_forward_underperformance"
    if closed >= int(ETH_M30_GUARDRAILS["main_guardrail_min_trades"]):
        if pf is not None and pf < float(ETH_M30_GUARDRAILS["main_pf_min"]):
            return "forward_pf_below_1_15"
        if expectancy is not None and expectancy <= float(ETH_M30_GUARDRAILS["main_expectancy_min"]):
            return "forward_expectancy_not_positive"
        if drawdown > float(ETH_M30_GUARDRAILS["max_forward_drawdown"]):
            return "forward_drawdown_limit_exceeded"
    return ""


def _state_reason(snapshot_available: bool, snapshot_recent: bool, context_ready: bool, risk: dict[str, Any], degraded: bool, degradation_reason: str) -> str:
    if degraded:
        return degradation_reason or "candidate_degraded"
    if not snapshot_available or not snapshot_recent:
        return "no_runtime_snapshot_for_requested_timeframe"
    if not context_ready:
        return "insufficient_bar_context"
    if not risk.get("allowed"):
        return f"risk_governor_block:{risk.get('reason') or 'blocked'}"
    return "paper_forward_candidate_ready"


def _context_ready(tick: dict[str, Any], snapshot: dict[str, Any]) -> bool:
    if bool(snapshot.get("runtime_snapshot_complete") or tick.get("runtime_snapshot_complete")):
        return True
    context = str(snapshot.get("runtime_snapshot_context") or tick.get("runtime_snapshot_context") or "").strip()
    bars_count = int(_number(snapshot.get("bars_count") or tick.get("bars_count")) or 0)
    min_bars = int(_number(snapshot.get("min_bars_required")) or 0)
    if context in {"tick_only", "insufficient_bar_context"} or (min_bars > 0 and bars_count < min_bars):
        return False
    return bool(
        tick
        and _number(tick.get("last") or tick.get("price")) is not None
        and _number(tick.get("score") or tick.get("final_score") or tick.get("entry_quality_score")) is not None
        and _number(tick.get("momentum_score")) is not None
        and _number(tick.get("trend_score")) is not None
        and _number(tick.get("volatility_score")) is not None
        and str(tick.get("regime") or tick.get("market_regime") or "").strip()
    )


def _snapshot_recent(value: str) -> bool:
    if not value:
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds()
    return 0 <= age_seconds <= SNAPSHOT_FRESHNESS_MINUTES * 60


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


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

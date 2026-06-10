from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from services.mt5.instrument_resolver import enrich_payload, normalize_mt5_symbol
from services.mt5.mt5_adaptive_strategy_governor import adaptive_governor_enforcement
from services.mt5.mt5_eth_m30_forward_degradation import ETH_M30_DEGRADATION_PROFILE, evaluate_eth_m30_forward_degradation
from services.mt5.mt5_eth_m30_paper_forward_candidate import active_eth_m30_paper_forward_candidate
from services.mt5.mt5_ingest_queue import enqueue_mt5_event
from services.mt5.mt5_persistent_intelligence_store import persist_risk_event, persist_shadow_trade
from services.mt5.mt5_promoted_profile import active_promoted_profile
from services.mt5.mt5_risk_guard import MT5BridgeConfig
from services.mt5.mt5_risk_governor import assess_runtime_risk
from services.mt5.mt5_runtime_snapshot import (
    append_closed_shadow_trade,
    get_snapshot,
    update_adaptive_state,
    update_open_shadow_trade,
    update_performance,
    update_snapshot,
)


def evaluate_paper_exploration(
    symbol: str,
    *,
    tick: dict[str, Any] | None = None,
    config: MT5BridgeConfig | None = None,
    trigger: str = "tick",
    timeframe: str = "",
) -> dict[str, Any]:
    cfg = config or MT5BridgeConfig.from_env()
    clean_symbol = str(symbol or (tick or {}).get("symbol") or "").upper().strip()
    normalized = normalize_mt5_symbol(clean_symbol)
    requested_timeframe = str(timeframe or (tick or {}).get("timeframe") or "").upper().strip()
    snapshot = get_snapshot(clean_symbol, requested_timeframe) if requested_timeframe else get_snapshot(clean_symbol)
    snapshot = snapshot or {}
    active_tick = tick if isinstance(tick, dict) and tick else snapshot.get("last_tick") if isinstance(snapshot.get("last_tick"), dict) else {}
    state = snapshot.get("paper_exploration_state") if isinstance(snapshot.get("paper_exploration_state"), dict) else {}
    open_trade = snapshot.get("open_shadow_trade") if isinstance(snapshot.get("open_shadow_trade"), dict) else {}
    active_timeframe = str(requested_timeframe or active_tick.get("timeframe") or "").upper().strip()
    promoted_profile = active_promoted_profile(clean_symbol, active_timeframe, snapshot=snapshot) if active_timeframe else {}
    if not promoted_profile and active_timeframe:
        promoted_profile = active_eth_m30_paper_forward_candidate(clean_symbol, active_timeframe, snapshot=snapshot)
    closed_event: dict[str, Any] | None = None
    opened_event: dict[str, Any] | None = None
    performance_payload: dict[str, Any] | None = None
    risk_governor = assess_runtime_risk(clean_symbol, timeframe=active_timeframe, tick=active_tick, open_trade=open_trade)
    initial_degradation_guardrail = _forward_degradation_guardrail(
        clean_symbol,
        active_timeframe,
        snapshot,
        promoted_profile,
        open_shadow_count=1 if open_trade else 0,
        risk_governor_reason=str(risk_governor.get("reason") or ""),
        performance_payload=None,
    )
    if initial_degradation_guardrail.get("degradation_guardrail_active"):
        result = _forward_degradation_blocked_result(
            cfg,
            trigger=trigger,
            guardrail=initial_degradation_guardrail,
            risk_governor=risk_governor,
            promoted_profile=promoted_profile,
            open_trade=open_trade,
            summary=snapshot.get("latest_performance_summary") if isinstance(snapshot.get("latest_performance_summary"), dict) else {},
        )
        _persist_paper_exploration_events(clean_symbol, active_timeframe, result)
        return result

    if open_trade:
        open_trade, closed_event = _update_open_trade(open_trade, active_tick, cfg)
        if closed_event:
            update_open_shadow_trade(clean_symbol, None, timeframe=active_timeframe)
            append_closed_shadow_trade(clean_symbol, closed_event, timeframe=active_timeframe)
            enqueue_mt5_event("mt5_shadow_trades", clean_symbol, closed_event)
            state = {
                **state,
                "last_closed_at": closed_event.get("closed_at") or _now(),
                "last_reason": closed_event.get("exit_reason") or "",
            }
            performance_payload = update_runtime_performance(clean_symbol)
            snapshot = get_snapshot(clean_symbol, active_timeframe) if active_timeframe else get_snapshot(clean_symbol)
            snapshot = snapshot or {}
        else:
            update_open_shadow_trade(clean_symbol, open_trade, timeframe=active_timeframe)

    degradation_guardrail = _forward_degradation_guardrail(
        clean_symbol,
        active_timeframe,
        snapshot,
        promoted_profile,
        open_shadow_count=1 if open_trade and not closed_event else 0,
        risk_governor_reason=str(risk_governor.get("reason") or ""),
        performance_payload=performance_payload,
    )
    if degradation_guardrail.get("should_degrade"):
        can_open, block_reason = False, f"forward_degraded:{degradation_guardrail.get('degradation_reason') or 'early_forward_edge_failed'}"
    else:
        adaptive_enforcement = adaptive_governor_enforcement(
            symbol=clean_symbol,
            timeframe=active_timeframe,
            profile=str((promoted_profile or {}).get("profile") or ""),
        )
        if adaptive_enforcement.get("blocked"):
            result = _adaptive_governor_blocked_result(
                cfg,
                trigger=trigger,
                enforcement=adaptive_enforcement,
                risk_governor=risk_governor,
                promoted_profile=promoted_profile,
                open_trade=open_trade if open_trade and not closed_event else {},
                closed_event=closed_event,
                summary=(performance_payload or {}).get("summary") if isinstance(performance_payload, dict) else {},
            )
            _persist_paper_exploration_events(clean_symbol, active_timeframe, result, closed_event=closed_event)
            return result
        can_open, block_reason = _can_open(
            clean_symbol,
            normalized,
            active_tick,
            cfg,
            state,
            bool(open_trade and not closed_event),
            promoted_profile,
            snapshot=snapshot,
        )
    if can_open:
        side = _candidate_side(active_tick, snapshot)
        risk_governor = assess_runtime_risk(
            clean_symbol,
            timeframe=active_timeframe,
            tick=active_tick,
            signal={
                "action": side.upper(),
                "side": side,
                "lot_multiplier": active_tick.get("lot_multiplier") or active_tick.get("risk_multiplier") or 1.0,
            },
            open_trade=None,
        )
        if not risk_governor.get("allowed"):
            can_open = False
            block_reason = f"risk_governor_block:{risk_governor.get('reason') or 'blocked'}"

    if can_open:
        opened_event = _open_trade(clean_symbol, normalized, active_tick, cfg, snapshot, promoted_profile)
        opened_event.update(
            {
                "risk_governor_allowed": True,
                "risk_governor_reason": risk_governor.get("reason") or "risk_governor_pass",
                "risk_state": risk_governor.get("risk_state") or "normal",
                "suggested_lot_multiplier": risk_governor.get("suggested_lot_multiplier", 1.0),
            }
        )
        update_open_shadow_trade(clean_symbol, opened_event, timeframe=active_timeframe)
        enqueue_mt5_event("mt5_shadow_trades", clean_symbol, opened_event)
        state = {
            **state,
            "last_opened_at": opened_event["opened_at"],
            "last_reason": opened_event["reason"],
            "last_shadow_trade_id": opened_event["shadow_trade_id"],
        }
        block_reason = ""

    update_snapshot(clean_symbol, {"paper_exploration_state": state})
    if performance_payload is None:
        performance_payload = update_runtime_performance(clean_symbol)
    result = {
        "paper_exploration_enabled": cfg.paper_exploration_enabled,
        "paper_exploration_attempted": bool(cfg.paper_exploration_enabled),
        "paper_exploration_created": bool(opened_event),
        "paper_exploration_closed": bool(closed_event),
        "paper_exploration_reason": (opened_event or closed_event or {}).get("reason")
        or (closed_event or {}).get("exit_reason")
        or block_reason,
        "risk_governor_allowed": bool(risk_governor.get("allowed")),
        "risk_governor_reason": risk_governor.get("reason") or "",
        "risk_state": risk_governor.get("risk_state") or "normal",
        "suggested_lot_multiplier": risk_governor.get("suggested_lot_multiplier", 0.0),
        "risk_governor": risk_governor,
        "promoted_profile": promoted_profile or None,
        "paper_forward_candidate_profile": promoted_profile.get("profile") if promoted_profile else "",
        "degradation_guardrail": degradation_guardrail,
        "degradation_guardrail_active": bool(degradation_guardrail.get("degradation_guardrail_active")),
        "degradation_reason": degradation_guardrail.get("degradation_reason") or "",
        "degradation_source": degradation_guardrail.get("degradation_source") or "",
        "registry_degraded": bool(degradation_guardrail.get("registry_degraded")),
        "registry_version": degradation_guardrail.get("registry_version") or "",
        "pending_degradation_until_shadow_closes": bool(degradation_guardrail.get("pending_degradation_until_shadow_closes")),
        "paper_probe_allowed": bool(degradation_guardrail.get("paper_probe_allowed")),
        "adaptive_governor": adaptive_enforcement if "adaptive_enforcement" in locals() else {},
        "adaptive_governor_blocked": False,
        "adaptive_governor_reason": "",
        "adaptive_governor_global_state": (adaptive_enforcement if "adaptive_enforcement" in locals() else {}).get("adaptive_governor_global_state", ""),
        "adaptive_governor_recommended_next_action": (adaptive_enforcement if "adaptive_enforcement" in locals() else {}).get("adaptive_governor_recommended_next_action", ""),
        "adaptive_governor_circuit_breakers": (adaptive_enforcement if "adaptive_enforcement" in locals() else {}).get("circuit_breakers", []),
        "shadow_trade_id": (opened_event or open_trade or {}).get("shadow_trade_id") or "",
        "open_shadow_trade": opened_event or ({} if closed_event else open_trade),
        "closed_shadow_trade": closed_event,
        "latest_performance_summary": performance_payload.get("summary") or {},
        "trigger": trigger,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
    _persist_paper_exploration_events(
        clean_symbol,
        active_timeframe,
        result,
        opened_event=opened_event,
        closed_event=closed_event,
    )
    return result


def update_runtime_performance(symbol: str) -> dict[str, Any]:
    clean_symbol = str(symbol or "").upper().strip()
    snapshot = get_snapshot(clean_symbol) or {}
    open_trade = snapshot.get("open_shadow_trade") if isinstance(snapshot.get("open_shadow_trade"), dict) else {}
    closed_trades = snapshot.get("recent_closed_shadow_trades") if isinstance(snapshot.get("recent_closed_shadow_trades"), list) else []
    trades = [trade for trade in ([open_trade] if open_trade else []) + [trade for trade in closed_trades if isinstance(trade, dict)]]
    summary = _summary(trades)
    normalized = normalize_mt5_symbol(clean_symbol)
    summary_payload = {
        **summary,
        "symbol": clean_symbol,
        "normalized_symbol": normalized,
        "instrument_type": "crypto_spot" if normalized == "BTCUSD" else "",
        "total_signals": 0,
        "actionable_signals": summary["shadow_trades"],
        "manual_shadow_trades": 0,
        "auto_shadow_trades": summary["shadow_trades"],
        "strict_shadow_trades": 0,
        "exploration_shadow_trades": summary["shadow_trades"],
        "forward_auto_shadow_trades": summary["shadow_trades"],
        "total_shadow_trades": summary["shadow_trades"],
        "sample_warning": "Muestra automatica insuficiente; no usar todavia para decidir rentabilidad." if summary["shadow_trades"] < 30 else "",
    }
    payload = {
        "ok": True,
        "status": "mt5_performance_ready",
        "symbol": clean_symbol,
        "normalized_symbol": normalized,
        "timeframe": str((open_trade or {}).get("timeframe") or ""),
        "paper_exploration_enabled": True,
        "summary": summary_payload,
        "summary_auto": summary_payload,
        "summary_forward_auto": summary_payload,
        "summary_exploration": {**summary_payload, "exploration_trades": summary["shadow_trades"], "paper_exploration": True},
        "summary_total": summary_payload,
        "recent_trades": trades[:10],
        "recent_auto_trades": trades[:10],
        "recent_exploration_trades": trades[:10],
        "recent_closed_trades": [trade for trade in trades if trade.get("lifecycle_status") == "closed"][:10],
        "latest_open_trade": open_trade or None,
        "data_source_used": "runtime_snapshot",
        "genesis_reading": _reading(clean_symbol, summary),
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "updated_at": _now(),
    }
    update_performance(clean_symbol, summary_payload, payload)
    update_adaptive_state(clean_symbol, _adaptive_from_summary(clean_symbol, summary_payload))
    return payload


def _forward_degradation_guardrail(
    symbol: str,
    timeframe: str,
    snapshot: dict[str, Any],
    promoted_profile: dict[str, Any] | None,
    *,
    open_shadow_count: int,
    risk_governor_reason: str,
    performance_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    profile = str((promoted_profile or {}).get("profile") or ETH_M30_DEGRADATION_PROFILE)
    summary = {}
    if isinstance(performance_payload, dict):
        summary = performance_payload.get("summary") if isinstance(performance_payload.get("summary"), dict) else {}
    if not summary:
        summary = snapshot.get("latest_performance_summary") if isinstance(snapshot.get("latest_performance_summary"), dict) else {}
    return evaluate_eth_m30_forward_degradation(
        symbol=symbol,
        timeframe=timeframe,
        profile=profile,
        stats=summary,
        open_shadow_count=open_shadow_count,
        current_status="paper_forward_candidate",
        risk_governor_reason=risk_governor_reason,
    )


def _persist_paper_exploration_events(
    symbol: str,
    timeframe: str,
    result: dict[str, Any],
    *,
    opened_event: dict[str, Any] | None = None,
    closed_event: dict[str, Any] | None = None,
) -> None:
    persisted: dict[str, Any] = {}
    if opened_event:
        persisted["opened_shadow_trade"] = persist_shadow_trade(opened_event)
    if closed_event:
        persisted["closed_shadow_trade"] = persist_shadow_trade(closed_event)
    risk_blocked = bool(
        result.get("degradation_guardrail_active")
        or result.get("adaptive_governor_blocked")
        or not bool(result.get("risk_governor_allowed", True))
    )
    if risk_blocked:
        reason = (
            result.get("adaptive_governor_reason")
            or result.get("paper_exploration_reason")
            or result.get("risk_governor_reason")
            or result.get("degradation_reason")
            or "paper_exploration_blocked"
        )
        circuit_breaker = "risk_governor"
        if result.get("adaptive_governor_blocked"):
            circuit_breaker = "adaptive_governor"
        elif result.get("degradation_guardrail_active"):
            circuit_breaker = "forward_degradation_guardrail"
        persisted["risk_event"] = persist_risk_event(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "risk_state": result.get("risk_state") or result.get("adaptive_governor_global_state") or "blocked",
                "allowed": False,
                "reason": reason,
                "circuit_breaker": circuit_breaker,
                "open_shadow_count": 1 if result.get("open_shadow_trade") else 0,
                "recommended_action": result.get("adaptive_governor_recommended_next_action") or "NO_TRADE",
            }
        )
    if persisted:
        result["persistent_intelligence"] = persisted


def _forward_degradation_blocked_result(
    cfg: MT5BridgeConfig,
    *,
    trigger: str,
    guardrail: dict[str, Any],
    risk_governor: dict[str, Any],
    promoted_profile: dict[str, Any] | None,
    open_trade: dict[str, Any],
    summary: dict[str, Any],
) -> dict[str, Any]:
    reason = _forward_degraded_reason(guardrail)
    return {
        "paper_exploration_enabled": cfg.paper_exploration_enabled,
        "paper_exploration_attempted": bool(cfg.paper_exploration_enabled),
        "paper_exploration_created": False,
        "paper_exploration_closed": False,
        "paper_exploration_reason": reason,
        "risk_governor_allowed": False,
        "risk_governor_reason": risk_governor.get("reason") or guardrail.get("risk_governor_reason") or "",
        "risk_state": risk_governor.get("risk_state") or "normal",
        "suggested_lot_multiplier": 0.0,
        "risk_governor": risk_governor,
        "promoted_profile": promoted_profile or None,
        "paper_forward_candidate_profile": guardrail.get("profile") or (promoted_profile or {}).get("profile") or "",
        "degradation_guardrail": guardrail,
        "degradation_guardrail_active": True,
        "degradation_reason": guardrail.get("degradation_reason") or "",
        "degradation_source": guardrail.get("degradation_source") or "",
        "registry_degraded": bool(guardrail.get("registry_degraded")),
        "registry_version": guardrail.get("registry_version") or "",
        "pending_degradation_until_shadow_closes": bool(guardrail.get("pending_degradation_until_shadow_closes")),
        "paper_probe_allowed": False,
        "shadow_trade_id": "",
        "open_shadow_trade": open_trade if guardrail.get("pending_degradation_until_shadow_closes") else {},
        "closed_shadow_trade": None,
        "latest_performance_summary": dict(summary or {}),
        "trigger": trigger,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _adaptive_governor_blocked_result(
    cfg: MT5BridgeConfig,
    *,
    trigger: str,
    enforcement: dict[str, Any],
    risk_governor: dict[str, Any],
    promoted_profile: dict[str, Any] | None,
    open_trade: dict[str, Any],
    closed_event: dict[str, Any] | None,
    summary: dict[str, Any] | None,
) -> dict[str, Any]:
    reason = str(enforcement.get("reason") or "adaptive_governor:blocked")
    return {
        "paper_exploration_enabled": cfg.paper_exploration_enabled,
        "paper_exploration_attempted": bool(cfg.paper_exploration_enabled),
        "paper_exploration_created": False,
        "paper_exploration_closed": bool(closed_event),
        "paper_exploration_reason": reason,
        "risk_governor_allowed": False,
        "risk_governor_reason": risk_governor.get("reason") or "",
        "risk_state": risk_governor.get("risk_state") or "normal",
        "suggested_lot_multiplier": 0.0,
        "risk_governor": risk_governor,
        "promoted_profile": promoted_profile or None,
        "paper_forward_candidate_profile": (promoted_profile or {}).get("profile") or "",
        "degradation_guardrail": {},
        "degradation_guardrail_active": False,
        "degradation_reason": "",
        "degradation_source": "",
        "registry_degraded": False,
        "registry_version": "",
        "pending_degradation_until_shadow_closes": False,
        "paper_probe_allowed": False,
        "adaptive_governor": enforcement,
        "adaptive_governor_blocked": True,
        "adaptive_governor_reason": reason,
        "adaptive_governor_global_state": enforcement.get("adaptive_governor_global_state") or "",
        "adaptive_governor_recommended_next_action": enforcement.get("adaptive_governor_recommended_next_action") or "",
        "adaptive_governor_circuit_breakers": enforcement.get("circuit_breakers") if isinstance(enforcement.get("circuit_breakers"), list) else [],
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "shadow_trade_id": "",
        "open_shadow_trade": open_trade or {},
        "closed_shadow_trade": closed_event,
        "latest_performance_summary": dict(summary or {}),
        "trigger": trigger,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _forward_degraded_reason(guardrail: dict[str, Any]) -> str:
    reason = str(guardrail.get("degradation_reason") or "early_forward_edge_failed")
    if guardrail.get("pending_degradation_until_shadow_closes"):
        return f"pending_degradation_until_shadow_closes:{reason}"
    return f"forward_degraded:{reason}"


def _can_open(
    symbol: str,
    normalized: str,
    tick: dict[str, Any],
    cfg: MT5BridgeConfig,
    state: dict[str, Any],
    has_open_trade: bool,
    promoted_profile: dict[str, Any] | None = None,
    snapshot: dict[str, Any] | None = None,
) -> tuple[bool, str]:
    snapshot = snapshot if isinstance(snapshot, dict) else get_snapshot(symbol) or {}
    closed_trades = snapshot.get("recent_closed_shadow_trades") if isinstance(snapshot.get("recent_closed_shadow_trades"), list) else []
    closed_trades = [trade for trade in closed_trades if isinstance(trade, dict)]
    defense = _defense_state(closed_trades)
    if not cfg.paper_exploration_enabled:
        return False, "paper_exploration_disabled"
    if normalized != "BTCUSD" and not promoted_profile:
        return False, "instrument_mismatch"
    if has_open_trade:
        return False, "active_open_trade"
    price = _price(tick)
    if price is None:
        return False, "no_tick_price"
    spread = _spread(tick)
    if spread is not None and spread > cfg.paper_exploration_max_spread_points:
        return False, "spread_too_high"
    last_opened = _parse_time(state.get("last_opened_at"))
    cooldown = cfg.paper_exploration_cooldown_sec * (2 if defense["caution_mode"] else 1)
    if last_opened and (_now_dt() - last_opened).total_seconds() < cooldown:
        return False, "cooldown_active"
    if defense["loss_cluster"]:
        last_closed = _parse_time(str((closed_trades[0] if closed_trades else {}).get("closed_at") or ""))
        if last_closed and (_now_dt() - last_closed) < timedelta(minutes=20):
            return False, "loss_cluster_cooldown"
    if defense["negative_recent_edge"]:
        return False, "negative_recent_edge"
    score = _score(tick)
    rules = _profile_rules(promoted_profile or {}, cfg)
    session = _session_name(tick)
    blocked_sessions = {str(item).casefold() for item in (promoted_profile or {}).get("profile_rules", {}).get("blocked_sessions", [])}
    if session and session in blocked_sessions:
        return False, f"session_blocked:{session}"
    has_evidence, evidence_reason = _has_entry_evidence(tick, snapshot)
    if not has_evidence:
        return False, evidence_reason
    min_score = rules["min_score"] + (10 if defense["caution_mode"] else 0)
    if score is not None and score < min_score:
        return False, "score_too_low"
    momentum = _number(tick.get("momentum_score"))
    if momentum is not None and momentum < rules["min_momentum_score"]:
        return False, "momentum_score_low"
    trend = _number(tick.get("trend_score"))
    if trend is not None and trend < rules["min_trend_score"]:
        return False, "trend_score_low"
    volatility = _number(tick.get("volatility_score"))
    if volatility is not None and volatility < 35:
        return False, "volatility_too_low"
    side = _candidate_side(tick, snapshot)
    rsi = _number(tick.get("rsi") or tick.get("rsi14"))
    if rsi is not None and side == "sell" and rsi < rules["min_rsi_for_sell"] and not _explicit_confirmation(tick, "sell"):
        return False, "rsi_extreme_block"
    if rsi is not None and side == "buy" and rsi > rules["max_rsi_for_buy"] and not _explicit_confirmation(tick, "buy"):
        return False, "rsi_extreme_block"
    if _late_entry_risk(tick, side=side, price=price, rsi=rsi) and not _explicit_confirmation(tick, side):
        return False, "late_entry_risk"
    regime = str(tick.get("regime") or tick.get("market_regime") or "").casefold().strip()
    if regime in {"chop", "range", "sideways"}:
        return False, "regime_chop"
    if defense["time_stop_cluster"] and (
        regime in {"chop", "range", "sideways", "not_confirmed"}
        or (momentum is not None and momentum < 65)
        or momentum is None
        or (trend is not None and trend < 60)
        or trend is None
    ):
        return False, "time_stop_cluster"
    return True, "paper_forward_candidate_probe" if promoted_profile else "paper_exploration_probe"


def _has_entry_evidence(tick: dict[str, Any], snapshot: dict[str, Any]) -> tuple[bool, str]:
    side = _candidate_side(tick, snapshot)
    if _explicit_confirmation(tick, side):
        return True, ""
    if _primary_entry_score(tick) is not None:
        return True, ""
    trend = _number(tick.get("trend_score"))
    momentum = _number(tick.get("momentum_score"))
    if trend is not None and momentum is not None:
        return True, ""
    regime = str(tick.get("regime") or tick.get("market_regime") or "").casefold().strip()
    if regime in {"breakout", "breakdown", "trend", "bullish_exploration", "bearish_exploration"} and (trend is not None or momentum is not None):
        return True, ""
    previous_tick = snapshot.get("previous_tick") if isinstance(snapshot.get("previous_tick"), dict) else {}
    previous_price = _price(previous_tick)
    current_price = _price(tick)
    if previous_price is None or current_price is None:
        return False, "insufficient_entry_evidence"
    move = abs(current_price - previous_price)
    spread = _spread(tick) or 0.0
    min_move = max(spread, abs(current_price) * 0.0002)
    if move < min_move:
        return False, "tick_momentum_too_weak"
    return True, ""


def _primary_entry_score(tick: dict[str, Any]) -> float | None:
    for key in ("score", "final_score", "entry_quality_score"):
        value = _number(tick.get(key))
        if value is not None:
            return value
    return None


def _open_trade(
    symbol: str,
    normalized: str,
    tick: dict[str, Any],
    cfg: MT5BridgeConfig,
    snapshot: dict[str, Any],
    promoted_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    entry = _price(tick) or 0.0
    previous_tick = snapshot.get("previous_tick") if isinstance(snapshot.get("previous_tick"), dict) else {}
    previous_price = _price(previous_tick)
    side = "sell" if previous_price is not None and entry < previous_price else "buy"
    min_rr = max(1.0, float(cfg.paper_exploration_min_rr or cfg.min_rr or 1.2))
    stop = round(entry * (0.985 if side == "buy" else 1.015), 6)
    risk = abs(entry - stop)
    target = round(entry + risk * min_rr, 6) if side == "buy" else round(entry - risk * min_rr, 6)
    now = _now()
    return enrich_payload(
        {
            "shadow_trade_id": f"paper-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}",
            "symbol": symbol,
            "normalized_symbol": normalized,
            "timeframe": str(tick.get("timeframe") or "").upper(),
            "side": side,
            "action": side.upper(),
            "entry_price": entry,
            "entry": entry,
            "stop_loss": stop,
            "take_profit": target,
            "risk_reward": min_rr,
            "risk_pct": float(cfg.paper_exploration_risk_pct or 0.1),
            "opened_at": now,
            "last_price": entry,
            "unrealized_pnl": 0.0,
            "unrealized_pnl_pct": 0.0,
            "r_multiple": 0.0,
            "max_favorable_excursion": 0.0,
            "max_adverse_excursion": 0.0,
            "initial_risk": risk,
            "status": "open",
            "lifecycle_status": "open",
            "exit_price": None,
            "exit_reason": "",
            "closed_at": "",
            "source": "mt5_paper_exploration",
            "auto_forward": True,
            "paper_exploration": True,
            "included_in_exploration_metrics": True,
            "excluded_from_live_grade": True,
            "manual_test": False,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
            "reason": "paper_forward_candidate_probe" if promoted_profile else "paper_exploration_probe",
            "confidence": "low",
            "strategy_profile": promoted_profile.get("profile") if promoted_profile else "BTCUSD_PAPER_EXPLORATION_V1",
            "filter_profile": promoted_profile.get("profile") if promoted_profile else "",
            "profile_mode": promoted_profile.get("mode") if promoted_profile else "",
            "promoted_by": promoted_profile.get("promoted_by") if promoted_profile else "",
            "paper_forward_candidate": bool(promoted_profile),
            "features_snapshot": {
                "spread": _spread(tick),
                "score": _score(tick),
                "momentum_score": _number(tick.get("momentum_score")),
                "trend_score": _number(tick.get("trend_score")),
                "volatility_score": _number(tick.get("volatility_score")),
                "rsi": _number(tick.get("rsi") or tick.get("rsi14")),
                "regime": str(tick.get("regime") or tick.get("market_regime") or ""),
                "previous_price": previous_price,
                "trigger": "fast_path_snapshot",
                "promoted_profile": promoted_profile.get("profile") if promoted_profile else "",
            },
            "updated_at": now,
        }
    )


def _update_open_trade(trade: dict[str, Any], tick: dict[str, Any], cfg: MT5BridgeConfig) -> tuple[dict[str, Any], dict[str, Any] | None]:
    price = _price(tick)
    if price is None:
        return trade, None
    updated = dict(trade)
    side = str(updated.get("side") or "").lower()
    entry = _number(updated.get("entry_price")) or _number(updated.get("entry")) or price
    stop = _number(updated.get("stop_loss"))
    target = _number(updated.get("take_profit"))
    initial_risk = _number(updated.get("initial_risk")) or abs(entry - (stop or entry)) or max(entry * 0.015, 0.000001)
    pnl = price - entry if side == "buy" else entry - price
    pnl_pct = (pnl / entry) * 100 if entry else 0.0
    r_multiple = pnl / initial_risk if initial_risk else 0.0
    updated.update(
        {
            "last_price": price,
            "unrealized_pnl": round(pnl, 6),
            "unrealized_pnl_pct": round(pnl_pct, 6),
            "r_multiple": round(r_multiple, 6),
            "max_favorable_excursion": max(_number(updated.get("max_favorable_excursion")) or 0.0, pnl),
            "max_adverse_excursion": min(_number(updated.get("max_adverse_excursion")) or 0.0, pnl),
            "updated_at": _now(),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }
    )
    exit_reason = ""
    if side == "buy" and stop is not None and price <= stop:
        exit_reason = "stop_loss"
    elif side == "sell" and stop is not None and price >= stop:
        exit_reason = "stop_loss"
    elif side == "buy" and target is not None and price >= target:
        exit_reason = "take_profit"
    elif side == "sell" and target is not None and price <= target:
        exit_reason = "take_profit"
    elif _minutes_open(updated) >= cfg.paper_exploration_time_stop_min:
        exit_reason = "time_stop"
    spread = _spread(tick)
    if not exit_reason and spread is not None and spread > cfg.paper_exploration_max_spread_points * 2:
        exit_reason = "spread_extreme"
    if not exit_reason:
        return updated, None
    status = "win" if pnl > 0 else "loss" if pnl < 0 else "breakeven"
    closed = {
        **updated,
        "status": status,
        "lifecycle_status": "closed",
        "exit_price": price,
        "exit_reason": exit_reason,
        "closed_at": _now(),
        "pnl": round(pnl, 6),
        "pnl_pct": round(pnl_pct, 6),
        "r_multiple": round(r_multiple, 6),
        "unrealized_pnl": 0.0,
        "unrealized_pnl_pct": 0.0,
        "reason": updated.get("reason") or "paper_exploration_probe",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
    return {}, closed


def _summary(trades: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed" or trade.get("status") in {"win", "loss", "breakeven"}]
    wins = [trade for trade in closed if trade.get("status") == "win"]
    losses = [trade for trade in closed if trade.get("status") == "loss"]
    pnls = [_number(trade.get("r_multiple")) or _number(trade.get("pnl")) or 0.0 for trade in closed]
    gross_win = sum(value for value in pnls if value > 0)
    gross_loss = abs(sum(value for value in pnls if value < 0))
    recent_defense = _defense_state(closed)
    buy_stats = _side_stat(closed, "buy")
    sell_stats = _side_stat(closed, "sell")
    return {
        "shadow_trades": len(trades),
        "open": sum(1 for trade in trades if trade.get("lifecycle_status") == "open" or trade.get("status") == "open"),
        "closed": len(closed),
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": sum(1 for trade in closed if trade.get("status") == "breakeven"),
        "win_rate": round((len(wins) / len(closed)) * 100, 2) if closed else 0.0,
        "profit_factor": round(gross_win / gross_loss, 4) if gross_loss else round(gross_win, 4) if gross_win else 0.0,
        "expectancy": round(sum(pnls) / len(closed), 4) if closed else 0.0,
        "net_pnl": round(sum(pnls), 4),
        "max_drawdown": 0.0,
        "avg_win": round(gross_win / len(wins), 4) if wins else 0.0,
        "avg_loss": round(-gross_loss / len(losses), 4) if losses else 0.0,
        "rr_avg": round(sum(abs(value) for value in pnls) / len(closed), 4) if closed else 0.0,
        "time_stop_count": _exit_count(closed, "time_stop"),
        "stop_loss_count": _exit_count(closed, "stop_loss"),
        "take_profit_count": _exit_count(closed, "take_profit"),
        "signal_flip_count": _exit_count(closed, "signal_flip"),
        "buy_win_rate": buy_stats["win_rate"],
        "sell_win_rate": sell_stats["win_rate"],
        "buy_pf": buy_stats["profit_factor"],
        "sell_pf": sell_stats["profit_factor"],
        "negative_recent_edge": recent_defense["negative_recent_edge"],
        "caution_mode": recent_defense["caution_mode"],
        "loss_cluster": recent_defense["loss_cluster"],
        "time_stop_cluster": recent_defense["time_stop_cluster"],
    }


def _adaptive_from_summary(symbol: str, summary: dict[str, Any]) -> dict[str, Any]:
    closed = int(summary.get("closed") or 0)
    pf = _number(summary.get("profit_factor")) or 0.0
    win_rate = _number(summary.get("win_rate")) or 0.0
    expectancy = _number(summary.get("expectancy")) or 0.0
    time_stop_count = int(summary.get("time_stop_count") or 0)
    losses = int(summary.get("losses") or 0)
    time_stop_cluster = bool(summary.get("time_stop_cluster")) or (closed >= 10 and time_stop_count > (closed / 2))
    loss_cluster = bool(summary.get("loss_cluster")) or (closed >= 5 and losses >= 3 and win_rate <= 45)
    negative_edge = closed >= 15 and pf < 1.0 and expectancy <= 0
    if loss_cluster:
        bot_state = "pause_new_entries"
    elif negative_edge or time_stop_cluster:
        bot_state = "caution"
    else:
        bot_state = "normal" if closed < 20 or pf >= 1.0 else "caution"
    return {
        "ok": True,
        "status": "mt5_adaptive_state_ready",
        "symbol": symbol,
        "timeframe": "",
        "bot_state": bot_state,
        "closed_trades": closed,
        "current_win_streak": 0,
        "current_loss_streak": 3 if loss_cluster else 0,
        "last_10_win_rate": win_rate,
        "last_20_win_rate": win_rate,
        "rolling_win_rate": win_rate,
        "rolling_profit_factor": pf,
        "rolling_expectancy": expectancy,
        "rolling_drawdown": _number(summary.get("max_drawdown")) or 0.0,
        "regime_health": {
            "negative_edge": negative_edge,
            "caution": bot_state == "caution",
            "pause_new_entries": bot_state == "pause_new_entries",
            "time_stop_cluster": time_stop_cluster,
            "loss_cluster": loss_cluster,
        },
        "negative_edge": negative_edge,
        "time_stop_cluster": time_stop_cluster,
        "loss_cluster": loss_cluster,
        "recommendation_summary": _adaptive_message(bot_state, negative_edge, time_stop_cluster, loss_cluster),
        "data_source_used": "runtime_snapshot",
        "updated_at": _now(),
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _reading(symbol: str, summary: dict[str, Any]) -> str:
    if summary["open"]:
        return f"{symbol}: paper exploration mantiene una operacion sombra abierta; sin broker real."
    if int(summary.get("time_stop_count") or 0) > max(0, int(summary.get("closed") or 0) // 2) and int(summary.get("closed") or 0) >= 10:
        return f"{symbol}: time_stop_cluster detectado; Genesis exige mas momentum antes de nuevas entradas paper."
    if summary["closed"]:
        return f"{symbol}: paper exploration cerro {summary['closed']} trade(s), win rate {summary['win_rate']}%, PF {summary['profit_factor']}."
    return f"{symbol}: paper exploration listo; aun sin cierres suficientes."


def _defense_state(closed_trades: list[dict[str, Any]]) -> dict[str, Any]:
    recent_5 = closed_trades[:5]
    recent_10 = closed_trades[:10]
    pf5 = _profit_factor_for(recent_5)
    wr5 = _win_rate_for(recent_5)
    pf_all = _profit_factor_for(closed_trades)
    losses5 = sum(1 for trade in recent_5 if trade.get("status") == "loss")
    time_stops10 = sum(1 for trade in recent_10 if str(trade.get("exit_reason") or "") == "time_stop")
    return {
        "negative_recent_edge": len(recent_5) >= 5 and pf5 < 1.0 and wr5 < 45,
        "caution_mode": len(closed_trades) >= 15 and pf_all < 1.0,
        "loss_cluster": len(recent_5) >= 5 and losses5 >= 3,
        "time_stop_cluster": len(recent_10) >= 10 and time_stops10 > len(recent_10) / 2,
    }


def _exit_count(closed: list[dict[str, Any]], reason: str) -> int:
    return sum(1 for trade in closed if str(trade.get("exit_reason") or "") == reason)


def _side_stat(closed: list[dict[str, Any]], side: str) -> dict[str, float]:
    items = [trade for trade in closed if str(trade.get("side") or "").casefold() == side]
    return {"win_rate": _win_rate_for(items), "profit_factor": _profit_factor_for(items)}


def _profit_factor_for(trades: list[dict[str, Any]]) -> float:
    pnls = [_number(trade.get("r_multiple")) or _number(trade.get("pnl")) or 0.0 for trade in trades if trade.get("status") in {"win", "loss", "breakeven"}]
    gross_win = sum(value for value in pnls if value > 0)
    gross_loss = abs(sum(value for value in pnls if value < 0))
    if gross_win <= 0 and gross_loss <= 0:
        return 0.0
    if gross_loss <= 0:
        return round(gross_win, 4)
    return round(gross_win / gross_loss, 4)


def _win_rate_for(trades: list[dict[str, Any]]) -> float:
    closed = [trade for trade in trades if trade.get("status") in {"win", "loss", "breakeven"}]
    if not closed:
        return 0.0
    wins = sum(1 for trade in closed if trade.get("status") == "win")
    return round((wins / len(closed)) * 100, 2)


def _adaptive_message(bot_state: str, negative_edge: bool, time_stop_cluster: bool, loss_cluster: bool) -> str:
    if loss_cluster:
        return "Loss cluster detectado: pausar nuevas entradas paper temporalmente."
    if time_stop_cluster:
        return "Time stop cluster detectado: evitar rango/chop y exigir momentum claro."
    if negative_edge:
        return "Negative edge paper detectado: activar caution y reducir entradas exploratorias."
    if bot_state == "caution":
        return "Estado caution: subir score minimo y duplicar cooldown paper."
    return "Paper exploration runtime snapshot; learning pesado aislado."


def _price(tick: dict[str, Any]) -> float | None:
    for key in ("last", "price"):
        value = _number(tick.get(key))
        if value is not None:
            return value
    bid = _number(tick.get("bid"))
    ask = _number(tick.get("ask"))
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    return bid if bid is not None else ask


def _spread(tick: dict[str, Any]) -> float | None:
    spread = _number(tick.get("spread"))
    if spread is not None:
        return spread
    bid = _number(tick.get("bid"))
    ask = _number(tick.get("ask"))
    if bid is not None and ask is not None:
        return abs(ask - bid)
    return None


def _score(tick: dict[str, Any]) -> float | None:
    for key in ("score", "final_score", "entry_quality_score", "trend_score", "momentum_score"):
        value = _number(tick.get(key))
        if value is not None:
            return value
    return None


def _candidate_side(tick: dict[str, Any], snapshot: dict[str, Any]) -> str:
    action = str(tick.get("action") or tick.get("decision") or tick.get("side") or "").casefold().strip()
    if action in {"buy", "sell"}:
        return action
    price = _price(tick)
    previous_tick = snapshot.get("previous_tick") if isinstance(snapshot.get("previous_tick"), dict) else {}
    previous_price = _price(previous_tick)
    return "sell" if price is not None and previous_price is not None and price < previous_price else "buy"


def _profile_rules(promoted_profile: dict[str, Any], cfg: MT5BridgeConfig) -> dict[str, float]:
    if promoted_profile.get("active") and promoted_profile.get("profile") == "eth_m30_vol_breakout_chop_guard_v1":
        rules = promoted_profile.get("profile_rules") if isinstance(promoted_profile.get("profile_rules"), dict) else {}
        return {
            "min_score": float(_number(rules.get("min_score")) or 58.0),
            "min_momentum_score": float(_number(rules.get("min_momentum_score")) or 50.0),
            "min_trend_score": float(_number(rules.get("min_trend_score")) or 50.0),
            "max_rsi_for_buy": 75.0,
            "min_rsi_for_sell": 25.0,
        }
    if promoted_profile.get("active") and promoted_profile.get("profile") == "quality_loose":
        return {
            "min_score": 50.0,
            "min_momentum_score": 35.0,
            "min_trend_score": 35.0,
            "max_rsi_for_buy": 80.0,
            "min_rsi_for_sell": 20.0,
        }
    return {
        "min_score": float(cfg.paper_exploration_min_score or 45.0),
        "min_momentum_score": 55.0,
        "min_trend_score": 55.0,
        "max_rsi_for_buy": 75.0,
        "min_rsi_for_sell": 25.0,
    }


def _explicit_confirmation(tick: dict[str, Any], side: str) -> bool:
    if bool(tick.get("breakout_confirmed")) or bool(tick.get("breakdown_confirmed")) or bool(tick.get("retest_confirmed")):
        return True
    confirmation = str(tick.get("confirmation") or tick.get("setup_confirmation") or "").casefold()
    if side == "buy":
        return "breakout" in confirmation or "retest" in confirmation
    if side == "sell":
        return "breakdown" in confirmation or "retest" in confirmation
    return False


def _session_name(tick: dict[str, Any]) -> str:
    hour = _hour(tick)
    if hour is None:
        return ""
    if 21 <= hour <= 23:
        return "off_session"
    if 13 <= hour <= 20:
        return "ny_core"
    if 7 <= hour <= 20:
        return "london_us"
    if 0 <= hour <= 7:
        return "asia"
    return ""


def _hour(tick: dict[str, Any]) -> int | None:
    parsed = _number(tick.get("hour") or tick.get("hour_utc"))
    if parsed is not None:
        hour = int(parsed)
        return hour if 0 <= hour <= 23 else None
    value = str(tick.get("time") or tick.get("timestamp") or tick.get("datetime") or "").strip()
    if not value:
        return None
    try:
        parsed_dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed_dt.hour


def _late_entry_risk(tick: dict[str, Any], *, side: str, price: float, rsi: float | None) -> bool:
    distance = _number(tick.get("ema20_distance_pct") or tick.get("distance_from_ema20_pct"))
    if distance is None:
        ema20 = _number(tick.get("ema20"))
        if ema20 and price:
            distance = abs(price - ema20) / price * 100
    distance50 = _number(tick.get("ema50_distance_pct") or tick.get("distance_from_ema50_pct"))
    if distance50 is None:
        ema50 = _number(tick.get("ema50"))
        if ema50 and price:
            distance50 = abs(price - ema50) / price * 100
    if distance is not None and abs(distance) > 3.5:
        return True
    if distance50 is not None and abs(distance50) > 7.0:
        return True
    if side == "sell" and rsi is not None and rsi < 30:
        return True
    if side == "buy" and rsi is not None and rsi > 70:
        return True
    return False


def _minutes_open(trade: dict[str, Any]) -> float:
    opened = _parse_time(trade.get("opened_at"))
    if not opened:
        return 0.0
    return (_now_dt() - opened).total_seconds() / 60.0


def _parse_time(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _number(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _now() -> str:
    return _now_dt().isoformat()

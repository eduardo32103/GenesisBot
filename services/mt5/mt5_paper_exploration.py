from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from services.mt5.instrument_resolver import enrich_payload, normalize_mt5_symbol
from services.mt5.mt5_ingest_queue import enqueue_mt5_event
from services.mt5.mt5_risk_guard import MT5BridgeConfig
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
) -> dict[str, Any]:
    cfg = config or MT5BridgeConfig.from_env()
    clean_symbol = str(symbol or (tick or {}).get("symbol") or "").upper().strip()
    normalized = normalize_mt5_symbol(clean_symbol)
    snapshot = get_snapshot(clean_symbol) or {}
    active_tick = tick if isinstance(tick, dict) and tick else snapshot.get("last_tick") if isinstance(snapshot.get("last_tick"), dict) else {}
    state = snapshot.get("paper_exploration_state") if isinstance(snapshot.get("paper_exploration_state"), dict) else {}
    open_trade = snapshot.get("open_shadow_trade") if isinstance(snapshot.get("open_shadow_trade"), dict) else {}
    closed_event: dict[str, Any] | None = None
    opened_event: dict[str, Any] | None = None

    if open_trade:
        open_trade, closed_event = _update_open_trade(open_trade, active_tick, cfg)
        if closed_event:
            update_open_shadow_trade(clean_symbol, None)
            append_closed_shadow_trade(clean_symbol, closed_event)
            enqueue_mt5_event("mt5_shadow_trades", clean_symbol, closed_event)
            state = {
                **state,
                "last_closed_at": closed_event.get("closed_at") or _now(),
                "last_reason": closed_event.get("exit_reason") or "",
            }
        else:
            update_open_shadow_trade(clean_symbol, open_trade)

    can_open, block_reason = _can_open(clean_symbol, normalized, active_tick, cfg, state, bool(open_trade and not closed_event))
    if can_open:
        opened_event = _open_trade(clean_symbol, normalized, active_tick, cfg, snapshot)
        update_open_shadow_trade(clean_symbol, opened_event)
        enqueue_mt5_event("mt5_shadow_trades", clean_symbol, opened_event)
        state = {
            **state,
            "last_opened_at": opened_event["opened_at"],
            "last_reason": opened_event["reason"],
            "last_shadow_trade_id": opened_event["shadow_trade_id"],
        }
        block_reason = ""

    update_snapshot(clean_symbol, {"paper_exploration_state": state})
    performance_payload = update_runtime_performance(clean_symbol)
    result = {
        "paper_exploration_enabled": cfg.paper_exploration_enabled,
        "paper_exploration_attempted": bool(cfg.paper_exploration_enabled),
        "paper_exploration_created": bool(opened_event),
        "paper_exploration_closed": bool(closed_event),
        "paper_exploration_reason": (opened_event or closed_event or {}).get("reason")
        or (closed_event or {}).get("exit_reason")
        or block_reason,
        "shadow_trade_id": (opened_event or open_trade or {}).get("shadow_trade_id") or "",
        "open_shadow_trade": opened_event or ({} if closed_event else open_trade),
        "closed_shadow_trade": closed_event,
        "latest_performance_summary": performance_payload.get("summary") or {},
        "trigger": trigger,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
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


def _can_open(
    symbol: str,
    normalized: str,
    tick: dict[str, Any],
    cfg: MT5BridgeConfig,
    state: dict[str, Any],
    has_open_trade: bool,
) -> tuple[bool, str]:
    if not cfg.paper_exploration_enabled:
        return False, "paper_exploration_disabled"
    if normalized != "BTCUSD":
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
    if last_opened and (_now_dt() - last_opened).total_seconds() < cfg.paper_exploration_cooldown_sec:
        return False, "cooldown_active"
    score = _score(tick)
    if score is not None and score < cfg.paper_exploration_min_score:
        return False, "score_too_low"
    return True, "paper_exploration_probe"


def _open_trade(symbol: str, normalized: str, tick: dict[str, Any], cfg: MT5BridgeConfig, snapshot: dict[str, Any]) -> dict[str, Any]:
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
            "reason": "paper_exploration_probe",
            "confidence": "low",
            "features_snapshot": {
                "spread": _spread(tick),
                "score": _score(tick),
                "previous_price": previous_price,
                "trigger": "fast_path_snapshot",
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
    }


def _adaptive_from_summary(symbol: str, summary: dict[str, Any]) -> dict[str, Any]:
    closed = int(summary.get("closed") or 0)
    pf = _number(summary.get("profit_factor")) or 0.0
    return {
        "ok": True,
        "status": "mt5_adaptive_state_ready",
        "symbol": symbol,
        "timeframe": "",
        "bot_state": "normal" if closed < 20 or pf >= 1.0 else "caution",
        "closed_trades": closed,
        "current_win_streak": 0,
        "current_loss_streak": 0,
        "last_10_win_rate": _number(summary.get("win_rate")) or 0.0,
        "last_20_win_rate": _number(summary.get("win_rate")) or 0.0,
        "rolling_win_rate": _number(summary.get("win_rate")) or 0.0,
        "rolling_profit_factor": pf,
        "rolling_expectancy": _number(summary.get("expectancy")) or 0.0,
        "rolling_drawdown": _number(summary.get("max_drawdown")) or 0.0,
        "regime_health": {},
        "recommendation_summary": "Paper exploration runtime snapshot; learning pesado aislado.",
        "data_source_used": "runtime_snapshot",
        "updated_at": _now(),
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _reading(symbol: str, summary: dict[str, Any]) -> str:
    if summary["open"]:
        return f"{symbol}: paper exploration mantiene una operacion sombra abierta; sin broker real."
    if summary["closed"]:
        return f"{symbol}: paper exploration cerro {summary['closed']} trade(s), win rate {summary['win_rate']}%, PF {summary['profit_factor']}."
    return f"{symbol}: paper exploration listo; aun sin cierres suficientes."


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

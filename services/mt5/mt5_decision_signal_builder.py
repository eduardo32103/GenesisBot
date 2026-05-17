from __future__ import annotations

import os
from typing import Any


def build_actionable_mt5_decision(
    symbol: str,
    tick: dict[str, Any] | None,
    context: dict[str, Any] | None,
    *,
    min_rr: float | None = None,
    risk_pct: float = 0.5,
) -> dict[str, Any]:
    """Build a complete journal-only MT5 decision from Genesis context.

    BUY/SELL decisions must include entry, stop_loss, take_profit and a valid
    risk/reward ratio. When those pieces cannot be created safely, the builder
    downgrades the decision to NO_TRADE instead of producing a half-actionable
    signal.
    """

    clean_symbol = str(symbol or "").upper().strip()
    clean_tick = tick or {}
    clean_context = context or {}
    rr_required = float(min_rr if min_rr is not None else _env_float("MT5_MIN_RR", 1.2))
    confidence = _confidence(clean_context.get("confidence"))
    no_trade_score = _int(clean_context.get("no_trade_score"))
    hedge_score = _int(clean_context.get("hedge_score"))
    strategy_profile = str(
        clean_context.get("recommended_strategy_profile")
        or clean_context.get("strategy_profile")
        or clean_context.get("recommended_preset")
        or "Genesis MT5 Auto Forward"
    )
    base = {
        "symbol": clean_symbol,
        "decision": "NO_TRADE",
        "actionable": False,
        "entry": None,
        "stop_loss": None,
        "take_profit": None,
        "risk_reward": 0.0,
        "risk_pct": risk_pct,
        "strategy_profile": strategy_profile,
        "confidence": confidence,
        "reason": "not_evaluated",
    }

    if not clean_symbol:
        return {**base, "reason": "missing_symbol"}

    raw_decision = _decision_from_context(clean_context)
    if raw_decision not in {"BUY", "SELL"}:
        return {**base, "decision": raw_decision, "reason": _reason(clean_context, raw_decision)}
    if no_trade_score >= 70:
        return {**base, "reason": "no_trade_score_block"}
    if hedge_score >= 80:
        return {**base, "reason": "hedge_score_hard_block"}
    if confidence not in {"medium", "high"}:
        return {**base, "reason": "confidence_low"}

    entry = _number(
        _first_present(clean_context, ("entry", "price"))
        or (clean_context.get("technical_context") or {}).get("price")
        or _price(clean_tick)
    )
    if entry is None or entry <= 0:
        return {**base, "decision": "NO_TRADE", "reason": "missing_entry"}

    stop_loss = _number(_first_present(clean_context, ("stop_loss", "stop", "sl")))
    take_profit = _number(_first_present(clean_context, ("take_profit", "target", "tp")))
    atr = _number(
        _first_present(clean_context, ("atr", "atr14"))
        or (clean_context.get("technical_context") or {}).get("atr")
        or (clean_context.get("technical_context") or {}).get("atr14")
    )
    stop_distance = _stop_distance(clean_symbol, clean_context, entry, atr)

    if stop_loss is None and stop_distance is not None:
        stop_loss = round(entry - stop_distance, 8) if raw_decision == "BUY" else round(entry + stop_distance, 8)
    if stop_loss is None:
        return {**base, "decision": "NO_TRADE", "entry": entry, "reason": "missing_risk_parameters"}

    risk = abs(entry - stop_loss)
    if risk <= 0:
        return {**base, "decision": "NO_TRADE", "entry": entry, "stop_loss": stop_loss, "reason": "invalid_risk_parameters"}
    if take_profit is None:
        take_profit = round(entry + risk * rr_required, 8) if raw_decision == "BUY" else round(entry - risk * rr_required, 8)

    risk_reward = _risk_reward(raw_decision, entry, stop_loss, take_profit)
    if risk_reward is None:
        return {
            **base,
            "decision": "NO_TRADE",
            "entry": entry,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "reason": "invalid_risk_parameters",
        }
    if risk_reward < rr_required:
        return {
            **base,
            "decision": "NO_TRADE",
            "entry": entry,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "risk_reward": risk_reward,
            "reason": "risk_reward_too_low",
        }

    return {
        **base,
        "decision": raw_decision,
        "actionable": True,
        "entry": entry,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "risk_reward": risk_reward,
        "reason": _reason(clean_context, raw_decision),
    }


def _decision_from_context(context: dict[str, Any]) -> str:
    explicit = str(context.get("decision") or context.get("action") or "").upper().strip()
    if explicit in {"BUY", "SELL", "WAIT", "NO_TRADE", "HEDGE", "REDUCE"}:
        return explicit
    if not context.get("ok", True):
        return "NO_TRADE"
    if _int(context.get("no_trade_score")) >= 70:
        return "NO_TRADE"
    hedge_score = _int(context.get("hedge_score"))
    if hedge_score >= 80:
        return "NO_TRADE"
    if hedge_score >= 65:
        return "HEDGE" if context.get("hedge_needed") else "REDUCE"
    confidence = _confidence(context.get("confidence"))
    if confidence not in {"medium", "high"}:
        return "WAIT"
    bias = str(context.get("bias") or "neutral").casefold()
    if bias == "bullish":
        return "BUY"
    if bias == "bearish":
        return "SELL"
    return "WAIT"


def _stop_distance(symbol: str, context: dict[str, Any], entry: float, atr: float | None) -> float | None:
    if atr is not None and atr > 0:
        return atr
    pct = _number(context.get("fallback_stop_pct"))
    if pct is None or pct <= 0:
        pct = 0.02 if _is_crypto(symbol, context) else 0.01
    return round(entry * pct, 8) if entry > 0 else None


def _is_crypto(symbol: str, context: dict[str, Any]) -> bool:
    asset_class = str(context.get("asset_class") or context.get("assetProfile") or "").casefold()
    return "crypto" in asset_class or "BTC" in symbol.upper() or "ETH" in symbol.upper()


def _risk_reward(action: str, entry: float, stop: float, target: float | None) -> float | None:
    if target is None:
        return None
    risk = abs(entry - stop)
    reward = abs(target - entry)
    if risk <= 0 or reward <= 0:
        return None
    if action == "BUY" and not (stop < entry < target):
        return None
    if action == "SELL" and not (target < entry < stop):
        return None
    return round(reward / risk, 4)


def _reason(context: dict[str, Any], decision: str) -> str:
    if context.get("reason"):
        return str(context.get("reason"))
    if decision in {"BUY", "SELL"}:
        return f"actionable_{decision.lower()}_from_genesis_context"
    if decision == "NO_TRADE":
        return "no_edge_or_risk_guard"
    return "wait_for_better_edge"


def _first_present(payload: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if payload.get(key) not in (None, ""):
            return payload.get(key)
    return None


def _price(payload: dict[str, Any]) -> float | None:
    for key in ("last", "price"):
        value = _number(payload.get(key))
        if value is not None:
            return value
    bid = _number(payload.get("bid"))
    ask = _number(payload.get("ask"))
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    return bid if bid is not None else ask


def _number(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: object) -> int:
    return int(_number(value) or 0)


def _confidence(value: object) -> str:
    text = str(value or "low").casefold().strip()
    aliases = {"alta": "high", "media": "medium", "baja": "low"}
    return aliases.get(text, text)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default

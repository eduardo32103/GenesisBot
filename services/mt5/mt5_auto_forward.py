from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from services.genesis.genesis_brain import GenesisBrain
from services.genesis.memory_store import MemoryStore
from services.mt5.mt5_decision_signal_builder import build_actionable_mt5_decision
from services.mt5.mt5_journal import MT5Journal
from services.mt5.mt5_order_model import sanitize_payload
from services.mt5.mt5_performance import MT5Performance
from services.mt5.mt5_risk_guard import MT5BridgeConfig
from services.mt5.mt5_shadow_trading import MT5ShadowTrading
from services.mt5.mt5_symbol_mapper import MT5SymbolMapper


class MT5AutoForward:
    """Turns MT5 ticks into journal-only forward-test decisions.

    This class never sends broker orders. It only asks Genesis for context,
    applies forward-test risk gates, and records synthetic shadow trades.
    """

    def __init__(
        self,
        *,
        memory: MemoryStore | None = None,
        config: MT5BridgeConfig | None = None,
        symbol_mapper: MT5SymbolMapper | None = None,
    ) -> None:
        self.memory = memory or MemoryStore()
        self.config = config or MT5BridgeConfig.from_env()
        self.symbol_mapper = symbol_mapper or MT5SymbolMapper()
        self.journal = MT5Journal(memory=self.memory)
        self.shadow = MT5ShadowTrading(memory=self.memory)
        self.performance = MT5Performance(memory=self.memory)

    def process_tick(self, tick: dict[str, Any] | None) -> dict[str, Any]:
        clean = sanitize_payload(tick or {})
        raw_symbol = _symbol(clean.get("symbol") or clean.get("ticker"))
        last = _price(clean)
        if not raw_symbol or last is None:
            return _blocked("tick_missing_symbol_or_price")

        symbol_info = self.symbol_mapper.map_symbol(raw_symbol)
        if not symbol_info["ok"]:
            decision = self._decision_payload(
                symbol=raw_symbol,
                genesis_symbol=str(symbol_info.get("genesis_symbol") or raw_symbol),
                decision="NO_TRADE",
                confidence="low",
                reason=str(symbol_info.get("reason") or "symbol_not_mapped_or_not_allowed"),
                tick=clean,
                context={},
                entry=last,
                stop_loss=None,
                take_profit=None,
                risk_reward=None,
                actionable=False,
                warnings=list(symbol_info.get("warnings") or []),
            )
            event = self.journal.save("mt5_decisions", raw_symbol, decision, confidence="low")
            self.shadow.record_no_trade_signal({**decision, "price": last})
            return {
                "ok": True,
                "status": "mt5_auto_forward_blocked",
                "reason": decision["reason"],
                "decision": decision,
                "event": event,
                "shadow_trade_created": False,
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }

        if self.config.kill_switch:
            decision = self._decision_payload(
                symbol=raw_symbol,
                genesis_symbol=str(symbol_info.get("genesis_symbol") or raw_symbol),
                decision="NO_TRADE",
                confidence="low",
                reason="kill_switch_active",
                tick=clean,
                context={},
                entry=last,
                stop_loss=None,
                take_profit=None,
                risk_reward=None,
                actionable=False,
                warnings=list(symbol_info.get("warnings") or []),
            )
            event = self.journal.save("mt5_decisions", raw_symbol, decision, confidence="low")
            self.shadow.record_no_trade_signal({**decision, "price": last})
            return {
                "ok": True,
                "status": "mt5_auto_forward_blocked",
                "auto_forward_enabled": False,
                "reason": "kill_switch_active",
                "decision": decision,
                "event": event,
                "shadow_trade_created": False,
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }

        context = GenesisBrain(memory=self.memory).build_trading_context(symbol_info["genesis_symbol"])
        built = build_actionable_mt5_decision(
            raw_symbol,
            clean,
            context,
            min_rr=self.config.min_rr,
            risk_pct=min(self.config.max_position_risk_pct, 0.5),
        )
        decision_name = str(built.get("decision") or "NO_TRADE")
        confidence = _confidence(built.get("confidence") or context.get("confidence"))
        no_trade_score = _int(context.get("no_trade_score"))
        hedge_score = _int(context.get("hedge_score"))
        entry = _number(built.get("entry"))
        stop_loss = _number(built.get("stop_loss"))
        take_profit = _number(built.get("take_profit"))
        risk_reward = _number(built.get("risk_reward"))
        reason = str(built.get("reason") or _reason_from_context(context, decision_name))
        block_reason = self._block_reason(
            symbol=raw_symbol,
            decision=decision_name,
            confidence=confidence,
            no_trade_score=no_trade_score,
            hedge_score=hedge_score,
            entry=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            risk_reward=risk_reward,
            spread=_number(clean.get("spread_points") or clean.get("spread")),
            is_demo=_is_demo(clean),
        )
        final_decision = decision_name if not block_reason else "NO_TRADE"
        final_actionable = bool(built.get("actionable")) and final_decision in {"BUY", "SELL"}
        if final_decision not in {"BUY", "SELL"}:
            entry = None
            stop_loss = None
            take_profit = None
            risk_reward = 0.0
        decision = self._decision_payload(
            symbol=raw_symbol,
            genesis_symbol=str(symbol_info.get("genesis_symbol") or raw_symbol),
            decision=final_decision,
            confidence=confidence,
            reason=block_reason or reason,
            tick=clean,
            context=context,
            entry=entry,
            stop_loss=stop_loss if final_decision in {"BUY", "SELL"} else None,
            take_profit=take_profit if final_decision in {"BUY", "SELL"} else None,
            risk_reward=risk_reward,
            actionable=final_actionable,
            warnings=list(symbol_info.get("warnings") or []),
        )
        logging.getLogger("genesis.mt5").info(
            "MT5_AUTO_FORWARD_EVAL symbol=%s decision=%s actionable=%s reason=%s",
            raw_symbol,
            final_decision,
            final_actionable,
            decision["reason"],
        )
        event = self.journal.save("mt5_decisions", raw_symbol, decision, confidence=confidence)

        shadow: dict[str, Any] = {"created": False, "status": "not_actionable", "reason": decision["reason"]}
        if final_decision in {"BUY", "SELL"}:
            shadow = self.shadow.create_shadow_trade(
                {
                    **decision,
                    "symbol": raw_symbol,
                    "action": final_decision,
                    "entry": entry,
                    "stop_loss": stop_loss,
                    "take_profit": take_profit,
                    "risk_reward": risk_reward,
                    "risk_pct": decision["risk_pct"],
                    "source": "mt5_auto_forward",
                    "auto_forward": True,
                    "timestamp": decision["generated_at"],
                }
            )
            shadow_trade = shadow.get("trade") if isinstance(shadow.get("trade"), dict) else {}
            if shadow.get("created"):
                logging.getLogger("genesis.mt5").info(
                    "MT5_AUTO_SHADOW_CREATED id=%s symbol=%s entry=%s stop=%s target=%s",
                    shadow_trade.get("shadow_trade_id"),
                    raw_symbol,
                    entry,
                    stop_loss,
                    take_profit,
                )
        else:
            self.shadow.record_no_trade_signal({**decision, "price": last})
            logging.getLogger("genesis.mt5").info("MT5_AUTO_FORWARD_BLOCKED symbol=%s reason=%s", raw_symbol, decision["reason"])

        shadow_trade = shadow.get("trade") if isinstance(shadow.get("trade"), dict) else {}
        return {
            "ok": True,
            "status": "mt5_auto_forward_decision_recorded",
            "auto_forward_enabled": not self.config.kill_switch,
            "decision": decision,
            "event": event,
            "shadow": shadow,
            "shadow_trade_created": bool(shadow.get("created")),
            "shadow_trade_id": shadow_trade.get("shadow_trade_id") or "",
            "reason": decision["reason"],
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def status(self, *, symbol: str = "") -> dict[str, Any]:
        clean_symbol = _symbol(symbol)
        last_tick = _latest_payload(self.memory, "mt5_ticks", clean_symbol)
        last_signal = _latest_payload(self.memory, "mt5_signals", clean_symbol)
        decision_rows = self.memory.get_mt5_events("mt5_decisions", clean_symbol or None, limit=50)
        last_decision = _payload_from_row(decision_rows[0]) if decision_rows else None
        if last_decision:
            last_decision = {**last_decision, "reason": _reason_alias(last_decision.get("reason") or "")}
        last_valid_decision = _latest_decision_by_actionable(decision_rows, True)
        last_invalid_decision = _latest_decision_by_actionable(decision_rows, False)
        snapshot = self.shadow.snapshot(symbol=clean_symbol)
        performance = self.performance.report(symbol=clean_symbol)
        summary = performance.get("summary") if isinstance(performance.get("summary"), dict) else {}
        open_trades = snapshot.get("open_trades") or []
        last_block_reason = _reason_alias((last_invalid_decision or {}).get("reason") or "")
        current_state_reason = "active_open_trade" if open_trades else ""
        last_reason = current_state_reason or _reason_alias((last_decision or {}).get("reason") or "no_auto_forward_decision_yet")
        return {
            "ok": True,
            "status": "mt5_auto_forward_status_ready",
            "symbol": clean_symbol,
            "original_symbol": (last_tick or last_decision or {}).get("original_symbol") or clean_symbol,
            "normalized_symbol": (last_tick or last_decision or {}).get("normalized_symbol") or clean_symbol,
            "instrument_type": (last_tick or last_decision or {}).get("instrument_type") or "",
            "is_spot_crypto": bool((last_tick or last_decision or {}).get("is_spot_crypto")),
            "auto_forward_enabled": not self.config.kill_switch,
            "last_tick": last_tick,
            "last_tick_status": "mt5_tick_recorded" if last_tick else "",
            "last_tick_ea_version": (last_tick or {}).get("ea_version") or "",
            "last_signal": last_signal,
            "last_signal_status": (last_signal or {}).get("signal_status") or (last_signal or {}).get("status") or "",
            "last_signal_error": (last_signal or {}).get("signal_error") or "",
            "last_decision": last_decision,
            "last_valid_decision": last_valid_decision,
            "last_invalid_decision": last_invalid_decision,
            "last_block_reason": last_block_reason,
            "current_state_reason": current_state_reason,
            "last_reason": last_reason,
            "last_actionable": bool((last_decision or {}).get("actionable")),
            "entry": (last_decision or {}).get("entry"),
            "stop_loss": (last_decision or {}).get("stop_loss"),
            "take_profit": (last_decision or {}).get("take_profit"),
            "risk_reward": (last_decision or {}).get("risk_reward") or 0.0,
            "last_shadow_trade": snapshot["items"][0] if snapshot.get("items") else None,
            "open_trades": open_trades,
            "closed_trades": snapshot.get("closed_trades") or [],
            "excluded_trades": snapshot.get("excluded_trades") or [],
            "excluded_count": snapshot.get("excluded_count", 0),
            "manual_shadow_trades": summary.get("manual_shadow_trades", 0),
            "auto_shadow_trades": summary.get("auto_shadow_trades", 0),
            "total_shadow_trades": summary.get("total_shadow_trades", summary.get("shadow_trades", 0)),
            "sample_warning": summary.get("sample_warning") or "",
            "win_rate": summary.get("win_rate", 0.0),
            "profit_factor": summary.get("profit_factor", 0.0),
            "expectancy": summary.get("expectancy", 0.0),
            "genesis_reading": performance.get("genesis_reading") or "",
            "reason": last_reason,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
            "updated_at": _now(),
        }

    def _block_reason(
        self,
        *,
        symbol: str,
        decision: str,
        confidence: str,
        no_trade_score: int,
        hedge_score: int,
        entry: float | None,
        stop_loss: float | None,
        take_profit: float | None,
        risk_reward: float | None,
        spread: float | None,
        is_demo: bool,
    ) -> str:
        if self.config.kill_switch:
            return "kill_switch_active"
        if decision not in {"BUY", "SELL"}:
            return ""
        if self.config.demo_only and not is_demo:
            return "demo_account_not_confirmed"
        if no_trade_score >= 70:
            return "no_trade_score_block"
        if hedge_score >= 80:
            return "hedge_score_hard_block"
        if confidence not in {"medium", "high"}:
            return "confidence_low"
        if entry is None:
            return "missing_risk_parameters"
        if stop_loss is None:
            return "missing_risk_parameters"
        if take_profit is None:
            return "missing_risk_parameters"
        if risk_reward is None or risk_reward < self.config.min_rr:
            return "risk_reward_too_low"
        if self.shadow.open_trades(symbol):
            return "duplicate_open_trade"
        if spread is not None and spread > self.config.max_spread_points:
            return "spread_too_high"
        return ""

    def _decision_payload(
        self,
        *,
        symbol: str,
        genesis_symbol: str,
        decision: str,
        confidence: str,
        reason: str,
        tick: dict[str, Any],
        context: dict[str, Any],
        entry: float | None,
        stop_loss: float | None,
        take_profit: float | None,
        risk_reward: float | None,
        actionable: bool,
        warnings: list[str],
    ) -> dict[str, Any]:
        return {
            "symbol": symbol,
            "genesis_symbol": genesis_symbol,
            "original_symbol": str(tick.get("original_symbol") or symbol),
            "normalized_symbol": str(tick.get("normalized_symbol") or symbol),
            "instrument_type": str(tick.get("instrument_type") or ""),
            "is_spot_crypto": bool(tick.get("is_spot_crypto")),
            "decision": decision,
            "action": decision,
            "confidence": confidence,
            "reason": reason,
            "entry": entry if decision in {"BUY", "SELL"} else None,
            "stop_loss": stop_loss if decision in {"BUY", "SELL"} else None,
            "take_profit": take_profit if decision in {"BUY", "SELL"} else None,
            "risk_pct": min(self.config.max_position_risk_pct, 0.5) if decision in {"BUY", "SELL"} else 0.0,
            "risk_reward": risk_reward if decision in {"BUY", "SELL"} else 0.0,
            "actionable": actionable,
            "timeframe": str(tick.get("timeframe") or context.get("recommended_timeframe") or "").upper(),
            "strategy_profile": str(context.get("recommended_strategy_profile") or context.get("recommended_preset") or "Genesis Auto Forward"),
            "hedge_score": _int(context.get("hedge_score")),
            "no_trade_score": _int(context.get("no_trade_score")),
            "genesis_context_score": _int(context.get("genesis_context_score")),
            "market_regime": context.get("market_regime") or "",
            "risk_flags": list(context.get("risk_flags") or []) + warnings,
            "what_to_watch": context.get("what_to_watch") or [],
            "auto_forward": True,
            "source": "mt5_auto_forward",
            "order_policy": "journal_only_no_broker",
            "broker_touched": False,
            "order_executed": False,
            "generated_at": _now(),
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


def _reason_from_context(context: dict[str, Any], decision: str) -> str:
    if context.get("reason"):
        return str(context.get("reason"))
    if decision in {"BUY", "SELL"}:
        return f"auto_forward_{decision.lower()}_from_genesis_context"
    if decision == "NO_TRADE":
        return "no_edge_or_risk_guard"
    return "wait_for_better_edge"


def _stop_from_context(decision: str, entry: float | None, context: dict[str, Any]) -> float | None:
    explicit = _number(_first_present(context, ("stop_loss", "stop", "sl")))
    if explicit is not None:
        return explicit
    if decision not in {"BUY", "SELL"} or entry is None:
        return None
    atr = _number((context.get("technical_context") or {}).get("atr"))
    if atr is None or atr <= 0:
        return None
    return round(entry - atr * 2.0, 8) if decision == "BUY" else round(entry + atr * 2.0, 8)


def _target_from_context(decision: str, entry: float | None, stop: float | None, context: dict[str, Any]) -> float | None:
    explicit = _number(_first_present(context, ("take_profit", "target", "tp")))
    if explicit is not None:
        return explicit
    if decision not in {"BUY", "SELL"} or entry is None or stop is None:
        return None
    risk = abs(entry - stop)
    if risk <= 0:
        return None
    return round(entry + risk * 1.8, 8) if decision == "BUY" else round(entry - risk * 1.8, 8)


def _risk_reward(action: str, entry: float | None, stop: float | None, target: float | None) -> float | None:
    if action not in {"BUY", "SELL"} or entry is None or stop is None or target is None:
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


def _blocked(reason: str) -> dict[str, Any]:
    return {
        "ok": False,
        "status": "mt5_auto_forward_blocked",
        "reason": reason,
        "shadow_trade_created": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _latest_payload(memory: MemoryStore, collection: str, symbol: str) -> dict[str, Any] | None:
    rows = memory.get_mt5_events(collection, symbol or None, limit=1)
    if not rows:
        return None
    payload = rows[0].get("payload")
    return _clean_payload(payload) if isinstance(payload, dict) else None


def _payload_from_row(row: dict[str, Any]) -> dict[str, Any] | None:
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else None
    return _clean_payload(payload) if isinstance(payload, dict) else None


def _latest_decision_by_actionable(rows: list[dict[str, Any]], actionable: bool) -> dict[str, Any] | None:
    for row in rows:
        payload = _payload_from_row(row)
        if not payload:
            continue
        if bool(payload.get("actionable")) is actionable:
            return payload
    return None


def _clean_payload(payload: dict[str, Any]) -> dict[str, Any]:
    clean = dict(payload)
    clean["reason"] = _reason_alias(clean.get("reason") or "")
    action = str(clean.get("decision") or clean.get("action") or "").upper().strip()
    if action not in {"BUY", "SELL"} or not bool(clean.get("actionable")):
        clean["entry"] = None
        clean["stop_loss"] = None
        clean["take_profit"] = None
        clean["risk_reward"] = 0.0
    return clean


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


def _symbol(value: object) -> str:
    return str(value or "").upper().strip()


def _confidence(value: object) -> str:
    text = str(value or "low").casefold().strip()
    aliases = {"alta": "high", "media": "medium", "baja": "low"}
    return aliases.get(text, text)


def _reason_alias(value: object) -> str:
    text = str(value or "")
    aliases = {
        "missing_entry": "missing_risk_parameters",
        "stop_loss_missing_from_context": "missing_risk_parameters",
        "stop_loss_required": "missing_risk_parameters",
        "take_profit_required": "missing_risk_parameters",
        "confidence_too_low": "confidence_low",
        "open_shadow_trade_exists": "duplicate_open_trade",
    }
    return aliases.get(text, text)


def _is_demo(payload: dict[str, Any]) -> bool:
    if bool(payload.get("is_demo")) or bool(payload.get("demo")):
        return True
    mode = str(payload.get("account_type") or payload.get("trade_mode") or payload.get("account_trade_mode") or "").casefold()
    server = str(payload.get("server") or payload.get("broker") or "").casefold()
    return "demo" in mode or "demo" in server


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from services.genesis.genesis_brain import GenesisBrain
from services.genesis.memory_store import MemoryStore
from services.mt5.mt5_adaptive_strategy_governor import adaptive_governor_enforcement
from services.mt5.mt5_decision_signal_builder import build_actionable_mt5_decision
from services.mt5.mt5_journal import MT5Journal
from services.mt5.mt5_order_model import sanitize_payload
from services.mt5.mt5_paper_defense import MT5PaperDefense
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
        self.performance = MT5Performance(memory=self.memory, config=self.config)
        self.paper_defense = MT5PaperDefense(memory=self.memory)

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
            self._record_no_trade_evaluation(
                symbol=raw_symbol,
                tick=clean,
                symbol_info=symbol_info,
                decision=decision,
                market_scores=_market_scores(raw_symbol, clean, self.memory, {}),
                block_reasons=[decision["reason"]],
                has_open_trade=bool(self.shadow.open_trades(raw_symbol)),
                exploration={"created": False, "enabled": self.config.paper_exploration_enabled, "reason": "symbol_not_mapped_or_not_allowed"},
            )
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
            self._record_no_trade_evaluation(
                symbol=raw_symbol,
                tick=clean,
                symbol_info=symbol_info,
                decision=decision,
                market_scores=_market_scores(raw_symbol, clean, self.memory, {}),
                block_reasons=["kill_switch_active"],
                has_open_trade=bool(self.shadow.open_trades(raw_symbol)),
                exploration={"created": False, "enabled": self.config.paper_exploration_enabled, "reason": "kill_switch_active"},
            )
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
        market_scores = _market_scores(raw_symbol, clean, self.memory, context)
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
        caution_filter: dict[str, Any] = {"allowed": True, "caution_mode_active": False, "reason": ""}
        if not block_reason and decision_name in {"BUY", "SELL"}:
            caution_filter = self.paper_defense.evaluate_new_entry(
                symbol=raw_symbol,
                tick=clean,
                market_scores=market_scores,
                decision={
                    **built,
                    "score": market_scores.get("score"),
                    "trend_score": market_scores.get("trend_score"),
                    "momentum_score": market_scores.get("momentum_score"),
                    "volatility_score": market_scores.get("volatility_score"),
                    "regime": market_scores.get("regime"),
                },
                max_spread_points=self.config.max_spread_points,
            )
            if not caution_filter.get("allowed"):
                block_reason = str(caution_filter.get("reason") or "paper_caution_block")
        open_trades_before = self.shadow.open_trades(raw_symbol)
        block_reasons = _block_reasons(block_reason or reason, context=context, has_open_trade=bool(open_trades_before), market_scores=market_scores)
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
        adaptive_enforcement = adaptive_governor_enforcement(
            symbol=raw_symbol,
            timeframe=str(clean.get("timeframe") or ""),
            profile=str(decision.get("strategy_profile") or decision.get("profile") or ""),
            open_trades=self.shadow.open_trades(),
            load_shadow_snapshot=False,
        )
        if adaptive_enforcement.get("blocked"):
            final_decision = "NO_TRADE"
            final_actionable = False
            decision.update(
                {
                    "decision": "NO_TRADE",
                    "action": "NO_TRADE",
                    "actionable": False,
                    "reason": adaptive_enforcement.get("reason") or "adaptive_governor:blocked",
                    "entry": None,
                    "stop_loss": None,
                    "take_profit": None,
                    "risk_pct": 0.0,
                    "risk_reward": 0.0,
                    "adaptive_governor": adaptive_enforcement,
                    "adaptive_governor_blocked": True,
                    "adaptive_governor_reason": adaptive_enforcement.get("reason") or "",
                    "adaptive_governor_global_state": adaptive_enforcement.get("adaptive_governor_global_state") or "",
                    "adaptive_governor_circuit_breakers": adaptive_enforcement.get("circuit_breakers") if isinstance(adaptive_enforcement.get("circuit_breakers"), list) else [],
                    "paper_exploration_created": False,
                    "shadow_trade_id": "",
                    "candidate_activated": False,
                    "paper_forward_onboarding_started": False,
                    "broker_touched": False,
                    "order_executed": False,
                    "order_policy": "journal_only_no_broker",
                }
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
        exploration: dict[str, Any] = {"created": False, "enabled": self.config.paper_exploration_enabled, "reason": "strict_signal_active_or_exploration_disabled"}
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
            exploration = self._maybe_create_exploration_trade(
                symbol=raw_symbol,
                tick=clean,
                symbol_info=symbol_info,
                context=context,
                market_scores=market_scores,
                strict_reason=decision["reason"],
            )

        self._record_no_trade_evaluation(
            symbol=raw_symbol,
            tick=clean,
            symbol_info=symbol_info,
            decision=decision,
            market_scores=market_scores,
            block_reasons=block_reasons,
            has_open_trade=bool(open_trades_before),
            exploration=exploration,
        )

        signal_flip_updates = (
            self.shadow.close_on_signal_flip(
                symbol=raw_symbol,
                tick=clean,
                decision={
                    **decision,
                    "decision": decision_name,
                    "action": decision_name,
                    "confidence": confidence,
                    "no_trade_score": no_trade_score,
                    "hedge_score": hedge_score,
                },
                market_scores=market_scores,
                config=self.config,
            )
            if open_trades_before
            else []
        )
        shadow_trade = shadow.get("trade") if isinstance(shadow.get("trade"), dict) else {}
        return {
            "ok": True,
            "status": "mt5_auto_forward_decision_recorded",
            "auto_forward_enabled": not self.config.kill_switch,
            "decision": decision,
            "event": event,
            "shadow": shadow,
            "exploration": exploration,
            "shadow_trade_created": bool(shadow.get("created")),
            "exploration_shadow_trade_created": bool(exploration.get("created")),
            "paper_caution_filter": caution_filter,
            "signal_flip_closed": bool(signal_flip_updates),
            "signal_flip_updates": signal_flip_updates,
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
        paper_defense = self.paper_defense.state(symbol=clean_symbol)
        summary = performance.get("summary") if isinstance(performance.get("summary"), dict) else {}
        report = self.no_trade_report(symbol=clean_symbol, limit=50)
        summary_strict = performance.get("summary_strict_auto") if isinstance(performance.get("summary_strict_auto"), dict) else {}
        summary_exploration = performance.get("summary_exploration") if isinstance(performance.get("summary_exploration"), dict) else {}
        open_trades = snapshot.get("open_trades") or []
        closed_trades = snapshot.get("closed_trades") or []
        last_block_reason = _reason_alias((last_invalid_decision or {}).get("reason") or "")
        current_state_reason = "active_open_trade" if open_trades else ""
        last_closed_trade = closed_trades[0] if closed_trades else {}
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
            "paper_exploration_enabled": self.config.paper_exploration_enabled,
            "caution_mode_active": paper_defense.get("caution_mode_active", False),
            "paper_defense_active": paper_defense.get("paper_defense_active", False),
            "paper_defense_reason": paper_defense.get("reason") or "",
            "paper_defense_filters_applied": paper_defense.get("filters_applied") or [],
            "last_tick": last_tick,
            "last_tick_status": "mt5_tick_recorded" if last_tick else "",
            "last_tick_ea_version": (last_tick or {}).get("ea_version") or "",
            "last_signal": last_signal,
            "last_signal_status": (last_signal or {}).get("signal_status") or (last_signal or {}).get("status") or "",
            "last_signal_error": (last_signal or {}).get("signal_error") or "",
            "last_decision": last_decision,
            "last_strict_decision": last_decision,
            "last_exploration_decision": _latest_exploration_decision(self.memory, clean_symbol),
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
            "closed_trades": closed_trades,
            "current_r_multiple": (open_trades[0] if open_trades else {}).get("current_r_multiple") or (open_trades[0] if open_trades else {}).get("r_multiple") or 0.0,
            "breakeven_armed": bool((open_trades[0] if open_trades else {}).get("breakeven_armed")),
            "trailing_stop_active": bool((open_trades[0] if open_trades else {}).get("trailing_stop_active")),
            "virtual_stop_loss": (open_trades[0] if open_trades else {}).get("virtual_stop_loss"),
            "bars_open": (open_trades[0] if open_trades else {}).get("bars_open") or 0,
            "hours_open": (open_trades[0] if open_trades else {}).get("hours_open") or 0.0,
            "last_exit_reason": last_closed_trade.get("exit_reason") or last_closed_trade.get("last_exit_reason") or "",
            "can_open_new_trade": not bool(open_trades),
            "excluded_trades": snapshot.get("excluded_trades") or [],
            "excluded_count": snapshot.get("excluded_count", 0),
            "manual_shadow_trades": summary.get("manual_shadow_trades", 0),
            "auto_shadow_trades": summary.get("auto_shadow_trades", 0),
            "total_shadow_trades": summary.get("total_shadow_trades", summary.get("shadow_trades", 0)),
            "strict_shadow_trades": summary_strict.get("shadow_trades", summary.get("auto_shadow_trades", 0)),
            "exploration_shadow_trades": summary_exploration.get("shadow_trades", 0),
            "open_strict_trades": summary_strict.get("open", 0),
            "open_exploration_trades": summary_exploration.get("open", 0),
            "top_no_trade_reasons": report.get("top_block_reasons") or [],
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

    def no_trade_report(self, *, symbol: str = "", limit: int = 50) -> dict[str, Any]:
        clean_symbol = _symbol(symbol)
        rows = self.memory.get_mt5_events("mt5_no_trade_evaluations", clean_symbol or None, limit=max(limit, 200))
        evaluations = [_payload_from_row(row) or {} for row in rows]
        evaluations = [item for item in evaluations if item]
        paper_enabled = self.config.paper_exploration_enabled
        reasons: dict[str, int] = {}
        for item in evaluations:
            block_reasons = item.get("block_reasons") if isinstance(item.get("block_reasons"), list) else []
            if not block_reasons and item.get("block_reason"):
                block_reasons = [item.get("block_reason")]
            for reason in block_reasons:
                clean_reason = _reason_alias(reason)
                if paper_enabled and clean_reason == "paper_exploration_disabled":
                    continue
                if clean_reason:
                    reasons[clean_reason] = reasons.get(clean_reason, 0) + 1
        top_reasons = [{"reason": reason, "count": count} for reason, count in sorted(reasons.items(), key=lambda item: item[1], reverse=True)]
        last_items = evaluations[: max(1, min(limit, 100))]
        current_regime = str((last_items[0] if last_items else {}).get("regime") or "")
        actionable_count = sum(1 for item in evaluations if bool(item.get("actionable")))
        exploration_attempts = sum(1 for item in evaluations if bool(item.get("exploration_attempted")) or bool(item.get("paper_exploration_enabled")))
        exploration_created = sum(1 for item in evaluations if bool(item.get("paper_exploration_created")))
        return {
            "ok": True,
            "status": "mt5_no_trade_report_ready",
            "symbol": clean_symbol,
            "normalized_symbol": str((last_items[0] if last_items else {}).get("normalized_symbol") or clean_symbol),
            "paper_exploration_enabled": paper_enabled,
            "exploration_attempts": exploration_attempts,
            "exploration_created": exploration_created,
            "total_evaluations": len(evaluations),
            "actionable_count": actionable_count,
            "no_trade_count": len(evaluations) - actionable_count,
            "top_block_reasons": top_reasons,
            "last_50_evaluations": last_items[:50],
            "current_regime": current_regime,
            "genesis_reading": _no_trade_reading(
                clean_symbol,
                len(evaluations),
                actionable_count,
                top_reasons,
                paper_exploration_enabled=paper_enabled,
                exploration_attempts=exploration_attempts,
                exploration_created=exploration_created,
            ),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def _maybe_create_exploration_trade(
        self,
        *,
        symbol: str,
        tick: dict[str, Any],
        symbol_info: dict[str, Any],
        context: dict[str, Any],
        market_scores: dict[str, Any],
        strict_reason: str,
    ) -> dict[str, Any]:
        if not self.config.paper_exploration_enabled:
            return {"created": False, "enabled": False, "reason": "paper_exploration_disabled"}
        if self.config.kill_switch:
            return {"created": False, "enabled": True, "reason": "kill_switch_active"}
        if str(symbol_info.get("normalized_symbol") or "").upper() != "BTCUSD" or str(symbol_info.get("instrument_type") or "") != "crypto_spot":
            return {"created": False, "enabled": True, "reason": "instrument_mismatch"}
        if self.shadow.open_trades(symbol):
            return {"created": False, "enabled": True, "reason": "active_open_trade"}
        spread = _number(tick.get("spread_points") or tick.get("spread"))
        if spread is not None and spread > self.config.max_spread_points:
            return {"created": False, "enabled": True, "reason": "spread_too_high"}
        built = _build_exploration_decision(symbol, tick, context, market_scores, min_rr=max(self.config.min_rr, 1.2), risk_pct=min(self.config.max_position_risk_pct, 0.5))
        if not built.get("actionable"):
            return {"created": False, "enabled": True, "reason": built.get("reason") or strict_reason, "decision": built}
        caution_filter = self.paper_defense.evaluate_new_entry(
            symbol=symbol,
            tick=tick,
            market_scores=market_scores,
            decision=built,
            max_spread_points=self.config.max_spread_points,
        )
        if not caution_filter.get("allowed"):
            return {
                "created": False,
                "enabled": True,
                "reason": caution_filter.get("reason") or "paper_caution_block",
                "decision": built,
                "paper_caution_filter": caution_filter,
            }
        adaptive_enforcement = adaptive_governor_enforcement(
            symbol=symbol,
            timeframe=str(tick.get("timeframe") or "H1").upper(),
            profile="BTCUSD_PAPER_EXPLORATION_V1",
            open_trades=self.shadow.open_trades(),
            load_shadow_snapshot=False,
        )
        if adaptive_enforcement.get("blocked"):
            return {
                "created": False,
                "enabled": True,
                "reason": adaptive_enforcement.get("reason") or "adaptive_governor:blocked",
                "decision": built,
                "shadow_trade_id": "",
                "adaptive_governor": adaptive_enforcement,
                "adaptive_governor_blocked": True,
                "candidate_activated": False,
                "paper_forward_onboarding_started": False,
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        shadow = self.shadow.create_shadow_trade(
            {
                **built,
                "symbol": symbol,
                "original_symbol": symbol,
                "normalized_symbol": "BTCUSD",
                "instrument_type": "crypto_spot",
                "is_spot_crypto": True,
                "action": built["decision"],
                "source": "mt5_auto_forward_exploration",
                "auto_forward": True,
                "manual_test": False,
                "paper_exploration": True,
                "excluded_from_live_grade": True,
                "included_in_exploration_metrics": True,
                "excluded_from_main_metrics": False,
                "exploration_profile": "BTCUSD_PAPER_EXPLORATION_V1",
                "timeframe": str(tick.get("timeframe") or "H1").upper(),
                "timestamp": str(tick.get("timestamp") or tick.get("bar_time") or _now()),
            }
        )
        if shadow.get("created"):
            self.journal.save("mt5_decisions", symbol, {**built, "source": "mt5_auto_forward_exploration", "paper_exploration": True}, confidence=built.get("confidence") or "medium")
        return {
            "created": bool(shadow.get("created")),
            "enabled": True,
            "reason": shadow.get("reason") or built.get("reason") or "exploration_shadow_trade_created",
            "decision": built,
            "shadow": shadow,
            "shadow_trade_id": ((shadow.get("trade") if isinstance(shadow.get("trade"), dict) else {}) or {}).get("shadow_trade_id") or "",
        }

    def _record_no_trade_evaluation(
        self,
        *,
        symbol: str,
        tick: dict[str, Any],
        symbol_info: dict[str, Any],
        decision: dict[str, Any],
        market_scores: dict[str, Any],
        block_reasons: list[str],
        has_open_trade: bool,
        exploration: dict[str, Any],
    ) -> None:
        price = _price(tick)
        exploration_decision = exploration.get("decision") if isinstance(exploration.get("decision"), dict) else {}
        payload = {
            "symbol": symbol,
            "original_symbol": symbol_info.get("original_symbol") or symbol,
            "normalized_symbol": symbol_info.get("normalized_symbol") or symbol,
            "instrument_type": symbol_info.get("instrument_type") or "",
            "timeframe": str(tick.get("timeframe") or decision.get("timeframe") or "").upper(),
            "price": price,
            "decision": decision.get("decision") or "NO_TRADE",
            "actionable": bool(decision.get("actionable")),
            "block_reason": block_reasons[0] if block_reasons else _reason_alias(decision.get("reason") or ""),
            "block_reasons": block_reasons,
            "confidence": decision.get("confidence") or "low",
            "score": market_scores.get("score", 0),
            "trend_score": market_scores.get("trend_score", 0),
            "momentum_score": market_scores.get("momentum_score", 0),
            "volatility_score": market_scores.get("volatility_score", 0),
            "regime": market_scores.get("regime") or "",
            "spread": _number(tick.get("spread_points") or tick.get("spread")) or 0.0,
            "has_open_trade": has_open_trade,
            "paper_exploration_enabled": self.config.paper_exploration_enabled,
            "exploration_attempted": bool(exploration.get("enabled")) or self.config.paper_exploration_enabled,
            "paper_exploration_created": bool(exploration.get("created")),
            "exploration_decision": exploration_decision.get("decision") or "",
            "exploration_reason": exploration.get("reason") or "",
            "timestamp": str(tick.get("timestamp") or tick.get("bar_time") or _now()),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }
        self.journal.save("mt5_no_trade_evaluations", symbol, payload, confidence=payload["confidence"])

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


def _latest_exploration_decision(memory: MemoryStore, symbol: str) -> dict[str, Any] | None:
    for row in memory.get_mt5_events("mt5_decisions", symbol or None, limit=50):
        payload = _payload_from_row(row)
        if payload and bool(payload.get("paper_exploration")):
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


def _market_scores(symbol: str, tick: dict[str, Any], memory: MemoryStore, context: dict[str, Any]) -> dict[str, Any]:
    prices = _recent_prices(symbol, tick, memory, limit=60)
    current = _price(tick)
    if current is None:
        current = prices[-1] if prices else 0.0
    ema20 = _ema(prices, 20) if prices else current
    ema50 = _ema(prices, 50) if prices else current
    rsi = _rsi(prices, 14)
    momentum = _momentum(prices)
    atr_pct = _atr_pct(prices, current)
    spread = abs(_number(tick.get("spread_points") or tick.get("spread")) or 0.0)
    trend_score = 50
    if current > ema20 >= ema50:
        trend_score = 72
    elif current < ema20 <= ema50:
        trend_score = 28
    elif current >= ema20:
        trend_score = 58
    elif current <= ema20:
        trend_score = 42
    momentum_score = 50
    if rsi > 55 or momentum > 0:
        momentum_score = 65
    if rsi < 45 or momentum < 0:
        momentum_score = 35
    volatility_score = 50
    if atr_pct >= 0.15:
        volatility_score = 65
    elif atr_pct < 0.03:
        volatility_score = 25
    if spread and current and spread / current > 0.003:
        volatility_score = min(volatility_score, 35)
    regime = "bullish_exploration" if trend_score >= 60 and momentum_score >= 55 else "bearish_exploration" if trend_score <= 40 and momentum_score <= 45 else "chop"
    score = round((trend_score + momentum_score + volatility_score) / 3, 2)
    return {
        "price": current,
        "ema20": ema20,
        "ema50": ema50,
        "rsi": rsi,
        "momentum": momentum,
        "atr_pct": atr_pct,
        "trend_score": trend_score,
        "momentum_score": momentum_score,
        "volatility_score": volatility_score,
        "score": score,
        "regime": str(context.get("market_regime") or regime),
        "prices": prices,
    }


def _build_exploration_decision(
    symbol: str,
    tick: dict[str, Any],
    context: dict[str, Any],
    scores: dict[str, Any],
    *,
    min_rr: float,
    risk_pct: float,
) -> dict[str, Any]:
    entry = _price(tick)
    if entry is None or entry <= 0:
        return _exploration_no_trade(symbol, "missing_risk_parameters")
    spread = abs(_number(tick.get("spread_points") or tick.get("spread")) or 0.0)
    if spread and spread / entry > 0.005:
        return _exploration_no_trade(symbol, "spread_too_high")
    prices = scores.get("prices") if isinstance(scores.get("prices"), list) else []
    if len(prices) < 2:
        return _exploration_no_trade(symbol, "no_tick_momentum")
    previous = _number(prices[-2])
    if previous is None or previous <= 0:
        return _exploration_no_trade(symbol, "no_data")
    atr = _estimated_atr(prices, entry)
    stop_distance = max(atr * 1.5, entry * 0.015) if atr > 0 else entry * 0.015
    volatility_score = _number(scores.get("volatility_score")) or 50.0
    if volatility_score < 25:
        return _exploration_no_trade(symbol, "volatility_too_low")
    if entry > previous:
        action = "BUY"
    elif entry < previous:
        action = "SELL"
    else:
        return _exploration_no_trade(symbol, "no_tick_momentum")
    stop_loss = round(entry - stop_distance, 8) if action == "BUY" else round(entry + stop_distance, 8)
    take_profit = round(entry + stop_distance * min_rr, 8) if action == "BUY" else round(entry - stop_distance * min_rr, 8)
    rr = _risk_reward(action, entry, stop_loss, take_profit)
    if rr is None or rr < min_rr:
        return _exploration_no_trade(symbol, "risk_reward_too_low")
    return {
        "symbol": symbol,
        "decision": action,
        "action": action,
        "actionable": True,
        "entry": entry,
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "risk_reward": rr,
        "risk_pct": risk_pct,
        "strategy_profile": "BTCUSD_PAPER_EXPLORATION_V1",
        "confidence": "medium",
        "reason": "paper_exploration_signal",
        "score": scores.get("score", 0),
        "trend_score": scores.get("trend_score", 0),
        "momentum_score": scores.get("momentum_score", 0),
        "volatility_score": scores.get("volatility_score", 0),
        "regime": scores.get("regime") or "",
        "hedge_score": _int(context.get("hedge_score")),
        "no_trade_score": _int(context.get("no_trade_score")),
        "genesis_context_score": _int(context.get("genesis_context_score")),
        "timeframe": str(tick.get("timeframe") or context.get("recommended_timeframe") or "H1").upper(),
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "generated_at": _now(),
    }


def _exploration_no_trade(symbol: str, reason: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "decision": "NO_TRADE",
        "action": "NO_TRADE",
        "actionable": False,
        "entry": None,
        "stop_loss": None,
        "take_profit": None,
        "risk_reward": 0.0,
        "risk_pct": 0.0,
        "strategy_profile": "BTCUSD_PAPER_EXPLORATION_V1",
        "confidence": "low",
        "reason": reason,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "generated_at": _now(),
    }


def _block_reasons(reason: str, *, context: dict[str, Any], has_open_trade: bool, market_scores: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    primary = _reason_alias(reason)
    if has_open_trade:
        reasons.append("active_open_trade")
    if primary:
        reasons.append(primary)
    if _int(context.get("no_trade_score")) >= 70:
        reasons.append("no_edge")
    if _confidence(context.get("confidence")) == "low":
        reasons.append("confidence_low")
    if str(market_scores.get("regime") or "").casefold() == "chop":
        reasons.append("regime_chop")
    if (_number(market_scores.get("trend_score")) or 0.0) < 45 and primary in {"wait_for_better_edge", "waiting_confirmation"}:
        reasons.append("trend_not_confirmed")
    if (_number(market_scores.get("momentum_score")) or 0.0) < 45 and primary in {"wait_for_better_edge", "waiting_confirmation"}:
        reasons.append("momentum_not_confirmed")
    deduped: list[str] = []
    for item in reasons or ["waiting_confirmation"]:
        if item and item not in deduped:
            deduped.append(item)
    return deduped


def _recent_prices(symbol: str, tick: dict[str, Any], memory: MemoryStore, *, limit: int = 60) -> list[float]:
    rows = memory.get_mt5_events("mt5_ticks", symbol, limit=limit)
    values: list[float] = []
    for row in reversed(rows):
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        price = _price(payload)
        if price is not None and price > 0:
            values.append(price)
    current = _price(tick)
    if current is not None and current > 0 and (not values or values[-1] != current):
        values.append(current)
    return values[-limit:]


def _ema(values: list[float], length: int) -> float:
    if not values:
        return 0.0
    alpha = 2 / (length + 1)
    ema = values[0]
    for value in values[1:]:
        ema = value * alpha + ema * (1 - alpha)
    return ema


def _rsi(values: list[float], length: int) -> float:
    if len(values) < 2:
        return 50.0
    changes = [values[index] - values[index - 1] for index in range(1, len(values))]
    window = changes[-length:]
    gains = [change for change in window if change > 0]
    losses = [-change for change in window if change < 0]
    avg_gain = sum(gains) / max(len(window), 1)
    avg_loss = sum(losses) / max(len(window), 1)
    if avg_loss == 0:
        return 70.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 4)


def _momentum(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    return values[-1] - values[max(0, len(values) - 4)]


def _estimated_atr(values: list[float], entry: float) -> float:
    if len(values) < 2:
        return entry * 0.015
    ranges = [abs(values[index] - values[index - 1]) for index in range(1, len(values))]
    return sum(ranges[-14:]) / max(len(ranges[-14:]), 1)


def _atr_pct(values: list[float], entry: float) -> float:
    if entry <= 0:
        return 0.0
    return round((_estimated_atr(values, entry) / entry) * 100, 4)


def _no_trade_reading(
    symbol: str,
    total: int,
    actionable: int,
    top_reasons: list[dict[str, Any]],
    *,
    paper_exploration_enabled: bool = False,
    exploration_attempts: int = 0,
    exploration_created: int = 0,
) -> str:
    scope = symbol or "MT5"
    if total <= 0:
        return f"{scope}: aun no hay evaluaciones de no-trade guardadas."
    top = top_reasons[0]["reason"] if top_reasons else "sin bloqueador dominante"
    if paper_exploration_enabled and exploration_created <= 0:
        return f"{scope}: exploracion paper activa; no creo trade por {top}."
    if paper_exploration_enabled:
        return f"{scope}: exploracion paper activa; {exploration_attempts} intentos y {exploration_created} shadow trades exploratorios. Broker sigue sin tocarse."
    return f"{scope}: {total} ticks evaluados, {actionable} accionables; filtro dominante: {top}. Broker sigue sin tocarse."


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

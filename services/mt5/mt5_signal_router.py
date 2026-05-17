from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.genesis.genesis_brain import GenesisBrain
from services.genesis.memory_store import MemoryStore
from services.mt5.mt5_account_state import normalize_account_state
from services.mt5.mt5_decision_signal_builder import build_actionable_mt5_decision
from services.mt5.mt5_forward_test import MT5ForwardTestEngine
from services.mt5.mt5_journal import MT5Journal
from services.mt5.mt5_order_model import MT5OrderIntent, sanitize_payload
from services.mt5.mt5_performance import MT5Performance
from services.mt5.mt5_risk_guard import MT5BridgeConfig, MT5RiskGuard
from services.mt5.mt5_shadow_trading import MT5ShadowTrading
from services.mt5.mt5_symbol_mapper import MT5SymbolMapper


class MT5SignalRouter:
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
        self.risk_guard = MT5RiskGuard(config=self.config, symbol_mapper=self.symbol_mapper)
        self.journal = MT5Journal(memory=self.memory)
        self.shadow = MT5ShadowTrading(memory=self.memory)
        self.forward_engine = MT5ForwardTestEngine(memory=self.memory, config=self.config, symbol_mapper=self.symbol_mapper)
        self.performance_engine = MT5Performance(memory=self.memory)

    def health(self) -> dict[str, Any]:
        return {
            "ok": True,
            "status": "mt5_bridge_ready_disabled_by_default" if not self.config.enabled else "mt5_bridge_ready",
            "mt5_enabled": self.config.enabled,
            "demo_only": self.config.demo_only,
            "live_trading_enabled": self.config.live_trading_enabled,
            "order_execution_enabled": self.config.order_execution_enabled,
            "kill_switch": self.config.kill_switch,
            "order_policy": "journal_only_no_broker",
            "broker_touched": False,
            "order_executed": False,
            "warnings": [
                "MT5 real/live trading esta desactivado en esta fase.",
                "No se guardan credenciales ni passwords.",
                "Kill switch bloquea ejecucion por defecto.",
            ],
        }

    def config_payload(self) -> dict[str, Any]:
        return {
            "ok": True,
            "config": self.config.to_payload(),
            "allowed_symbols": sorted(self.symbol_mapper.allowed_symbols),
            "symbol_map": self.symbol_mapper.symbol_map,
            "order_policy": "journal_only_no_broker",
            "broker_touched": False,
        }

    def status(self) -> dict[str, Any]:
        health = self.health()
        journal = self.journal_recent(limit=100)
        last_account_sync = _first_event(journal["items"], "mt5_account_sync")
        last_decision = _first_event(journal["items"], "mt5_decision")
        last_signal = _first_event(journal["items"], "mt5_signal")
        last_order_request = _first_event(journal["items"], "mt5_order_request")
        risk_blocks = [item for item in journal["items"] if item.get("event_type") == "mt5_risk_block"][:10]
        symbols = _unique_symbols(journal["items"])
        return {
            "ok": True,
            "status": "mt5_status_ready",
            "bridge": {
                "mt5_enabled": health["mt5_enabled"],
                "demo_only": health["demo_only"],
                "live_trading_enabled": health["live_trading_enabled"],
                "order_execution_enabled": health["order_execution_enabled"],
                "kill_switch": health["kill_switch"],
                "order_policy": "journal_only_no_broker",
                "broker_touched": False,
                "order_executed": False,
            },
            "last_account_sync": last_account_sync,
            "last_decision": last_decision,
            "last_signal": last_signal,
            "last_order_request": last_order_request,
            "risk_blocks": risk_blocks,
            "symbols": symbols,
            "updated_at": _now(),
        }

    def journal_recent(self, *, limit: int = 25, symbol: str = "") -> dict[str, Any]:
        safe_limit = max(1, min(int(limit or 25), 200))
        clean_symbol = str(symbol or "").upper().strip()
        rows: list[dict[str, Any]] = []
        for collection in _MT5_COLLECTIONS:
            rows.extend(self.memory.get_mt5_events(collection, clean_symbol or None, limit=safe_limit))
        rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
        items = [_journal_item(row) for row in rows[:safe_limit]]
        return {
            "ok": True,
            "status": "mt5_journal_ready",
            "items": items,
            "count": len(items),
            "limit": safe_limit,
            "symbol": clean_symbol,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def account_sync(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        state = normalize_account_state(payload)
        event = self.journal.save("mt5_account_sync", state.get("account_id") or "ACCOUNT", state)
        return {
            "ok": True,
            "status": "account_state_recorded",
            "account_state": state,
            "event": event,
            "order_executed": False,
            "broker_touched": False,
            "order_policy": "journal_only_no_broker",
        }

    def signal(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        clean = sanitize_payload(payload or {})
        symbol = str(clean.get("symbol") or clean.get("ticker") or "").upper().strip()
        event = self.journal.save("mt5_signals", symbol, {**clean, "timestamp": _now()})
        shadow = self.shadow.record_signal(clean) if symbol else {"created": False, "status": "missing_symbol"}
        return {
            "ok": bool(symbol),
            "status": "mt5_signal_recorded" if symbol else "missing_symbol",
            "event": event,
            "shadow": shadow,
            "order_executed": False,
            "broker_touched": False,
            "order_policy": "journal_only_no_broker",
        }

    def decision(self, symbol: str) -> dict[str, Any]:
        symbol_info = self.symbol_mapper.map_symbol(symbol)
        if not symbol_info["ok"]:
            payload = _base_decision(symbol_info, "NO_TRADE", "low", "symbol_not_mapped_or_not_allowed")
            self.journal.save("mt5_decisions", symbol_info.get("mt5_symbol") or symbol, payload)
            return payload

        context = GenesisBrain(memory=self.memory).build_trading_context(symbol_info["genesis_symbol"])
        no_trade_score = int(context.get("no_trade_score") or 0)
        hedge_score = int(context.get("hedge_score") or 0)
        context_score = int(context.get("genesis_context_score") or 0)
        built = build_actionable_mt5_decision(
            symbol_info["mt5_symbol"],
            {"symbol": symbol_info["mt5_symbol"], "last": (context.get("technical_context") or {}).get("price")},
            context,
            min_rr=self.config.min_rr,
            risk_pct=min(self.config.max_position_risk_pct, 0.5),
        )
        decision = str(built.get("decision") or "NO_TRADE")
        reason = str(built.get("reason") or _decision_reason(context, decision, symbol_info))
        entry = _maybe_float(built.get("entry"))
        stop_loss = _maybe_float(built.get("stop_loss"))
        take_profit = _maybe_float(built.get("take_profit"))
        risk_pct = min(self.config.max_position_risk_pct, 0.5)
        intent = MT5OrderIntent(
            symbol=symbol_info["mt5_symbol"],
            action=decision,
            entry=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            trailing_stop=None,
            risk_pct=risk_pct,
            confidence=str(context.get("confidence") or "low"),
            strategy_profile=str(context.get("recommended_strategy_profile") or ""),
            timeframe=str(context.get("recommended_timeframe") or ""),
            hedge_score=hedge_score,
            no_trade_score=no_trade_score,
            genesis_context_score=context_score,
        )
        guard = self.risk_guard.evaluate_order(intent)
        if decision in {"BUY", "SELL"} and guard["blocked"]:
            decision = "NO_TRADE"
            reason = guard["primary_reason"]
        payload = {
            "ok": True,
            "symbol": symbol_info["mt5_symbol"],
            "genesis_symbol": symbol_info["genesis_symbol"],
            "decision": decision,
            "confidence": context.get("confidence") or "low",
            "reason": reason,
            "actionable": bool(built.get("actionable")) and decision in {"BUY", "SELL"},
            "strategy_profile": context.get("recommended_strategy_profile") or "",
            "timeframe": context.get("recommended_timeframe") or "",
            "entry": entry,
            "stop_loss": stop_loss if decision in {"BUY", "SELL"} else None,
            "take_profit": take_profit if decision in {"BUY", "SELL"} else None,
            "trailing_stop": intent.trailing_stop,
            "risk_pct": risk_pct if decision in {"BUY", "SELL"} else 0.0,
            "risk_reward": built.get("risk_reward") or 0.0,
            "lot_size_hint": None,
            "hedge_needed": bool(context.get("hedge_needed")),
            "hedge_score": hedge_score,
            "no_trade_score": no_trade_score,
            "genesis_context_score": context_score,
            "market_regime": context.get("market_regime") or "",
            "warnings": list(symbol_info.get("warnings") or []),
            "instrument_warning": symbol_info.get("instrument_warning") or "",
            "risk_flags": list(symbol_info.get("warnings") or []) + list(context.get("risk_flags") or []) + (guard["reasons"] if guard["blocked"] else []),
            "what_to_watch": context.get("what_to_watch") or [],
            "order_policy": "journal_only_no_broker",
            "broker_touched": False,
            "order_executed": False,
            "guard": guard,
            "generated_at": _now(),
        }
        event = self.journal.save("mt5_decisions", symbol_info["mt5_symbol"], payload, confidence=payload["confidence"])
        payload["event"] = event
        return payload

    def order_request(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        body = payload or {}
        intent = MT5OrderIntent.from_payload(body)
        account = self._account_state_for_order(body, intent.symbol)
        guard = self.risk_guard.evaluate_order(intent, account_state=account)
        shadow = self.shadow.create_from_order_request(body, account_state=account, min_rr=self.config.min_rr)
        shadow_trade = shadow.get("trade") if isinstance(shadow.get("trade"), dict) else {}
        order_payload = {
            **intent.to_payload(),
            "guard": guard,
            "status": "blocked" if guard["blocked"] else "journal_only",
            "order_policy": "journal_only_no_broker",
            "broker_touched": False,
            "order_executed": False,
            "shadow_trade_created": bool(shadow.get("created")),
            "shadow_trade_id": shadow_trade.get("shadow_trade_id") or "",
            "shadow_trade_status": shadow.get("status") or "",
            "shadow_trade_reason": shadow.get("reason") or "",
            "reason": guard["primary_reason"] if guard["blocked"] else "execution_disabled_in_phase",
            "phase": "Fase 11 demo/journal only",
        }
        request_event = self.journal.save("mt5_order_requests", intent.symbol, order_payload)
        risk_event = None
        if guard["blocked"]:
            risk_event = self.journal.save("mt5_risk_blocks", intent.symbol, order_payload)
        return {
            "ok": True,
            "status": order_payload["status"],
            "order_request": order_payload,
            "risk_guard": guard,
            "request_event": request_event,
            "risk_block_event": risk_event,
            "shadow": shadow,
            "shadow_trade_created": bool(shadow.get("created")),
            "shadow_trade_id": shadow_trade.get("shadow_trade_id") or "",
            "order_executed": False,
            "broker_touched": False,
            "order_policy": order_payload["order_policy"],
        }

    def order_result(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        clean = sanitize_payload(payload or {})
        symbol = str(clean.get("symbol") or clean.get("ticker") or "").upper().strip()
        result = {
            **clean,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
            "timestamp": _now(),
        }
        event = self.journal.save("mt5_order_results", symbol, result)
        return {"ok": bool(symbol), "status": "mt5_order_result_recorded" if symbol else "missing_symbol", "event": event, **result}

    def tick(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        return self.forward_engine.record_tick(payload)

    def performance(self, *, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
        return self.performance_engine.report(symbol=symbol, timeframe=timeframe)

    def forward_test(self, *, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
        return self.forward_engine.forward_test(symbol=symbol, timeframe=timeframe)

    def outcomes_recent(self, *, symbol: str = "", limit: int = 25) -> dict[str, Any]:
        return self.forward_engine.outcomes_recent(symbol=symbol, limit=limit)

    def shadow_trades(self, *, symbol: str = "", limit: int = 100) -> dict[str, Any]:
        return self.shadow.snapshot(symbol=symbol, limit=limit)

    def auto_forward_status(self, *, symbol: str = "") -> dict[str, Any]:
        return self.forward_engine.auto_forward_status(symbol=symbol)

    def _account_state_for_order(self, payload: dict[str, Any], symbol: str) -> dict[str, Any] | None:
        account_payload = payload.get("account") if isinstance(payload.get("account"), dict) else {}
        direct_keys = ("is_demo", "demo", "account_type", "trade_mode", "account_trade_mode", "mode", "server", "broker", "account_id", "login", "account")
        if account_payload:
            return normalize_account_state(account_payload)
        if any(key in payload for key in direct_keys):
            return normalize_account_state({key: payload.get(key) for key in direct_keys if key in payload})
        recent = self.memory.get_mt5_events("mt5_account_sync", symbol, limit=10)
        if not recent:
            recent = self.memory.get_mt5_events("mt5_account_sync", None, limit=10)
        for row in recent:
            payload_row = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            if payload_row.get("is_demo"):
                return normalize_account_state(payload_row)
        return None


def _base_decision(symbol_info: dict[str, Any], decision: str, confidence: str, reason: str) -> dict[str, Any]:
    return {
        "ok": True,
        "symbol": symbol_info.get("mt5_symbol") or symbol_info.get("genesis_symbol") or "",
        "genesis_symbol": symbol_info.get("genesis_symbol") or "",
        "decision": decision,
        "confidence": confidence,
        "reason": reason,
        "strategy_profile": "",
        "timeframe": "",
        "entry": None,
        "stop_loss": None,
        "take_profit": None,
        "trailing_stop": None,
        "risk_pct": 0.0,
        "lot_size_hint": None,
        "hedge_needed": False,
        "hedge_score": 0,
        "no_trade_score": 100,
        "genesis_context_score": 0,
        "market_regime": "",
        "risk_flags": [reason],
        "what_to_watch": ["Configurar MT5_SYMBOL_MAP_JSON o MT5_ALLOWED_SYMBOLS para este broker."],
        "order_policy": "journal_only_no_broker",
        "broker_touched": False,
        "order_executed": False,
    }


def _decision_from_context(context: dict[str, Any], hedge_score: int, no_trade_score: int) -> str:
    if not context.get("ok"):
        return "NO_TRADE"
    if no_trade_score >= 70:
        return "NO_TRADE"
    if hedge_score >= 80:
        return "NO_TRADE"
    if hedge_score >= 65:
        return "HEDGE" if context.get("hedge_needed") else "REDUCE"
    bias = str(context.get("bias") or "neutral")
    confidence = str(context.get("confidence") or "low")
    if confidence == "low":
        return "WAIT"
    if bias == "bullish" and hedge_score < 56:
        return "BUY"
    if bias == "bearish" and hedge_score < 56:
        return "SELL"
    return "WAIT"


def _decision_reason(context: dict[str, Any], decision: str, symbol_info: dict[str, Any]) -> str:
    if decision == "NO_TRADE":
        return str(context.get("reason") or "no_edge_or_risk_guard")
    if decision in {"HEDGE", "REDUCE"}:
        return f"hedge_score {context.get('hedge_score')}/100; proteger capital en demo/journal."
    return f"Genesis context {context.get('bias')} con confianza {context.get('confidence')} para {symbol_info.get('mt5_symbol')}."


def _stop_from_context(decision: str, entry: float | None, context: dict[str, Any]) -> float | None:
    if decision not in {"BUY", "SELL"} or entry is None:
        return None
    atr = _maybe_float((context.get("technical_context") or {}).get("atr"))
    if atr and atr > 0:
        return round(entry - atr * 2.0, 6) if decision == "BUY" else round(entry + atr * 2.0, 6)
    return None


def _target_from_context(decision: str, entry: float | None, stop: float | None) -> float | None:
    if decision not in {"BUY", "SELL"} or entry is None or stop is None:
        return None
    risk = abs(entry - stop)
    return round(entry + risk * 1.8, 6) if decision == "BUY" else round(entry - risk * 1.8, 6)


def _maybe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


_MT5_COLLECTIONS = (
    "mt5_account_sync",
    "mt5_decisions",
    "mt5_signals",
    "mt5_order_requests",
    "mt5_order_results",
    "mt5_risk_blocks",
    "mt5_shadow_trades",
    "mt5_signal_outcomes",
    "mt5_no_trade_outcomes",
    "mt5_hedge_outcomes",
    "mt5_forward_metrics",
    "mt5_journal",
)


def _journal_item(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
    guard = payload.get("guard") if isinstance(payload.get("guard"), dict) else {}
    decision = payload.get("decision") or payload.get("action") or payload.get("status") or ""
    reason = payload.get("reason") or guard.get("primary_reason") or payload.get("comment") or ""
    return {
        "event_type": row.get("event_type") or payload.get("event_type") or "",
        "symbol": str(payload.get("symbol") or "").upper(),
        "decision": decision,
        "reason": reason,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": payload.get("order_policy") or "journal_only_no_broker",
        "created_at": row.get("created_at") or payload.get("timestamp") or "",
        "confidence": row.get("confidence") or payload.get("confidence") or "",
        "risk_reasons": guard.get("reasons") if isinstance(guard.get("reasons"), list) else payload.get("risk_reasons") or [],
    }


def _first_event(items: list[dict[str, Any]], event_type: str) -> dict[str, Any] | None:
    for item in items:
        if item.get("event_type") == event_type:
            return item
    return None


def _unique_symbols(items: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    symbols: list[str] = []
    for item in items:
        symbol = str(item.get("symbol") or "").upper().strip()
        if symbol and symbol not in seen:
            symbols.append(symbol)
            seen.add(symbol)
    return symbols

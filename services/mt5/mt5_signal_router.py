from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote
from urllib.request import urlopen

from services.genesis.genesis_brain import GenesisBrain
from services.genesis.memory_store import MemoryStore
from services.mt5.instrument_resolver import normalize_mt5_symbol, resolve_instrument, symbol_aliases
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

    def instrument(self, *, symbol: str = "", payload: dict[str, Any] | None = None) -> dict[str, Any]:
        info = resolve_instrument({**(payload or {}), "symbol": symbol or (payload or {}).get("symbol")})
        return {
            **info,
            "ok": True,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
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
        symbol = _signal_symbol(clean)
        signal_status = "mt5_signal_recorded" if symbol else "missing_symbol"
        signal_payload = {
            **clean,
            "symbol": symbol,
            "status": signal_status,
            "signal_status": signal_status,
            "signal_error": "" if symbol else "missing_symbol",
            "timestamp": _now(),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }
        event = self.journal.save("mt5_signals", symbol or "UNKNOWN", signal_payload)
        shadow = self.shadow.record_signal(signal_payload) if symbol else {"created": False, "status": "missing_symbol", "reason": "missing_symbol"}
        return {
            "ok": bool(symbol),
            "status": signal_status,
            "symbol": symbol,
            "reason": "" if symbol else "missing_symbol",
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
            "original_symbol": symbol_info.get("original_symbol") or symbol_info["mt5_symbol"],
            "normalized_symbol": symbol_info.get("normalized_symbol") or _normalized_symbol(symbol_info["mt5_symbol"]),
            "instrument_type": symbol_info.get("instrument_type") or "",
            "is_spot_crypto": bool(symbol_info.get("is_spot_crypto")),
            "decision": decision,
            "confidence": context.get("confidence") or "low",
            "reason": reason,
            "actionable": bool(built.get("actionable")) and decision in {"BUY", "SELL"},
            "strategy_profile": context.get("recommended_strategy_profile") or "",
            "timeframe": context.get("recommended_timeframe") or "",
            "entry": entry if decision in {"BUY", "SELL"} else None,
            "stop_loss": stop_loss if decision in {"BUY", "SELL"} else None,
            "take_profit": take_profit if decision in {"BUY", "SELL"} else None,
            "trailing_stop": intent.trailing_stop,
            "risk_pct": risk_pct if decision in {"BUY", "SELL"} else 0.0,
            "risk_reward": built.get("risk_reward") if decision in {"BUY", "SELL"} else 0.0,
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

    def performance_auto(self, *, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
        return self.performance_engine.auto_report(symbol=symbol, timeframe=timeframe)

    def forward_test(self, *, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
        return self.forward_engine.forward_test(symbol=symbol, timeframe=timeframe)

    def outcomes_recent(self, *, symbol: str = "", limit: int = 25) -> dict[str, Any]:
        return self.forward_engine.outcomes_recent(symbol=symbol, limit=limit)

    def no_trade_report(self, *, symbol: str = "", limit: int = 50) -> dict[str, Any]:
        return self.forward_engine.auto_forward.no_trade_report(symbol=symbol, limit=limit)

    def shadow_trades(self, *, symbol: str = "", limit: int = 100) -> dict[str, Any]:
        return self.shadow.snapshot(symbol=symbol, limit=limit)

    def debug_storage(self, *, symbol: str = "") -> dict[str, Any]:
        clean_symbol = str(symbol or "").upper().strip()
        aliases = _symbol_aliases(clean_symbol)
        normalized = _normalized_symbol(clean_symbol)
        collections = (
            "mt5_shadow_trades",
            "mt5_ticks",
            "mt5_decisions",
            "mt5_signals",
            "mt5_order_requests",
            "mt5_forward_metrics",
            "mt5_signal_outcomes",
            "mt5_no_trade_outcomes",
            "mt5_no_trade_evaluations",
            "mt5_hedge_outcomes",
        )
        counts: dict[str, int] = {}
        latest: dict[str, dict[str, Any] | None] = {}
        for collection in collections:
            rows = self.memory.get_mt5_events(collection, clean_symbol or None, limit=500)
            counts[collection] = len(rows)
            latest[collection] = _payload(rows[0]) if rows else None
        snapshot = self.shadow.snapshot(symbol=clean_symbol, limit=500)
        performance = self.performance(symbol=clean_symbol)
        trades = snapshot.get("items") or []
        raw_trades = self.shadow.trades(clean_symbol, limit=500)
        excluded_trades = snapshot.get("excluded_trades") or []
        auto_trades = [trade for trade in trades if trade.get("auto_forward")]
        manual_trades = [trade for trade in trades if trade.get("manual_test")]
        proxy_trades = [trade for trade in self.shadow.trades("BTC_PROXY", limit=500)]
        btcusd_real_trades = [trade for trade in trades if str(trade.get("normalized_symbol") or "").upper() == "BTCUSD" and str(trade.get("instrument_type") or "") == "crypto_spot"]
        return {
            "ok": True,
            "status": "mt5_storage_debug_ready",
            "symbol": clean_symbol,
            "normalized_symbol": normalized,
            "symbol_filters_applied": aliases,
            "symbol_aliases": aliases,
            "collections": list(collections),
            "counts": counts,
            "collection_counts": counts,
            "mt5_ticks_count": counts.get("mt5_ticks", 0),
            "mt5_shadow_trades_count": len(raw_trades),
            "mt5_auto_trades_count": len(auto_trades),
            "btcusd_real_count": len(btcusd_real_trades),
            "btc_proxy_count": len(proxy_trades),
            "manual_count": len(manual_trades),
            "database_enabled": getattr(self.memory, "backend", "") == "postgres",
            "memory_fallback": getattr(self.memory, "backend", "") != "postgres",
            "memory_backend": getattr(self.memory, "backend", "unknown"),
            "active_backend": getattr(self.memory, "backend", "unknown"),
            "persistence_backend": getattr(self.memory, "backend", "unknown"),
            "shadow_snapshot_count": snapshot["count"],
            "performance_total_shadow_trades": (performance.get("summary") or {}).get("total_shadow_trades", 0),
            "last_shadow_trade": trades[0] if trades else None,
            "last_shadow_trade_real": btcusd_real_trades[0] if btcusd_real_trades else None,
            "last_excluded_trade": excluded_trades[0] if excluded_trades else None,
            "last_auto_trade": auto_trades[0] if auto_trades else None,
            "last_manual_trade": manual_trades[0] if manual_trades else None,
            "last_proxy_trade": proxy_trades[0] if proxy_trades else None,
            "excluded_count": len(excluded_trades),
            "last_tick": latest["mt5_ticks"],
            "last_decision": latest["mt5_decisions"],
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
            "updated_at": _now(),
        }

    def auto_forward_status(self, *, symbol: str = "") -> dict[str, Any]:
        return self.forward_engine.auto_forward_status(symbol=symbol)

    def reset_manual_tests(self, *, symbol: str = "") -> dict[str, Any]:
        return self.shadow.exclude_manual_tests(symbol=symbol)

    def exclude_old_proxy_metrics(self, *, symbol: str = "") -> dict[str, Any]:
        return self.shadow.exclude_old_proxy(symbol=symbol)

    def replay_run(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        body = payload or {}
        symbol = str(body.get("symbol") or "BTCUSD").upper().strip()
        timeframe = str(body.get("timeframe") or "H1").upper().strip()
        profile = str(body.get("profile") or "BTCUSD_PAPER_EXPLORATION_V1").strip() or "BTCUSD_PAPER_EXPLORATION_V1"
        bars = max(1, min(int(body.get("bars") or 500), 2000))
        normalized = _normalized_symbol(symbol)
        source_bars = _bars_from_payload(body) or _fetch_yahoo_bars(symbol, timeframe=timeframe, bars=bars)
        replay_id = f"replay-{symbol}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        trades, no_trade_count, blocked_reasons = _run_replay_engine(
            replay_id=replay_id,
            symbol=symbol,
            timeframe=timeframe,
            bars=source_bars[:bars],
            min_rr=self.config.min_rr,
            profile=profile,
        )
        for trade in trades:
            self.journal.save("mt5_replay_shadow_trades", symbol, trade, confidence=trade.get("confidence") or "medium")
        summary = _replay_summary(trades)
        if not source_bars:
            blocked_reasons.append("historical_market_data_not_available_locally")
        result = {
            "replay_id": replay_id,
            "symbol": symbol,
            "normalized_symbol": normalized,
            "instrument_type": "crypto_spot" if normalized == "BTCUSD" else "",
            "timeframe": timeframe,
            "profile": profile,
            "bars_requested": bars,
            "bars_loaded": len(source_bars),
            "replay_trades": summary["replay_trades"],
            "wins": summary["wins"],
            "losses": summary["losses"],
            "win_rate": summary["win_rate"],
            "profit_factor": summary["profit_factor"],
            "expectancy": summary["expectancy"],
            "max_drawdown": summary["max_drawdown"],
            "no_trade_count": no_trade_count,
            "blocked_reasons": sorted(set(blocked_reasons)),
            "best_profile": profile if trades else "",
            "recent_replay_trades": trades[-10:],
            "genesis_reading": _replay_reading(symbol, summary, len(source_bars), no_trade_count),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
            "created_at": _now(),
        }
        event = self.journal.save("mt5_replay_runs", symbol, result)
        return {"ok": True, "status": "mt5_replay_recorded", "result": result, "event": event, **{k: result[k] for k in ("broker_touched", "order_executed", "order_policy")}}

    def replay_results(self, *, symbol: str = "") -> dict[str, Any]:
        clean_symbol = str(symbol or "").upper().strip()
        rows = self.memory.get_mt5_events("mt5_replay_runs", clean_symbol or None, limit=1)
        payload = rows[0].get("payload") if rows and isinstance(rows[0].get("payload"), dict) else {}
        if not payload:
            payload = {
                "symbol": clean_symbol,
                "normalized_symbol": _normalized_symbol(clean_symbol),
                "replay_trades": 0,
                "wins": 0,
                "losses": 0,
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "expectancy": 0.0,
                "max_drawdown": 0.0,
                "no_trade_count": 0,
                "blocked_reasons": [],
                "genesis_reading": "Aun no hay replay historico para este simbolo.",
            }
        return {"ok": True, "status": "mt5_replay_results_ready", **payload, "broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}

    def replay_status(self, *, symbol: str = "") -> dict[str, Any]:
        result = self.replay_results(symbol=symbol)
        return {
            "ok": True,
            "status": "mt5_replay_status_ready",
            "symbol": result.get("symbol") or str(symbol or "").upper().strip(),
            "normalized_symbol": result.get("normalized_symbol") or _normalized_symbol(symbol),
            "last_replay": result,
            "replay_trades": result.get("replay_trades", 0),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def replay_reset(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        body = payload or {}
        symbol = str(body.get("symbol") or body.get("ticker") or "").upper().strip()
        reset = {
            "symbol": symbol or "ALL",
            "normalized_symbol": _normalized_symbol(symbol) if symbol else "",
            "status": "replay_reset_marker_recorded",
            "note": "Los eventos previos no se borran; este marcador excluye la lectura principal de replay anterior.",
            "excluded_from_main_metrics": True,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
            "created_at": _now(),
        }
        event = self.journal.save("mt5_replay_runs", symbol or "MT5", reset)
        return {"ok": True, "status": "mt5_replay_reset_recorded", "event": event, **reset}

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
    "mt5_ticks",
    "mt5_order_requests",
    "mt5_order_results",
    "mt5_risk_blocks",
    "mt5_shadow_trades",
    "mt5_signal_outcomes",
    "mt5_no_trade_outcomes",
    "mt5_no_trade_evaluations",
    "mt5_hedge_outcomes",
    "mt5_forward_metrics",
    "mt5_replay_runs",
    "mt5_replay_shadow_trades",
    "mt5_journal",
)


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
    return payload


def _bars_from_payload(body: dict[str, Any]) -> list[dict[str, Any]]:
    raw = body.get("bars_data") or body.get("bars_history") or body.get("candles") or []
    if not isinstance(raw, list):
        return []
    bars: list[dict[str, Any]] = []
    for row in raw:
        if not isinstance(row, dict):
            continue
        close = _maybe_float(row.get("close") or row.get("last"))
        high = _maybe_float(row.get("high") or close)
        low = _maybe_float(row.get("low") or close)
        open_price = _maybe_float(row.get("open") or close)
        if close is None or high is None or low is None or open_price is None:
            continue
        bars.append(
            {
                "time": str(row.get("time") or row.get("timestamp") or row.get("date") or ""),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
            }
        )
    return bars


def _fetch_yahoo_bars(symbol: str, *, timeframe: str, bars: int) -> list[dict[str, Any]]:
    yahoo_symbol = "BTC-USD" if _normalized_symbol(symbol) == "BTCUSD" else symbol
    interval = {"M1": "1m", "M5": "5m", "M15": "15m", "M30": "30m", "H1": "1h", "H4": "1d", "D1": "1d"}.get(timeframe.upper(), "1h")
    range_value = "60d" if interval.endswith("h") or interval.endswith("m") else "2y"
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(yahoo_symbol)}?range={range_value}&interval={quote(interval)}"
    try:
        with urlopen(url, timeout=8) as response:
            data = json.loads(response.read().decode("utf-8"))
    except Exception:
        return []
    result = ((data.get("chart") or {}).get("result") or [{}])[0]
    timestamps = result.get("timestamp") or []
    quote_row = (((result.get("indicators") or {}).get("quote") or [{}])[0]) if isinstance(result, dict) else {}
    opens = quote_row.get("open") or []
    highs = quote_row.get("high") or []
    lows = quote_row.get("low") or []
    closes = quote_row.get("close") or []
    parsed: list[dict[str, Any]] = []
    for index, timestamp in enumerate(timestamps):
        close = _maybe_float(closes[index] if index < len(closes) else None)
        high = _maybe_float(highs[index] if index < len(highs) else close)
        low = _maybe_float(lows[index] if index < len(lows) else close)
        open_price = _maybe_float(opens[index] if index < len(opens) else close)
        if close is None or high is None or low is None or open_price is None:
            continue
        parsed.append({"time": str(timestamp), "open": open_price, "high": high, "low": low, "close": close})
    return parsed[-bars:]


def _run_replay_engine(*, replay_id: str, symbol: str, timeframe: str, bars: list[dict[str, Any]], min_rr: float, profile: str = "BTCUSD_PAPER_EXPLORATION_V1") -> tuple[list[dict[str, Any]], int, list[str]]:
    trades: list[dict[str, Any]] = []
    blocked: list[str] = []
    no_trade_count = 0
    open_trade: dict[str, Any] | None = None
    prev_close: float | None = None
    for index, bar in enumerate(bars):
        close = _maybe_float(bar.get("close"))
        high = _maybe_float(bar.get("high"))
        low = _maybe_float(bar.get("low"))
        if close is None or high is None or low is None:
            continue
        if open_trade:
            closed = _close_replay_trade(open_trade, high=high, low=low, close=close, timestamp=str(bar.get("time") or ""))
            if closed:
                trades.append(closed)
                open_trade = None
        if open_trade or prev_close is None:
            prev_close = close
            continue
        decision = _replay_decision(profile, closes=[_maybe_float(item.get("close")) or close for item in bars[max(0, index - 60) : index + 1]], close=close, prev_close=prev_close)
        if decision == "NO_TRADE":
            no_trade_count += 1
            prev_close = close
            continue
        atr = max(abs(high - low), close * 0.02)
        built = build_actionable_mt5_decision(
            symbol,
            {"symbol": symbol, "last": close, "timeframe": timeframe},
            {
                "ok": True,
                "decision": decision,
                "confidence": "high",
                "no_trade_score": 0,
                "hedge_score": 0,
                "technical_context": {"atr": atr},
                "recommended_strategy_profile": "BTCUSD Replay Momentum",
                "strategy_profile": profile,
                "recommended_timeframe": timeframe,
                "reason": "replay_momentum_signal",
            },
            min_rr=min_rr,
            risk_pct=0.5,
        )
        if not built.get("actionable"):
            blocked.append(str(built.get("reason") or "not_actionable"))
            prev_close = close
            continue
        open_trade = {
            "shadow_trade_id": f"{replay_id}-{index}",
            "replay_id": replay_id,
            "symbol": symbol,
            "original_symbol": symbol,
            "normalized_symbol": _normalized_symbol(symbol),
            "instrument_type": "crypto_spot" if _normalized_symbol(symbol) == "BTCUSD" else "",
            "is_spot_crypto": _normalized_symbol(symbol) == "BTCUSD",
            "action": built["decision"],
            "entry": built["entry"],
            "stop_loss": built["stop_loss"],
            "take_profit": built["take_profit"],
            "risk_reward": built["risk_reward"],
            "risk_pct": built["risk_pct"],
            "timeframe": timeframe,
            "strategy_profile": built["strategy_profile"],
            "profile": profile,
            "confidence": built["confidence"],
            "status": "open",
            "source": "mt5_replay",
            "replay": True,
            "auto_forward": False,
            "manual_test": False,
            "excluded_from_main_metrics": False,
            "opened_at": str(bar.get("time") or _now()),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }
        prev_close = close
    if open_trade:
        open_trade = _close_replay_trade(open_trade, high=_maybe_float(bars[-1].get("high")) or open_trade["entry"], low=_maybe_float(bars[-1].get("low")) or open_trade["entry"], close=_maybe_float(bars[-1].get("close")) or open_trade["entry"], timestamp=str(bars[-1].get("time") or ""))
        if open_trade:
            trades.append(open_trade)
    return trades, no_trade_count, blocked


def _close_replay_trade(trade: dict[str, Any], *, high: float, low: float, close: float, timestamp: str) -> dict[str, Any] | None:
    action = str(trade.get("action") or "").upper()
    entry = _maybe_float(trade.get("entry")) or close
    stop = _maybe_float(trade.get("stop_loss"))
    target = _maybe_float(trade.get("take_profit"))
    status = ""
    exit_price = close
    reason = "time_close"
    if action == "BUY" and target is not None and high >= target:
        status, exit_price, reason = "win", target, "take_profit"
    elif action == "BUY" and stop is not None and low <= stop:
        status, exit_price, reason = "loss", stop, "stop_loss"
    elif action == "SELL" and target is not None and low <= target:
        status, exit_price, reason = "win", target, "take_profit"
    elif action == "SELL" and stop is not None and high >= stop:
        status, exit_price, reason = "loss", stop, "stop_loss"
    if not status and reason != "time_close":
        return None
    if not status:
        status = "win" if (exit_price - entry if action == "BUY" else entry - exit_price) > 0 else "loss"
    pnl = exit_price - entry if action == "BUY" else entry - exit_price
    risk = abs(entry - (_maybe_float(trade.get("stop_loss")) or entry))
    return {
        **trade,
        "status": status,
        "exit_price": exit_price,
        "exit_reason": reason,
        "pnl": round(pnl, 8),
        "pnl_pct": round((pnl / entry) * 100, 6) if entry else 0.0,
        "r_multiple": round(pnl / risk, 6) if risk > 0 else 0.0,
        "closed_at": timestamp or _now(),
        "updated_at": timestamp or _now(),
    }


def _replay_decision(profile: str, *, closes: list[float], close: float, prev_close: float) -> str:
    if str(profile or "").upper() != "BTCUSD_PAPER_EXPLORATION_V1":
        move = (close - prev_close) / prev_close if prev_close else 0.0
        return "BUY" if move > 0.003 else "SELL" if move < -0.003 else "NO_TRADE"
    ema20 = _replay_ema(closes, 20)
    ema50 = _replay_ema(closes, 50)
    rsi = _replay_rsi(closes, 14)
    momentum = close - closes[max(0, len(closes) - 4)] if closes else 0.0
    if close >= ema20 and ema20 >= ema50 and rsi > 48 and momentum >= 0:
        return "BUY"
    if close <= ema20 and ema20 <= ema50 and rsi < 52 and momentum <= 0:
        return "SELL"
    return "NO_TRADE"


def _replay_ema(values: list[float], length: int) -> float:
    if not values:
        return 0.0
    alpha = 2 / (length + 1)
    ema = values[0]
    for value in values[1:]:
        ema = value * alpha + ema * (1 - alpha)
    return ema


def _replay_rsi(values: list[float], length: int) -> float:
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
    return 100 - (100 / (1 + (avg_gain / avg_loss)))


def _replay_summary(trades: list[dict[str, Any]]) -> dict[str, Any]:
    wins = [trade for trade in trades if trade.get("status") == "win"]
    losses = [trade for trade in trades if trade.get("status") == "loss"]
    win_pnl = sum(max(_maybe_float(trade.get("r_multiple")) or 0.0, 0.0) for trade in trades)
    loss_pnl = abs(sum(min(_maybe_float(trade.get("r_multiple")) or 0.0, 0.0) for trade in trades))
    total = len(wins) + len(losses)
    expectancy = sum(_maybe_float(trade.get("r_multiple")) or 0.0 for trade in trades) / total if total else 0.0
    return {
        "replay_trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round((len(wins) / total) * 100, 2) if total else 0.0,
        "profit_factor": round(win_pnl / loss_pnl, 4) if loss_pnl else round(win_pnl, 4) if win_pnl else 0.0,
        "expectancy": round(expectancy, 4),
        "max_drawdown": _replay_drawdown(trades),
    }


def _replay_drawdown(trades: list[dict[str, Any]]) -> float:
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for trade in trades:
        equity += _maybe_float(trade.get("r_multiple")) or 0.0
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return round(drawdown, 4)


def _replay_reading(symbol: str, summary: dict[str, Any], bars_loaded: int, no_trade_count: int) -> str:
    if bars_loaded <= 0:
        return f"{symbol}: replay historico no encontro velas disponibles; forward live sigue separado y journal-only."
    return (
        f"{symbol}: replay separado con {summary['replay_trades']} trades, win rate {summary['win_rate']}%, "
        f"PF {summary['profit_factor']} y {no_trade_count} barras sin trade. No toca broker."
    )


def _journal_item(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
    guard = payload.get("guard") if isinstance(payload.get("guard"), dict) else {}
    decision = payload.get("decision") or payload.get("action") or payload.get("status") or ""
    reason = _reason_alias(payload.get("reason") or guard.get("primary_reason") or payload.get("comment") or "")
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


def _signal_symbol(payload: dict[str, Any]) -> str:
    candidates: list[Any] = [
        payload.get("symbol"),
        payload.get("Symbol"),
        payload.get("ticker"),
        payload.get("Ticker"),
    ]
    nested_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    event_payload = payload.get("event") if isinstance(payload.get("event"), dict) else {}
    event_nested_payload = event_payload.get("payload") if isinstance(event_payload.get("payload"), dict) else {}
    for container in (nested_payload, event_payload, event_nested_payload):
        candidates.extend(
            [
                container.get("symbol"),
                container.get("Symbol"),
                container.get("ticker"),
                container.get("Ticker"),
            ]
        )
    for candidate in candidates:
        symbol = str(candidate or "").upper().strip()
        if symbol:
            return symbol
    return ""


def _normalized_symbol(symbol: object) -> str:
    return normalize_mt5_symbol(symbol)


def _symbol_aliases(symbol: object) -> list[str]:
    return sorted(symbol_aliases(symbol))


def _unique_symbols(items: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    symbols: list[str] = []
    for item in items:
        symbol = str(item.get("symbol") or "").upper().strip()
        if symbol and symbol not in seen:
            symbols.append(symbol)
            seen.add(symbol)
    return symbols


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

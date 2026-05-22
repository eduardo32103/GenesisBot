from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote
from urllib.request import urlopen

from services.genesis.genesis_brain import GenesisBrain
from services.genesis.memory_store import MemoryStore
from services.mt5.instrument_resolver import enrich_payload, normalize_mt5_symbol, resolve_instrument, symbol_aliases
from services.mt5.mt5_account_state import normalize_account_state
from services.mt5.mt5_backtester import MT5Backtester
from services.mt5.mt5_decision_signal_builder import build_actionable_mt5_decision
from services.mt5.mt5_db_circuit_breaker import is_db_degraded, record_db_error, status_payload as db_status_payload
from services.mt5.mt5_eth_m30_paper_forward_candidate import (
    eth_m30_forward_profile_state,
    is_eth_m30_candidate_scope,
)
from services.mt5.mt5_forward_replay import MT5ForwardReplay
from services.mt5.mt5_forward_test import MT5ForwardTestEngine
from services.mt5.mt5_ingest_queue import enqueue_mt5_event, ingest_status
from services.mt5.mt5_journal import MT5Journal
from services.mt5.mt5_order_model import MT5OrderIntent, sanitize_payload
from services.mt5.mt5_paper_defense import MT5PaperDefense
from services.mt5.mt5_paper_exploration import evaluate_paper_exploration, update_runtime_performance
from services.mt5.mt5_performance import MT5Performance
from services.mt5.mt5_promoted_profile import forward_profile_state, get_promoted_profile
from services.mt5.mt5_risk_guard import MT5BridgeConfig, MT5RiskGuard
from services.mt5.mt5_risk_governor import assess_runtime_risk, risk_state_payload
from services.mt5.mt5_runtime_snapshot import (
    append_closed_shadow_trade,
    get_snapshot,
    snapshot_status,
    update_account_sync,
    update_adaptive_state,
    update_decision,
    update_open_shadow_trade,
    update_performance,
    update_recommendations,
    update_tick,
)
from services.mt5.mt5_shadow_trading import MT5ShadowTrading, get_recent_mt5_shadow_trades_fast, is_main_metric_trade
from services.mt5.mt5_symbol_mapper import MT5SymbolMapper
from services.mt5.mt5_ui_summary import build_mt5_ui_summary, load_robust_optimizer_payload
from services.mt5.mt5_adaptive_recommendations import MT5AdaptiveRecommendationEngine
from services.mt5.mt5_adaptive_state import MT5AdaptiveStateEngine
from services.mt5.mt5_trade_memory import MT5TradeMemoryEngine


class MT5SignalRouter:
    def __init__(
        self,
        *,
        memory: MemoryStore | None = None,
        config: MT5BridgeConfig | None = None,
        symbol_mapper: MT5SymbolMapper | None = None,
    ) -> None:
        self.config = config or MT5BridgeConfig.from_env()
        self.memory = memory if memory is not None else None if self.config.fast_path_only else MemoryStore()
        self.symbol_mapper = symbol_mapper or MT5SymbolMapper()
        self.risk_guard = MT5RiskGuard(config=self.config, symbol_mapper=self.symbol_mapper)
        self._journal: MT5Journal | None = None
        self._shadow: MT5ShadowTrading | None = None
        self._forward_engine: MT5ForwardTestEngine | None = None
        self._performance_engine: MT5Performance | None = None
        self._trade_memory_engine: MT5TradeMemoryEngine | None = None
        self._adaptive_state_engine: MT5AdaptiveStateEngine | None = None
        self._adaptive_recommendation_engine: MT5AdaptiveRecommendationEngine | None = None
        self._paper_defense: MT5PaperDefense | None = None
        self._backtester: MT5Backtester | None = None
        self._forward_replay: MT5ForwardReplay | None = None

    def _memory(self) -> MemoryStore:
        if self.memory is None:
            self.memory = MemoryStore()
        return self.memory

    @property
    def journal(self) -> MT5Journal:
        if self._journal is None:
            self._journal = MT5Journal(memory=self._memory())
        return self._journal

    @property
    def shadow(self) -> MT5ShadowTrading:
        if self._shadow is None:
            self._shadow = MT5ShadowTrading(memory=self._memory())
        return self._shadow

    @property
    def forward_engine(self) -> MT5ForwardTestEngine:
        if self._forward_engine is None:
            self._forward_engine = MT5ForwardTestEngine(memory=self._memory(), config=self.config, symbol_mapper=self.symbol_mapper)
        return self._forward_engine

    @property
    def performance_engine(self) -> MT5Performance:
        if self._performance_engine is None:
            self._performance_engine = MT5Performance(memory=self._memory(), config=self.config)
        return self._performance_engine

    @property
    def trade_memory_engine(self) -> MT5TradeMemoryEngine:
        if self._trade_memory_engine is None:
            self._trade_memory_engine = MT5TradeMemoryEngine(memory=self._memory())
        return self._trade_memory_engine

    @property
    def adaptive_state_engine(self) -> MT5AdaptiveStateEngine:
        if self._adaptive_state_engine is None:
            self._adaptive_state_engine = MT5AdaptiveStateEngine(memory=self._memory())
        return self._adaptive_state_engine

    @property
    def adaptive_recommendation_engine(self) -> MT5AdaptiveRecommendationEngine:
        if self._adaptive_recommendation_engine is None:
            self._adaptive_recommendation_engine = MT5AdaptiveRecommendationEngine(memory=self._memory())
        return self._adaptive_recommendation_engine

    @property
    def paper_defense(self) -> MT5PaperDefense:
        if self._paper_defense is None:
            self._paper_defense = MT5PaperDefense(memory=self._memory())
        return self._paper_defense

    @property
    def backtester(self) -> MT5Backtester:
        if self._backtester is None:
            self._backtester = MT5Backtester(memory=self.memory, config=self.config)
        return self._backtester

    @property
    def forward_replay(self) -> MT5ForwardReplay:
        if self._forward_replay is None:
            self._forward_replay = MT5ForwardReplay(memory=self.memory, config=self.config)
        return self._forward_replay

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
        config_payload = self.config.to_payload()
        return {
            "ok": True,
            **config_payload,
            "paper_exploration_enabled": self.config.paper_exploration_enabled,
            "config": config_payload,
            "allowed_symbols": sorted(self.symbol_mapper.allowed_symbols),
            "symbol_map": self.symbol_mapper.symbol_map,
            "order_policy": "journal_only_no_broker",
            "broker_touched": False,
            "order_executed": False,
        }

    def ops_status(self, *, symbol: str = "") -> dict[str, Any]:
        clean_symbol = str(symbol or "").upper().strip()
        snap = snapshot_status(clean_symbol)
        latest_snapshot = snap.get("latest_snapshot") if isinstance(snap.get("latest_snapshot"), dict) else {}
        open_trade = latest_snapshot.get("open_shadow_trade") if isinstance(latest_snapshot.get("open_shadow_trade"), dict) else {}
        closed_trades = latest_snapshot.get("recent_closed_shadow_trades") if isinstance(latest_snapshot.get("recent_closed_shadow_trades"), list) else []
        paper_state = latest_snapshot.get("paper_exploration_state") if isinstance(latest_snapshot.get("paper_exploration_state"), dict) else {}
        return {
            "ok": True,
            "status": "mt5_ops_status_ready",
            "symbol": clean_symbol,
            **db_status_payload(),
            **ingest_status(),
            "paper_exploration_enabled": self.config.paper_exploration_enabled,
            "open_shadow_trades": 1 if open_trade else 0,
            "closed_shadow_trades": len(closed_trades),
            "last_shadow_trade_opened_at": open_trade.get("opened_at") or paper_state.get("last_opened_at") or "",
            "last_shadow_trade_closed_at": paper_state.get("last_closed_at") or latest_snapshot.get("last_shadow_trade_closed_at") or "",
            "last_shadow_trade_reason": paper_state.get("last_reason") or latest_snapshot.get("last_shadow_trade_reason") or "",
            "last_tick_at": latest_snapshot.get("last_tick_at") or "",
            "last_flush_at": ingest_status().get("last_flush_at") or "",
            "snapshot": snap,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
            "updated_at": _now(),
        }

    def risk_state(self, *, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
        return risk_state_payload(symbol or "BTCUSD", timeframe=timeframe)

    def ui_summary(self, *, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
        clean_symbol = str(symbol or "BTCUSD").upper().strip()
        clean_timeframe = str(timeframe or "M30").upper().strip()
        risk = self.risk_state(symbol=clean_symbol, timeframe=clean_timeframe)
        snapshot = get_snapshot(clean_symbol, clean_timeframe) or {}
        generic_snapshot = get_snapshot(clean_symbol) or {}
        decision = snapshot.get("last_decision") if isinstance(snapshot.get("last_decision"), dict) else {}
        if not decision:
            decision = generic_snapshot.get("last_decision") if isinstance(generic_snapshot.get("last_decision"), dict) else {}
        if not decision:
            available_tick = generic_snapshot.get("last_tick") if isinstance(generic_snapshot.get("last_tick"), dict) else {}
            available_timeframe = str(available_tick.get("timeframe") or generic_snapshot.get("timeframe") or "").upper().strip()
            reason = "no_runtime_snapshot_for_requested_timeframe" if clean_timeframe and available_timeframe and available_timeframe != clean_timeframe else "snapshot_missing"
            decision = {
                "ok": True,
                "symbol": clean_symbol,
                "timeframe": clean_timeframe,
                "requested_timeframe": clean_timeframe,
                "available_timeframe": available_timeframe,
                "decision": "NO_TRADE",
                "reason": reason,
                "strategy_profile": "",
                "paper_forward_candidate_profile": "",
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        forward = self.forward_profile_state(symbol=clean_symbol, timeframe=clean_timeframe)
        robust = load_robust_optimizer_payload()
        return build_mt5_ui_summary(
            symbol=clean_symbol,
            timeframe=clean_timeframe,
            risk_state=risk,
            decision=decision,
            forward_profile=forward,
            robust_optimizer=robust,
        )

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
        if self.config.fast_path_only and self.memory is None:
            return {
                "ok": True,
                "status": "mt5_journal_ready",
                "items": [],
                "count": 0,
                "limit": safe_limit,
                "symbol": clean_symbol,
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
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
        if self.config.fast_path_only and self.memory is None:
            symbol = str((payload or {}).get("symbol") or (payload or {}).get("ticker") or "MT5").upper().strip()
            snapshot = update_account_sync(symbol, state)
            queued = enqueue_mt5_event("mt5_account_sync", state.get("account_id") or symbol or "ACCOUNT", state)
            return {
                "ok": True,
                "status": "account_state_recorded_fast_path",
                "account_state": state,
                "snapshot": {"updated_at": snapshot.get("updated_at"), "last_account_sync_at": snapshot.get("last_account_sync_at")},
                "event": None,
                "queue": queued,
                "order_executed": False,
                "broker_touched": False,
                "order_policy": "journal_only_no_broker",
            }
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

    def decision(self, symbol: str, timeframe: str = "") -> dict[str, Any]:
        symbol_info = self.symbol_mapper.map_symbol(symbol)
        requested_timeframe = str(timeframe or "").upper().strip()
        if not symbol_info["ok"]:
            payload = _base_decision(symbol_info, "NO_TRADE", "low", "symbol_not_mapped_or_not_allowed")
            if not (self.config.fast_path_only and self.memory is None):
                self.journal.save("mt5_decisions", symbol_info.get("mt5_symbol") or symbol, payload)
            return payload

        if self.config.fast_path_only and self.memory is None:
            snapshot = get_snapshot(symbol_info["mt5_symbol"], requested_timeframe) if requested_timeframe else get_snapshot(symbol_info["mt5_symbol"])
            snapshot = snapshot or {}
            generic_snapshot = get_snapshot(symbol_info["mt5_symbol"]) or {}
            last_tick = snapshot.get("last_tick") if isinstance(snapshot.get("last_tick"), dict) else {}
            available_tick = generic_snapshot.get("last_tick") if isinstance(generic_snapshot.get("last_tick"), dict) else {}
            available_timeframe = str(available_tick.get("timeframe") or generic_snapshot.get("timeframe") or "").upper().strip()
            promoted_status = self.promoted_profile(symbol=symbol_info["mt5_symbol"], timeframe=requested_timeframe) if requested_timeframe else {}
            candidate_status = (
                eth_m30_forward_profile_state(symbol=symbol_info["mt5_symbol"], timeframe=requested_timeframe)
                if is_eth_m30_candidate_scope(symbol_info["mt5_symbol"], requested_timeframe)
                else {}
            )
            if requested_timeframe and not last_tick:
                risk_governor = assess_runtime_risk(symbol_info["mt5_symbol"], timeframe=requested_timeframe)
                reason = "no_runtime_snapshot_for_requested_timeframe"
                payload = {
                    "ok": True,
                    "symbol": symbol_info["mt5_symbol"],
                    "genesis_symbol": symbol_info["genesis_symbol"],
                    "original_symbol": symbol_info.get("original_symbol") or symbol_info["mt5_symbol"],
                    "normalized_symbol": symbol_info.get("normalized_symbol") or _normalized_symbol(symbol_info["mt5_symbol"]),
                    "instrument_type": symbol_info.get("instrument_type") or "",
                    "is_spot_crypto": bool(symbol_info.get("is_spot_crypto")),
                    "decision": "NO_TRADE",
                    "confidence": "low",
                    "reason": reason,
                    "actionable": False,
                    "strategy_profile": promoted_status.get("profile") if promoted_status.get("active") else "",
                    "timeframe": requested_timeframe,
                    "requested_timeframe": requested_timeframe,
                    "available_timeframe": available_timeframe,
                    "entry": None,
                    "stop_loss": None,
                    "take_profit": None,
                    "risk_pct": 0.0,
                    "risk_reward": 0.0,
                    "no_trade_score": 100,
                    "market_regime": "fast_path_only",
                    "warnings": ["Requested MT5 timeframe has no runtime snapshot yet."],
                    "risk_flags": ["fast_path_only", reason],
                    "last_tick": None,
                    "promoted_profile": promoted_status if promoted_status.get("active") else None,
                    "paper_forward_candidate": candidate_status or None,
                    "paper_forward_candidate_profile": candidate_status.get("profile") or (promoted_status.get("profile") if promoted_status.get("active") else ""),
                    "paper_forward_candidate_active": bool(candidate_status.get("active")),
                    "applies_to_real_trading": False,
                    "paper_exploration_enabled": self.config.paper_exploration_enabled,
                    "paper_exploration_created": False,
                    "paper_exploration_reason": reason,
                    "risk_governor_allowed": bool(risk_governor.get("allowed")),
                    "risk_governor_reason": risk_governor.get("reason") or "",
                    "risk_state": risk_governor.get("risk_state") or "normal",
                    "suggested_lot_multiplier": risk_governor.get("suggested_lot_multiplier", 0.0),
                    "risk_governor": risk_governor,
                    "order_policy": "journal_only_no_broker",
                    "broker_touched": False,
                    "order_executed": False,
                    "generated_at": _now(),
                }
                update_decision(symbol_info["mt5_symbol"], payload)
                queued = enqueue_mt5_event("mt5_decisions", symbol_info["mt5_symbol"], payload)
                payload["event"] = None
                payload["queue"] = queued
                return payload
            exploration = evaluate_paper_exploration(
                symbol_info["mt5_symbol"],
                tick=last_tick if requested_timeframe else None,
                config=self.config,
                trigger="decision",
                timeframe=requested_timeframe,
            )
            promoted_profile = exploration.get("promoted_profile") if isinstance(exploration.get("promoted_profile"), dict) else None
            reason = "fast_path_snapshot_only" if last_tick else "no_fast_snapshot"
            if exploration.get("paper_exploration_created"):
                reason = "real_trade_disabled_paper_probe_created"
            if not exploration.get("risk_governor_allowed", True):
                reason = f"risk_governor_block:{exploration.get('risk_governor_reason') or 'blocked'}"
            payload = {
                "ok": True,
                "symbol": symbol_info["mt5_symbol"],
                "genesis_symbol": symbol_info["genesis_symbol"],
                "original_symbol": symbol_info.get("original_symbol") or symbol_info["mt5_symbol"],
                "normalized_symbol": symbol_info.get("normalized_symbol") or _normalized_symbol(symbol_info["mt5_symbol"]),
                "instrument_type": symbol_info.get("instrument_type") or "",
                "is_spot_crypto": bool(symbol_info.get("is_spot_crypto")),
                "decision": "NO_TRADE",
                "confidence": "low",
                "reason": reason,
                "actionable": False,
                "strategy_profile": promoted_profile.get("profile") if promoted_profile else "",
                "timeframe": str(requested_timeframe or last_tick.get("timeframe") or ""),
                "requested_timeframe": requested_timeframe,
                "available_timeframe": available_timeframe,
                "entry": None,
                "stop_loss": None,
                "take_profit": None,
                "trailing_stop": None,
                "risk_pct": 0.0,
                "risk_reward": 0.0,
                "lot_size_hint": None,
                "hedge_needed": False,
                "hedge_score": 0,
                "no_trade_score": 100,
                "genesis_context_score": 0,
                "market_regime": "fast_path_only",
                "warnings": ["MT5_FAST_PATH_ONLY active: decision uses runtime snapshot only"],
                "instrument_warning": symbol_info.get("instrument_warning") or "",
                "risk_flags": ["fast_path_only", reason],
                "what_to_watch": [],
                "last_tick": last_tick or None,
                "paper_exploration_enabled": self.config.paper_exploration_enabled,
                "paper_exploration_created": bool(exploration.get("paper_exploration_created")),
                "paper_exploration_reason": exploration.get("paper_exploration_reason") or "",
                "risk_governor_allowed": bool(exploration.get("risk_governor_allowed", True)),
                "risk_governor_reason": exploration.get("risk_governor_reason") or "",
                "risk_state": exploration.get("risk_state") or "normal",
                "suggested_lot_multiplier": exploration.get("suggested_lot_multiplier", 0.0),
                "risk_governor": exploration.get("risk_governor") if isinstance(exploration.get("risk_governor"), dict) else {},
                "promoted_profile": promoted_profile,
                "paper_forward_candidate": candidate_status or None,
                "paper_forward_candidate_profile": candidate_status.get("profile") or exploration.get("paper_forward_candidate_profile") or "",
                "paper_forward_candidate_active": bool(candidate_status.get("active") or (promoted_profile or {}).get("active")),
                "applies_to_real_trading": False,
                "open_shadow_trade_id": exploration.get("shadow_trade_id") or "",
                "shadow_trade_id": exploration.get("shadow_trade_id") if exploration.get("paper_exploration_created") else "",
                "order_policy": "journal_only_no_broker",
                "broker_touched": False,
                "order_executed": False,
                "generated_at": _now(),
            }
            update_decision(symbol_info["mt5_symbol"], payload)
            queued = enqueue_mt5_event("mt5_decisions", symbol_info["mt5_symbol"], payload)
            payload["event"] = None
            payload["queue"] = queued
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
        risk_governor = assess_runtime_risk(
            symbol_info["mt5_symbol"],
            timeframe=str(context.get("recommended_timeframe") or ""),
            tick={"symbol": symbol_info["mt5_symbol"], "last": entry, "spread": context.get("spread_points"), "regime": context.get("market_regime") or "trend"},
            signal={"action": decision, "lot_multiplier": 1.0},
        )
        if built.get("decision") in {"BUY", "SELL"} and not risk_governor.get("allowed"):
            decision = "NO_TRADE"
            reason = f"risk_governor_block:{risk_governor.get('reason') or 'blocked'}"
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
            "risk_governor_allowed": bool(risk_governor.get("allowed")),
            "risk_governor_reason": risk_governor.get("reason") or "",
            "risk_state": risk_governor.get("risk_state") or "normal",
            "suggested_lot_multiplier": risk_governor.get("suggested_lot_multiplier", 0.0),
            "risk_governor": risk_governor,
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
        if self.config.fast_path_only and self.memory is None:
            return _fast_tick(payload, config=self.config)
        return self.forward_engine.record_tick(payload)

    def performance(self, *, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
        if self.config.fast_path_only and self.memory is None:
            snapshot = get_snapshot(symbol) or {}
            cached = snapshot.get("latest_performance_payload") if isinstance(snapshot.get("latest_performance_payload"), dict) else {}
            if cached:
                return {**cached, "data_source_used": "runtime_snapshot", "broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}
            if snapshot.get("open_shadow_trade") or snapshot.get("recent_closed_shadow_trades"):
                return update_runtime_performance(symbol)
            return _empty_performance_from_snapshot(symbol, timeframe, reason="snapshot_missing")
        return self.performance_engine.report(symbol=symbol, timeframe=timeframe)

    def performance_auto(self, *, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
        if self.config.fast_path_only and self.memory is None:
            report = self.performance(symbol=symbol, timeframe=timeframe)
            summary_auto = {
                **(report.get("summary_auto") or report.get("summary") or {}),
                "auto_shadow_trades": (report.get("summary_auto") or report.get("summary") or {}).get("shadow_trades", 0),
                "drawdown": (report.get("summary_auto") or report.get("summary") or {}).get("max_drawdown", 0.0),
            }
            return {
                "ok": True,
                "status": "mt5_auto_performance_ready",
                "symbol": report.get("symbol") or str(symbol or "").upper().strip(),
                "normalized_symbol": report.get("normalized_symbol") or "",
                "summary": summary_auto,
                "summary_auto": summary_auto,
                "auto_shadow_trades": summary_auto.get("auto_shadow_trades", 0),
                "closed": summary_auto.get("closed", 0),
                "open": summary_auto.get("open", 0),
                "wins": summary_auto.get("wins", 0),
                "losses": summary_auto.get("losses", 0),
                "win_rate": summary_auto.get("win_rate", 0.0),
                "profit_factor": summary_auto.get("profit_factor", 0.0),
                "expectancy": summary_auto.get("expectancy", 0.0),
                "net_pnl": summary_auto.get("net_pnl", 0.0),
                "drawdown": summary_auto.get("drawdown", 0.0),
                "recent_trades": report.get("recent_auto_trades") or report.get("recent_trades") or [],
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
                "updated_at": _now(),
            }
        return self.performance_engine.auto_report(symbol=symbol, timeframe=timeframe)

    def forward_test(self, *, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
        return self.forward_engine.forward_test(symbol=symbol, timeframe=timeframe)

    def outcomes_recent(self, *, symbol: str = "", limit: int = 25) -> dict[str, Any]:
        return self.forward_engine.outcomes_recent(symbol=symbol, limit=limit)

    def no_trade_report(self, *, symbol: str = "", limit: int = 50) -> dict[str, Any]:
        return self.forward_engine.auto_forward.no_trade_report(symbol=symbol, limit=limit)

    def shadow_trades(self, *, symbol: str = "", limit: int = 100) -> dict[str, Any]:
        if self.config.fast_path_only and self.memory is None:
            snapshot = get_snapshot(symbol) or {}
            open_trade = snapshot.get("open_shadow_trade") if isinstance(snapshot.get("open_shadow_trade"), dict) else {}
            closed = snapshot.get("recent_closed_shadow_trades") if isinstance(snapshot.get("recent_closed_shadow_trades"), list) else []
            items = ([open_trade] if open_trade else []) + [trade for trade in closed if isinstance(trade, dict)]
            return {
                "ok": True,
                "status": "mt5_shadow_trades_ready",
                "symbol": str(symbol or "").upper().strip(),
                "items": items[: max(1, min(int(limit or 100), 100))],
                "open_trades": [open_trade] if open_trade else [],
                "closed_trades": [trade for trade in closed if isinstance(trade, dict)],
                "count": len(items),
                "open": 1 if open_trade else 0,
                "closed": len([trade for trade in closed if isinstance(trade, dict)]),
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        return self.shadow.snapshot(symbol=symbol, limit=limit)

    def shadow_trades_open(self, *, symbol: str = "", limit: int = 100) -> dict[str, Any]:
        clean_symbol = str(symbol or "BTCUSD").upper().strip()
        safe_limit = max(1, min(int(limit or 100), 100))
        if self.config.fast_path_only and self.memory is None:
            snapshot = get_snapshot(clean_symbol) or {}
            open_trade = snapshot.get("open_shadow_trade") if isinstance(snapshot.get("open_shadow_trade"), dict) else {}
            trades = [_format_open_shadow_trade(open_trade)] if _is_open_shadow_trade(open_trade) else []
        else:
            trades = [_format_open_shadow_trade(trade) for trade in self.shadow.open_trades(clean_symbol)[:safe_limit]]
        return {
            "ok": True,
            "status": "mt5_shadow_trades_open_ready",
            "symbol": clean_symbol,
            "open_count": len(trades),
            "trades": trades[:safe_limit],
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def shadow_trades_close_expired(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        body = payload if isinstance(payload, dict) else {}
        clean_symbol = str(body.get("symbol") or body.get("ticker") or "BTCUSD").upper().strip()
        max_age = _maybe_float(body.get("max_age_minutes"))
        if max_age is None:
            max_age = float(self.config.paper_exploration_time_stop_min or self.config.shadow_time_stop_hours * 60 or 15)
        open_payload = self.shadow_trades_open(symbol=clean_symbol, limit=100)
        closed: list[dict[str, Any]] = []
        for trade in open_payload.get("trades") if isinstance(open_payload.get("trades"), list) else []:
            if not _shadow_trade_expired(trade, max_age):
                continue
            result = self.shadow_trade_close(
                {
                    "symbol": clean_symbol,
                    "shadow_trade_id": trade.get("shadow_trade_id"),
                    "reason": "expired_paper_close",
                }
            )
            if isinstance(result.get("closed_trade"), dict):
                closed.append(result["closed_trade"])
        return {
            "ok": True,
            "status": "mt5_shadow_trades_close_expired_completed",
            "symbol": clean_symbol,
            "closed_count": len(closed),
            "closed": closed,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def shadow_trade_close(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        body = payload if isinstance(payload, dict) else {}
        clean_symbol = str(body.get("symbol") or body.get("ticker") or "BTCUSD").upper().strip()
        trade_id = str(body.get("shadow_trade_id") or body.get("id") or "").strip()
        reason = str(body.get("reason") or "manual_paper_close").strip() or "manual_paper_close"
        if not trade_id:
            return _shadow_close_response(False, clean_symbol, "missing_shadow_trade_id", reason=reason)
        if self.config.fast_path_only and self.memory is None:
            snapshot = get_snapshot(clean_symbol) or {}
            open_trade = snapshot.get("open_shadow_trade") if isinstance(snapshot.get("open_shadow_trade"), dict) else {}
            if str(open_trade.get("shadow_trade_id") or "") != trade_id:
                return _shadow_close_response(False, clean_symbol, "shadow_trade_not_found", trade_id=trade_id, reason=reason)
            closed = _close_runtime_shadow_trade(open_trade, reason)
            timeframe = str(closed.get("timeframe") or "").upper().strip()
            update_open_shadow_trade(clean_symbol, None, timeframe=timeframe)
            append_closed_shadow_trade(clean_symbol, closed, timeframe=timeframe)
            enqueue_mt5_event("mt5_shadow_trades", clean_symbol, closed)
            update_runtime_performance(clean_symbol)
            return _shadow_close_response(True, clean_symbol, "mt5_shadow_trade_closed", trade_id=trade_id, reason=reason, closed_trade=closed)
        result = self.shadow.close_shadow_trade(shadow_trade_id=trade_id, reason=reason, symbol=clean_symbol)
        if not result.get("ok"):
            return _shadow_close_response(False, clean_symbol, str(result.get("status") or "shadow_trade_not_found"), trade_id=trade_id, reason=reason)
        return _shadow_close_response(True, clean_symbol, "mt5_shadow_trade_closed", trade_id=trade_id, reason=reason, closed_trade=result.get("closed_trade"))

    def debug_storage(self, *, symbol: str = "", limit: int = 20) -> dict[str, Any]:
        started = time.monotonic()
        clean_symbol = str(symbol or "").upper().strip()
        safe_limit = max(1, min(int(limit or 20), 20))
        aliases = _symbol_aliases(clean_symbol)
        normalized = _normalized_symbol(clean_symbol)
        snap = snapshot_status(clean_symbol)
        if self.config.fast_path_only and self.memory is None:
            return {
                "ok": True,
                "status": "mt5_storage_debug_snapshot_only",
                "symbol": clean_symbol,
                "normalized_symbol": normalized,
                "symbol_aliases": aliases,
                "limit": safe_limit,
                "approximate_counts_only": True,
                "latest_snapshot": snap.get("latest_snapshot"),
                **db_status_payload(),
                **ingest_status(),
                "duration_ms": _elapsed_ms(started),
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        if is_db_degraded():
            return {
                "ok": True,
                "status": "mt5_storage_debug_degraded",
                "symbol": clean_symbol,
                "normalized_symbol": normalized,
                "symbol_aliases": aliases,
                "limit": safe_limit,
                "approximate_counts_only": True,
                "latest_snapshot": snap.get("latest_snapshot"),
                **db_status_payload(),
                **ingest_status(),
                "duration_ms": _elapsed_ms(started),
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        collections = (
            "mt5_ticks",
            "mt5_decisions",
            "mt5_signals",
            "mt5_order_requests",
            "mt5_shadow_trades",
        )
        counts: dict[str, int] = {}
        latest: dict[str, dict[str, Any] | None] = {}
        try:
            for collection in collections:
                rows = self.memory.get_mt5_events(collection, clean_symbol or None, limit=safe_limit)
                counts[collection] = len(rows)
                latest[collection] = _payload(rows[0]) if rows else None
            raw_trades = get_recent_mt5_shadow_trades_fast(self.memory, clean_symbol, limit=safe_limit)
            trades = [trade for trade in raw_trades if is_main_metric_trade(trade, query_symbol=clean_symbol)]
            excluded_trades = [trade for trade in raw_trades if trade not in trades]
            auto_trades = [trade for trade in trades if trade.get("auto_forward")]
            manual_trades = [trade for trade in raw_trades if trade.get("manual_test")]
            proxy_trades = get_recent_mt5_shadow_trades_fast(self.memory, "BTC_PROXY", limit=safe_limit)
            btcusd_real_trades = [
                trade
                for trade in trades
                if str(trade.get("normalized_symbol") or "").upper() == "BTCUSD"
                and str(trade.get("instrument_type") or "") == "crypto_spot"
            ]
        except Exception as exc:
            return {
                "ok": False,
                "status": "mt5_storage_debug_error",
                "symbol": clean_symbol,
                "normalized_symbol": normalized,
                "symbol_aliases": aliases,
                "limit": safe_limit,
                "latest_error": str(exc)[:240],
                "duration_ms": _elapsed_ms(started),
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        return {
            "ok": True,
            "status": "mt5_storage_debug_ready",
            "symbol": clean_symbol,
            "normalized_symbol": normalized,
            "symbol_filters_applied": aliases,
            "symbol_aliases": aliases,
            "limit": safe_limit,
            "approximate_counts": True,
            "approximate_counts_only": True,
            "latest_snapshot": snap.get("latest_snapshot"),
            **db_status_payload(),
            **ingest_status(),
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
            "shadow_snapshot_count": len(trades),
            "performance_total_shadow_trades": len(trades),
            "last_shadow_trade": trades[0] if trades else None,
            "last_shadow_trade_real": btcusd_real_trades[0] if btcusd_real_trades else None,
            "last_excluded_trade": excluded_trades[0] if excluded_trades else None,
            "last_auto_trade": auto_trades[0] if auto_trades else None,
            "last_manual_trade": manual_trades[0] if manual_trades else None,
            "last_proxy_trade": proxy_trades[0] if proxy_trades else None,
            "excluded_count": len(excluded_trades),
            "last_tick": latest["mt5_ticks"],
            "last_decision": latest["mt5_decisions"],
            "latest_error": "",
            "duration_ms": _elapsed_ms(started),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
            "updated_at": _now(),
        }

    def auto_forward_status(self, *, symbol: str = "") -> dict[str, Any]:
        if self.config.fast_path_only and self.memory is None:
            snapshot = get_snapshot(symbol) or {}
            open_trade = snapshot.get("open_shadow_trade") if isinstance(snapshot.get("open_shadow_trade"), dict) else {}
            closed = snapshot.get("recent_closed_shadow_trades") if isinstance(snapshot.get("recent_closed_shadow_trades"), list) else []
            paper_state = snapshot.get("paper_exploration_state") if isinstance(snapshot.get("paper_exploration_state"), dict) else {}
            summary = snapshot.get("latest_performance_summary") if isinstance(snapshot.get("latest_performance_summary"), dict) else {}
            return {
                "ok": True,
                "status": "mt5_auto_forward_status_ready",
                "symbol": str(symbol or "").upper().strip(),
                "paper_exploration_enabled": self.config.paper_exploration_enabled,
                "last_tick": snapshot.get("last_tick"),
                "last_decision": snapshot.get("last_decision"),
                "last_actionable": bool(open_trade),
                "last_reason": paper_state.get("last_reason") or "",
                "open_trades": [open_trade] if open_trade else [],
                "closed_trades": [trade for trade in closed if isinstance(trade, dict)],
                "open_shadow_trades": 1 if open_trade else 0,
                "closed_shadow_trades": len(closed),
                "auto_shadow_trades": int(summary.get("shadow_trades") or ((1 if open_trade else 0) + len(closed))),
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
                "updated_at": _now(),
            }
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
        if self.memory is None:
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

    def backtest_run(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.backtester.run(payload)

    def backtest_optimize(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.backtester.optimize(payload)

    def backtest_latest(self, *, symbol: str = "") -> dict[str, Any]:
        return self.backtester.latest(symbol=symbol)

    def forward_replay_run(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.forward_replay.run(payload)

    def learning_run(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.trade_memory_engine.run_learning(payload)

    def memory_summary(self, *, symbol: str = "", limit: int = 50) -> dict[str, Any]:
        return self.trade_memory_engine.memory_summary(symbol=symbol, limit=limit)

    def learning_status(self, *, symbol: str = "") -> dict[str, Any]:
        return self.trade_memory_engine.learning_status(symbol=symbol)

    def adaptive_state(self, *, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
        if self.config.fast_path_only and self.memory is None:
            snapshot = get_snapshot(symbol) or {}
            state = snapshot.get("latest_adaptive_state") if isinstance(snapshot.get("latest_adaptive_state"), dict) else {}
            if state:
                return {**state, "data_source_used": "runtime_snapshot", "broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}
            summary = snapshot.get("latest_performance_summary") if isinstance(snapshot.get("latest_performance_summary"), dict) else {}
            return _fast_state_from_summary(symbol=symbol, timeframe=timeframe, summary=summary, reason="snapshot_missing" if not summary else "performance_snapshot")
        return self.adaptive_state_engine.compute(symbol=symbol, timeframe=timeframe)

    def strategy_profiles(self, *, symbol: str = "") -> dict[str, Any]:
        return self.trade_memory_engine.strategy_profiles(symbol=symbol)

    def adaptive_recommendations(self, *, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
        if self.config.fast_path_only and self.memory is None:
            state = self.adaptive_state(symbol=symbol, timeframe=timeframe)
            recommendations = _fast_recommendations(symbol=symbol, timeframe=timeframe, state=state)
            update_recommendations(symbol, recommendations)
            return recommendations
        state = self.adaptive_state_engine.compute(symbol=symbol, timeframe=timeframe)
        profiles = self.trade_memory_engine.strategy_profiles(symbol=symbol).get("items") or []
        return self.adaptive_recommendation_engine.recommend(
            symbol=symbol,
            timeframe=timeframe,
            state=state,
            profile_stats=profiles,
        )

    def paper_defense_status(self, *, symbol: str = "") -> dict[str, Any]:
        return self.paper_defense.state(symbol=symbol)

    def promoted_profile(self, *, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
        return get_promoted_profile(symbol=symbol or "BTCUSD", timeframe=timeframe or "M30", memory=self.memory)

    def forward_profile_state(self, *, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
        if is_eth_m30_candidate_scope(symbol or "BTCUSD", timeframe or "M30"):
            return eth_m30_forward_profile_state(symbol=symbol or "ETHUSD", timeframe=timeframe or "M30")
        return forward_profile_state(symbol=symbol or "BTCUSD", timeframe=timeframe or "M30", memory=self.memory)

    def _account_state_for_order(self, payload: dict[str, Any], symbol: str) -> dict[str, Any] | None:
        account_payload = payload.get("account") if isinstance(payload.get("account"), dict) else {}
        direct_keys = ("is_demo", "demo", "account_type", "trade_mode", "account_trade_mode", "mode", "server", "broker", "account_id", "login", "account")
        if account_payload:
            return normalize_account_state(account_payload)
        if any(key in payload for key in direct_keys):
            return normalize_account_state({key: payload.get(key) for key in direct_keys if key in payload})
        if self.memory is None:
            return None
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


def _fast_recommendations(*, symbol: str, timeframe: str, state: dict[str, Any]) -> dict[str, Any]:
    clean_symbol = str(symbol or "").upper().strip()
    clean_timeframe = str(timeframe or "").upper().strip()
    closed = int(state.get("closed_trades") or 0)
    pf = _maybe_float(state.get("rolling_profit_factor")) or 0.0
    expectancy = _maybe_float(state.get("rolling_expectancy")) or 0.0
    bot_state = str(state.get("bot_state") or "normal")
    recommendations: list[dict[str, Any]] = []
    if closed < 30:
        recommendations.append(
            {
                "symbol": clean_symbol,
                "recommendation_type": "sample_warning",
                "recommendation": "Mantener paper exploration, no usar todavia para decidir rentabilidad.",
                "reason": f"Muestra insuficiente: {closed} trades cerrados; Genesis exige minimo 30.",
                "confidence": "low",
                "requires_approval": True,
                "applied": False,
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        )
    if bot_state == "caution" or pf < 1.0 or expectancy < 0:
        recommendations.append(
            {
                "symbol": clean_symbol,
                "recommendation_type": "strategy_filter",
                "recommendation": "Estado caution: mantener paper, filtrar nuevas entradas y exigir mayor confirmacion.",
                "reason": f"Fast path detecta estado {bot_state}, PF {pf} y expectancy {expectancy}.",
                "confidence": "medium" if closed >= 30 else "low",
                "requires_approval": True,
                "applied": False,
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        )
    if not recommendations:
        recommendations.append(
            {
                "symbol": clean_symbol,
                "recommendation_type": "risk_adjustment",
                "recommendation": "Mantener configuracion paper actual y seguir midiendo.",
                "reason": f"Estado {bot_state}, PF {pf}, closed {closed}.",
                "confidence": "medium" if closed >= 30 else "low",
                "requires_approval": True,
                "applied": False,
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        )
    return {
        "ok": True,
        "status": "mt5_adaptive_recommendations_ready",
        "symbol": clean_symbol,
        "timeframe": clean_timeframe,
        "closed_trades": closed,
        "bot_state": bot_state,
        "profile_stats_count": 0,
        "data_source_used": state.get("data_source_used") or "snapshot_fast_path",
        "rolling_win_rate": _maybe_float(state.get("rolling_win_rate")) or 0.0,
        "rolling_profit_factor": pf,
        "rolling_expectancy": expectancy,
        "rolling_drawdown": _maybe_float(state.get("rolling_drawdown")) or 0.0,
        "current_win_streak": int(state.get("current_win_streak") or 0),
        "current_loss_streak": int(state.get("current_loss_streak") or 0),
        "recommendations": recommendations,
        "count": len(recommendations),
        "updated_at": _now(),
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _fast_tick(payload: dict[str, Any] | None, *, config: MT5BridgeConfig | None = None) -> dict[str, Any]:
    clean = sanitize_payload(payload or {})
    symbol = str(clean.get("symbol") or clean.get("ticker") or "").upper().strip()
    last = _fast_price(clean)
    if not symbol or last is None:
        return {
            "ok": False,
            "status": "tick_missing_symbol_or_price",
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }
    tick = enrich_payload(
        {
            **clean,
            "symbol": symbol,
            "last": last,
            "timeframe": str(clean.get("timeframe") or "").upper(),
            "source": str(clean.get("source") or "mt5_bridge"),
            "timestamp": str(clean.get("timestamp") or clean.get("bar_time") or _now()),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }
    )
    snapshot = update_tick(symbol, tick)
    queued = enqueue_mt5_event("mt5_ticks", symbol, tick)
    exploration = evaluate_paper_exploration(symbol, tick=tick, config=config or MT5BridgeConfig.from_env(), trigger="tick")
    return {
        "ok": True,
        "status": "mt5_tick_recorded_fast_path",
        "symbol": symbol,
        "tick_saved": bool(queued.get("queued")),
        "auto_forward_checked": True,
        "tick": tick,
        "snapshot": {"updated_at": snapshot.get("updated_at"), "last_tick_at": snapshot.get("last_tick_at")},
        "queue": queued,
        "warning": queued.get("warning") or "",
        "shadow_updates": [exploration.get("open_shadow_trade")] if exploration.get("open_shadow_trade") else [],
        "auto_forward": exploration,
        "auto_shadow_trade_created": bool(exploration.get("paper_exploration_created")),
        "paper_exploration_enabled": bool(exploration.get("paper_exploration_enabled")),
        "paper_exploration_created": bool(exploration.get("paper_exploration_created")),
        "paper_exploration_closed": bool(exploration.get("paper_exploration_closed")),
        "paper_exploration_reason": exploration.get("paper_exploration_reason") or "",
        "risk_governor_allowed": bool(exploration.get("risk_governor_allowed", True)),
        "risk_governor_reason": exploration.get("risk_governor_reason") or "",
        "risk_state": exploration.get("risk_state") or "normal",
        "suggested_lot_multiplier": exploration.get("suggested_lot_multiplier", 0.0),
        "promoted_profile": exploration.get("promoted_profile") if isinstance(exploration.get("promoted_profile"), dict) else None,
        "paper_forward_candidate_profile": exploration.get("paper_forward_candidate_profile") or "",
        "shadow_trade_id": exploration.get("shadow_trade_id") or "",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _fast_price(payload: dict[str, Any]) -> float | None:
    for key in ("last", "price"):
        value = _maybe_float(payload.get(key))
        if value is not None:
            return value
    bid = _maybe_float(payload.get("bid"))
    ask = _maybe_float(payload.get("ask"))
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    return bid if bid is not None else ask


def _empty_performance_from_snapshot(symbol: str, timeframe: str, *, reason: str) -> dict[str, Any]:
    clean_symbol = str(symbol or "").upper().strip()
    normalized = _normalized_symbol(clean_symbol)
    empty = {
        "shadow_trades": 0,
        "closed": 0,
        "open": 0,
        "wins": 0,
        "losses": 0,
        "breakeven": 0,
        "win_rate": 0.0,
        "profit_factor": 0.0,
        "expectancy": 0.0,
        "net_pnl": 0.0,
        "max_drawdown": 0.0,
        "avg_win": 0.0,
        "avg_loss": 0.0,
        "rr_avg": 0.0,
        "symbol": clean_symbol,
        "normalized_symbol": normalized,
        "instrument_type": "crypto_spot" if normalized == "BTCUSD" else "",
        "total_signals": 0,
        "actionable_signals": 0,
        "manual_shadow_trades": 0,
        "auto_shadow_trades": 0,
        "strict_shadow_trades": 0,
        "exploration_shadow_trades": 0,
        "forward_auto_shadow_trades": 0,
        "total_shadow_trades": 0,
    }
    return {
        "ok": True,
        "status": "no_snapshot_yet",
        "symbol": clean_symbol,
        "normalized_symbol": normalized,
        "timeframe": str(timeframe or "").upper().strip(),
        "summary": empty,
        "summary_auto": empty,
        "summary_forward_auto": empty,
        "summary_total": empty,
        "data_source_used": reason,
        "genesis_reading": f"{clean_symbol or 'MT5'}: sin snapshot MT5 todavia; hot path protegido.",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "updated_at": _now(),
    }


def _is_open_shadow_trade(trade: dict[str, Any]) -> bool:
    return bool(trade) and isinstance(trade, dict) and str(trade.get("status") or trade.get("lifecycle_status") or "").casefold() in {"open", ""}


def _format_open_shadow_trade(trade: dict[str, Any]) -> dict[str, Any]:
    opened_at = str(trade.get("opened_at") or trade.get("created_at") or "")
    entry = _maybe_float(trade.get("entry_price") or trade.get("entry")) or 0.0
    last = _maybe_float(trade.get("last_price") or trade.get("last") or entry) or entry
    pnl = _maybe_float(trade.get("unrealized_pnl"))
    if pnl is None:
        side = str(trade.get("side") or trade.get("action") or "buy").casefold()
        pnl = entry - last if side == "sell" else last - entry
    pnl_pct = _maybe_float(trade.get("unrealized_pnl_pct"))
    if pnl_pct is None:
        pnl_pct = round((pnl / entry) * 100, 6) if entry else 0.0
    return {
        "shadow_trade_id": str(trade.get("shadow_trade_id") or ""),
        "symbol": str(trade.get("symbol") or "").upper().strip(),
        "timeframe": str(trade.get("timeframe") or "").upper().strip(),
        "side": str(trade.get("side") or trade.get("action") or "").casefold(),
        "entry_price": entry,
        "last_price": last,
        "unrealized_pnl": round(pnl, 8),
        "unrealized_pnl_pct": round(pnl_pct, 6),
        "r_multiple": _maybe_float(trade.get("r_multiple") or trade.get("current_r_multiple")) or 0.0,
        "opened_at": opened_at,
        "age_minutes": _age_minutes(opened_at),
        "stop_loss": _maybe_float(trade.get("stop_loss")),
        "take_profit": _maybe_float(trade.get("take_profit")),
        "source": str(trade.get("source") or ""),
        "strategy_profile": str(trade.get("strategy_profile") or ""),
        "paper_forward_candidate": bool(trade.get("paper_forward_candidate")),
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _shadow_trade_expired(trade: dict[str, Any], max_age_minutes: float) -> bool:
    if bool(trade.get("expired")):
        return True
    expires_at = _parse_dt(trade.get("expires_at"))
    if expires_at and datetime.now(timezone.utc) >= expires_at:
        return True
    age = _maybe_float(trade.get("age_minutes")) or 0.0
    return age >= max(0.0, float(max_age_minutes or 0.0))


def _close_runtime_shadow_trade(trade: dict[str, Any], reason: str) -> dict[str, Any]:
    entry = _maybe_float(trade.get("entry_price") or trade.get("entry")) or 0.0
    last = _maybe_float(trade.get("last_price") or trade.get("last") or entry) or entry
    side = str(trade.get("side") or trade.get("action") or "buy").casefold()
    pnl = entry - last if side == "sell" else last - entry
    risk = _maybe_float(trade.get("initial_risk"))
    if risk is None:
        stop = _maybe_float(trade.get("stop_loss"))
        risk = abs(entry - stop) if stop is not None else 0.0
    now = _now()
    status = "win" if pnl > 0 else "loss" if pnl < 0 else "breakeven"
    return {
        **trade,
        "status": status,
        "lifecycle_status": "closed",
        "last_price": last,
        "exit_price": last,
        "exit_reason": reason,
        "last_exit_reason": reason,
        "closed_at": now,
        "updated_at": now,
        "pnl": round(pnl, 8),
        "pnl_pct": round((pnl / entry) * 100, 6) if entry else 0.0,
        "unrealized_pnl": round(pnl, 8),
        "r_multiple": round(pnl / risk, 6) if risk and risk > 0 else 0.0,
        "current_r_multiple": round(pnl / risk, 6) if risk and risk > 0 else 0.0,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _shadow_close_response(
    ok: bool,
    symbol: str,
    status: str,
    *,
    trade_id: str = "",
    reason: str = "",
    closed_trade: Any = None,
) -> dict[str, Any]:
    payload = {
        "ok": ok,
        "status": status,
        "symbol": str(symbol or "").upper().strip(),
        "shadow_trade_id": trade_id,
        "reason": reason,
        "closed_trade": closed_trade if isinstance(closed_trade, dict) else None,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
    return payload


def _age_minutes(value: object) -> float:
    opened = _parse_dt(value)
    if not opened:
        return 0.0
    return round(max(0.0, (datetime.now(timezone.utc) - opened).total_seconds() / 60.0), 4)


def _parse_dt(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _fast_state_from_summary(*, symbol: str, timeframe: str, summary: dict[str, Any], reason: str) -> dict[str, Any]:
    closed = int(summary.get("closed") or 0)
    pf = _maybe_float(summary.get("profit_factor")) or 0.0
    expectancy = _maybe_float(summary.get("expectancy")) or 0.0
    win_rate = _maybe_float(summary.get("win_rate")) or 0.0
    time_stop_cluster = bool(summary.get("time_stop_cluster")) or (closed >= 10 and int(summary.get("time_stop_count") or 0) > closed / 2)
    loss_cluster = bool(summary.get("loss_cluster"))
    negative_edge = closed >= 15 and pf < 1.0 and expectancy < 0
    bot_state = "pause_new_entries" if loss_cluster else "caution" if negative_edge or time_stop_cluster or (closed >= 20 and pf < 1.0) else "normal"
    return {
        "ok": True,
        "status": "mt5_adaptive_state_ready" if summary else "no_snapshot_yet",
        "symbol": str(symbol or "").upper().strip(),
        "timeframe": str(timeframe or "").upper().strip(),
        "bot_state": bot_state,
        "closed_trades": closed,
        "current_win_streak": 0,
        "current_loss_streak": 3 if loss_cluster else 0,
        "last_10_win_rate": win_rate,
        "last_20_win_rate": win_rate,
        "rolling_win_rate": win_rate,
        "rolling_profit_factor": pf,
        "rolling_expectancy": expectancy,
        "rolling_drawdown": _maybe_float(summary.get("max_drawdown") or summary.get("drawdown")) or 0.0,
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
        "recommendation_summary": "Fast path usa snapshot; learning pesado aislado.",
        "data_source_used": reason,
        "updated_at": _now(),
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
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


def _elapsed_ms(started: float) -> int:
    return int(round((time.monotonic() - started) * 1000))


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
    "mt5_trade_memory",
    "mt5_trade_lessons",
    "mt5_strategy_profile_stats",
    "mt5_adaptive_state",
    "mt5_adaptive_recommendations",
    "mt5_paper_defense_events",
    "mt5_learning_runs",
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

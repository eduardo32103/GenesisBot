from __future__ import annotations

import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from services.genesis.memory_store import MemoryStore
from services.mt5.mt5_adaptive_recommendations import MT5AdaptiveRecommendationEngine
from services.mt5.mt5_adaptive_state import MT5AdaptiveStateEngine
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_journal import MT5Journal
from services.mt5.mt5_shadow_trading import MT5ShadowTrading, is_main_metric_trade


CLOSED_STATUSES = {"win", "loss", "breakeven"}
SAFETY_FLAGS = {
    "broker_touched": False,
    "order_executed": False,
    "order_policy": "journal_only_no_broker",
}
LEARNING_TIMEOUT_SECONDS = 8.0
DEFAULT_MAX_TRADES = 25
MAX_TRADES_CAP = 50
DEFAULT_SUMMARY_LIMIT = 50
SUMMARY_LIMIT_CAP = 100


class MT5TradeMemoryEngine:
    """Builds adaptive memory from closed journal-only MT5 shadow trades."""

    def __init__(self, *, memory: MemoryStore | None = None) -> None:
        self.memory = memory or MemoryStore()
        self.journal = MT5Journal(memory=self.memory)
        self.shadow = MT5ShadowTrading(memory=self.memory)

    def run_learning(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        started = time.monotonic()
        body = payload if isinstance(payload, dict) else {}
        symbol = _symbol(body.get("symbol") or body.get("ticker") or "BTCUSD")
        timeframe = str(body.get("timeframe") or "").upper().strip()
        mode = str(body.get("mode") or "paper").strip().lower() or "paper"
        max_trades = _clamp_int(body.get("max_trades"), DEFAULT_MAX_TRADES, 1, MAX_TRADES_CAP)
        runtime = get_mt5_config()
        if runtime.fast_path_only or not runtime.learning_run_enabled or not runtime.adaptive_learning_enabled:
            return _disabled_payload(
                status="learning_disabled_by_fast_path",
                message="Learning temporarily disabled to protect MT5 fast path.",
                symbol=symbol,
                timeframe=timeframe,
                started=started,
                max_trades=max_trades,
            )
        errors: list[dict[str, Any]] = []
        warnings: list[str] = []
        memories_created = 0
        lessons_created = 0
        trades_seen = 0
        trades_processed = 0
        profile_stats: list[dict[str, Any]] = []
        adaptive_state: dict[str, Any] = {}
        recommendations: dict[str, Any] = {}
        status = "mt5_learning_run_completed"
        try:
            closed_trades = self._closed_main_trades(symbol=symbol, timeframe=timeframe, limit=max_trades)
            trades_seen = len(closed_trades)
            existing_limit = max(100, max_trades * 4)
            existing_memory_ids = _existing_ids(self.memory, "mt5_trade_memory", symbol, "trade_id", limit=existing_limit)
            existing_lesson_ids = _existing_ids(self.memory, "mt5_trade_lessons", symbol, "trade_id", limit=existing_limit)

            for index, trade in enumerate(closed_trades):
                if _elapsed_ms(started) > LEARNING_TIMEOUT_SECONDS * 1000:
                    status = "partial_completed_timeout_guard"
                    warnings.append("processing capped to avoid Railway timeout")
                    break
                try:
                    snapshot = build_trade_memory_snapshot(trade)
                    trade_id = str(snapshot.get("trade_id") or "")
                    if not trade_id:
                        raise ValueError("missing_trade_id")
                    if trade_id and trade_id not in existing_memory_ids:
                        self.journal.save("mt5_trade_memory", symbol, snapshot, confidence=snapshot.get("confidence") or "media")
                        existing_memory_ids.add(trade_id)
                        memories_created += 1
                    if trade_id and trade_id not in existing_lesson_ids:
                        lesson = analyze_closed_trade(snapshot)
                        self.journal.save("mt5_trade_lessons", symbol, lesson, confidence=lesson.get("confidence") or "media")
                        existing_lesson_ids.add(trade_id)
                        lessons_created += 1
                    trades_processed += 1
                except Exception as exc:
                    errors.append({"trade_index": index, "trade_id": str(trade.get("shadow_trade_id") or ""), "error": str(exc)[:240]})
                    continue

            if closed_trades and _elapsed_ms(started) <= LEARNING_TIMEOUT_SECONDS * 1000:
                memories = self._memories(symbol=symbol, timeframe=timeframe, limit=max(50, max_trades * 2))
                profile_stats = build_strategy_profile_stats(memories)
                for stat in profile_stats[:MAX_TRADES_CAP]:
                    self.journal.save("mt5_strategy_profile_stats", symbol, stat, confidence="media")
                adaptive_state = MT5AdaptiveStateEngine(memory=self.memory).compute(
                    symbol=symbol,
                    timeframe=timeframe,
                    limit=max(50, max_trades * 2),
                )
                self.journal.save("mt5_adaptive_state", symbol, adaptive_state, confidence="media")
                recommendations = MT5AdaptiveRecommendationEngine(memory=self.memory).recommend(
                    symbol=symbol,
                    timeframe=timeframe,
                    state=adaptive_state,
                    profile_stats=profile_stats,
                )
                self.journal.save("mt5_adaptive_recommendations", symbol, recommendations, confidence="media")
            elif _elapsed_ms(started) > LEARNING_TIMEOUT_SECONDS * 1000:
                status = "partial_completed_timeout_guard"
                if "processing capped to avoid Railway timeout" not in warnings:
                    warnings.append("processing capped to avoid Railway timeout")
        except Exception as exc:
            result = _error_payload(
                status="mt5_learning_error",
                symbol=symbol,
                timeframe=timeframe,
                error=str(exc),
                started=started,
                trades_seen=trades_seen,
                trades_processed=trades_processed,
                memories_created=memories_created,
                lessons_created=lessons_created,
                warnings=warnings,
                errors=errors,
                memory=self.memory,
            )
            self._safe_save_learning_run(symbol, result)
            return result

        result = {
            "ok": True,
            "status": status,
            "symbol": symbol,
            "timeframe": timeframe,
            "mode": mode,
            "max_trades": max_trades,
            "trades_seen": trades_seen,
            "trades_processed": trades_processed,
            "memories_created": memories_created,
            "lessons_created": lessons_created,
            "profile_stats_updated": len(profile_stats),
            "adaptive_state": adaptive_state,
            "recommendations": recommendations.get("recommendations") or [],
            "duration_ms": _elapsed_ms(started),
            "errors": errors,
            "warnings": warnings,
            **_storage_payload(self.memory),
            "updated_at": _now(),
            **SAFETY_FLAGS,
        }
        self._safe_save_learning_run(symbol, result)
        return result

    def memory_summary(self, *, symbol: str = "", limit: int = DEFAULT_SUMMARY_LIMIT) -> dict[str, Any]:
        started = time.monotonic()
        clean_symbol = _symbol(symbol)
        safe_limit = _clamp_int(limit, DEFAULT_SUMMARY_LIMIT, 1, SUMMARY_LIMIT_CAP)
        runtime = get_mt5_config()
        if runtime.fast_path_only or not runtime.memory_summary_enabled:
            return _disabled_payload(
                status="memory_summary_disabled_by_fast_path",
                message="Memory summary temporarily disabled to protect MT5 fast path.",
                symbol=clean_symbol,
                timeframe="",
                started=started,
                limit=safe_limit,
            )
        warnings: list[str] = []
        errors: list[dict[str, Any]] = []
        try:
            memories = self._memories(symbol=clean_symbol, limit=safe_limit)
            lessons = self._lessons(symbol=clean_symbol, limit=safe_limit)
            win_reasons = Counter(_compact(item.get("primary_win_reason")) for item in lessons if item.get("primary_win_reason"))
            loss_reasons = Counter(_compact(item.get("primary_loss_reason")) for item in lessons if item.get("primary_loss_reason"))
            mistakes = Counter(reason for item in lessons for reason in _as_list(item.get("mistakes")))
            strengths = Counter(reason for item in lessons for reason in _as_list(item.get("strengths")))
            regimes = Counter(_compact(item.get("market_regime_label") or item.get("regime")) for item in memories if item.get("regime") or item.get("market_regime_label"))
            stats = build_strategy_profile_stats(memories[:safe_limit])
            best_contexts = sorted(stats, key=lambda item: (_number(item.get("profit_factor")) or 0.0, _number(item.get("expectancy")) or 0.0), reverse=True)[:5]
            worst_contexts = sorted(stats, key=lambda item: (_number(item.get("profit_factor")) or 0.0, _number(item.get("expectancy")) or 0.0))[:5]
            return {
                "ok": True,
                "status": "mt5_memory_summary_ready",
                "symbol": clean_symbol,
                "limit": safe_limit,
                "total_memories": len(memories),
                "lessons_count": len(lessons),
                "top_win_reasons": _top(win_reasons),
                "top_loss_reasons": _top(loss_reasons),
                "top_mistakes": _top(mistakes),
                "top_strengths": _top(strengths),
                "most_common_regimes": _top(regimes),
                "best_contexts": best_contexts,
                "worst_contexts": worst_contexts,
                "genesis_reading": _summary_reading(clean_symbol, len(memories), lessons, best_contexts),
                "duration_ms": _elapsed_ms(started),
                "errors": errors,
                "warnings": warnings,
                **_storage_payload(self.memory),
                "updated_at": _now(),
                **SAFETY_FLAGS,
            }
        except Exception as exc:
            fallback = self._summary_fallback(clean_symbol, safe_limit, str(exc), started)
            fallback["errors"] = [{"error": str(exc)[:240]}]
            return fallback

    def learning_status(self, *, symbol: str = "") -> dict[str, Any]:
        started = time.monotonic()
        clean_symbol = _symbol(symbol)
        try:
            rows = self.memory.get_mt5_events("mt5_learning_runs", clean_symbol or None, limit=5)
            last = rows[0].get("payload") if rows and isinstance(rows[0].get("payload"), dict) else {}
            errors = last.get("errors") if isinstance(last.get("errors"), list) else []
            last_error = ""
            if not last.get("ok"):
                last_error = str(last.get("error") or "")
            elif errors:
                last_error = str(errors[0].get("error") if isinstance(errors[0], dict) else errors[0])
            return {
                "ok": True,
                "status": "mt5_learning_status_ready",
                "symbol": clean_symbol,
                "last_learning_run": last or None,
                "last_error": last_error,
                "last_duration_ms": last.get("duration_ms", 0) if isinstance(last, dict) else 0,
                "trades_seen": last.get("trades_seen", 0) if isinstance(last, dict) else 0,
                "trades_processed": last.get("trades_processed", 0) if isinstance(last, dict) else 0,
                "duration_ms": _elapsed_ms(started),
                **_storage_payload(self.memory),
                **SAFETY_FLAGS,
            }
        except Exception as exc:
            return {
                "ok": False,
                "status": "mt5_learning_status_error",
                "symbol": clean_symbol,
                "last_learning_run": None,
                "last_error": str(exc)[:240],
                "last_duration_ms": 0,
                "trades_seen": 0,
                "trades_processed": 0,
                "duration_ms": _elapsed_ms(started),
                "warnings": [],
                **_storage_payload(self.memory),
                **SAFETY_FLAGS,
            }

    def strategy_profiles(self, *, symbol: str = "") -> dict[str, Any]:
        clean_symbol = _symbol(symbol)
        memories = self._memories(symbol=clean_symbol, limit=SUMMARY_LIMIT_CAP)
        items = build_strategy_profile_stats(memories)
        return {
            "ok": True,
            "status": "mt5_strategy_profiles_ready",
            "symbol": clean_symbol,
            "items": items,
            "count": len(items),
            "updated_at": _now(),
            **SAFETY_FLAGS,
        }

    def _closed_main_trades(self, *, symbol: str, timeframe: str = "", limit: int = DEFAULT_MAX_TRADES) -> list[dict[str, Any]]:
        clean_timeframe = str(timeframe or "").upper().strip()
        safe_limit = _clamp_int(limit, DEFAULT_MAX_TRADES, 1, MAX_TRADES_CAP)
        trades = [
            trade
            for trade in self.shadow.trades(symbol, limit=max(100, safe_limit * 4))
            if trade.get("status") in CLOSED_STATUSES
            and is_main_metric_trade(trade, query_symbol=symbol)
            and (not clean_timeframe or str(trade.get("timeframe") or "").upper() == clean_timeframe)
        ]
        return sorted(trades, key=lambda trade: str(trade.get("closed_at") or trade.get("updated_at") or ""), reverse=True)[:safe_limit]

    def _memories(self, *, symbol: str = "", timeframe: str = "", limit: int = DEFAULT_SUMMARY_LIMIT) -> list[dict[str, Any]]:
        clean_timeframe = str(timeframe or "").upper().strip()
        safe_limit = _clamp_int(limit, DEFAULT_SUMMARY_LIMIT, 1, SUMMARY_LIMIT_CAP)
        rows = self.memory.get_mt5_events("mt5_trade_memory", _symbol(symbol) or None, limit=safe_limit)
        latest: dict[str, dict[str, Any]] = {}
        for row in rows:
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            trade_id = str(payload.get("trade_id") or row.get("created_at") or "")
            if not trade_id or trade_id in latest:
                continue
            if clean_timeframe and str(payload.get("timeframe") or "").upper() != clean_timeframe:
                continue
            latest[trade_id] = payload
        return list(latest.values())[:safe_limit]

    def _lessons(self, *, symbol: str = "", limit: int = DEFAULT_SUMMARY_LIMIT) -> list[dict[str, Any]]:
        safe_limit = _clamp_int(limit, DEFAULT_SUMMARY_LIMIT, 1, SUMMARY_LIMIT_CAP)
        rows = self.memory.get_mt5_events("mt5_trade_lessons", _symbol(symbol) or None, limit=safe_limit)
        return [row.get("payload") for row in rows if isinstance(row.get("payload"), dict)]

    def _summary_fallback(self, symbol: str, limit: int, error: str, started: float) -> dict[str, Any]:
        warnings = ["memory summary fallback used"]
        try:
            state = MT5AdaptiveStateEngine(memory=self.memory).compute(symbol=symbol, limit=limit)
        except Exception as exc:
            state = {"error": str(exc)[:240]}
            warnings.append("adaptive_state_fallback_failed")
        return {
            "ok": True,
            "status": "mt5_memory_summary_fallback",
            "symbol": symbol,
            "limit": limit,
            "total_memories": 0,
            "lessons_count": 0,
            "top_win_reasons": [],
            "top_loss_reasons": [],
            "top_mistakes": [],
            "top_strengths": [],
            "most_common_regimes": [],
            "best_contexts": [],
            "worst_contexts": [],
            "fallback_adaptive_state": state,
            "genesis_reading": f"{symbol}: memory summary uso fallback rapido; revisar error controlado.",
            "duration_ms": _elapsed_ms(started),
            "warnings": warnings,
            "error": error[:240],
            **_storage_payload(self.memory),
            "updated_at": _now(),
            **SAFETY_FLAGS,
        }

    def _safe_save_learning_run(self, symbol: str, result: dict[str, Any]) -> None:
        try:
            self.journal.save("mt5_learning_runs", symbol or "MT5", result, confidence="media")
        except Exception:
            return


def build_trade_memory_snapshot(trade: dict[str, Any]) -> dict[str, Any]:
    opened_at = str(trade.get("opened_at") or "")
    closed_at = str(trade.get("closed_at") or trade.get("updated_at") or "")
    entry = _number(trade.get("entry")) or 0.0
    stop = _number(trade.get("stop_loss")) or 0.0
    target = _number(trade.get("take_profit")) or 0.0
    exit_price = _number(trade.get("exit_price")) or _number(trade.get("last_price")) or 0.0
    return {
        "trade_id": str(trade.get("shadow_trade_id") or trade.get("trade_id") or ""),
        "shadow_trade_id": str(trade.get("shadow_trade_id") or trade.get("trade_id") or ""),
        "symbol": _symbol(trade.get("symbol")),
        "original_symbol": _symbol(trade.get("original_symbol") or trade.get("symbol")),
        "normalized_symbol": _symbol(trade.get("normalized_symbol") or trade.get("symbol")),
        "timeframe": str(trade.get("timeframe") or "").upper(),
        "strategy_profile": str(trade.get("strategy_profile") or trade.get("exploration_profile") or "unknown"),
        "source": str(trade.get("source") or "mt5_bridge"),
        "auto_forward": bool(trade.get("auto_forward")),
        "paper_exploration": bool(trade.get("paper_exploration")),
        "entry": entry,
        "stop_loss": stop,
        "take_profit": target,
        "exit_price": exit_price,
        "exit_reason": str(trade.get("exit_reason") or trade.get("last_exit_reason") or ""),
        "status": str(trade.get("status") or "").casefold(),
        "pnl": _number(trade.get("pnl")) or 0.0,
        "pnl_pct": _number(trade.get("pnl_pct")) or 0.0,
        "r_multiple": _number(trade.get("r_multiple")) or 0.0,
        "opened_at": opened_at,
        "closed_at": closed_at,
        "duration_minutes": _duration_minutes(opened_at, closed_at),
        "bars_open": int(_number(trade.get("bars_open")) or 0),
        "spread_at_entry": _number(trade.get("spread_at_entry") or trade.get("spread")) or 0.0,
        "spread_at_exit": _number(trade.get("spread_at_exit")) or 0.0,
        "trend_score": _number(trade.get("trend_score")) or 0.0,
        "momentum_score": _number(trade.get("momentum_score")) or 0.0,
        "volatility_score": _number(trade.get("volatility_score")) or 0.0,
        "regime": str(trade.get("regime") or trade.get("market_regime") or "unknown"),
        "confidence": str(trade.get("confidence") or "medium"),
        "decision_reason": str(trade.get("decision_reason") or trade.get("reason") or ""),
        "last_block_reason": str(trade.get("last_block_reason") or ""),
        "market_context": _dict_or_empty(trade.get("market_context")),
        "news_context": _dict_or_empty(trade.get("news_context")),
        "whale_context": _dict_or_empty(trade.get("whale_context")),
        "macro_context": _dict_or_empty(trade.get("macro_context")),
        "max_favorable_excursion": _number(trade.get("max_favorable_excursion")) or 0.0,
        "max_adverse_excursion": _number(trade.get("max_adverse_excursion")) or 0.0,
        "breakeven_armed": bool(trade.get("breakeven_armed")),
        "trailing_stop_active": bool(trade.get("trailing_stop_active")),
        "virtual_stop_loss": _number(trade.get("virtual_stop_loss")),
        "risk_reward": _risk_reward(trade, entry, stop, target),
        "updated_at": _now(),
        **SAFETY_FLAGS,
    }


def analyze_closed_trade(trade: dict[str, Any]) -> dict[str, Any]:
    status = str(trade.get("status") or "").casefold()
    r_multiple = _number(trade.get("r_multiple")) or 0.0
    exit_reason = str(trade.get("exit_reason") or "unknown")
    regime = str(trade.get("regime") or "unknown")
    confidence = str(trade.get("confidence") or "").casefold()
    rr = _number(trade.get("risk_reward")) or 0.0
    spread = _number(trade.get("spread_at_entry")) or 0.0
    trade_quality = "good" if status == "win" or r_multiple > 0 else "bad" if status == "loss" or r_multiple < 0 else "neutral"
    mistakes: list[str] = []
    strengths: list[str] = []
    tags: list[str] = []

    if trade_quality == "good":
        strengths.append("good_risk_control")
        tags.append("momentum_followed")
        if exit_reason == "time_stop":
            tags.append("time_stop_win")
            strengths.append("time_stop_protected_profit")
        if bool(trade.get("breakeven_armed")):
            tags.append("good_risk_control")
    elif trade_quality == "bad":
        mistakes.append("loss_after_signal")
        tags.append("weak_confirmation")
        if exit_reason == "time_stop":
            tags.append("time_stop_loss")
            mistakes.append("time_stop_did_not_help")
        if regime in {"chop", "range", "not_confirmed"}:
            tags.append("chop_market")
            mistakes.append("chop_market")
        if confidence == "low":
            mistakes.append("weak_confirmation")
    else:
        tags.append("neutral_outcome")
        strengths.append("risk_contained")

    if rr and rr < 1.2:
        mistakes.append("poor_rr")
        tags.append("poor_rr")
    if spread > 0 and spread > max((_number(trade.get("entry")) or 0.0) * 0.0025, 1.0):
        mistakes.append("high_spread")
        tags.append("high_spread")
    if bool(trade.get("trailing_stop_active")):
        strengths.append("trailing_protected_trade")
        tags.append("trailing_used")

    primary_win_reason = _win_reason(exit_reason, trade) if trade_quality == "good" else ""
    primary_loss_reason = _loss_reason(exit_reason, trade, mistakes) if trade_quality == "bad" else ""
    lesson = _lesson(trade_quality, primary_win_reason, primary_loss_reason, regime)
    return {
        "trade_id": str(trade.get("trade_id") or trade.get("shadow_trade_id") or ""),
        "symbol": _symbol(trade.get("symbol")),
        "normalized_symbol": _symbol(trade.get("normalized_symbol")),
        "timeframe": str(trade.get("timeframe") or "").upper(),
        "strategy_profile": str(trade.get("strategy_profile") or "unknown"),
        "trade_quality": trade_quality,
        "primary_win_reason": primary_win_reason,
        "primary_loss_reason": primary_loss_reason,
        "mistakes": sorted(set(mistakes)),
        "strengths": sorted(set(strengths)),
        "market_regime_label": regime,
        "lesson": lesson,
        "future_rule_candidate": _future_rule_candidate(trade_quality, mistakes, exit_reason),
        "tags": sorted(set(tags)),
        "confidence": "media",
        "created_at": _now(),
        **SAFETY_FLAGS,
    }


def build_strategy_profile_stats(memories: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for trade in memories:
        key = (
            _symbol(trade.get("symbol")),
            str(trade.get("timeframe") or "").upper() or "UNKNOWN",
            str(trade.get("strategy_profile") or "unknown"),
            str(trade.get("regime") or "unknown"),
            _score_bucket(trade.get("trend_score")),
            _score_bucket(trade.get("momentum_score")),
            _score_bucket(trade.get("volatility_score")),
            _spread_bucket(trade.get("spread_at_entry")),
            _hour_bucket(trade.get("opened_at")),
        )
        groups.setdefault(key, []).append(trade)
    stats: list[dict[str, Any]] = []
    for key, items in groups.items():
        symbol, timeframe, profile, regime, trend_bucket, momentum_bucket, volatility_bucket, spread_bucket, hour_bucket = key
        wins = [trade for trade in items if trade.get("status") == "win"]
        losses = [trade for trade in items if trade.get("status") == "loss"]
        pnls = [_pnl_value(trade) for trade in items if trade.get("status") in CLOSED_STATUSES]
        gross_win = sum(value for value in pnls if value > 0)
        gross_loss = abs(sum(value for value in pnls if value < 0))
        exit_reasons = Counter(str(trade.get("exit_reason") or "unknown") for trade in items)
        stats.append(
            {
                "profile_stat_id": "|".join(key),
                "symbol": symbol,
                "timeframe": timeframe,
                "strategy_profile": profile,
                "regime": regime,
                "trend_bucket": trend_bucket,
                "momentum_bucket": momentum_bucket,
                "volatility_bucket": volatility_bucket,
                "spread_bucket": spread_bucket,
                "hour_bucket": hour_bucket,
                "trades": len(items),
                "wins": len(wins),
                "losses": len(losses),
                "win_rate": round((len(wins) / len(items)) * 100, 2) if items else 0.0,
                "profit_factor": _profit_factor(gross_win, gross_loss),
                "expectancy": round(sum(pnls) / len(pnls), 4) if pnls else 0.0,
                "avg_r": round(sum((_number(trade.get("r_multiple")) or 0.0) for trade in items) / len(items), 4) if items else 0.0,
                "max_drawdown": _max_drawdown(items),
                "best_exit_reason": exit_reasons.most_common(1)[0][0] if exit_reasons else "",
                "worst_exit_reason": _worst_exit_reason(items),
                "updated_at": _now(),
                **SAFETY_FLAGS,
            }
        )
    return sorted(stats, key=lambda item: (int(item.get("trades") or 0), _number(item.get("profit_factor")) or 0.0), reverse=True)


def _existing_ids(memory: MemoryStore, collection: str, symbol: str, key: str, *, limit: int = DEFAULT_SUMMARY_LIMIT) -> set[str]:
    safe_limit = _clamp_int(limit, DEFAULT_SUMMARY_LIMIT, 1, SUMMARY_LIMIT_CAP * 2)
    rows = memory.get_mt5_events(collection, symbol or None, limit=safe_limit)
    ids = set()
    for row in rows:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        value = str(payload.get(key) or "")
        if value:
            ids.add(value)
    return ids


def _error_payload(
    *,
    status: str,
    symbol: str,
    timeframe: str,
    error: str,
    started: float,
    trades_seen: int,
    trades_processed: int,
    memories_created: int,
    lessons_created: int,
    warnings: list[str],
    errors: list[dict[str, Any]],
    memory: MemoryStore,
) -> dict[str, Any]:
    return {
        "ok": False,
        "status": status,
        "symbol": symbol,
        "timeframe": timeframe,
        "error": str(error or "")[:500],
        "warnings": warnings,
        "errors": errors,
        "trades_seen": trades_seen,
        "trades_processed": trades_processed,
        "memories_created": memories_created,
        "lessons_created": lessons_created,
        "duration_ms": _elapsed_ms(started),
        **_storage_payload(memory),
        "updated_at": _now(),
        **SAFETY_FLAGS,
    }


def _disabled_payload(
    *,
    status: str,
    message: str,
    symbol: str,
    timeframe: str,
    started: float,
    limit: int | None = None,
    max_trades: int | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": False,
        "status": status,
        "message": message,
        "symbol": symbol,
        "timeframe": timeframe,
        "duration_ms": _elapsed_ms(started),
        "warnings": ["MT5_FAST_PATH_ONLY protects tick/decision/performance endpoints"],
        "errors": [],
        "trades_seen": 0,
        "trades_processed": 0,
        "memories_created": 0,
        "lessons_created": 0,
        "updated_at": _now(),
        **SAFETY_FLAGS,
    }
    if limit is not None:
        payload["limit"] = limit
        payload["total_memories"] = 0
        payload["lessons_count"] = 0
    if max_trades is not None:
        payload["max_trades"] = max_trades
    return payload


def _storage_payload(memory: MemoryStore) -> dict[str, Any]:
    backend = str(getattr(memory, "backend", "") or "unknown")
    return {
        "storage_backend": backend,
        "database_enabled": backend == "postgres",
        "memory_fallback": backend != "postgres",
    }


def _elapsed_ms(started: float) -> int:
    return int(round((time.monotonic() - started) * 1000))


def _clamp_int(value: object, default: int, minimum: int, maximum: int) -> int:
    try:
        number = int(value) if value is not None and value != "" else default
    except (TypeError, ValueError):
        number = default
    return max(minimum, min(maximum, number))


def _win_reason(exit_reason: str, trade: dict[str, Any]) -> str:
    if exit_reason == "take_profit":
        return "take_profit_hit"
    if exit_reason == "trailing_stop":
        return "trailing_locked_profit"
    if exit_reason == "time_stop":
        return "time_stop_positive"
    if bool(trade.get("breakeven_armed")):
        return "breakeven_protected_trade"
    return "positive_r_multiple"


def _loss_reason(exit_reason: str, trade: dict[str, Any], mistakes: list[str]) -> str:
    if "high_spread" in mistakes:
        return "high_spread"
    if "chop_market" in mistakes:
        return "chop_market"
    if exit_reason == "stop_loss":
        return "stop_loss_hit"
    if exit_reason == "time_stop":
        return "time_stop_loss"
    return "negative_r_multiple"


def _lesson(quality: str, win_reason: str, loss_reason: str, regime: str) -> str:
    if quality == "good":
        return f"Trade bueno en regimen {regime}; razon principal: {win_reason or 'resultado positivo'}."
    if quality == "bad":
        return f"Trade malo en regimen {regime}; revisar filtro por {loss_reason or 'resultado negativo'}."
    return f"Trade neutral en regimen {regime}; mantener en observacion paper."


def _future_rule_candidate(quality: str, mistakes: list[str], exit_reason: str) -> str:
    if quality == "bad" and "chop_market" in mistakes:
        return "Exigir filtro anti-chop mas fuerte antes de abrir shadow trade."
    if quality == "bad" and exit_reason == "time_stop":
        return "Revisar duracion maxima o trailing para evitar time_stop negativo."
    if quality == "good" and exit_reason == "time_stop":
        return "Validar si time_stop captura ganancias pequenas sin bloquear el bot."
    if quality == "good":
        return "Conservar setup en paper hasta aumentar muestra."
    return "No cambiar reglas con resultado neutral."


def _summary_reading(symbol: str, total: int, lessons: list[dict[str, Any]], best_contexts: list[dict[str, Any]]) -> str:
    if total < 30:
        return f"{symbol}: Genesis ya guarda memoria, pero la muestra sigue baja; continuar paper hasta 30 cierres."
    if best_contexts:
        best = best_contexts[0]
        return (
            f"{symbol}: mejor contexto observado {best.get('strategy_profile')} / {best.get('regime')} "
            f"con PF {best.get('profit_factor')} y win rate {best.get('win_rate')}%."
        )
    return f"{symbol}: memoria lista, sin contexto ganador dominante todavia."


def _top(counter: Counter[str], limit: int = 5) -> list[dict[str, Any]]:
    return [{"name": key, "count": count} for key, count in counter.most_common(limit) if key]


def _as_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item or "").strip()]
    if value:
        return [str(value)]
    return []


def _compact(value: object) -> str:
    return str(value or "").strip()


def _dict_or_empty(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _risk_reward(trade: dict[str, Any], entry: float, stop: float, target: float) -> float:
    existing = _number(trade.get("risk_reward"))
    if existing is not None and existing > 0:
        return round(existing, 4)
    action = str(trade.get("action") or "").upper()
    risk = abs(entry - stop)
    if risk <= 0:
        return 0.0
    reward = abs(target - entry)
    if action == "BUY" and not (stop < entry < target):
        return 0.0
    if action == "SELL" and not (target < entry < stop):
        return 0.0
    return round(reward / risk, 4)


def _duration_minutes(opened_at: str, closed_at: str) -> float:
    opened = _parse_datetime(opened_at)
    closed = _parse_datetime(closed_at)
    if not opened or not closed:
        return 0.0
    return round(max((closed - opened).total_seconds(), 0.0) / 60, 2)


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _score_bucket(value: object) -> str:
    number = _number(value)
    if number is None:
        return "unknown"
    if number >= 70:
        return "high"
    if number >= 45:
        return "medium"
    return "low"


def _spread_bucket(value: object) -> str:
    spread = _number(value) or 0.0
    if spread <= 0:
        return "unknown"
    if spread <= 10:
        return "tight"
    if spread <= 50:
        return "normal"
    return "wide"


def _hour_bucket(value: object) -> str:
    parsed = _parse_datetime(str(value or ""))
    if not parsed:
        return "unknown"
    hour = parsed.hour
    if hour < 8:
        return "asia"
    if hour < 16:
        return "us_morning"
    return "us_afternoon"


def _pnl_value(trade: dict[str, Any]) -> float:
    r_multiple = _number(trade.get("r_multiple"))
    if r_multiple is not None:
        return r_multiple
    return _number(trade.get("pnl")) or _number(trade.get("pnl_pct")) or 0.0


def _profit_factor(gross_win: float, gross_loss: float) -> float:
    if gross_win <= 0 and gross_loss <= 0:
        return 0.0
    if gross_loss <= 0:
        return round(gross_win, 4)
    return round(gross_win / gross_loss, 4)


def _max_drawdown(trades: list[dict[str, Any]]) -> float:
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    ordered = sorted(trades, key=lambda trade: str(trade.get("closed_at") or trade.get("updated_at") or ""))
    for trade in ordered:
        if trade.get("status") not in CLOSED_STATUSES:
            continue
        equity += _pnl_value(trade)
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return round(drawdown, 4)


def _worst_exit_reason(trades: list[dict[str, Any]]) -> str:
    losses = [trade for trade in trades if trade.get("status") == "loss"]
    if not losses:
        return ""
    return Counter(str(trade.get("exit_reason") or "unknown") for trade in losses).most_common(1)[0][0]


def _number(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _symbol(value: object) -> str:
    return str(value or "").upper().strip()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

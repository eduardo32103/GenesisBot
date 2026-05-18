from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from services.genesis.memory_store import MemoryStore
from services.mt5.instrument_resolver import enrich_payload, normalize_mt5_symbol, symbol_aliases
from services.mt5.mt5_journal import MT5Journal
from services.mt5.mt5_order_model import MT5OrderIntent, sanitize_payload


class MT5ShadowTrading:
    """Journal-only trade simulator for MT5 decisions.

    This never touches a broker. It stores synthetic trades and outcomes in
    MemoryStore so Genesis can measure forward-test quality from later ticks.
    """

    def __init__(self, *, memory: MemoryStore | None = None) -> None:
        self.memory = memory or MemoryStore()
        self.journal = MT5Journal(memory=self.memory)

    def record_signal(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        clean = sanitize_payload(payload or {})
        action = _action(clean)
        if action in {"BUY", "SELL"}:
            return self.create_shadow_trade(clean)
        if action == "NO_TRADE":
            return self.record_no_trade_signal(clean)
        if action == "WAIT":
            return {"created": False, "status": "wait_signal_recorded", "action": action}
        if action in {"HEDGE", "REDUCE"}:
            return self.record_hedge_signal(clean)
        return {"created": False, "status": "ignored_non_actionable_signal", "action": action}

    def create_shadow_trade(self, payload: dict[str, Any]) -> dict[str, Any]:
        clean_payload = enrich_payload(payload)
        intent = MT5OrderIntent.from_payload(clean_payload)
        action = intent.action
        symbol = _symbol(intent.symbol or clean_payload.get("symbol") or clean_payload.get("ticker"))
        entry = intent.entry if intent.entry is not None else _number(clean_payload.get("last") or clean_payload.get("bid") or clean_payload.get("ask"))
        if action not in {"BUY", "SELL"} or not symbol:
            return {"created": False, "status": "not_actionable", "action": action, "symbol": symbol}
        if entry is None or intent.stop_loss is None or intent.take_profit is None:
            invalid = {
                "symbol": symbol,
                "action": action,
                "entry": entry,
                "stop_loss": intent.stop_loss,
                "take_profit": intent.take_profit,
                "status": "invalid_signal",
                "reason": "missing_risk_parameters",
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
                "created_at": _now(),
            }
            event = self.journal.save("mt5_signal_outcomes", symbol, invalid)
            return {"created": False, "status": "invalid_signal", "reason": "missing_risk_parameters", "event": event}

        risk = abs(entry - intent.stop_loss)
        if risk <= 0:
            invalid = {
                "symbol": symbol,
                "action": action,
                "entry": entry,
                "stop_loss": intent.stop_loss,
                "take_profit": intent.take_profit,
                "status": "invalid_signal",
                "reason": "invalid_stop_distance",
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
                "created_at": _now(),
            }
            event = self.journal.save("mt5_signal_outcomes", symbol, invalid)
            return {"created": False, "status": "invalid_signal", "reason": "invalid_stop_distance", "event": event}

        trade = {
            "shadow_trade_id": str(payload.get("shadow_trade_id") or f"shadow-{uuid.uuid4().hex[:12]}"),
            "symbol": symbol,
            "original_symbol": _symbol(clean_payload.get("original_symbol") or clean_payload.get("symbol") or symbol),
            "normalized_symbol": _symbol(clean_payload.get("normalized_symbol") or _normalized_symbol(symbol)),
            "instrument_type": clean_payload.get("instrument_type") or "unknown",
            "is_spot_crypto": bool(clean_payload.get("is_spot_crypto")),
            "underlying": clean_payload.get("underlying") or "",
            "instrument_warning": clean_payload.get("instrument_warning") or "",
            "action": action,
            "entry": entry,
            "stop_loss": intent.stop_loss,
            "take_profit": intent.take_profit,
            "trailing_stop": intent.trailing_stop,
            "risk_pct": intent.risk_pct,
            "timeframe": str(intent.timeframe or payload.get("timeframe") or "").upper(),
            "strategy_profile": intent.strategy_profile or str(payload.get("strategy") or ""),
            "confidence": intent.confidence,
            "hedge_score": intent.hedge_score,
            "no_trade_score": intent.no_trade_score,
            "genesis_context_score": intent.genesis_context_score,
            "opened_at": str(clean_payload.get("timestamp") or clean_payload.get("bar_time") or _now()),
            "updated_at": _now(),
            "status": "open",
            "source": str(clean_payload.get("source") or "mt5_bridge"),
            "auto_forward": bool(clean_payload.get("auto_forward")),
            "paper_exploration": bool(clean_payload.get("paper_exploration")),
            "excluded_from_live_grade": bool(clean_payload.get("excluded_from_live_grade")),
            "included_in_exploration_metrics": bool(clean_payload.get("included_in_exploration_metrics")),
            "exploration_profile": str(clean_payload.get("exploration_profile") or ""),
            "manual_test": bool(clean_payload.get("manual_test")) or not bool(clean_payload.get("auto_forward")),
            "excluded_from_main_metrics": bool(clean_payload.get("excluded_from_main_metrics")),
            "risk_reward": _number(clean_payload.get("risk_reward")) or 0.0,
            "max_favorable_excursion": 0.0,
            "max_adverse_excursion": 0.0,
            "unrealized_pnl": 0.0,
            "r_multiple": 0.0,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }
        event = self.journal.save("mt5_shadow_trades", symbol, trade, confidence=trade["confidence"])
        return {"created": True, "status": "shadow_trade_opened", "trade": trade, "event": event}

    def create_from_order_request(
        self,
        payload: dict[str, Any],
        *,
        account_state: dict[str, Any] | None = None,
        min_rr: float = 1.2,
    ) -> dict[str, Any]:
        clean = sanitize_payload(payload or {})
        intent = MT5OrderIntent.from_payload(clean)
        account = account_state or {}
        if intent.action not in {"BUY", "SELL"}:
            return {"created": False, "status": "not_actionable", "reason": "action_not_buy_or_sell"}
        if not bool(account.get("is_demo")):
            return {"created": False, "status": "blocked", "reason": "demo_account_not_confirmed"}
        if intent.entry is None or intent.stop_loss is None or intent.take_profit is None:
            return {"created": False, "status": "invalid_signal", "reason": "missing_risk_parameters"}
        rr = _risk_reward(intent.action, intent.entry, intent.stop_loss, intent.take_profit)
        if rr is None or rr < min_rr:
            return {"created": False, "status": "invalid_signal", "reason": "risk_reward_too_low", "risk_reward": rr}
        result = self.create_shadow_trade(
            {
                **clean,
                "symbol": intent.symbol,
                "action": intent.action,
                "entry": intent.entry,
                "stop_loss": intent.stop_loss,
                "take_profit": intent.take_profit,
                "risk_pct": intent.risk_pct,
                "timeframe": intent.timeframe,
                "strategy_profile": intent.strategy_profile,
                "confidence": intent.confidence,
                "risk_reward": rr,
                "source": clean.get("source") or "mt5_order_request_shadow",
                "manual_test": True,
            }
        )
        if result.get("created") and isinstance(result.get("trade"), dict):
            result["trade"]["risk_reward"] = rr
        result["risk_reward"] = rr
        return result

    def record_no_trade_signal(self, payload: dict[str, Any]) -> dict[str, Any]:
        symbol = _symbol(payload.get("symbol") or payload.get("ticker"))
        if not symbol:
            return {"created": False, "status": "missing_symbol"}
        price = _number(payload.get("price") or payload.get("entry") or payload.get("last"))
        event_payload = {
            "outcome_id": str(payload.get("outcome_id") or f"notrade-{uuid.uuid4().hex[:12]}"),
            "symbol": symbol,
            "decision": _action(payload),
            "reference_price": price,
            "timeframe": str(payload.get("timeframe") or "").upper(),
            "reason": str(payload.get("reason") or "no_trade_signal"),
            "status": "pending",
            "outcome": "pending",
            "created_at": str(payload.get("timestamp") or payload.get("bar_time") or _now()),
            "updated_at": _now(),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }
        event = self.journal.save("mt5_no_trade_outcomes", symbol, event_payload)
        return {"created": True, "status": "no_trade_outcome_pending", "event": event, "outcome": event_payload}

    def record_hedge_signal(self, payload: dict[str, Any]) -> dict[str, Any]:
        symbol = _symbol(payload.get("symbol") or payload.get("ticker"))
        if not symbol:
            return {"created": False, "status": "missing_symbol"}
        price = _number(payload.get("price") or payload.get("entry") or payload.get("last"))
        event_payload = {
            "outcome_id": str(payload.get("outcome_id") or f"hedge-{uuid.uuid4().hex[:12]}"),
            "symbol": symbol,
            "decision": _action(payload),
            "reference_price": price,
            "timeframe": str(payload.get("timeframe") or "").upper(),
            "hedge_score": int(_number(payload.get("hedge_score")) or 0),
            "reason": str(payload.get("reason") or "hedge_signal"),
            "status": "pending",
            "outcome": "pending",
            "created_at": str(payload.get("timestamp") or payload.get("bar_time") or _now()),
            "updated_at": _now(),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }
        event = self.journal.save("mt5_hedge_outcomes", symbol, event_payload)
        return {"created": True, "status": "hedge_outcome_pending", "event": event, "outcome": event_payload}

    def update_with_tick(self, tick: dict[str, Any]) -> dict[str, Any]:
        clean_tick = sanitize_payload(tick or {})
        symbol = _symbol(clean_tick.get("symbol") or clean_tick.get("ticker"))
        last = _price_from_tick(clean_tick)
        if not symbol or last is None:
            return {"ok": False, "status": "tick_missing_symbol_or_price", "updates": []}

        updates = []
        for trade in self.open_trades(symbol):
            update = self._evaluate_trade(trade, clean_tick, last)
            event = self.journal.save("mt5_shadow_trades", symbol, update, confidence=update.get("confidence") or "media")
            updates.append({"trade": update, "event": event})
            if update.get("status") in {"win", "loss"}:
                self.journal.save(
                    "mt5_signal_outcomes",
                    symbol,
                    {
                        "shadow_trade_id": update["shadow_trade_id"],
                        "symbol": symbol,
                        "action": update["action"],
                        "timeframe": update.get("timeframe") or "",
                        "strategy_profile": update.get("strategy_profile") or "",
                        "outcome": update["status"],
                        "pnl": update["pnl"],
                        "pnl_pct": update["pnl_pct"],
                        "r_multiple": update["r_multiple"],
                        "closed_at": update["closed_at"],
                        "broker_touched": False,
                        "order_executed": False,
                        "order_policy": "journal_only_no_broker",
                    },
                )

        no_trade_updates = self._update_no_trade_outcomes(symbol, clean_tick, last)
        hedge_updates = self._update_hedge_outcomes(symbol, clean_tick, last)
        return {
            "ok": True,
            "status": "shadow_trades_updated",
            "updates": updates,
            "no_trade_updates": no_trade_updates,
            "hedge_updates": hedge_updates,
            "broker_touched": False,
            "order_executed": False,
        }

    def open_trades(self, symbol: str | None = None) -> list[dict[str, Any]]:
        return [trade for trade in self.trades(symbol) if trade.get("status") == "open" and is_main_metric_trade(trade, query_symbol=symbol or "")]

    def exclude_manual_tests(self, *, symbol: str = "") -> dict[str, Any]:
        clean_symbol = _symbol(symbol)
        updates = []
        for trade in self.trades(clean_symbol):
            if _is_auto_forward_trade(trade):
                continue
            update = {
                **trade,
                "manual_test": True,
                "excluded_from_main_metrics": True,
                "updated_at": _now(),
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
            event = self.journal.save("mt5_shadow_trades", update["symbol"], update, confidence=update.get("confidence") or "media")
            updates.append({"trade": update, "event": event})
        return {
            "ok": True,
            "status": "manual_tests_excluded",
            "symbol": clean_symbol,
            "updated": len(updates),
            "updates": updates,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def exclude_old_proxy(self, *, symbol: str = "") -> dict[str, Any]:
        clean_symbol = _symbol(symbol)
        updates = []
        for trade in self.trades(clean_symbol or None, limit=1000):
            if not _should_exclude_old_proxy(trade):
                continue
            update = {
                **trade,
                "excluded_from_main_metrics": True,
                "excluded_reason": "old_proxy_or_manual_test",
                "normalized_symbol": "BTC_PROXY",
                "instrument_type": "crypto_etf_proxy",
                "is_spot_crypto": False,
                "underlying": "BTC",
                "updated_at": _now(),
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
            event = self.journal.save("mt5_shadow_trades", update["symbol"], update, confidence=update.get("confidence") or "media")
            updates.append({"trade": update, "event": event})
        return {
            "ok": True,
            "status": "old_proxy_metrics_excluded",
            "symbol": clean_symbol,
            "updated": len(updates),
            "updates": updates,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def snapshot(self, symbol: str | None = None, limit: int = 100) -> dict[str, Any]:
        clean_symbol = _symbol(symbol)
        raw_trades = self.trades(clean_symbol, limit=limit)
        excluded_trades = [trade for trade in raw_trades if not is_main_metric_trade(trade, query_symbol=clean_symbol)]
        trades = [trade for trade in raw_trades if is_main_metric_trade(trade, query_symbol=clean_symbol)]
        open_trades = [trade for trade in trades if trade.get("status") == "open"]
        closed_trades = [trade for trade in trades if trade.get("status") in {"win", "loss"}]
        return {
            "ok": True,
            "status": "mt5_shadow_trades_ready",
            "symbol": clean_symbol,
            "normalized_symbol": _normalized_symbol(clean_symbol),
            "symbol_aliases": sorted(_symbol_aliases(clean_symbol)) if clean_symbol else [],
            "items": trades,
            "open_trades": open_trades,
            "closed_trades": closed_trades,
            "excluded_trades": excluded_trades,
            "excluded_count": len(excluded_trades),
            "count": len(trades),
            "open": len(open_trades),
            "closed": len(closed_trades),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def trades(self, symbol: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.memory.get_mt5_events("mt5_shadow_trades", _symbol(symbol), limit=limit)
        latest: dict[str, dict[str, Any]] = {}
        for row in rows:
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            trade_id = str(payload.get("shadow_trade_id") or "")
            if trade_id and trade_id not in latest:
                latest[trade_id] = _normalize_trade_for_read({**payload, "created_at": row.get("created_at") or payload.get("created_at")})
        return list(latest.values())

    def _evaluate_trade(self, trade: dict[str, Any], tick: dict[str, Any], last: float) -> dict[str, Any]:
        action = _action(trade)
        entry = _number(trade.get("entry")) or last
        stop = _number(trade.get("stop_loss"))
        target = _number(trade.get("take_profit"))
        pnl = _directional_pnl(action, entry, last)
        risk = abs(entry - stop) if stop is not None else 0.0
        r_multiple = round(pnl / risk, 6) if risk > 0 else 0.0
        mfe = max(_number(trade.get("max_favorable_excursion")) or 0.0, pnl)
        mae = min(_number(trade.get("max_adverse_excursion")) or 0.0, pnl)
        status = "open"
        exit_price = None
        outcome = ""
        if action == "BUY" and target is not None and last >= target:
            status, exit_price, outcome = "win", target, "take_profit"
        elif action == "BUY" and stop is not None and last <= stop:
            status, exit_price, outcome = "loss", stop, "stop_loss"
        elif action == "SELL" and target is not None and last <= target:
            status, exit_price, outcome = "win", target, "take_profit"
        elif action == "SELL" and stop is not None and last >= stop:
            status, exit_price, outcome = "loss", stop, "stop_loss"

        if exit_price is not None:
            pnl = _directional_pnl(action, entry, exit_price)
            r_multiple = round(pnl / risk, 6) if risk > 0 else 0.0

        update = {
            **trade,
            "status": status,
            "last_price": last,
            "updated_at": str(tick.get("timestamp") or tick.get("bar_time") or _now()),
            "unrealized_pnl": round(pnl, 8),
            "max_favorable_excursion": round(mfe, 8),
            "max_adverse_excursion": round(mae, 8),
            "r_multiple": r_multiple,
            "pnl": round(pnl, 8) if status in {"win", "loss"} else 0.0,
            "pnl_pct": round((pnl / entry) * 100, 6) if entry else 0.0,
            "exit_price": exit_price,
            "exit_reason": outcome,
            "closed_at": str(tick.get("timestamp") or tick.get("bar_time") or _now()) if status in {"win", "loss"} else "",
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }
        return update

    def _update_no_trade_outcomes(self, symbol: str, tick: dict[str, Any], last: float) -> list[dict[str, Any]]:
        updates: list[dict[str, Any]] = []
        for row in self.memory.get_mt5_events("mt5_no_trade_outcomes", symbol, limit=50):
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            if payload.get("status") not in {"pending", ""}:
                continue
            reference = _number(payload.get("reference_price"))
            if reference is None or reference <= 0:
                continue
            threshold = _noise_threshold(reference, tick)
            move = last - reference
            move_pct = (move / reference) * 100
            if abs(move) <= threshold:
                outcome = "correct_sideways"
                correct = True
            elif move < -threshold:
                outcome = "protected_loss"
                correct = True
            else:
                outcome = "missed_opportunity"
                correct = False
            update = {
                **payload,
                "status": "evaluated",
                "outcome": outcome,
                "correct": correct,
                "reference_price": reference,
                "evaluated_price": last,
                "move_pct": round(move_pct, 6),
                "updated_at": str(tick.get("timestamp") or tick.get("bar_time") or _now()),
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
            event = self.journal.save("mt5_no_trade_outcomes", symbol, update)
            updates.append({"outcome": update, "event": event})
        return updates

    def _update_hedge_outcomes(self, symbol: str, tick: dict[str, Any], last: float) -> list[dict[str, Any]]:
        updates: list[dict[str, Any]] = []
        for row in self.memory.get_mt5_events("mt5_hedge_outcomes", symbol, limit=50):
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            if payload.get("status") not in {"pending", ""}:
                continue
            reference = _number(payload.get("reference_price"))
            if reference is None or reference <= 0:
                continue
            threshold = _noise_threshold(reference, tick)
            move = last - reference
            move_pct = (move / reference) * 100
            if move < -threshold:
                outcome = "hedge_correct"
                correct = True
            elif move > threshold:
                outcome = "hedge_false_alarm"
                correct = False
            else:
                outcome = "hedge_watch"
                correct = True
            update = {
                **payload,
                "status": "evaluated",
                "outcome": outcome,
                "correct": correct,
                "reference_price": reference,
                "evaluated_price": last,
                "move_pct": round(move_pct, 6),
                "updated_at": str(tick.get("timestamp") or tick.get("bar_time") or _now()),
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
            event = self.journal.save("mt5_hedge_outcomes", symbol, update)
            updates.append({"outcome": update, "event": event})
        return updates


def _action(payload: dict[str, Any]) -> str:
    return str(payload.get("action") or payload.get("decision") or "WAIT").upper().strip()


def _symbol(value: object) -> str:
    return str(value or "").upper().strip()


def _normalized_symbol(value: object) -> str:
    return normalize_mt5_symbol(value)


def _symbol_aliases(value: object) -> set[str]:
    return symbol_aliases(value)


def _should_exclude_old_proxy(trade: dict[str, Any]) -> bool:
    source = str(trade.get("source") or "").casefold()
    description = str(trade.get("symbol_description") or trade.get("description") or trade.get("instrument_warning") or "").casefold()
    original = _symbol(trade.get("original_symbol") or trade.get("symbol"))
    entry = _number(trade.get("entry")) or 0.0
    last_price = _number(trade.get("last_price")) or 0.0
    exit_price = _number(trade.get("exit_price")) or 0.0
    if str(trade.get("excluded_reason") or "") == "old_proxy_or_manual_test":
        return True
    if bool(trade.get("manual_test")):
        return True
    if source in {"manual_shadow_test", "manual_tick_test"}:
        return True
    if original == "BTC" and 20 <= entry <= 100:
        return True
    if any(token in description for token in ("grayscale", "trust", "etf", "fund", "mini trust")):
        return True
    if last_price > 1000 and 0 < exit_price < 100:
        return True
    return False


def is_main_metric_trade(trade: dict[str, Any], *, query_symbol: str = "") -> bool:
    query_normalized = _normalized_symbol(query_symbol) if query_symbol else ""
    strict_btcusd = query_normalized == "BTCUSD"
    trade_normalized = _trade_normalized_symbol(trade)
    source = str(trade.get("source") or "").casefold()
    instrument_type = str(trade.get("instrument_type") or "").casefold()
    if bool(trade.get("excluded_from_main_metrics")):
        return False
    if str(trade.get("excluded_reason") or "").strip():
        return False
    if strict_btcusd and bool(trade.get("manual_test")):
        return False
    if strict_btcusd and source.startswith("manual_"):
        return False
    if _is_legacy_proxy_trade(trade):
        return False
    if strict_btcusd and instrument_type in {"crypto_etf_proxy", "legacy_proxy"}:
        return False
    if query_normalized and trade_normalized and trade_normalized != query_normalized:
        return False
    return True


def _normalize_trade_for_read(trade: dict[str, Any]) -> dict[str, Any]:
    if not _is_legacy_proxy_trade(trade):
        return trade
    return {
        **trade,
        "excluded_from_main_metrics": True,
        "excluded_reason": str(trade.get("excluded_reason") or "old_proxy_or_manual_test"),
        "normalized_symbol": "BTC_PROXY",
        "instrument_type": "legacy_proxy",
        "is_spot_crypto": False,
        "underlying": "BTC",
    }


def _is_legacy_proxy_trade(trade: dict[str, Any]) -> bool:
    last_price = _number(trade.get("last_price")) or 0.0
    exit_price = _number(trade.get("exit_price")) or 0.0
    original = _symbol(trade.get("original_symbol") or trade.get("symbol"))
    source = str(trade.get("source") or "").casefold()
    if str(trade.get("excluded_reason") or "") == "old_proxy_or_manual_test":
        return True
    if original == "BTC" and 0 < exit_price < 100 and last_price > 1000:
        return True
    return last_price > 1000 and 0 < exit_price < 100


def _trade_normalized_symbol(trade: dict[str, Any]) -> str:
    stored = _symbol(trade.get("normalized_symbol"))
    if stored:
        return stored
    return _normalized_symbol(trade.get("symbol") or trade.get("original_symbol"))


def _number(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _price_from_tick(tick: dict[str, Any]) -> float | None:
    last = _number(tick.get("last") or tick.get("price"))
    if last is not None:
        return last
    bid = _number(tick.get("bid"))
    ask = _number(tick.get("ask"))
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    return bid if bid is not None else ask


def _directional_pnl(action: str, entry: float, price: float) -> float:
    if action == "SELL":
        return entry - price
    return price - entry


def _risk_reward(action: str, entry: float, stop: float, target: float) -> float | None:
    risk = abs(entry - stop)
    reward = abs(target - entry)
    if risk <= 0 or reward <= 0:
        return None
    if action == "BUY" and not (stop < entry < target):
        return None
    if action == "SELL" and not (target < entry < stop):
        return None
    return round(reward / risk, 4)


def _is_auto_forward_trade(trade: dict[str, Any]) -> bool:
    return bool(trade.get("auto_forward")) or str(trade.get("source") or "").casefold() == "mt5_auto_forward"


def _noise_threshold(reference: float, tick: dict[str, Any]) -> float:
    spread = abs(_number(tick.get("spread")) or 0.0)
    pct_threshold = reference * 0.001
    return max(spread, pct_threshold)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

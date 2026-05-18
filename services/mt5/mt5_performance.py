from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.genesis.memory_store import MemoryStore
from services.mt5.instrument_resolver import normalize_mt5_symbol
from services.mt5.mt5_journal import MT5Journal
from services.mt5.mt5_shadow_trading import MT5ShadowTrading


class MT5Performance:
    def __init__(self, *, memory: MemoryStore | None = None) -> None:
        self.memory = memory or MemoryStore()
        self.journal = MT5Journal(memory=self.memory)
        self.shadow = MT5ShadowTrading(memory=self.memory)

    def report(self, *, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
        clean_symbol = _symbol(symbol)
        clean_timeframe = str(timeframe or "").upper().strip()
        trades = [
            trade
            for trade in self.shadow.trades(clean_symbol)
            if not clean_timeframe or str(trade.get("timeframe") or "").upper() == clean_timeframe
        ]
        signals = self._events("mt5_signals", clean_symbol, 200)
        decisions = self._events("mt5_decisions", clean_symbol, 200)
        order_requests = self._events("mt5_order_requests", clean_symbol, 200)
        risk_blocks = self._events("mt5_risk_blocks", clean_symbol, 25)
        latest_decision = _latest_payload(decisions)
        no_trade = self._latest_outcomes("mt5_no_trade_outcomes", clean_symbol)
        hedge = self._latest_outcomes("mt5_hedge_outcomes", clean_symbol)
        replay = self._latest_replay(clean_symbol)
        strict_auto_trades = [
            trade
            for trade in trades
            if _is_strict_auto_trade(trade) and not bool(trade.get("manual_test")) and not _is_excluded(trade)
        ]
        exploration_trades = [
            trade
            for trade in trades
            if _is_exploration_trade(trade) and not bool(trade.get("manual_test")) and not _is_excluded(trade)
        ]
        forward_auto_trades = strict_auto_trades + exploration_trades
        auto_trades = strict_auto_trades
        manual_trades = [trade for trade in trades if not _is_auto_forward_trade(trade) or bool(trade.get("manual_test"))]
        included_trades = [trade for trade in trades if not _is_excluded(trade)]
        excluded_manual = [trade for trade in manual_trades if _is_excluded(trade)]
        open_auto_trades = [trade for trade in auto_trades if trade.get("status") == "open"]
        summary_total = _summary_for_trades(included_trades)
        summary_auto = _summary_for_trades(auto_trades)
        summary_exploration = _summary_for_trades(exploration_trades)
        summary_forward_auto = _summary_for_trades(forward_auto_trades)
        summary_manual = {**_summary_for_trades(manual_trades), "excluded_from_main_metrics": len(excluded_manual)}
        normalized_symbol = normalize_mt5_symbol(clean_symbol)
        proxy_summary = _summary_for_trades([trade for trade in self.shadow.trades("BTC_PROXY") if not _is_excluded(trade)])
        last_valid_decision = _latest_payload_by_actionable(decisions, True)
        last_invalid_decision = _latest_payload_by_actionable(decisions, False)
        last_block_reason = _reason_alias((last_invalid_decision or {}).get("reason") or "")
        current_state_reason = "active_open_trade" if open_auto_trades else ""
        last_reason = current_state_reason or _reason_alias((latest_decision or {}).get("reason") or "")
        instrument_type = "crypto_spot" if normalized_symbol == "BTCUSD" else "crypto_etf_proxy" if normalized_symbol == "BTC_PROXY" else ""
        summary_auto_payload = {**summary_auto, "symbol": clean_symbol, "normalized_symbol": normalized_symbol, "instrument_type": instrument_type, "sample_warning": _sample_warning(summary_auto)}
        summary = {
            **summary_auto,
            "symbol": clean_symbol,
            "normalized_symbol": normalized_symbol,
            "instrument_type": instrument_type,
            "total_signals": len(signals) + len(decisions) + len(order_requests),
            "actionable_signals": sum(1 for row in signals + decisions + order_requests if _action(row) in {"BUY", "SELL"}),
            "manual_shadow_trades": summary_manual["shadow_trades"],
            "auto_shadow_trades": summary_auto["shadow_trades"],
            "strict_shadow_trades": summary_auto["shadow_trades"],
            "exploration_shadow_trades": summary_exploration["shadow_trades"],
            "forward_auto_shadow_trades": summary_forward_auto["shadow_trades"],
            "total_shadow_trades": summary_total["shadow_trades"],
            "sample_warning": _sample_warning(summary_auto),
        }
        no_trade_metrics = _binary_accuracy(no_trade, correct_outcomes={"correct_sideways", "protected_loss"})
        hedge_metrics = _binary_accuracy(hedge, correct_outcomes={"hedge_correct", "hedge_watch"})
        payload = {
            "ok": True,
            "status": "mt5_performance_ready",
            "symbol": clean_symbol,
            "normalized_symbol": normalized_symbol,
            "timeframe": clean_timeframe,
            "summary": summary,
            "summary_btcusd_auto": summary_auto_payload if normalized_symbol == "BTCUSD" else {},
            "summary_total": {**summary_total, "manual_shadow_trades": summary_manual["shadow_trades"], "auto_shadow_trades": summary_auto["shadow_trades"]},
            "summary_auto": summary_auto_payload,
            "summary_strict_auto": summary_auto_payload,
            "summary_exploration": {
                **summary_exploration,
                "symbol": clean_symbol,
                "normalized_symbol": normalized_symbol,
                "instrument_type": instrument_type,
                "profile": "BTCUSD_PAPER_EXPLORATION_V1",
                "paper_exploration": True,
            },
            "summary_forward_auto": {
                **summary_forward_auto,
                "symbol": clean_symbol,
                "normalized_symbol": normalized_symbol,
                "instrument_type": instrument_type,
                "journal_only": True,
            },
            "summary_manual": summary_manual,
            "summary_proxy": proxy_summary,
            "summary_replay": replay,
            "auto_sample_warning": _sample_warning(summary_auto),
            "by_action": self._by_action(included_trades, signals, decisions, order_requests, no_trade, hedge),
            "by_timeframe": _group_trades(included_trades, "timeframe"),
            "by_strategy": _group_trades(included_trades, "strategy_profile"),
            "no_trade_accuracy": no_trade_metrics,
            "missed_opportunity_count": no_trade_metrics["missed_opportunity_count"],
            "protected_loss_count": no_trade_metrics["protected_loss_count"],
            "hedge_accuracy": hedge_metrics,
            "recent_trades": sorted(included_trades, key=lambda item: str(item.get("updated_at") or item.get("opened_at") or ""), reverse=True)[:10],
            "recent_auto_trades": sorted(auto_trades, key=lambda item: str(item.get("updated_at") or item.get("opened_at") or ""), reverse=True)[:10],
            "recent_exploration_trades": sorted(exploration_trades, key=lambda item: str(item.get("updated_at") or item.get("opened_at") or ""), reverse=True)[:10],
            "recent_manual_trades": sorted(manual_trades, key=lambda item: str(item.get("updated_at") or item.get("opened_at") or ""), reverse=True)[:10],
            "risk_blocks": [_journal_item(row) for row in risk_blocks],
            "last_decision": latest_decision,
            "last_valid_decision": last_valid_decision,
            "last_invalid_decision": last_invalid_decision,
            "last_block_reason": last_block_reason,
            "current_state_reason": current_state_reason,
            "last_reason": last_reason,
            "genesis_reading": _reading(clean_symbol, summary_auto, no_trade_metrics, hedge_metrics, current_state_reason),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
            "updated_at": _now(),
        }
        self.journal.save("mt5_forward_metrics", clean_symbol or "MT5", payload)
        return payload

    def auto_report(self, *, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
        report = self.report(symbol=symbol, timeframe=timeframe)
        summary_auto = {
            **report["summary_auto"],
            "auto_shadow_trades": report["summary_auto"].get("shadow_trades", 0),
            "drawdown": report["summary_auto"].get("max_drawdown", 0.0),
        }
        return {
            "ok": True,
            "status": "mt5_auto_performance_ready",
            "symbol": report["symbol"],
            "normalized_symbol": report.get("normalized_symbol") or "",
            "instrument_type": "crypto_spot" if report.get("normalized_symbol") == "BTCUSD" else "",
            "timeframe": report["timeframe"],
            "summary": summary_auto,
            "summary_auto": summary_auto,
            "auto_shadow_trades": summary_auto["auto_shadow_trades"],
            "closed": summary_auto.get("closed", 0),
            "open": summary_auto.get("open", 0),
            "wins": summary_auto.get("wins", 0),
            "losses": summary_auto.get("losses", 0),
            "win_rate": summary_auto.get("win_rate", 0.0),
            "profit_factor": summary_auto.get("profit_factor", 0.0),
            "expectancy": summary_auto.get("expectancy", 0.0),
            "net_pnl": summary_auto.get("net_pnl", 0.0),
            "drawdown": summary_auto.get("drawdown", 0.0),
            "recent_trades": report.get("recent_auto_trades") or [],
            "recent_auto_trades": report.get("recent_auto_trades") or [],
            "sample_warning": report.get("auto_sample_warning") or "",
            "genesis_reading": report.get("genesis_reading") or "",
            "last_reason": report.get("last_reason") or "",
            "last_valid_decision": report.get("last_valid_decision"),
            "last_invalid_decision": report.get("last_invalid_decision"),
            "last_block_reason": report.get("last_block_reason") or "",
            "current_state_reason": report.get("current_state_reason") or "",
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
            "updated_at": _now(),
        }

    def outcomes_recent(self, *, symbol: str = "", limit: int = 25) -> dict[str, Any]:
        clean_symbol = _symbol(symbol)
        safe_limit = max(1, min(int(limit or 25), 100))
        rows: list[dict[str, Any]] = []
        for collection in ("mt5_signal_outcomes", "mt5_no_trade_outcomes", "mt5_hedge_outcomes"):
            rows.extend(self.memory.get_mt5_events(collection, clean_symbol or None, limit=safe_limit))
        rows.sort(key=lambda row: str(row.get("created_at") or ""), reverse=True)
        items = [_journal_item(row) for row in rows[:safe_limit]]
        return {
            "ok": True,
            "status": "mt5_outcomes_ready",
            "items": items,
            "count": len(items),
            "symbol": clean_symbol,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def _events(self, collection: str, symbol: str, limit: int) -> list[dict[str, Any]]:
        return self.memory.get_mt5_events(collection, symbol or None, limit=limit)

    def _latest_outcomes(self, collection: str, symbol: str) -> list[dict[str, Any]]:
        rows = self.memory.get_mt5_events(collection, symbol or None, limit=200)
        latest: dict[str, dict[str, Any]] = {}
        for row in rows:
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            outcome_id = str(payload.get("outcome_id") or payload.get("shadow_trade_id") or row.get("created_at") or "")
            if outcome_id and outcome_id not in latest:
                latest[outcome_id] = payload
        return list(latest.values())

    def _latest_replay(self, symbol: str) -> dict[str, Any]:
        rows = self.memory.get_mt5_events("mt5_replay_runs", symbol or None, limit=1)
        payload = rows[0].get("payload") if rows and isinstance(rows[0].get("payload"), dict) else {}
        if not payload:
            return {"replay_trades": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "profit_factor": 0.0, "expectancy": 0.0, "max_drawdown": 0.0}
        return {
            "replay_trades": int(payload.get("replay_trades") or 0),
            "wins": int(payload.get("wins") or 0),
            "losses": int(payload.get("losses") or 0),
            "win_rate": _number(payload.get("win_rate")) or 0.0,
            "profit_factor": _number(payload.get("profit_factor")) or 0.0,
            "expectancy": _number(payload.get("expectancy")) or 0.0,
            "max_drawdown": _number(payload.get("max_drawdown")) or 0.0,
            "blocked_reasons": payload.get("blocked_reasons") if isinstance(payload.get("blocked_reasons"), list) else [],
        }

    def _by_action(
        self,
        trades: list[dict[str, Any]],
        signals: list[dict[str, Any]],
        decisions: list[dict[str, Any]],
        order_requests: list[dict[str, Any]],
        no_trade: list[dict[str, Any]],
        hedge: list[dict[str, Any]],
    ) -> dict[str, Any]:
        output = {
            "BUY": _trade_bucket([trade for trade in trades if str(trade.get("action") or "").upper() == "BUY"]),
            "SELL": _trade_bucket([trade for trade in trades if str(trade.get("action") or "").upper() == "SELL"]),
            "NO_TRADE": _outcome_bucket(no_trade, correct_outcomes={"correct_sideways", "protected_loss"}),
            "HEDGE": _outcome_bucket(hedge, correct_outcomes={"hedge_correct", "hedge_watch"}),
        }
        output["WAIT"] = {"signals": sum(1 for row in signals + decisions + order_requests if _action(row) == "WAIT")}
        return output


def _trade_bucket(trades: list[dict[str, Any]]) -> dict[str, Any]:
    summary = _summary_for_trades(trades)
    return {
        "trades": summary["shadow_trades"],
        "closed": summary["closed"],
        "wins": summary["wins"],
        "losses": summary["losses"],
        "win_rate": summary["win_rate"],
        "profit_factor": summary["profit_factor"],
        "net_pnl": summary["net_pnl"],
    }


def _summary_for_trades(trades: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [trade for trade in trades if trade.get("status") in {"win", "loss"}]
    wins = [trade for trade in closed if trade.get("status") == "win"]
    pnls = [_pnl_value(trade) for trade in closed]
    win_values = [value for value in pnls if value > 0]
    loss_values = [value for value in pnls if value < 0]
    gross_win = sum(win_values)
    gross_loss = abs(sum(loss_values))
    return {
        "shadow_trades": len(trades),
        "closed": len(closed),
        "open": sum(1 for trade in trades if trade.get("status") == "open"),
        "wins": len(wins),
        "losses": len(closed) - len(wins),
        "win_rate": round((len(wins) / len(closed)) * 100, 2) if closed else 0.0,
        "profit_factor": _profit_factor(gross_win, gross_loss),
        "expectancy": round(sum(pnls) / len(closed), 4) if closed else 0.0,
        "net_pnl": round(sum(pnls), 4),
        "max_drawdown": _max_drawdown(closed),
        "avg_win": round(sum(win_values) / len(win_values), 4) if win_values else 0.0,
        "avg_loss": round(sum(loss_values) / len(loss_values), 4) if loss_values else 0.0,
        "rr_avg": round(sum(abs(_number(trade.get("r_multiple")) or 0.0) for trade in closed) / len(closed), 4) if closed else 0.0,
    }


def _group_trades(trades: list[dict[str, Any]], key: str) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        group_key = str(trade.get(key) or "unknown")
        groups.setdefault(group_key, []).append(trade)
    return {group_key: _trade_bucket(items) for group_key, items in groups.items()}


def _outcome_bucket(outcomes: list[dict[str, Any]], *, correct_outcomes: set[str]) -> dict[str, Any]:
    metrics = _binary_accuracy(outcomes, correct_outcomes=correct_outcomes)
    return {
        "signals": metrics["total"],
        "evaluated": metrics["evaluated"],
        "accuracy": metrics["accuracy"],
        "correct": metrics["correct"],
        "incorrect": metrics["incorrect"],
    }


def _binary_accuracy(outcomes: list[dict[str, Any]], *, correct_outcomes: set[str]) -> dict[str, Any]:
    evaluated = [item for item in outcomes if item.get("status") == "evaluated"]
    correct = [item for item in evaluated if item.get("outcome") in correct_outcomes or item.get("correct") is True]
    missed = [item for item in evaluated if item.get("outcome") == "missed_opportunity"]
    protected = [item for item in evaluated if item.get("outcome") == "protected_loss"]
    return {
        "total": len(outcomes),
        "evaluated": len(evaluated),
        "correct": len(correct),
        "incorrect": len(evaluated) - len(correct),
        "accuracy": round((len(correct) / len(evaluated)) * 100, 2) if evaluated else 0.0,
        "missed_opportunity_count": len(missed),
        "protected_loss_count": len(protected),
    }


def _journal_item(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
    return {
        "event_type": row.get("event_type") or payload.get("event_type") or "",
        "symbol": str(payload.get("symbol") or "").upper(),
        "decision": payload.get("decision") or payload.get("action") or payload.get("outcome") or payload.get("status") or "",
        "reason": _reason_alias(payload.get("reason") or payload.get("exit_reason") or ""),
        "broker_touched": False,
        "order_executed": False,
        "order_policy": payload.get("order_policy") or "journal_only_no_broker",
        "created_at": row.get("created_at") or payload.get("updated_at") or payload.get("created_at") or "",
        "payload": payload,
    }


def _pnl_value(trade: dict[str, Any]) -> float:
    r_multiple = _number(trade.get("r_multiple"))
    if r_multiple is not None:
        return r_multiple
    return _number(trade.get("pnl_pct")) or _number(trade.get("pnl")) or 0.0


def _is_auto_forward_trade(trade: dict[str, Any]) -> bool:
    return bool(trade.get("auto_forward")) or str(trade.get("source") or "").casefold() == "mt5_auto_forward"


def _is_strict_auto_trade(trade: dict[str, Any]) -> bool:
    source = str(trade.get("source") or "").casefold()
    return _is_auto_forward_trade(trade) and not _is_exploration_trade(trade) and source != "mt5_replay"


def _is_exploration_trade(trade: dict[str, Any]) -> bool:
    source = str(trade.get("source") or "").casefold()
    return bool(trade.get("paper_exploration")) or bool(trade.get("included_in_exploration_metrics")) or source == "mt5_auto_forward_exploration"


def _is_excluded(trade: dict[str, Any]) -> bool:
    if bool(trade.get("excluded_from_main_metrics")):
        return True
    last_price = _number(trade.get("last_price")) or 0.0
    exit_price = _number(trade.get("exit_price")) or 0.0
    return last_price > 1000 and 0 < exit_price < 100


def _sample_warning(summary: dict[str, Any]) -> str:
    if int(summary.get("shadow_trades") or 0) < 30:
        return "Muestra automatica insuficiente; no usar todavia para decidir rentabilidad."
    return ""


def _latest_payload(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    payload = rows[0].get("payload")
    if not isinstance(payload, dict):
        return None
    clean = dict(payload)
    clean["reason"] = _reason_alias(clean.get("reason") or "")
    action = str(clean.get("decision") or clean.get("action") or "").upper().strip()
    if action not in {"BUY", "SELL"} or not bool(clean.get("actionable")):
        clean["entry"] = None
        clean["stop_loss"] = None
        clean["take_profit"] = None
        clean["risk_reward"] = 0.0
    return clean


def _latest_payload_by_actionable(rows: list[dict[str, Any]], actionable: bool) -> dict[str, Any] | None:
    for row in rows:
        payload = _latest_payload([row])
        if not payload:
            continue
        if bool(payload.get("actionable")) is actionable:
            return payload
    return None


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


def _profit_factor(gross_win: float, gross_loss: float) -> float:
    if gross_win <= 0 and gross_loss <= 0:
        return 0.0
    if gross_loss <= 0:
        return round(gross_win, 4)
    return round(gross_win / gross_loss, 4)


def _max_drawdown(closed: list[dict[str, Any]]) -> float:
    ordered = sorted(closed, key=lambda trade: str(trade.get("closed_at") or trade.get("updated_at") or ""))
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for trade in ordered:
        equity += _pnl_value(trade)
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return round(max_dd, 4)


def _action(row: dict[str, Any]) -> str:
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else row
    return str(payload.get("action") or payload.get("decision") or "WAIT").upper().strip()


def _symbol(value: object) -> str:
    return str(value or "").upper().strip()


def _number(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _reading(symbol: str, summary: dict[str, Any], no_trade: dict[str, Any], hedge: dict[str, Any], current_state_reason: str = "") -> str:
    scope = symbol or "MT5"
    if current_state_reason == "active_open_trade":
        return f"{scope}: hay una operacion sombra abierta; Genesis espera cierre por TP/SL antes de abrir otra."
    if summary["shadow_trades"] == 0:
        return f"{scope}: forward test automatico listo, pero aun no hay trades automaticos {scope} suficientes para medir ventaja."
    warning = _sample_warning(summary)
    suffix = f" {warning}" if warning else ""
    return (
        f"{scope}: auto win rate {summary['win_rate']}%, auto PF {summary['profit_factor']}, "
        f"expectancy {summary['expectancy']}R. No-trade accuracy {no_trade['accuracy']}%, "
        f"hedge accuracy {hedge['accuracy']}%. Todo sigue journal-only.{suffix}"
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

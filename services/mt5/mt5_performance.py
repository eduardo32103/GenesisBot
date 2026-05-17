from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.genesis.memory_store import MemoryStore
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
        risk_blocks = self._events("mt5_risk_blocks", clean_symbol, 25)
        no_trade = self._latest_outcomes("mt5_no_trade_outcomes", clean_symbol)
        hedge = self._latest_outcomes("mt5_hedge_outcomes", clean_symbol)
        closed = [trade for trade in trades if trade.get("status") in {"win", "loss"}]
        wins = [trade for trade in closed if trade.get("status") == "win"]
        losses = [trade for trade in closed if trade.get("status") == "loss"]
        open_trades = [trade for trade in trades if trade.get("status") == "open"]
        pnls = [_pnl_value(trade) for trade in closed]
        win_values = [value for value in pnls if value > 0]
        loss_values = [value for value in pnls if value < 0]
        gross_win = sum(win_values)
        gross_loss = abs(sum(loss_values))
        profit_factor = _profit_factor(gross_win, gross_loss)
        summary = {
            "total_signals": len(signals) + len(decisions),
            "actionable_signals": sum(1 for row in signals + decisions if _action(row) in {"BUY", "SELL"}),
            "shadow_trades": len(trades),
            "wins": len(wins),
            "losses": len(losses),
            "open": len(open_trades),
            "win_rate": round((len(wins) / len(closed)) * 100, 2) if closed else 0.0,
            "profit_factor": profit_factor,
            "expectancy": round(sum(pnls) / len(closed), 4) if closed else 0.0,
            "net_pnl": round(sum(pnls), 4),
            "max_drawdown": _max_drawdown(closed),
            "avg_win": round(sum(win_values) / len(win_values), 4) if win_values else 0.0,
            "avg_loss": round(sum(loss_values) / len(loss_values), 4) if loss_values else 0.0,
            "rr_avg": round(sum(abs(_number(trade.get("r_multiple")) or 0.0) for trade in closed) / len(closed), 4) if closed else 0.0,
        }
        no_trade_metrics = _binary_accuracy(no_trade, correct_outcomes={"correct_sideways", "protected_loss"})
        hedge_metrics = _binary_accuracy(hedge, correct_outcomes={"hedge_correct", "hedge_watch"})
        payload = {
            "ok": True,
            "status": "mt5_performance_ready",
            "symbol": clean_symbol,
            "timeframe": clean_timeframe,
            "summary": summary,
            "by_action": self._by_action(trades, signals, decisions, no_trade, hedge),
            "by_timeframe": _group_trades(trades, "timeframe"),
            "by_strategy": _group_trades(trades, "strategy_profile"),
            "no_trade_accuracy": no_trade_metrics,
            "missed_opportunity_count": no_trade_metrics["missed_opportunity_count"],
            "protected_loss_count": no_trade_metrics["protected_loss_count"],
            "hedge_accuracy": hedge_metrics,
            "recent_trades": sorted(trades, key=lambda item: str(item.get("updated_at") or item.get("opened_at") or ""), reverse=True)[:10],
            "risk_blocks": [_journal_item(row) for row in risk_blocks],
            "genesis_reading": _reading(clean_symbol, summary, no_trade_metrics, hedge_metrics),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
            "updated_at": _now(),
        }
        self.journal.save("mt5_forward_metrics", clean_symbol or "MT5", payload)
        return payload

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

    def _by_action(
        self,
        trades: list[dict[str, Any]],
        signals: list[dict[str, Any]],
        decisions: list[dict[str, Any]],
        no_trade: list[dict[str, Any]],
        hedge: list[dict[str, Any]],
    ) -> dict[str, Any]:
        output = {
            "BUY": _trade_bucket([trade for trade in trades if str(trade.get("action") or "").upper() == "BUY"]),
            "SELL": _trade_bucket([trade for trade in trades if str(trade.get("action") or "").upper() == "SELL"]),
            "NO_TRADE": _outcome_bucket(no_trade, correct_outcomes={"correct_sideways", "protected_loss"}),
            "HEDGE": _outcome_bucket(hedge, correct_outcomes={"hedge_correct", "hedge_watch"}),
        }
        output["WAIT"] = {"signals": sum(1 for row in signals + decisions if _action(row) == "WAIT")}
        return output


def _trade_bucket(trades: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [trade for trade in trades if trade.get("status") in {"win", "loss"}]
    wins = [trade for trade in closed if trade.get("status") == "win"]
    pnls = [_pnl_value(trade) for trade in closed]
    gross_win = sum(value for value in pnls if value > 0)
    gross_loss = abs(sum(value for value in pnls if value < 0))
    return {
        "trades": len(trades),
        "closed": len(closed),
        "wins": len(wins),
        "losses": len(closed) - len(wins),
        "win_rate": round((len(wins) / len(closed)) * 100, 2) if closed else 0.0,
        "profit_factor": _profit_factor(gross_win, gross_loss),
        "net_pnl": round(sum(pnls), 4),
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
        "reason": payload.get("reason") or payload.get("exit_reason") or "",
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


def _reading(symbol: str, summary: dict[str, Any], no_trade: dict[str, Any], hedge: dict[str, Any]) -> str:
    scope = symbol or "MT5"
    if summary["shadow_trades"] == 0:
        return f"{scope}: forward test listo, pero aun no hay shadow trades cerrados para medir ventaja."
    return (
        f"{scope}: win rate {summary['win_rate']}%, PF {summary['profit_factor']}, "
        f"expectancy {summary['expectancy']}R. No-trade accuracy {no_trade['accuracy']}%, "
        f"hedge accuracy {hedge['accuracy']}%. Todo sigue journal-only."
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

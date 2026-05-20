from __future__ import annotations

import csv
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import quote
from urllib.request import urlopen

from services.genesis.memory_store import MemoryStore
from services.mt5.instrument_resolver import normalize_mt5_symbol
from services.mt5.mt5_config import MT5RuntimeConfig, get_mt5_config


_LATEST_LOCK = Lock()
_LATEST_RESULTS: dict[str, dict[str, Any]] = {}


@dataclass(frozen=True)
class BacktestSettings:
    symbol: str
    normalized_symbol: str
    timeframe: str
    source: str
    initial_balance: float
    spread_points: float
    slippage_points: float
    commission: float
    point: float
    min_rr: float
    risk_pct: float
    max_spread_points: float
    min_score: float
    time_stop_bars: int
    max_bars: int
    timeout_seconds: float
    profile: str
    save_results: bool


class MT5Backtester:
    """Cold-path BTCUSD paper backtester. It never touches broker execution."""

    def __init__(self, *, memory: MemoryStore | None = None, config: MT5RuntimeConfig | None = None) -> None:
        self.memory = memory
        self.config = config or get_mt5_config()

    def run(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        started = time.monotonic()
        body = payload or {}
        settings = _settings(body, self.config)
        warnings: list[str] = []
        errors: list[str] = []
        try:
            bars, load_warnings = _load_bars(body, settings)
            warnings.extend(load_warnings)
            if not bars:
                result = _empty_result(settings, started, warnings + ["historical_data_not_available"])
                _store_latest(settings.normalized_symbol, result)
                return result
            bars = bars[: settings.max_bars]
            if body.get("walk_forward"):
                result = self._run_walk_forward(settings, bars, started, warnings, body)
            else:
                trades, no_trade_count, blocked = _simulate(settings, bars, started)
                summary = _metrics(trades, initial_balance=settings.initial_balance)
                result = _result_payload(settings, bars, trades, summary, no_trade_count, blocked, started, warnings)
            _store_latest(settings.normalized_symbol, result)
            if settings.save_results and self.memory is not None:
                try:
                    self.memory.save_mt5_event("mt5_backtest_runs", settings.symbol, result, "mt5_backtester", "media")
                    result["saved"] = True
                except Exception as exc:
                    result["saved"] = False
                    result.setdefault("warnings", []).append(f"save_results_failed:{str(exc)[:160]}")
            return result
        except Exception as exc:
            errors.append(str(exc)[:500])
            result = _empty_result(settings, started, warnings, errors=errors, status="mt5_backtest_error", ok=False)
            _store_latest(settings.normalized_symbol, result)
            return result

    def latest(self, *, symbol: str = "") -> dict[str, Any]:
        clean_symbol = str(symbol or "BTCUSD").upper().strip()
        normalized = normalize_mt5_symbol(clean_symbol) or clean_symbol
        with _LATEST_LOCK:
            latest = dict(_LATEST_RESULTS.get(normalized) or {})
        if latest:
            return {
                "ok": True,
                "status": "mt5_backtest_latest_ready",
                "symbol": clean_symbol,
                "normalized_symbol": normalized,
                "result": latest,
                **_safety(),
            }
        return {
            "ok": True,
            "status": "mt5_backtest_latest_empty",
            "symbol": clean_symbol,
            "normalized_symbol": normalized,
            "result": None,
            "genesis_reading": "Aun no hay backtest historico en memoria para este simbolo.",
            **_safety(),
        }

    def _run_walk_forward(
        self,
        settings: BacktestSettings,
        bars: list[dict[str, Any]],
        started: float,
        warnings: list[str],
        body: dict[str, Any],
    ) -> dict[str, Any]:
        train_months = max(1, int(_number(body.get("train_months")) or 3))
        test_months = max(1, int(_number(body.get("test_months")) or 1))
        bars_per_month = _rough_bars_per_month(settings.timeframe)
        train_size = max(5, train_months * bars_per_month)
        test_size = max(5, test_months * bars_per_month)
        periods: list[dict[str, Any]] = []
        combined_trades: list[dict[str, Any]] = []
        no_trade_count = 0
        blocked: list[str] = []
        index = train_size
        period_index = 1
        while index < len(bars) and not _timed_out(started, settings.timeout_seconds):
            test_bars = bars[index : index + test_size]
            if len(test_bars) < 2:
                break
            period_settings = settings
            trades, period_no_trade, period_blocked = _simulate(period_settings, test_bars, started, prefix=f"wf{period_index}")
            period_summary = _metrics(trades, initial_balance=settings.initial_balance)
            periods.append(
                {
                    "period": period_index,
                    "train_bars": min(train_size, index),
                    "test_bars": len(test_bars),
                    "from": test_bars[0].get("time") or "",
                    "to": test_bars[-1].get("time") or "",
                    "summary": period_summary,
                    "trades": len(trades),
                }
            )
            combined_trades.extend(trades)
            no_trade_count += period_no_trade
            blocked.extend(period_blocked)
            index += test_size
            period_index += 1
        summary = _metrics(combined_trades, initial_balance=settings.initial_balance)
        result = _result_payload(settings, bars, combined_trades, summary, no_trade_count, blocked, started, warnings)
        result["walk_forward"] = True
        result["walk_forward_results"] = periods
        result["train_months"] = train_months
        result["test_months"] = test_months
        return result


def _settings(body: dict[str, Any], config: MT5RuntimeConfig) -> BacktestSettings:
    symbol = str(body.get("symbol") or "BTCUSD").upper().strip()
    normalized = normalize_mt5_symbol(symbol) or symbol
    timeframe = str(body.get("timeframe") or "H1").upper().strip()
    max_bars = int(_number(body.get("max_bars") or body.get("bars")) or _number(os.getenv("MT5_BACKTEST_MAX_BARS")) or 2000)
    max_bars = max(10, min(max_bars, 10000))
    timeout_seconds = float(_number(body.get("timeout_seconds")) or _number(os.getenv("MT5_BACKTEST_TIMEOUT_SECONDS")) or 8.0)
    time_stop_min = float(_number(body.get("time_stop_min")) or config.paper_exploration_time_stop_min or 15.0)
    time_stop_bars = max(1, int((time_stop_min + _timeframe_minutes(timeframe) - 1) // _timeframe_minutes(timeframe)))
    return BacktestSettings(
        symbol=symbol,
        normalized_symbol=normalized,
        timeframe=timeframe,
        source=str(body.get("source") or "csv_or_fmp").strip(),
        initial_balance=float(_number(body.get("initial_balance")) or 100000.0),
        spread_points=float(_number(body.get("spread_points")) or 0.0),
        slippage_points=float(_number(body.get("slippage_points")) or 0.0),
        commission=float(_number(body.get("commission")) or 0.0),
        point=float(_number(body.get("point")) or 0.01),
        min_rr=max(1.0, float(_number(body.get("min_rr")) or config.paper_exploration_min_rr or 1.2)),
        risk_pct=float(_number(body.get("risk_pct")) or config.paper_exploration_risk_pct or 0.1),
        max_spread_points=float(_number(body.get("max_spread_points")) or config.paper_exploration_max_spread_points or 60.0),
        min_score=float(_number(body.get("min_score")) or config.paper_exploration_min_score or 45.0),
        time_stop_bars=time_stop_bars,
        max_bars=max_bars,
        timeout_seconds=max(1.0, min(timeout_seconds, 20.0)),
        profile=str(body.get("profile") or "BTCUSD_PAPER_EXPLORATION_V1").strip() or "BTCUSD_PAPER_EXPLORATION_V1",
        save_results=bool(body.get("save_results") is True),
    )


def _load_bars(body: dict[str, Any], settings: BacktestSettings) -> tuple[list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    direct = body.get("bars_data")
    if direct is None and isinstance(body.get("bars"), list):
        direct = body.get("bars")
    if direct is None:
        direct = body.get("candles") or body.get("history")
    if isinstance(direct, list):
        return _normalize_bars(direct), warnings
    csv_text = body.get("csv") or body.get("csv_text")
    if isinstance(csv_text, str) and csv_text.strip():
        return _bars_from_csv_text(csv_text), warnings
    csv_path = str(body.get("csv_path") or body.get("file") or "").strip()
    if csv_path:
        path = Path(csv_path).expanduser()
        if path.exists() and path.is_file():
            return _bars_from_csv_text(path.read_text(encoding="utf-8-sig")), warnings
        warnings.append("csv_path_not_found")
    if "fmp" in settings.source.casefold():
        fmp = _fetch_fmp_bars(settings, body)
        if fmp:
            return fmp, warnings
        warnings.append("fmp_historical_not_available")
    return [], warnings


def _bars_from_csv_text(csv_text: str) -> list[dict[str, Any]]:
    rows = list(csv.DictReader(StringIO(csv_text.strip())))
    return _normalize_bars(rows)


def _normalize_bars(rows: list[Any]) -> list[dict[str, Any]]:
    bars: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        lowered = {str(key).strip().casefold(): value for key, value in row.items()}
        close = _number(_pick(lowered, "close", "last", "price"))
        open_price = _number(_pick(lowered, "open", "open_price")) if _pick(lowered, "open", "open_price") is not None else close
        high = _number(_pick(lowered, "high")) if _pick(lowered, "high") is not None else close
        low = _number(_pick(lowered, "low")) if _pick(lowered, "low") is not None else close
        if close is None or open_price is None or high is None or low is None:
            continue
        bars.append(
            {
                "time": str(_pick(lowered, "time", "datetime", "date", "timestamp") or ""),
                "open": float(open_price),
                "high": float(max(high, open_price, close)),
                "low": float(min(low, open_price, close)),
                "close": float(close),
                "volume": _number(_pick(lowered, "volume")) or 0.0,
            }
        )
    return bars


def _fetch_fmp_bars(settings: BacktestSettings, body: dict[str, Any]) -> list[dict[str, Any]]:
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        return []
    from_date = str(body.get("from") or body.get("from_date") or "").strip()
    to_date = str(body.get("to") or body.get("to_date") or "").strip()
    fmp_symbol = "BTCUSD" if settings.normalized_symbol == "BTCUSD" else settings.symbol
    query = f"?apikey={quote(api_key)}"
    if from_date:
        query += f"&from={quote(from_date)}"
    if to_date:
        query += f"&to={quote(to_date)}"
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{quote(fmp_symbol)}{query}"
    try:
        with urlopen(url, timeout=8) as response:
            data = json.loads(response.read().decode("utf-8"))
    except Exception:
        return []
    historical = data.get("historical") if isinstance(data, dict) else []
    if not isinstance(historical, list):
        return []
    bars = _normalize_bars(list(reversed(historical)))
    return bars[-settings.max_bars :]


def _simulate(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    started: float,
    *,
    prefix: str = "bt",
) -> tuple[list[dict[str, Any]], int, list[str]]:
    trades: list[dict[str, Any]] = []
    blocked: list[str] = []
    no_trade_count = 0
    open_trade: dict[str, Any] | None = None
    cooldown_until = -1
    for index in range(1, len(bars)):
        if _timed_out(started, settings.timeout_seconds):
            blocked.append("timeout_guard")
            break
        bar = bars[index]
        if open_trade:
            open_trade, closed = _update_trade(settings, open_trade, bar, index)
            if closed:
                trades.append(closed)
                open_trade = None
        if index >= len(bars) - 1:
            continue
        if open_trade:
            continue
        if index < cooldown_until:
            no_trade_count += 1
            blocked.append("cooldown_active")
            continue
        history = bars[:index]
        decision = _decision_from_history(history, settings)
        if not decision["actionable"]:
            no_trade_count += 1
            blocked.append(str(decision.get("reason") or "no_edge"))
            continue
        open_trade = _open_trade(settings, decision, bar, index, f"{prefix}-{index}")
        if open_trade is None:
            no_trade_count += 1
            blocked.append("missing_risk_parameters")
            continue
        if _recent_loss_cluster(trades):
            cooldown_until = index + max(1, int(20 / _timeframe_minutes(settings.timeframe)))
    if open_trade:
        closed = _force_close(settings, open_trade, bars[-1], len(bars) - 1, "time_stop")
        trades.append(closed)
    return trades, no_trade_count, blocked


def _decision_from_history(history: list[dict[str, Any]], settings: BacktestSettings) -> dict[str, Any]:
    if len(history) < 3:
        return {"actionable": False, "reason": "insufficient_history"}
    closes = [float(row["close"]) for row in history if _number(row.get("close")) is not None]
    highs = [float(row["high"]) for row in history if _number(row.get("high")) is not None]
    lows = [float(row["low"]) for row in history if _number(row.get("low")) is not None]
    close = closes[-1]
    prev_close = closes[-2]
    ema20 = _ema(closes, min(20, len(closes)))
    ema50 = _ema(closes, min(50, len(closes)))
    rsi = _rsi(closes, min(14, max(2, len(closes) - 1)))
    momentum = close - closes[max(0, len(closes) - 4)]
    momentum_pct = (momentum / closes[max(0, len(closes) - 4)]) * 100 if closes[max(0, len(closes) - 4)] else 0.0
    trend_score = 60.0 if close >= ema20 else 40.0
    trend_score += 10.0 if ema20 >= ema50 else -10.0
    momentum_score = min(80.0, max(20.0, 50.0 + momentum_pct * 25.0))
    range_pct = ((max(highs[-10:] or [close]) - min(lows[-10:] or [close])) / close) * 100 if close else 0.0
    volatility_score = min(80.0, max(20.0, range_pct * 20.0))
    buy_score = round((trend_score + momentum_score + volatility_score) / 3, 2)
    sell_score = round(((100.0 - trend_score) + (100.0 - momentum_score) + volatility_score) / 3, 2)
    if settings.spread_points > settings.max_spread_points:
        return {"actionable": False, "reason": "spread_too_high", "score": max(buy_score, sell_score)}
    min_score = max(0.0, float(settings.min_score or 45.0))
    side = ""
    score = max(buy_score, sell_score)
    if close > prev_close and close >= ema20 and momentum_score >= 55 and trend_score >= 55 and buy_score >= min_score:
        side = "buy"
        score = buy_score
    elif close < prev_close and close <= ema20 and momentum_score <= 45 and trend_score <= 55 and sell_score >= min_score:
        side = "sell"
        score = sell_score
    if not side:
        if score < min_score:
            reason = "score_too_low"
        else:
            reason = "waiting_confirmation"
        return {
            "actionable": False,
            "reason": reason,
            "score": score,
            "trend_score": round(trend_score, 2),
            "momentum_score": round(momentum_score, 2),
            "volatility_score": round(volatility_score, 2),
        }
    regime = "trend" if volatility_score >= 35 else "chop"
    return {
        "actionable": True,
        "side": side,
        "score": score,
        "trend_score": round(trend_score, 2),
        "momentum_score": round(momentum_score, 2),
        "volatility_score": round(volatility_score, 2),
        "regime": regime,
        "confidence": "low" if score < 60 else "medium",
        "reason": "paper_exploration_backtest_signal",
        "rsi": round(rsi, 2),
        "ema20": round(ema20, 6),
        "ema50": round(ema50, 6),
    }


def _open_trade(settings: BacktestSettings, decision: dict[str, Any], bar: dict[str, Any], index: int, trade_key: str) -> dict[str, Any] | None:
    raw_entry = _number(bar.get("open"))
    if raw_entry is None or raw_entry <= 0:
        return None
    spread_cost = settings.spread_points * settings.point
    slippage_cost = settings.slippage_points * settings.point
    side = str(decision.get("side") or "").lower()
    entry = raw_entry + (spread_cost / 2) + slippage_cost if side == "buy" else raw_entry - (spread_cost / 2) - slippage_cost
    stop_loss = entry * (0.985 if side == "buy" else 1.015)
    initial_risk = abs(entry - stop_loss)
    if initial_risk <= 0:
        return None
    take_profit = entry + initial_risk * settings.min_rr if side == "buy" else entry - initial_risk * settings.min_rr
    return {
        "shadow_trade_id": f"backtest-{settings.symbol}-{trade_key}",
        "symbol": settings.symbol,
        "original_symbol": settings.symbol,
        "normalized_symbol": settings.normalized_symbol,
        "instrument_type": "crypto_spot" if settings.normalized_symbol == "BTCUSD" else "unknown",
        "is_spot_crypto": settings.normalized_symbol == "BTCUSD",
        "timeframe": settings.timeframe,
        "side": side,
        "action": side.upper(),
        "entry_price": round(entry, 6),
        "entry": round(entry, 6),
        "stop_loss": round(stop_loss, 6),
        "take_profit": round(take_profit, 6),
        "risk_reward": settings.min_rr,
        "risk_pct": settings.risk_pct,
        "opened_at": str(bar.get("time") or ""),
        "opened_index": index,
        "last_price": round(entry, 6),
        "initial_risk": round(initial_risk, 6),
        "max_favorable_excursion": 0.0,
        "max_adverse_excursion": 0.0,
        "status": "open",
        "lifecycle_status": "open",
        "exit_price": None,
        "exit_reason": "",
        "closed_at": "",
        "source": "mt5_backtest",
        "paper_exploration": True,
        "auto_forward": False,
        "manual_test": False,
        "replay": True,
        "excluded_from_main_metrics": False,
        "confidence": decision.get("confidence") or "low",
        "reason": decision.get("reason") or "paper_exploration_backtest_signal",
        "features_snapshot": {
            "score": decision.get("score"),
            "trend_score": decision.get("trend_score"),
            "momentum_score": decision.get("momentum_score"),
            "volatility_score": decision.get("volatility_score"),
            "regime": decision.get("regime"),
            "rsi": decision.get("rsi"),
            "ema20": decision.get("ema20"),
            "ema50": decision.get("ema50"),
        },
        **_safety(),
    }


def _update_trade(settings: BacktestSettings, trade: dict[str, Any], bar: dict[str, Any], index: int) -> tuple[dict[str, Any], dict[str, Any] | None]:
    high = float(_number(bar.get("high")) or _number(bar.get("close")) or 0.0)
    low = float(_number(bar.get("low")) or _number(bar.get("close")) or 0.0)
    close = float(_number(bar.get("close")) or 0.0)
    side = str(trade.get("side") or "").lower()
    entry = float(_number(trade.get("entry_price")) or _number(trade.get("entry")) or close)
    stop_loss = float(_number(trade.get("stop_loss")) or entry)
    take_profit = float(_number(trade.get("take_profit")) or entry)
    if side == "buy":
        mfe = high - entry
        mae = low - entry
        stop_hit = low <= stop_loss
        target_hit = high >= take_profit
    else:
        mfe = entry - low
        mae = entry - high
        stop_hit = high >= stop_loss
        target_hit = low <= take_profit
    updated = {
        **trade,
        "last_price": close,
        "max_favorable_excursion": round(max(float(_number(trade.get("max_favorable_excursion")) or 0.0), mfe), 6),
        "max_adverse_excursion": round(min(float(_number(trade.get("max_adverse_excursion")) or 0.0), mae), 6),
        "bars_open": max(0, index - int(trade.get("opened_index") or index)),
        "updated_at": str(bar.get("time") or ""),
        **_safety(),
    }
    if stop_hit and target_hit:
        return {}, _close(settings, updated, stop_loss, "stop_loss", bar)
    if stop_hit:
        return {}, _close(settings, updated, stop_loss, "stop_loss", bar)
    if target_hit:
        return {}, _close(settings, updated, take_profit, "take_profit", bar)
    if int(updated.get("bars_open") or 0) >= settings.time_stop_bars:
        return {}, _close(settings, updated, close, "time_stop", bar)
    return updated, None


def _force_close(settings: BacktestSettings, trade: dict[str, Any], bar: dict[str, Any], index: int, reason: str) -> dict[str, Any]:
    updated, closed = _update_trade(settings, {**trade, "opened_index": trade.get("opened_index", index)}, bar, index)
    if closed:
        return closed
    close = float(_number(bar.get("close")) or _number(trade.get("entry")) or 0.0)
    return _close(settings, updated or trade, close, reason, bar)


def _close(settings: BacktestSettings, trade: dict[str, Any], exit_price: float, reason: str, bar: dict[str, Any]) -> dict[str, Any]:
    side = str(trade.get("side") or "").lower()
    entry = float(_number(trade.get("entry_price")) or _number(trade.get("entry")) or exit_price)
    slippage = settings.slippage_points * settings.point
    adjusted_exit = exit_price - slippage if side == "buy" else exit_price + slippage
    pnl = adjusted_exit - entry if side == "buy" else entry - adjusted_exit
    pnl -= settings.commission
    pnl_pct = (pnl / entry) * 100 if entry else 0.0
    risk = abs(entry - float(_number(trade.get("stop_loss")) or entry)) or max(entry * 0.015, 0.000001)
    r_multiple = pnl / risk if risk else 0.0
    status = "win" if pnl > 0 else "loss" if pnl < 0 else "breakeven"
    return {
        **trade,
        "status": status,
        "lifecycle_status": "closed",
        "exit_price": round(adjusted_exit, 6),
        "exit_reason": reason,
        "closed_at": str(bar.get("time") or ""),
        "pnl": round(pnl, 6),
        "pnl_pct": round(pnl_pct, 6),
        "r_multiple": round(r_multiple, 6),
        "unrealized_pnl": 0.0,
        "unrealized_pnl_pct": 0.0,
        **_safety(),
    }


def _metrics(trades: list[dict[str, Any]], *, initial_balance: float) -> dict[str, Any]:
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    open_trades = [trade for trade in trades if trade.get("lifecycle_status") == "open"]
    wins = [trade for trade in closed if trade.get("status") == "win"]
    losses = [trade for trade in closed if trade.get("status") == "loss"]
    pnls = [float(_number(trade.get("pnl")) or 0.0) for trade in closed]
    r_values = [float(_number(trade.get("r_multiple")) or 0.0) for trade in closed]
    gross_win = sum(value for value in pnls if value > 0)
    gross_loss = abs(sum(value for value in pnls if value < 0))
    equity_curve = _equity_curve(closed, initial_balance)
    buy_stats = _side_stats(closed, "buy")
    sell_stats = _side_stats(closed, "sell")
    best_trade = max(closed, key=lambda item: float(_number(item.get("r_multiple")) or 0.0), default=None)
    worst_trade = min(closed, key=lambda item: float(_number(item.get("r_multiple")) or 0.0), default=None)
    return {
        "total_trades": len(trades),
        "shadow_trades": len(trades),
        "closed": len(closed),
        "open": len(open_trades),
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": sum(1 for trade in closed if trade.get("status") == "breakeven"),
        "win_rate": round((len(wins) / len(closed)) * 100, 2) if closed else 0.0,
        "profit_factor": round(gross_win / gross_loss, 4) if gross_loss else round(gross_win, 4) if gross_win else 0.0,
        "expectancy": round(sum(r_values) / len(r_values), 4) if r_values else 0.0,
        "net_pnl": round(sum(pnls), 6),
        "max_drawdown": _max_drawdown(equity_curve),
        "avg_win": round(gross_win / len(wins), 6) if wins else 0.0,
        "avg_loss": round(-gross_loss / len(losses), 6) if losses else 0.0,
        "rr_avg": round(sum(abs(value) for value in r_values) / len(r_values), 4) if r_values else 0.0,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "exit_reason_counts": _counts(closed, "exit_reason"),
        "buy_win_rate": buy_stats["win_rate"],
        "sell_win_rate": sell_stats["win_rate"],
        "buy_pf": buy_stats["profit_factor"],
        "sell_pf": sell_stats["profit_factor"],
        "trades_by_hour": _trades_by_hour(closed),
        "trades_by_regime": _trades_by_regime(closed),
        "equity_curve": equity_curve[-500:],
        "recent_trades": trades[-25:],
    }


def _result_payload(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    summary: dict[str, Any],
    no_trade_count: int,
    blocked: list[str],
    started: float,
    warnings: list[str],
) -> dict[str, Any]:
    timed_out = _timed_out(started, settings.timeout_seconds)
    status = "mt5_backtest_partial_timeout_guard" if timed_out else "mt5_backtest_completed"
    if timed_out:
        warnings = [*warnings, "processing capped to protect MT5 hot path"]
    return {
        "ok": True,
        "status": status,
        "symbol": settings.symbol,
        "normalized_symbol": settings.normalized_symbol,
        "instrument_type": "crypto_spot" if settings.normalized_symbol == "BTCUSD" else "unknown",
        "timeframe": settings.timeframe,
        "source": settings.source,
        "mode": "paper",
        "profile": settings.profile,
        "bars_loaded": len(bars),
        "from": bars[0].get("time") if bars else "",
        "to": bars[-1].get("time") if bars else "",
        "no_trade_count": no_trade_count,
        "blocked_reasons": _top_reasons(blocked),
        "warnings": warnings,
        "save_results": settings.save_results,
        "saved": False,
        **summary,
        **_safety(),
        "duration_ms": _elapsed_ms(started),
        "created_at": _now(),
        "genesis_reading": _reading(settings.symbol, summary, no_trade_count),
    }


def _empty_result(
    settings: BacktestSettings,
    started: float,
    warnings: list[str],
    *,
    errors: list[str] | None = None,
    status: str = "mt5_backtest_no_data",
    ok: bool = True,
) -> dict[str, Any]:
    summary = _metrics([], initial_balance=settings.initial_balance)
    return {
        "ok": ok,
        "status": status,
        "symbol": settings.symbol,
        "normalized_symbol": settings.normalized_symbol,
        "timeframe": settings.timeframe,
        "source": settings.source,
        "mode": "paper",
        "bars_loaded": 0,
        "warnings": warnings,
        "errors": errors or [],
        "no_trade_count": 0,
        "blocked_reasons": [],
        **summary,
        **_safety(),
        "duration_ms": _elapsed_ms(started),
        "created_at": _now(),
        "genesis_reading": "Backtest sin datos historicos cargados; no toca broker.",
    }


def _store_latest(symbol: str, result: dict[str, Any]) -> None:
    with _LATEST_LOCK:
        _LATEST_RESULTS[str(symbol or "").upper().strip() or "BTCUSD"] = dict(result)


def _ema(values: list[float], length: int) -> float:
    if not values:
        return 0.0
    length = max(1, length)
    alpha = 2 / (length + 1)
    ema = values[0]
    for value in values[1:]:
        ema = value * alpha + ema * (1 - alpha)
    return ema


def _rsi(values: list[float], length: int) -> float:
    if len(values) < 2:
        return 50.0
    changes = [values[index] - values[index - 1] for index in range(1, len(values))]
    window = changes[-max(1, length) :]
    gains = [change for change in window if change > 0]
    losses = [-change for change in window if change < 0]
    avg_gain = sum(gains) / max(len(window), 1)
    avg_loss = sum(losses) / max(len(window), 1)
    if avg_loss == 0:
        return 70.0 if avg_gain > 0 else 50.0
    return 100 - (100 / (1 + (avg_gain / avg_loss)))


def _side_stats(trades: list[dict[str, Any]], side: str) -> dict[str, float]:
    scoped = [trade for trade in trades if str(trade.get("side") or "").lower() == side]
    wins = [trade for trade in scoped if trade.get("status") == "win"]
    gross_win = sum(max(float(_number(trade.get("pnl")) or 0.0), 0.0) for trade in scoped)
    gross_loss = abs(sum(min(float(_number(trade.get("pnl")) or 0.0), 0.0) for trade in scoped))
    return {
        "win_rate": round((len(wins) / len(scoped)) * 100, 2) if scoped else 0.0,
        "profit_factor": round(gross_win / gross_loss, 4) if gross_loss else round(gross_win, 4) if gross_win else 0.0,
    }


def _equity_curve(trades: list[dict[str, Any]], initial_balance: float) -> list[dict[str, Any]]:
    equity = initial_balance
    curve: list[dict[str, Any]] = []
    for trade in trades:
        equity += float(_number(trade.get("pnl")) or 0.0)
        curve.append({"time": trade.get("closed_at") or "", "equity": round(equity, 6), "pnl": trade.get("pnl") or 0.0})
    return curve


def _max_drawdown(equity_curve: list[dict[str, Any]]) -> float:
    peak: float | None = None
    max_dd = 0.0
    for point in equity_curve:
        equity = float(_number(point.get("equity")) or 0.0)
        peak = equity if peak is None else max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return round(max_dd, 6)


def _counts(trades: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for trade in trades:
        value = str(trade.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return counts


def _trades_by_hour(trades: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for trade in trades:
        hour = _hour_from_time(str(trade.get("opened_at") or ""))
        counts[hour] = counts.get(hour, 0) + 1
    return counts


def _trades_by_regime(trades: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for trade in trades:
        features = trade.get("features_snapshot") if isinstance(trade.get("features_snapshot"), dict) else {}
        regime = str(features.get("regime") or "unknown")
        counts[regime] = counts.get(regime, 0) + 1
    return counts


def _top_reasons(reasons: list[str]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for reason in reasons:
        clean = str(reason or "unknown")
        counts[clean] = counts.get(clean, 0) + 1
    return [{"reason": reason, "count": count} for reason, count in sorted(counts.items(), key=lambda item: item[1], reverse=True)[:10]]


def _recent_loss_cluster(trades: list[dict[str, Any]]) -> bool:
    recent = [trade for trade in trades[-5:] if trade.get("lifecycle_status") == "closed"]
    return len(recent) >= 5 and sum(1 for trade in recent if trade.get("status") == "loss") >= 3


def _rough_bars_per_month(timeframe: str) -> int:
    minutes = _timeframe_minutes(timeframe)
    return max(5, int((30 * 24 * 60) / minutes))


def _timeframe_minutes(timeframe: str) -> int:
    value = str(timeframe or "H1").upper().strip()
    if value.startswith("M"):
        return max(1, int(_number(value[1:]) or 1))
    if value.startswith("H"):
        return max(1, int(_number(value[1:]) or 1) * 60)
    if value.startswith("D"):
        return 24 * 60
    return 60


def _pick(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    return None


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _hour_from_time(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return f"{parsed.hour:02d}"
    except Exception:
        return "unknown"


def _timed_out(started: float, timeout_seconds: float) -> bool:
    return (time.monotonic() - started) >= timeout_seconds


def _elapsed_ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _reading(symbol: str, summary: dict[str, Any], no_trade_count: int) -> str:
    return (
        f"{symbol}: backtest paper con {summary.get('total_trades', 0)} trades, "
        f"win rate {summary.get('win_rate', 0)}%, PF {summary.get('profit_factor', 0)} "
        f"y {no_trade_count} barras sin entrada. No toca broker."
    )

from __future__ import annotations

import csv
import json
import os
import time
from dataclasses import dataclass, replace
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
_DEFAULT_PROFILES = [
    "baseline",
    "quality_v2",
    "quality_loose",
    "quality_strict",
    "momentum_v1",
    "trend_v1",
    "anti_chop_v1",
    "rsi_reversal_safe",
]
_FILTER_PROFILES: dict[str, dict[str, Any]] = {
    "baseline": {
        "min_trend_score": 0.0,
        "min_momentum_score": 0.0,
        "max_rsi_for_buy": 100.0,
        "min_rsi_for_sell": 0.0,
        "score_cap_when_weak": 0.0,
        "allow_reversal": True,
        "avoid_chop": False,
        "min_score": 45.0,
    },
    "quality_v2": {
        "min_trend_score": 45.0,
        "min_momentum_score": 45.0,
        "max_rsi_for_buy": 75.0,
        "min_rsi_for_sell": 25.0,
        "score_cap_when_weak": 60.0,
        "allow_reversal": True,
        "avoid_chop": False,
        "min_score": 60.0,
    },
    "quality_loose": {
        "min_trend_score": 35.0,
        "min_momentum_score": 35.0,
        "max_rsi_for_buy": 80.0,
        "min_rsi_for_sell": 20.0,
        "score_cap_when_weak": 65.0,
        "allow_reversal": True,
        "avoid_chop": False,
        "min_score": 50.0,
    },
    "quality_strict": {
        "min_trend_score": 55.0,
        "min_momentum_score": 55.0,
        "max_rsi_for_buy": 70.0,
        "min_rsi_for_sell": 30.0,
        "score_cap_when_weak": 55.0,
        "allow_reversal": False,
        "avoid_chop": True,
        "min_score": 65.0,
    },
    "momentum_v1": {
        "min_trend_score": 35.0,
        "min_momentum_score": 60.0,
        "max_rsi_for_buy": 78.0,
        "min_rsi_for_sell": 22.0,
        "score_cap_when_weak": 60.0,
        "allow_reversal": True,
        "avoid_chop": False,
        "min_score": 60.0,
    },
    "trend_v1": {
        "min_trend_score": 60.0,
        "min_momentum_score": 35.0,
        "max_rsi_for_buy": 78.0,
        "min_rsi_for_sell": 22.0,
        "score_cap_when_weak": 60.0,
        "allow_reversal": True,
        "avoid_chop": False,
        "min_score": 60.0,
    },
    "anti_chop_v1": {
        "min_trend_score": 50.0,
        "min_momentum_score": 50.0,
        "max_rsi_for_buy": 76.0,
        "min_rsi_for_sell": 24.0,
        "score_cap_when_weak": 58.0,
        "allow_reversal": True,
        "avoid_chop": True,
        "min_score": 62.0,
    },
    "rsi_reversal_safe": {
        "min_trend_score": 45.0,
        "min_momentum_score": 45.0,
        "max_rsi_for_buy": 72.0,
        "min_rsi_for_sell": 28.0,
        "score_cap_when_weak": 58.0,
        "allow_reversal": False,
        "avoid_chop": False,
        "min_score": 58.0,
    },
}


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
    filter_profile: str
    filter_params: dict[str, Any]
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
                if body.get("compare_filters", True) is not False:
                    result["filter_comparison"] = _filter_comparison(settings, bars)
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

    def optimize(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        started = time.monotonic()
        body = payload or {}
        settings = _settings(body, self.config)
        warnings: list[str] = []
        try:
            bars, load_warnings = _load_bars(body, settings)
            warnings.extend(load_warnings)
            bars = bars[: settings.max_bars]
            if not bars:
                return {
                    "ok": False,
                    "status": "mt5_backtest_optimize_no_data",
                    "symbol": settings.symbol,
                    "normalized_symbol": settings.normalized_symbol,
                    "timeframe": settings.timeframe,
                    "warnings": warnings + ["historical_data_not_available"],
                    "ranking": [],
                    "table": [],
                    "table_markdown": "",
                    **_safety(),
                    "duration_ms": _elapsed_ms(started),
                }
            profiles = _requested_profiles(body)
            ranking: list[dict[str, Any]] = []
            for profile in profiles:
                profile_settings = _settings_for_profile(settings, profile, body)
                ranked = _rank_profile(profile_settings, bars, body)
                ranking.append(ranked)
            ranking.sort(key=lambda item: (bool(item.get("promoted")), float(item.get("robustness_score") or 0.0)), reverse=True)
            result = {
                "ok": True,
                "status": "mt5_backtest_optimize_completed",
                "symbol": settings.symbol,
                "normalized_symbol": settings.normalized_symbol,
                "instrument_type": "crypto_spot" if settings.normalized_symbol == "BTCUSD" else "unknown",
                "timeframe": settings.timeframe,
                "source": settings.source,
                "profiles": profiles,
                "walk_forward": bool(body.get("walk_forward", True)),
                "bars_loaded": len(bars),
                "ranking": ranking,
                "table": ranking,
                "table_markdown": _ranking_markdown(ranking),
                "best_profile": ranking[0].get("profile") if ranking else "",
                "promoted_profiles": [item["profile"] for item in ranking if item.get("promoted")],
                "warnings": warnings,
                **_safety(),
                "duration_ms": _elapsed_ms(started),
                "created_at": _now(),
            }
            _store_latest(f"{settings.normalized_symbol}:OPTIMIZE", result)
            if settings.save_results and self.memory is not None:
                try:
                    self.memory.save_mt5_event("mt5_backtest_runs", settings.symbol, result, "mt5_backtest_optimizer", "media")
                    result["saved"] = True
                except Exception as exc:
                    result["saved"] = False
                    result.setdefault("warnings", []).append(f"save_results_failed:{str(exc)[:160]}")
            return result
        except Exception as exc:
            return {
                "ok": False,
                "status": "mt5_backtest_optimize_error",
                "symbol": settings.symbol,
                "normalized_symbol": settings.normalized_symbol,
                "timeframe": settings.timeframe,
                "error": str(exc)[:500],
                "ranking": [],
                **_safety(),
                "duration_ms": _elapsed_ms(started),
            }

    def _run_walk_forward(
        self,
        settings: BacktestSettings,
        bars: list[dict[str, Any]],
        started: float,
        warnings: list[str],
        body: dict[str, Any],
    ) -> dict[str, Any]:
        split = _walk_forward_metrics(settings, bars, body)
        full_trades = list(split["train_trades_items"]) + list(split["test_trades_items"])
        summary = _metrics(full_trades, initial_balance=settings.initial_balance)
        no_trade_count = int(split.get("train_no_trade_count") or 0) + int(split.get("test_no_trade_count") or 0)
        blocked = list(split.get("train_blocked") or []) + list(split.get("test_blocked") or [])
        result = _result_payload(settings, bars, full_trades, summary, no_trade_count, blocked, started, warnings)
        result["walk_forward"] = True
        result.update(_walk_forward_public_payload(split))
        return result


def _settings(body: dict[str, Any], config: MT5RuntimeConfig) -> BacktestSettings:
    symbol = str(body.get("symbol") or "BTCUSD").upper().strip()
    normalized = normalize_mt5_symbol(symbol) or symbol
    timeframe = str(body.get("timeframe") or "H1").upper().strip()
    filter_profile = str(body.get("filter_profile") or "quality_v2").strip().casefold() or "quality_v2"
    if filter_profile not in {"baseline", "quality_v2"}:
        filter_profile = filter_profile if filter_profile in _FILTER_PROFILES else "quality_v2"
    max_bars = int(_number(body.get("max_bars") or body.get("bars")) or _number(os.getenv("MT5_BACKTEST_MAX_BARS")) or 2000)
    max_bars = max(10, min(max_bars, 10000))
    timeout_seconds = float(_number(body.get("timeout_seconds")) or _number(os.getenv("MT5_BACKTEST_TIMEOUT_SECONDS")) or 8.0)
    time_stop_min = float(_number(body.get("time_stop_min")) or config.paper_exploration_time_stop_min or 15.0)
    time_stop_bars = max(1, int((time_stop_min + _timeframe_minutes(timeframe) - 1) // _timeframe_minutes(timeframe)))
    requested_min_score = _number(body.get("min_score"))
    min_score = float(requested_min_score if requested_min_score is not None else config.paper_exploration_min_score or 45.0)
    profile_params = _profile_params(filter_profile, body)
    if requested_min_score is None and "min_score" in profile_params:
        min_score = float(profile_params["min_score"])
    if filter_profile == "quality_v2" and requested_min_score is None:
        min_score = max(min_score, 60.0)
    if requested_min_score is not None:
        profile_params["min_score"] = float(requested_min_score)
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
        min_score=min_score,
        time_stop_bars=time_stop_bars,
        max_bars=max_bars,
        timeout_seconds=max(1.0, min(timeout_seconds, 20.0)),
        profile=str(body.get("profile") or "BTCUSD_PAPER_EXPLORATION_V1").strip() or "BTCUSD_PAPER_EXPLORATION_V1",
        filter_profile=filter_profile,
        filter_params=profile_params,
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
        history = bars[max(0, index - 80) : index]
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
    params = settings.filter_params or _profile_params(settings.filter_profile, {})
    max_spread = _param_number(params, "max_spread_points", settings.max_spread_points)
    if settings.spread_points > max_spread:
        return {"actionable": False, "reason": "spread_too_high", "score": max(buy_score, sell_score)}
    min_score = max(0.0, _param_number(params, "min_score", settings.min_score or 45.0))
    side = ""
    score = max(buy_score, sell_score)
    raw_score = score
    score_cap = _param_number(params, "score_cap_when_weak", 0.0)
    if settings.filter_profile != "baseline" and score_cap > 0 and (trend_score < 40 or momentum_score < 40):
        score = min(score, score_cap)
    if close > prev_close and close >= ema20 and momentum_score >= 55 and trend_score >= 55 and buy_score >= min_score:
        side = "buy"
        score = min(buy_score, score) if settings.filter_profile != "baseline" else buy_score
    elif close < prev_close and close <= ema20 and momentum_score <= 45 and trend_score <= 55 and sell_score >= min_score:
        side = "sell"
        score = min(sell_score, score) if settings.filter_profile != "baseline" else sell_score
    if not side:
        if settings.filter_profile != "baseline" and (trend_score < 40 or momentum_score < 40):
            reason = "weak_internal_scores"
        elif score < min_score:
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
            "raw_score": raw_score,
        }
    quality_block = _profile_block(
        side=side,
        close=close,
        ema20=ema20,
        ema50=ema50,
        rsi=rsi,
        trend_score=trend_score,
        momentum_score=momentum_score,
        volatility_score=volatility_score,
        score=score,
        min_score=min_score,
        history=history,
        params=params,
        filter_profile=settings.filter_profile,
    )
    if quality_block:
        return {
            "actionable": False,
            "reason": quality_block,
            "score": round(score, 2),
            "raw_score": raw_score,
            "trend_score": round(trend_score, 2),
            "momentum_score": round(momentum_score, 2),
            "volatility_score": round(volatility_score, 2),
            "rsi": round(rsi, 2),
            "ema20": round(ema20, 6),
            "ema50": round(ema50, 6),
        }
    regime = "trend" if volatility_score >= 35 else "chop"
    return {
        "actionable": True,
        "side": side,
        "score": round(score, 2),
        "raw_score": raw_score,
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
        "filter_profile": settings.filter_profile,
        "features_snapshot": {
            "score": decision.get("score"),
            "raw_score": decision.get("raw_score"),
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
        "side_stats": {
            "buy": _group_metric([trade for trade in closed if str(trade.get("side") or "").lower() == "buy"]),
            "sell": _group_metric([trade for trade in closed if str(trade.get("side") or "").lower() == "sell"]),
        },
        "regime_stats": _group_stats(closed, "regime"),
        "hour_stats": _hour_stats(closed),
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
        "filter_profile": settings.filter_profile,
        **summary,
        "blocked_reason_counts": _reason_counts(blocked),
        "weak_internal_scores_count": blocked.count("weak_internal_scores"),
        "late_entry_risk_count": blocked.count("late_entry_risk"),
        "rsi_extreme_block_count": blocked.count("rsi_extreme_block"),
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
        "filter_profile": settings.filter_profile,
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


def _filter_comparison(settings: BacktestSettings, bars: list[dict[str, Any]]) -> dict[str, Any]:
    baseline = _profile_summary(_settings_for_profile(settings, "baseline", {}), bars)
    quality = _profile_summary(_settings_for_profile(settings, "quality_v2", {}), bars)
    return {
        "baseline": baseline,
        "quality_v2": quality,
        "baseline_pf": baseline["profit_factor"],
        "quality_v2_pf": quality["profit_factor"],
        "baseline_drawdown": baseline["max_drawdown"],
        "quality_v2_drawdown": quality["max_drawdown"],
        "baseline_trades": baseline["total_trades"],
        "quality_v2_trades": quality["total_trades"],
        "pf_delta": round(quality["profit_factor"] - baseline["profit_factor"], 4),
        "drawdown_delta": round(quality["max_drawdown"] - baseline["max_drawdown"], 6),
        "trades_delta": int(quality["total_trades"] - baseline["total_trades"]),
    }


def _requested_profiles(body: dict[str, Any]) -> list[str]:
    raw = body.get("profiles") or _DEFAULT_PROFILES
    if isinstance(raw, str):
        candidates = [part.strip().casefold() for part in raw.split(",")]
    elif isinstance(raw, list):
        candidates = [str(part or "").strip().casefold() for part in raw]
    else:
        candidates = list(_DEFAULT_PROFILES)
    profiles: list[str] = []
    for candidate in candidates:
        if candidate in _FILTER_PROFILES and candidate not in profiles:
            profiles.append(candidate)
    return profiles or list(_DEFAULT_PROFILES)


def _profile_params(profile: str, body: dict[str, Any]) -> dict[str, Any]:
    clean_profile = str(profile or "quality_v2").strip().casefold()
    params = dict(_FILTER_PROFILES.get(clean_profile) or _FILTER_PROFILES["quality_v2"])
    raw_profile_params = body.get("profile_params") if isinstance(body, dict) else None
    overrides: dict[str, Any] = {}
    if isinstance(raw_profile_params, dict):
        scoped = raw_profile_params.get(clean_profile)
        if isinstance(scoped, dict):
            overrides.update(scoped)
        else:
            overrides.update({key: value for key, value in raw_profile_params.items() if key in params})
    raw_filter_params = body.get("filter_params") if isinstance(body, dict) else None
    if isinstance(raw_filter_params, dict):
        overrides.update({key: value for key, value in raw_filter_params.items() if key in params})
    for key in params:
        if key in body:
            overrides[key] = body[key]
    for key, value in overrides.items():
        if key in {"allow_reversal", "avoid_chop"}:
            params[key] = _truthy(value)
        elif key in params:
            parsed = _number(value)
            if parsed is not None:
                params[key] = float(parsed)
    return params


def _settings_for_profile(settings: BacktestSettings, profile: str, body: dict[str, Any]) -> BacktestSettings:
    clean_profile = str(profile or "quality_v2").strip().casefold()
    if clean_profile not in _FILTER_PROFILES:
        clean_profile = "quality_v2"
    params = _profile_params(clean_profile, body or {})
    min_score = _param_number(params, "min_score", settings.min_score)
    max_spread = _param_number(params, "max_spread_points", settings.max_spread_points)
    return replace(
        settings,
        filter_profile=clean_profile,
        filter_params=params,
        min_score=min_score,
        max_spread_points=max_spread,
    )


def _rank_profile(settings: BacktestSettings, bars: list[dict[str, Any]], body: dict[str, Any]) -> dict[str, Any]:
    full_started = time.monotonic()
    trades, no_trade_count, blocked = _simulate(settings, bars, full_started, prefix=settings.filter_profile)
    summary = _metrics(trades, initial_balance=settings.initial_balance)
    split = _walk_forward_metrics(settings, bars, body)
    train_summary = dict(split.get("train_summary") or {})
    test_summary = dict(split.get("test_summary") or {})
    promotion = _promotion_decision(train_summary, test_summary, settings)
    robustness_score = _robustness_score(train_summary, test_summary, settings)
    return {
        "profile": settings.filter_profile,
        "timeframe": settings.timeframe,
        "trades": summary["total_trades"],
        "closed": summary["closed"],
        "wins": summary["wins"],
        "losses": summary["losses"],
        "win_rate": summary["win_rate"],
        "profit_factor": summary["profit_factor"],
        "expectancy": summary["expectancy"],
        "max_drawdown": summary["max_drawdown"],
        "net_pnl": summary["net_pnl"],
        "train_trades": train_summary.get("closed", 0),
        "train_profit_factor": train_summary.get("profit_factor", 0.0),
        "train_pf": train_summary.get("profit_factor", 0.0),
        "train_expectancy": train_summary.get("expectancy", 0.0),
        "train_drawdown": train_summary.get("max_drawdown", 0.0),
        "test_trades": test_summary.get("closed", 0),
        "test_profit_factor": test_summary.get("profit_factor", 0.0),
        "test_pf": test_summary.get("profit_factor", 0.0),
        "test_expectancy": test_summary.get("expectancy", 0.0),
        "test_drawdown": test_summary.get("max_drawdown", 0.0),
        "test_max_drawdown": test_summary.get("max_drawdown", 0.0),
        "robustness_score": robustness_score,
        "promoted": promotion["promoted"],
        "promotion_reasons": promotion["reasons"],
        "no_trade_count": no_trade_count,
        "blocked_reason_counts": _reason_counts(blocked),
        "weak_internal_scores_count": blocked.count("weak_internal_scores"),
        "late_entry_risk_count": blocked.count("late_entry_risk"),
        "rsi_extreme_block_count": blocked.count("rsi_extreme_block"),
        **_safety(),
    }


def _walk_forward_metrics(settings: BacktestSettings, bars: list[dict[str, Any]], body: dict[str, Any]) -> dict[str, Any]:
    if _truthy(body.get("rolling_windows")) or _truthy(body.get("rolling")):
        return _rolling_walk_forward_metrics(settings, bars, body)
    train_bars = int(_number(body.get("train_bars")) or 0)
    test_bars = int(_number(body.get("test_bars")) or 0)
    if train_bars <= 0 and test_bars <= 0:
        train_months = int(_number(body.get("train_months")) or 0)
        test_months = int(_number(body.get("test_months")) or 0)
        rough = _rough_bars_per_month(settings.timeframe)
        train_bars = train_months * rough if train_months > 0 else 0
        test_bars = test_months * rough if test_months > 0 else 0
    if train_bars > 0 and test_bars > 0 and len(bars) >= train_bars + test_bars:
        train_count = train_bars
        test_count = test_bars
    else:
        train_ratio = float(_number(body.get("train_ratio")) or 0.6)
        train_ratio = min(0.9, max(0.1, train_ratio))
        train_count = max(3, min(len(bars) - 2, int(len(bars) * train_ratio))) if len(bars) >= 6 else max(1, len(bars) // 2)
        test_count = max(0, len(bars) - train_count)
    train_slice = bars[:train_count]
    test_slice = bars[train_count : train_count + test_count]
    train_trades, train_no_trade, train_blocked = _simulate(settings, train_slice, time.monotonic(), prefix=f"{settings.filter_profile}-train")
    test_trades, test_no_trade, test_blocked = _simulate(settings, test_slice, time.monotonic(), prefix=f"{settings.filter_profile}-test")
    train_summary = _metrics(train_trades, initial_balance=settings.initial_balance)
    test_summary = _metrics(test_trades, initial_balance=settings.initial_balance)
    return {
        "train_bars": len(train_slice),
        "test_bars": len(test_slice),
        "train_trades_items": train_trades,
        "test_trades_items": test_trades,
        "train_summary": train_summary,
        "test_summary": test_summary,
        "train_no_trade_count": train_no_trade,
        "test_no_trade_count": test_no_trade,
        "train_blocked": train_blocked,
        "test_blocked": test_blocked,
        "walk_forward_results": [
            {
                "window": 1,
                "train_bars": len(train_slice),
                "test_bars": len(test_slice),
                "train_pf": train_summary["profit_factor"],
                "test_pf": test_summary["profit_factor"],
                "train_expectancy": train_summary["expectancy"],
                "test_expectancy": test_summary["expectancy"],
                "train_drawdown": train_summary["max_drawdown"],
                "test_drawdown": test_summary["max_drawdown"],
                "train_trades": train_summary["closed"],
                "test_trades": test_summary["closed"],
            }
        ],
    }


def _rolling_walk_forward_metrics(settings: BacktestSettings, bars: list[dict[str, Any]], body: dict[str, Any]) -> dict[str, Any]:
    train_window = int(_number(body.get("train_window_bars") or body.get("train_bars")) or 500)
    test_window = int(_number(body.get("test_window_bars") or body.get("test_bars")) or 250)
    train_window = max(10, min(train_window, max(10, len(bars) - 2)))
    test_window = max(5, min(test_window, max(5, len(bars) - train_window)))
    train_trades_all: list[dict[str, Any]] = []
    test_trades_all: list[dict[str, Any]] = []
    train_blocked_all: list[str] = []
    test_blocked_all: list[str] = []
    train_no_trade = 0
    test_no_trade = 0
    windows: list[dict[str, Any]] = []
    start = 0
    window_index = 1
    while start + train_window + test_window <= len(bars) and window_index <= 12:
        train_slice = bars[start : start + train_window]
        test_slice = bars[start + train_window : start + train_window + test_window]
        train_trades, train_nt, train_blocked = _simulate(settings, train_slice, time.monotonic(), prefix=f"{settings.filter_profile}-rw{window_index}-train")
        test_trades, test_nt, test_blocked = _simulate(settings, test_slice, time.monotonic(), prefix=f"{settings.filter_profile}-rw{window_index}-test")
        train_summary = _metrics(train_trades, initial_balance=settings.initial_balance)
        test_summary = _metrics(test_trades, initial_balance=settings.initial_balance)
        windows.append(
            {
                "window": window_index,
                "start_index": start,
                "train_bars": len(train_slice),
                "test_bars": len(test_slice),
                "train_pf": train_summary["profit_factor"],
                "test_pf": test_summary["profit_factor"],
                "train_expectancy": train_summary["expectancy"],
                "test_expectancy": test_summary["expectancy"],
                "train_drawdown": train_summary["max_drawdown"],
                "test_drawdown": test_summary["max_drawdown"],
                "train_trades": train_summary["closed"],
                "test_trades": test_summary["closed"],
            }
        )
        train_trades_all.extend(train_trades)
        test_trades_all.extend(test_trades)
        train_blocked_all.extend(train_blocked)
        test_blocked_all.extend(test_blocked)
        train_no_trade += train_nt
        test_no_trade += test_nt
        start += test_window
        window_index += 1
    if not windows:
        return _walk_forward_metrics(settings, bars, {**body, "rolling_windows": False, "rolling": False})
    return {
        "train_bars": sum(int(item["train_bars"]) for item in windows),
        "test_bars": sum(int(item["test_bars"]) for item in windows),
        "train_trades_items": train_trades_all,
        "test_trades_items": test_trades_all,
        "train_summary": _metrics(train_trades_all, initial_balance=settings.initial_balance),
        "test_summary": _metrics(test_trades_all, initial_balance=settings.initial_balance),
        "train_no_trade_count": train_no_trade,
        "test_no_trade_count": test_no_trade,
        "train_blocked": train_blocked_all,
        "test_blocked": test_blocked_all,
        "walk_forward_results": windows,
    }


def _walk_forward_public_payload(split: dict[str, Any]) -> dict[str, Any]:
    train_summary = dict(split.get("train_summary") or {})
    test_summary = dict(split.get("test_summary") or {})
    return {
        "train_summary": train_summary,
        "test_summary": test_summary,
        "train_pf": train_summary.get("profit_factor", 0.0),
        "test_pf": test_summary.get("profit_factor", 0.0),
        "train_expectancy": train_summary.get("expectancy", 0.0),
        "test_expectancy": test_summary.get("expectancy", 0.0),
        "train_drawdown": train_summary.get("max_drawdown", 0.0),
        "test_drawdown": test_summary.get("max_drawdown", 0.0),
        "train_trades": train_summary.get("closed", 0),
        "test_trades": test_summary.get("closed", 0),
        "train_bars": split.get("train_bars", 0),
        "test_bars": split.get("test_bars", 0),
        "walk_forward_results": split.get("walk_forward_results") or [],
    }


def _promotion_decision(train_summary: dict[str, Any], test_summary: dict[str, Any], settings: BacktestSettings) -> dict[str, Any]:
    reasons: list[str] = []
    train_pf = float(_number(train_summary.get("profit_factor")) or 0.0)
    test_pf = float(_number(test_summary.get("profit_factor")) or 0.0)
    test_expectancy = float(_number(test_summary.get("expectancy")) or 0.0)
    test_trades = int(_number(test_summary.get("closed")) or 0)
    test_drawdown = float(_number(test_summary.get("max_drawdown")) or 0.0)
    max_allowed_drawdown = settings.initial_balance * 0.12
    if test_pf < 1.25:
        reasons.append("test_pf_below_1_25")
    if test_expectancy <= 0:
        reasons.append("test_expectancy_not_positive")
    if test_trades < 50:
        reasons.append("test_trades_below_50")
    if test_drawdown > max_allowed_drawdown:
        reasons.append("test_drawdown_too_high")
    if train_pf >= 1.25 and test_pf < train_pf * 0.75:
        reasons.append("train_test_decay")
    return {"promoted": not reasons, "reasons": reasons or ["passes_promotion_rules"]}


def _robustness_score(train_summary: dict[str, Any], test_summary: dict[str, Any], settings: BacktestSettings) -> float:
    train_pf = float(_number(train_summary.get("profit_factor")) or 0.0)
    test_pf = float(_number(test_summary.get("profit_factor")) or 0.0)
    test_expectancy = float(_number(test_summary.get("expectancy")) or 0.0)
    test_trades = int(_number(test_summary.get("closed")) or 0)
    test_drawdown = float(_number(test_summary.get("max_drawdown")) or 0.0)
    decay_penalty = max(0.0, train_pf - test_pf) * 8.0
    drawdown_penalty = (test_drawdown / max(settings.initial_balance, 1.0)) * 120.0
    trade_credit = min(test_trades, 200) / 4.0
    score = (test_pf * 35.0) + (max(test_expectancy, 0.0) * 100.0) + trade_credit - drawdown_penalty - decay_penalty
    return round(max(0.0, score), 4)


def _ranking_markdown(ranking: list[dict[str, Any]]) -> str:
    if not ranking:
        return ""
    lines = [
        "profile | trades | WR | PF | expectancy | DD | test_PF | test_exp | test_DD | robustness_score | promoted",
        "--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---",
    ]
    for item in ranking:
        lines.append(
            f"{item.get('profile')} | {item.get('trades', 0)} | {item.get('win_rate', 0)} | "
            f"{item.get('profit_factor', 0)} | {item.get('expectancy', 0)} | {item.get('max_drawdown', 0)} | "
            f"{item.get('test_profit_factor', 0)} | {item.get('test_expectancy', 0)} | "
            f"{item.get('test_max_drawdown', 0)} | {item.get('robustness_score', 0)} | {bool(item.get('promoted'))}"
        )
    return "\n".join(lines)


def _param_number(params: dict[str, Any], key: str, default: float) -> float:
    value = params.get(key)
    parsed = _number(value)
    if parsed is None:
        return float(default)
    return float(parsed)


def _profile_summary(settings: BacktestSettings, bars: list[dict[str, Any]]) -> dict[str, Any]:
    started = time.monotonic()
    trades, _no_trade, blocked = _simulate(settings, bars, started)
    summary = _metrics(trades, initial_balance=settings.initial_balance)
    return {
        "filter_profile": settings.filter_profile,
        "total_trades": summary["total_trades"],
        "closed": summary["closed"],
        "wins": summary["wins"],
        "losses": summary["losses"],
        "win_rate": summary["win_rate"],
        "profit_factor": summary["profit_factor"],
        "expectancy": summary["expectancy"],
        "net_pnl": summary["net_pnl"],
        "max_drawdown": summary["max_drawdown"],
        "blocked_reason_counts": _reason_counts(blocked),
    }


def _profile_block(
    *,
    side: str,
    close: float,
    ema20: float,
    ema50: float,
    rsi: float,
    trend_score: float,
    momentum_score: float,
    volatility_score: float,
    score: float,
    min_score: float,
    history: list[dict[str, Any]],
    params: dict[str, Any],
    filter_profile: str,
) -> str:
    if filter_profile == "baseline":
        return ""
    allow_reversal = bool(params.get("allow_reversal"))
    avoid_chop = bool(params.get("avoid_chop"))
    min_trend = _param_number(params, "min_trend_score", 45.0)
    min_momentum = _param_number(params, "min_momentum_score", 45.0)
    max_rsi_for_buy = _param_number(params, "max_rsi_for_buy", 75.0)
    min_rsi_for_sell = _param_number(params, "min_rsi_for_sell", 25.0)
    confirmed_retest = allow_reversal and _confirmed_breakout_or_retest(side, close, history, trend_score, momentum_score)
    if avoid_chop and volatility_score < 35:
        return "regime_chop"
    if side == "sell" and rsi < min_rsi_for_sell and not confirmed_retest:
        return "rsi_extreme_block"
    if side == "buy" and rsi > max_rsi_for_buy and not confirmed_retest:
        return "rsi_extreme_block"
    if (trend_score < min_trend or momentum_score < min_momentum) and not confirmed_retest:
        return "weak_internal_scores"
    if score < min_score:
        return "weak_internal_scores"
    distance20 = abs(close - ema20) / close * 100 if close and ema20 else 0.0
    distance50 = abs(close - ema50) / close * 100 if close and ema50 else 0.0
    if (distance20 > 3.5 or distance50 > 7.0 or (side == "sell" and rsi < 30) or (side == "buy" and rsi > 70)) and not confirmed_retest:
        return "late_entry_risk"
    return ""


def _confirmed_breakout_or_retest(side: str, close: float, history: list[dict[str, Any]], trend_score: float, momentum_score: float) -> bool:
    previous = history[:-1]
    if len(previous) < 5:
        return False
    recent_high = max(float(row.get("high") or row.get("close") or close) for row in previous[-5:])
    recent_low = min(float(row.get("low") or row.get("close") or close) for row in previous[-5:])
    if side == "buy":
        return close > recent_high and trend_score >= 55 and momentum_score >= 55
    if side == "sell":
        return close < recent_low and trend_score >= 45 and momentum_score >= 45
    return False


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
        return 100.0 if avg_gain > 0 else 50.0
    return 100 - (100 / (1 + (avg_gain / avg_loss)))


def _group_metric(trades: list[dict[str, Any]]) -> dict[str, Any]:
    wins = [trade for trade in trades if trade.get("status") == "win"]
    losses = [trade for trade in trades if trade.get("status") == "loss"]
    pnls = [float(_number(trade.get("pnl")) or 0.0) for trade in trades]
    r_values = [float(_number(trade.get("r_multiple")) or 0.0) for trade in trades]
    gross_win = sum(value for value in pnls if value > 0)
    gross_loss = abs(sum(value for value in pnls if value < 0))
    return {
        "trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round((len(wins) / len(trades)) * 100, 2) if trades else 0.0,
        "profit_factor": round(gross_win / gross_loss, 4) if gross_loss else round(gross_win, 4) if gross_win else 0.0,
        "expectancy": round(sum(r_values) / len(r_values), 4) if r_values else 0.0,
    }


def _side_stats(trades: list[dict[str, Any]], side: str) -> dict[str, float]:
    scoped = [trade for trade in trades if str(trade.get("side") or "").lower() == side]
    wins = [trade for trade in scoped if trade.get("status") == "win"]
    gross_win = sum(max(float(_number(trade.get("pnl")) or 0.0), 0.0) for trade in scoped)
    gross_loss = abs(sum(min(float(_number(trade.get("pnl")) or 0.0), 0.0) for trade in scoped))
    return {
        "win_rate": round((len(wins) / len(scoped)) * 100, 2) if scoped else 0.0,
        "profit_factor": round(gross_win / gross_loss, 4) if gross_loss else round(gross_win, 4) if gross_win else 0.0,
    }


def _group_stats(trades: list[dict[str, Any]], feature_key: str) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        features = trade.get("features_snapshot") if isinstance(trade.get("features_snapshot"), dict) else {}
        value = str(features.get(feature_key) or "unknown")
        groups.setdefault(value, []).append(trade)
    return {key: _group_metric(items) for key, items in sorted(groups.items())}


def _hour_stats(trades: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        hour = _hour_from_time(str(trade.get("opened_at") or ""))
        groups.setdefault(hour, []).append(trade)
    return {key: _group_metric(items) for key, items in sorted(groups.items())}


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


def _reason_counts(reasons: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for reason in reasons:
        clean = str(reason or "unknown")
        counts[clean] = counts.get(clean, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: item[1], reverse=True))


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


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().casefold() in {"1", "true", "yes", "on", "y", "si", "sí"}


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

from __future__ import annotations

import csv
import json
import math
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from services.mt5.mt5_backtester import (
    BacktestSettings,
    _close,
    _load_bars,
    _metrics,
    _number,
    _reason_counts,
    _safety,
    _settings,
)
from services.mt5.mt5_capital_preservation_optimizer import (
    _depends_on_single_trade,
    _drawdown_accelerating,
    _loss_streak,
    _market_features,
    _monte_carlo_stress,
    _recent_edge_negative,
)
from services.mt5.mt5_config import get_mt5_config


STRATEGY_RESEARCH_V2_FAMILIES = [
    "trend_pullback",
    "breakout_retest",
    "volatility_expansion",
    "mean_reversion_safe",
    "liquidity_sweep_confirmed",
    "range_breakout_anti_chop",
    "momentum_continuation_filtered",
    "session_open_reversal_safe",
]

STRATEGY_RESEARCH_V2_TIMEFRAMES = ["M15", "M30", "H1"]
_DEFAULT_SESSIONS: dict[str, list[int] | None] = {
    "all": None,
    "asia": list(range(0, 8)),
    "london_us": list(range(7, 21)),
    "ny_core": list(range(13, 21)),
}


@dataclass(frozen=True)
class ResearchVariant:
    family: str
    timeframe: str
    side_mode: str
    session_name: str
    volatility_regime: str
    trend_regime: str
    score_threshold: float
    risk_reward: float
    time_stop_bars: int
    atr_stop_multiplier: float
    mae_exit_r: float
    momentum_loss_exit: bool

    def key(self) -> str:
        return (
            f"{self.timeframe}|{self.family}|side={self.side_mode}|session={self.session_name}|"
            f"vol={self.volatility_regime}|regime={self.trend_regime}|score={self.score_threshold}|"
            f"rr={self.risk_reward}|ts={self.time_stop_bars}|atr={self.atr_stop_multiplier}|"
            f"mae={self.mae_exit_r}|mom={int(self.momentum_loss_exit)}"
        )


def run_strategy_research_v2(body: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    body = body or {}
    symbol = str(body.get("symbol") or "BTCUSD").upper().strip()
    csv_dir = Path(str(body.get("csv_dir") or Path("data") / "backtests"))
    output_mode = str(body.get("mode") or "paper").casefold()
    timeframes = _requested_list(body.get("timeframes"), STRATEGY_RESEARCH_V2_TIMEFRAMES)
    families = [item for item in _requested_list(body.get("families"), STRATEGY_RESEARCH_V2_FAMILIES) if item in STRATEGY_RESEARCH_V2_FAMILIES]
    max_bars = max(200, min(int(_number(body.get("max_bars")) or 30000), 35000))
    max_evaluations = max(1, int(_number(body.get("max_evaluations")) or 240))
    per_evaluation_timeout = max(0.25, float(_number(body.get("per_evaluation_timeout_seconds")) or 3.0))
    spread_points = float(_number(body.get("spread_points")) or 25.0)
    variants = _build_variants(timeframes, families, max_evaluations=max_evaluations)
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[str] = []

    bars_by_timeframe: dict[str, list[dict[str, Any]]] = {}
    features_by_timeframe: dict[str, dict[int, dict[str, Any]]] = {}
    settings_by_timeframe: dict[str, BacktestSettings] = {}
    csv_by_timeframe: dict[str, str] = {}
    for timeframe in timeframes:
        csv_path = _csv_path_for(body, csv_dir, symbol, timeframe)
        csv_by_timeframe[timeframe] = str(csv_path)
        if not csv_path.exists():
            errors.append({"timeframe": timeframe, "path": str(csv_path), "error": "csv_not_found"})
            continue
        base_body = {
            "symbol": symbol,
            "timeframe": timeframe,
            "csv_path": str(csv_path),
            "max_bars": max_bars,
            "spread_points": spread_points,
            "save_results": False,
            "source": "mt5_csv",
            "timeout_seconds": per_evaluation_timeout,
        }
        settings = replace(_settings(base_body, get_mt5_config()), max_bars=max_bars, timeout_seconds=per_evaluation_timeout)
        bars, load_warnings = _load_bars(base_body, settings)
        warnings.extend([f"{timeframe}:{warning}" for warning in load_warnings])
        bars = bars[-settings.max_bars :]
        if not bars:
            errors.append({"timeframe": timeframe, "path": str(csv_path), "error": "csv_bars_not_loaded"})
            continue
        settings_by_timeframe[timeframe] = settings
        bars_by_timeframe[timeframe] = bars
        features_by_timeframe[timeframe] = _features_by_index(bars)

    for variant in variants:
        bars = bars_by_timeframe.get(variant.timeframe)
        settings = settings_by_timeframe.get(variant.timeframe)
        if not bars or settings is None:
            continue
        rows.append(
            evaluate_strategy_research_variant(
                settings,
                    bars,
                    variant,
                    source_csv=csv_by_timeframe.get(variant.timeframe, ""),
                    features_by_index=features_by_timeframe.get(variant.timeframe, {}),
                    timeout_seconds=per_evaluation_timeout,
                )
            )

    rows.sort(key=_research_rank, reverse=True)
    candidates = [row for row in rows if row.get("candidate")]
    candidate_profiles = [
        _candidate_profile_name(row)
        for row in candidates[:3]
    ]
    result = {
        "ok": True,
        "status": "mt5_strategy_research_v2_completed",
        "symbol": symbol,
        "mode": output_mode,
        "timeframes": timeframes,
        "families": families,
        "csv_paths": csv_by_timeframe,
        "evaluations": len(rows),
        "results": rows,
        "candidates": candidates,
        "top_3_for_optimizer": rows[:3],
        "candidate_profile_names": candidate_profiles,
        "summary": _summary(rows, candidates),
        "errors": errors,
        "warnings": warnings,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "live_runtime_mutated": False,
        "shadow_trades_mutated": False,
        "martingale_enabled": False,
        "grid_enabled": False,
        "averaging_down_enabled": False,
        "increase_size_after_loss_enabled": False,
        "max_open_trades": 1,
        **_safety(),
        "duration_ms": int((time.monotonic() - started) * 1000),
    }
    return result


def evaluate_strategy_research_variant(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    variant: ResearchVariant,
    *,
    source_csv: str = "",
    features_by_index: dict[int, dict[str, Any]] | None = None,
    timeout_seconds: float = 3.0,
) -> dict[str, Any]:
    started = time.monotonic()
    trades, blocked, signals, state = _simulate_research(
        settings,
        bars,
        variant,
        started,
        timeout_seconds=timeout_seconds,
        features_by_index=features_by_index,
    )
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    metrics = _metrics(closed, initial_balance=settings.initial_balance)
    terciles = _tercile_stats(settings, bars, closed)
    windows_ok = _windows_ok(terciles)
    monte_carlo = _monte_carlo_stress(closed, initial_balance=settings.initial_balance, max_drawdown_limit=5000.0, simulations=350)
    fragile = _fragile_regime_dependency(metrics, terciles)
    single_trade = _depends_on_single_trade(metrics)
    gate = _research_gate(metrics, terciles, monte_carlo, fragile, single_trade)
    score = _research_score(metrics, monte_carlo, gate, fragile, single_trade)
    return {
        "timeframe": variant.timeframe,
        "family": variant.family,
        "profile": variant.family,
        "variant_id": variant.key(),
        "candidate_profile_name": _candidate_profile_name({"family": variant.family, "timeframe": variant.timeframe, "side_mode": variant.side_mode}),
        "source_csv": source_csv,
        "bars_loaded": len(bars),
        "bars_evaluated": max(0, len(bars) - 80),
        "first_bar_time": str(bars[0].get("time") or "") if bars else "",
        "last_bar_time": str(bars[-1].get("time") or "") if bars else "",
        "side_mode": variant.side_mode,
        "session_filter": variant.session_name,
        "volatility_regime": variant.volatility_regime,
        "trend_regime": variant.trend_regime,
        "score_threshold": variant.score_threshold,
        "risk_reward": variant.risk_reward,
        "time_stop_bars": variant.time_stop_bars,
        "mae_exit_r": variant.mae_exit_r,
        "momentum_loss_exit": variant.momentum_loss_exit,
        "generated_signal_count": signals["generated"],
        "actionable_signal_count": signals["actionable"],
        "opened_trade_count": len(trades),
        "closed": metrics["closed"],
        "wins": metrics["wins"],
        "losses": metrics["losses"],
        "win_rate": metrics["win_rate"],
        "profit_factor": metrics["profit_factor"],
        "expectancy": metrics["expectancy"],
        "max_drawdown": metrics["max_drawdown"],
        "net_pnl": metrics["net_pnl"],
        "buy_win_rate": metrics["buy_win_rate"],
        "sell_win_rate": metrics["sell_win_rate"],
        "buy_pf": metrics["buy_pf"],
        "sell_pf": metrics["sell_pf"],
        "side_stats": metrics["side_stats"],
        "hour_stats": metrics["hour_stats"],
        "regime_stats": metrics["regime_stats"],
        "exit_reason_counts": metrics["exit_reason_counts"],
        "tercile_stats": terciles,
        "tercile_pf_positive": windows_ok["pf_positive"],
        "tercile_expectancy_positive": windows_ok["expectancy_positive"],
        "fragile_regime_dependency": fragile,
        "single_trade_dependency": single_trade,
        "monte_carlo": monte_carlo,
        "monte_carlo_stressed_pf": monte_carlo.get("profit_factor_stressed", 0.0),
        "monte_carlo_p95_drawdown": monte_carlo.get("max_drawdown_p95", 0.0),
        "monte_carlo_fail_reasons": list(monte_carlo.get("fail_reasons") or []),
        "blocked_reason_counts": _reason_counts(blocked),
        "risk_governor_blocks": state["risk_governor_blocks"],
        "max_open_trades_observed": state["max_open_trades_observed"],
        "candidate": gate["passed"],
        "recommendation": "research_candidate" if gate["passed"] else ("observation_only" if _observation_quality(metrics) else "reject"),
        "reject_reasons": gate["reasons"],
        "research_score": score,
        "applies_to_paper_shadow": False,
        "applies_to_real_trading": False,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "live_runtime_mutated": False,
        "shadow_trades_mutated": False,
        "martingale_enabled": False,
        "grid_enabled": False,
        "averaging_down_enabled": False,
        "increase_size_after_loss_enabled": False,
        **_safety(),
    }


def write_strategy_research_v2_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "strategy_research_v2_results.csv"
    json_path = root / "strategy_research_v2_results.json"
    summary_path = root / "strategy_research_v2_summary.md"
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    headers = [
        "timeframe",
        "family",
        "variant_id",
        "candidate_profile_name",
        "source_csv",
        "bars_loaded",
        "side_mode",
        "session_filter",
        "volatility_regime",
        "trend_regime",
        "score_threshold",
        "risk_reward",
        "time_stop_bars",
        "generated_signal_count",
        "actionable_signal_count",
        "opened_trade_count",
        "closed",
        "wins",
        "losses",
        "win_rate",
        "profit_factor",
        "expectancy",
        "max_drawdown",
        "buy_pf",
        "sell_pf",
        "monte_carlo_stressed_pf",
        "monte_carlo_p95_drawdown",
        "fragile_regime_dependency",
        "single_trade_dependency",
        "candidate",
        "recommendation",
        "reject_reasons",
        "research_score",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({**row, "reject_reasons": ";".join(str(item) for item in row.get("reject_reasons") or [])})
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")
    summary_path.write_text(strategy_research_v2_summary_markdown(result), encoding="utf-8")
    return csv_path, json_path, summary_path


def strategy_research_v2_summary_markdown(result: dict[str, Any]) -> str:
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else _summary(rows, [])
    lines = [
        "# MT5 Strategy Research V2 Summary",
        "",
        "Regime-segmented signal discovery. Paper/offline only; no broker, no order execution, no automatic promotion.",
        "",
        f"Evaluations: `{result.get('evaluations', len(rows))}`.",
        f"Candidates: `{len(result.get('candidates') or [])}`.",
        "",
        "## Top 20",
    ]
    for row in rows[:20]:
        lines.append(
            f"- `{row.get('timeframe')}` `{row.get('family')}` side `{row.get('side_mode')}` session `{row.get('session_filter')}` "
            f"closed `{row.get('closed')}`, PF `{row.get('profit_factor')}`, expectancy `{row.get('expectancy')}`, "
            f"DD `{row.get('max_drawdown')}`, MC PF `{row.get('monte_carlo_stressed_pf')}`, recommendation `{row.get('recommendation')}`."
        )
    lines.extend(
        [
            "",
            "## Answers",
            f"1. Most valid trades: {summary.get('most_trades_answer')}",
            f"2. Best PF/expectancy with sample: {summary.get('best_quality_answer')}",
            f"3. Best timeframe balance: {summary.get('timeframe_answer')}",
            f"4. Best side: {summary.get('side_answer')}",
            f"5. Best session/hour: {summary.get('session_answer')}",
            f"6. Best regime: {summary.get('regime_answer')}",
            f"7. Monte Carlo fragile families: {summary.get('monte_carlo_answer')}",
            f"8. Sample too small families: {summary.get('sample_answer')}",
            f"9. Top 3 for capital optimizer: {summary.get('top_3_answer')}",
            "10. No automatic promotion; all research candidates remain observation/research mode.",
            "",
            "## Safety",
            "- No real trading.",
            "- No order_send.",
            "- No broker credentials.",
            "- MaxOpenTrades=1.",
            "- No martingale, no grid, no averaging down, no size increase after loss.",
            "- broker_touched=false",
            "- order_executed=false",
            "- order_policy=journal_only_no_broker",
        ]
    )
    return "\n".join(lines) + "\n"


def _simulate_research(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    variant: ResearchVariant,
    started: float,
    *,
    timeout_seconds: float,
    features_by_index: dict[int, dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[str], dict[str, int], dict[str, int]]:
    trades: list[dict[str, Any]] = []
    blocked: list[str] = []
    open_trade: dict[str, Any] | None = None
    cooldown_until = -1
    signals = {"generated": 0, "actionable": 0}
    state = {"risk_governor_blocks": 0, "max_open_trades_observed": 0}
    features_by_index = features_by_index if features_by_index is not None else _features_by_index(bars)
    max_iterations = len(bars) + 5
    iterations = 0
    for index in range(80, len(bars)):
        iterations += 1
        if iterations > max_iterations:
            blocked.append("loop_guard")
            break
        if time.monotonic() - started > timeout_seconds:
            blocked.append("timeout_guard")
            break
        bar = bars[index]
        if open_trade:
            open_trade, closed = _update_research_trade(settings, open_trade, bar, index, variant)
            state["max_open_trades_observed"] = max(state["max_open_trades_observed"], 1)
            if closed:
                trades.append(closed)
                if closed.get("status") == "loss":
                    cooldown_until = max(cooldown_until, index + 2)
                open_trade = None
        if index >= len(bars) - 1:
            continue
        if open_trade:
            blocked.append("max_open_trades_reached")
            continue
        if index < cooldown_until:
            blocked.append("cooldown_after_loss")
            continue
        risk_reason = _research_risk_block(settings, trades, variant)
        if risk_reason:
            state["risk_governor_blocks"] += 1
            blocked.append(f"risk_governor_{risk_reason}")
            continue
        features = features_by_index.get(index - 1)
        if not features:
            blocked.append("insufficient_history")
            continue
        decision = _decision_for_variant(features, variant)
        if decision.get("generated"):
            signals["generated"] += 1
        if not decision.get("actionable"):
            blocked.append(str(decision.get("reason") or "no_signal"))
            continue
        signals["actionable"] += 1
        open_trade = _open_research_trade(settings, decision, bars[index], index, variant)
        if open_trade is None:
            blocked.append("missing_risk_parameters")
            continue
        state["max_open_trades_observed"] = max(state["max_open_trades_observed"], 1)
    if open_trade:
        trades.append(_close(settings, open_trade, float(_number(bars[-1].get("close")) or open_trade.get("entry_price") or 0.0), "time_stop", bars[-1]))
    return trades, blocked, signals, state


def _features_by_index(bars: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    payload: dict[int, dict[str, Any]] = {}
    for index in range(20, len(bars)):
        features = _market_features(bars[max(0, index - 80) : index + 1])
        if features:
            payload[index] = features
    return payload


def _decision_for_variant(features: dict[str, Any], variant: ResearchVariant) -> dict[str, Any]:
    if not _session_allowed(features, variant):
        return {"actionable": False, "generated": False, "reason": "session_filter"}
    if not _volatility_allowed(features, variant):
        return {"actionable": False, "generated": False, "reason": "volatility_regime_filter"}
    if not _trend_regime_allowed(features, variant):
        return {"actionable": False, "generated": False, "reason": "trend_regime_filter"}
    side, score, reason = _family_signal(features, variant.family)
    if not side:
        return {"actionable": False, "generated": False, "reason": reason or "family_no_signal", "score": round(score, 2)}
    if variant.side_mode != "both" and side != variant.side_mode:
        return {"actionable": False, "generated": True, "reason": "side_filter", "score": round(score, 2)}
    if score < variant.score_threshold:
        return {"actionable": False, "generated": True, "reason": "score_threshold", "score": round(score, 2)}
    return {
        "actionable": True,
        "generated": True,
        "side": side,
        "score": round(score, 2),
        "reason": reason,
        "confidence": "medium" if score >= 62 else "low",
        "regime": features.get("regime") or "unknown",
        "trend_score": round(float(features.get("trend_score") or 0.0), 2),
        "momentum_score": round(float(features.get("momentum_score") or 0.0), 2),
        "volatility_score": round(float(features.get("volatility_score") or 0.0), 2),
        "rsi": round(float(features.get("rsi") or 0.0), 2),
        "ema20": round(float(features.get("ema20") or 0.0), 6),
        "ema50": round(float(features.get("ema50") or 0.0), 6),
        "atr": round(float(features.get("atr") or 0.0), 6),
        "atr_pct": round(float(features.get("atr_pct") or 0.0), 6),
        "hour": features.get("hour"),
    }


def _family_signal(features: dict[str, Any], family: str) -> tuple[str, float, str]:
    close = float(features["close"])
    prev_close = float(features["prev_close"])
    ema20 = float(features["ema20"])
    ema50 = float(features["ema50"])
    rsi = float(features["rsi"])
    trend_score = float(features["trend_score"])
    momentum_score = float(features["momentum_score"])
    volatility_score = float(features["volatility_score"])
    body_ratio = float(features["body_ratio"])
    distance20 = float(features["distance20_atr"])
    base_trend = trend_score if close >= ema20 else 100.0 - trend_score
    base_momentum = momentum_score if close >= prev_close else 100.0 - momentum_score
    score = base_trend * 0.42 + base_momentum * 0.38 + volatility_score * 0.20
    if family == "trend_pullback":
        if close > ema20 > ema50 and distance20 <= 1.15 and momentum_score >= 50 and rsi < 72:
            return "buy", score + 4.0, "trend_pullback_buy"
        if close < ema20 < ema50 and distance20 <= 1.15 and momentum_score <= 50 and rsi > 28:
            return "sell", score + 4.0, "trend_pullback_sell"
        return "", score, "trend_pullback_not_confirmed"
    if family == "breakout_retest":
        if close > float(features["recent_high"]) and float(features["low"]) <= float(features["recent_high"]) and rsi < 76:
            return "buy", score + 6.0, "breakout_retest_buy"
        if close < float(features["recent_low"]) and float(features["high"]) >= float(features["recent_low"]) and rsi > 24:
            return "sell", score + 6.0, "breakout_retest_sell"
        return "", score, "breakout_retest_not_confirmed"
    if family == "volatility_expansion":
        expanding = float(features["recent_range"]) > max(float(features["previous_range"]) * 1.08, float(features["atr"]) * 1.2)
        if expanding and close > prev_close and momentum_score >= 55 and rsi < 78:
            return "buy", score + volatility_score * 0.10, "volatility_expansion_buy"
        if expanding and close < prev_close and momentum_score <= 45 and rsi > 22:
            return "sell", score + volatility_score * 0.10, "volatility_expansion_sell"
        return "", score, "volatility_expansion_not_confirmed"
    if family == "mean_reversion_safe":
        reversion_score = 58.0 + min(distance20 * 8.0, 18.0) + body_ratio * 8.0
        if features["regime"] == "chop" and rsi <= 38 and close > prev_close:
            return "buy", reversion_score, "mean_reversion_safe_buy"
        if features["regime"] == "chop" and rsi >= 62 and close < prev_close:
            return "sell", reversion_score, "mean_reversion_safe_sell"
        return "", reversion_score, "mean_reversion_safe_not_confirmed"
    if family == "liquidity_sweep_confirmed":
        sweep_low = float(features["low"]) < float(features["recent_low"]) and close > float(features["recent_low"]) and close > float(features["open"])
        sweep_high = float(features["high"]) > float(features["recent_high"]) and close < float(features["recent_high"]) and close < float(features["open"])
        sweep_score = 54.0 + body_ratio * 20.0 + volatility_score * 0.12
        if sweep_low and rsi <= 48:
            return "buy", sweep_score, "liquidity_sweep_confirmed_buy"
        if sweep_high and rsi >= 52:
            return "sell", sweep_score, "liquidity_sweep_confirmed_sell"
        return "", sweep_score, "liquidity_sweep_not_confirmed"
    if family == "range_breakout_anti_chop":
        compressed = float(features["recent_range"]) <= max(float(features["previous_range"]) * 0.86, float(features["atr"]) * 1.1)
        if compressed and close > float(features["recent_high"]) and momentum_score >= 54:
            return "buy", score + 3.0, "range_breakout_buy"
        if compressed and close < float(features["recent_low"]) and momentum_score <= 46:
            return "sell", score + 3.0, "range_breakout_sell"
        return "", score, "range_breakout_not_confirmed"
    if family == "momentum_continuation_filtered":
        if close > ema20 and momentum_score >= 61 and rsi < 78 and distance20 <= 2.6:
            return "buy", score + 5.0, "momentum_continuation_buy"
        if close < ema20 and momentum_score <= 39 and rsi > 22 and distance20 <= 2.6:
            return "sell", score + 5.0, "momentum_continuation_sell"
        return "", score, "momentum_continuation_not_confirmed"
    if family == "session_open_reversal_safe":
        hour = int(features.get("hour") if features.get("hour") is not None else -1)
        near_open = hour in {0, 7, 8, 13, 14}
        reversal_score = 56.0 + body_ratio * 16.0 + min(distance20 * 4.0, 10.0)
        if near_open and rsi <= 44 and close > prev_close:
            return "buy", reversal_score, "session_open_reversal_buy"
        if near_open and rsi >= 56 and close < prev_close:
            return "sell", reversal_score, "session_open_reversal_sell"
        return "", reversal_score, "session_open_reversal_not_confirmed"
    return "", score, "unknown_family"


def _open_research_trade(settings: BacktestSettings, decision: dict[str, Any], bar: dict[str, Any], index: int, variant: ResearchVariant) -> dict[str, Any] | None:
    raw_entry = _number(bar.get("open"))
    if raw_entry is None or raw_entry <= 0:
        return None
    side = str(decision.get("side") or "").lower()
    spread_cost = settings.spread_points * settings.point
    entry = float(raw_entry) + (spread_cost / 2 if side == "buy" else -spread_cost / 2)
    atr = float(_number(decision.get("atr")) or entry * 0.006)
    stop_distance = max(entry * 0.003, min(atr * variant.atr_stop_multiplier, entry * 0.018))
    stop = entry - stop_distance if side == "buy" else entry + stop_distance
    target = entry + stop_distance * variant.risk_reward if side == "buy" else entry - stop_distance * variant.risk_reward
    return {
        "shadow_trade_id": f"research-v2-{variant.family}-{variant.timeframe}-{index}",
        "symbol": settings.symbol,
        "normalized_symbol": settings.normalized_symbol,
        "timeframe": settings.timeframe,
        "side": side,
        "action": side.upper(),
        "entry_price": round(entry, 6),
        "entry": round(entry, 6),
        "stop_loss": round(stop, 6),
        "take_profit": round(target, 6),
        "initial_risk": round(stop_distance, 6),
        "risk_reward": variant.risk_reward,
        "risk_pct": settings.risk_pct,
        "opened_at": str(bar.get("time") or ""),
        "opened_index": index,
        "last_price": round(entry, 6),
        "max_favorable_excursion": 0.0,
        "max_adverse_excursion": 0.0,
        "status": "open",
        "lifecycle_status": "open",
        "exit_price": None,
        "exit_reason": "",
        "source": "mt5_strategy_research_v2",
        "strategy_profile": variant.family,
        "filter_profile": variant.family,
        "research_mode": True,
        "auto_forward": False,
        "paper_exploration": True,
        "manual_test": False,
        "regime": decision.get("regime") or "unknown",
        "hour": decision.get("hour"),
        "features_snapshot": {
            "score": decision.get("score"),
            "trend_score": decision.get("trend_score"),
            "momentum_score": decision.get("momentum_score"),
            "volatility_score": decision.get("volatility_score"),
            "rsi": decision.get("rsi"),
            "regime": decision.get("regime"),
            "atr": decision.get("atr"),
            "atr_pct": decision.get("atr_pct"),
            "family": variant.family,
            "session_filter": variant.session_name,
            "side_mode": variant.side_mode,
        },
        **_safety(),
    }


def _update_research_trade(settings: BacktestSettings, trade: dict[str, Any], bar: dict[str, Any], index: int, variant: ResearchVariant) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    high = float(_number(bar.get("high")) or _number(bar.get("close")) or 0.0)
    low = float(_number(bar.get("low")) or _number(bar.get("close")) or 0.0)
    close = float(_number(bar.get("close")) or 0.0)
    side = str(trade.get("side") or "").lower()
    entry = float(_number(trade.get("entry_price")) or _number(trade.get("entry")) or close)
    stop = float(_number(trade.get("stop_loss")) or entry)
    target = float(_number(trade.get("take_profit")) or entry)
    risk = abs(entry - stop) or max(entry * 0.003, 0.000001)
    if side == "buy":
        mfe = high - entry
        mae = low - entry
        stop_hit = low <= stop
        target_hit = high >= target
    else:
        mfe = entry - low
        mae = entry - high
        stop_hit = high >= stop
        target_hit = low <= target
    updated = {
        **trade,
        "last_price": close,
        "bars_open": max(0, index - int(trade.get("opened_index") or index)),
        "max_favorable_excursion": round(max(float(_number(trade.get("max_favorable_excursion")) or 0.0), mfe), 6),
        "max_adverse_excursion": round(min(float(_number(trade.get("max_adverse_excursion")) or 0.0), mae), 6),
        "updated_at": str(bar.get("time") or ""),
        **_safety(),
    }
    if stop_hit and target_hit:
        return None, _close(settings, updated, stop, "stop_loss", bar)
    if stop_hit:
        return None, _close(settings, updated, stop, "stop_loss", bar)
    if target_hit:
        return None, _close(settings, updated, target, "take_profit", bar)
    if abs(float(_number(updated.get("max_adverse_excursion")) or 0.0)) >= risk * variant.mae_exit_r:
        return None, _close(settings, updated, close, "mae_defense_exit", bar)
    if variant.momentum_loss_exit and int(updated.get("bars_open") or 0) >= 1:
        open_price = float(_number(bar.get("open")) or close)
        if (side == "buy" and close < open_price and close < entry) or (side == "sell" and close > open_price and close > entry):
            return None, _close(settings, updated, close, "momentum_loss_exit", bar)
    if int(updated.get("bars_open") or 0) >= variant.time_stop_bars:
        return None, _close(settings, updated, close, "time_stop", bar)
    return updated, None


def _build_variants(timeframes: list[str], families: list[str], *, max_evaluations: int) -> list[ResearchVariant]:
    variants_by_timeframe: dict[str, list[ResearchVariant]] = {timeframe: [] for timeframe in timeframes}
    for timeframe in timeframes:
        for family in families:
            profile = _family_defaults(family, timeframe)
            for side in ["both", "buy", "sell"]:
                for session in profile["sessions"]:
                    for vol in profile["volatility"]:
                        variants_by_timeframe[timeframe].append(
                            ResearchVariant(
                                family=family,
                                timeframe=timeframe,
                                side_mode=side,
                                session_name=session,
                                volatility_regime=vol,
                                trend_regime=profile["trend_regime"],
                                score_threshold=profile["score"],
                                risk_reward=profile["rr"],
                                time_stop_bars=profile["time_stop"],
                                atr_stop_multiplier=profile["atr_stop"],
                                mae_exit_r=profile["mae_exit"],
                                momentum_loss_exit=True,
                            )
                        )
            variants_by_timeframe[timeframe].append(
                ResearchVariant(
                    family=family,
                    timeframe=timeframe,
                    side_mode="both",
                    session_name="all",
                    volatility_regime="any",
                    trend_regime="any",
                    score_threshold=max(48.0, profile["score"] - 5.0),
                    risk_reward=max(0.8, profile["rr"] - 0.2),
                    time_stop_bars=max(1, profile["time_stop"] - 1),
                    atr_stop_multiplier=profile["atr_stop"],
                    mae_exit_r=max(0.65, profile["mae_exit"] - 0.05),
                    momentum_loss_exit=True,
                )
            )
    per_timeframe = max(1, math.ceil(max_evaluations / max(1, len(timeframes))))
    selected: list[ResearchVariant] = []
    for timeframe in timeframes:
        unique: dict[str, ResearchVariant] = {}
        for variant in variants_by_timeframe.get(timeframe, []):
            unique.setdefault(variant.key(), variant)
        ordered = list(unique.values())
        ordered.sort(key=lambda item: (item.family, item.score_threshold, item.session_name, item.side_mode))
        selected.extend(ordered[:per_timeframe])
    return selected[:max_evaluations]


def _family_defaults(family: str, timeframe: str) -> dict[str, Any]:
    base = {
        "sessions": ["all", "london_us"],
        "volatility": ["any", "normal_high"],
        "trend_regime": "any",
        "score": 56.0,
        "rr": 1.0,
        "time_stop": 2 if timeframe != "H1" else 3,
        "atr_stop": 1.1,
        "mae_exit": 0.9,
    }
    if family in {"trend_pullback", "momentum_continuation_filtered"}:
        base.update({"trend_regime": "trend", "score": 58.0, "rr": 1.2, "atr_stop": 1.05})
    elif family in {"mean_reversion_safe", "liquidity_sweep_confirmed", "session_open_reversal_safe"}:
        base.update({"trend_regime": "chop", "score": 54.0, "rr": 1.0, "sessions": ["all", "asia", "ny_core"], "mae_exit": 0.8})
    elif family in {"breakout_retest", "range_breakout_anti_chop", "volatility_expansion"}:
        base.update({"trend_regime": "any", "score": 57.0, "rr": 1.15, "volatility": ["normal_high", "high"], "atr_stop": 1.0})
    return base


def _session_allowed(features: dict[str, Any], variant: ResearchVariant) -> bool:
    hours = _DEFAULT_SESSIONS.get(variant.session_name)
    if hours is None:
        return True
    hour = features.get("hour")
    return hour is not None and int(hour) in set(hours)


def _volatility_allowed(features: dict[str, Any], variant: ResearchVariant) -> bool:
    bucket = _volatility_bucket(float(features.get("volatility_score") or 0.0))
    if variant.volatility_regime == "any":
        return True
    if variant.volatility_regime == "normal_high":
        return bucket in {"normal", "high"}
    return bucket == variant.volatility_regime


def _trend_regime_allowed(features: dict[str, Any], variant: ResearchVariant) -> bool:
    regime = str(features.get("regime") or "unknown").casefold()
    if variant.trend_regime == "any":
        return True
    if variant.trend_regime == "range":
        return regime == "chop"
    return regime == variant.trend_regime


def _volatility_bucket(score: float) -> str:
    if score < 28:
        return "low"
    if score > 58:
        return "high"
    return "normal"


def _research_risk_block(settings: BacktestSettings, trades: list[dict[str, Any]], variant: ResearchVariant) -> str:
    if settings.spread_points > 30:
        return "spread_too_high"
    if _loss_streak(trades) >= 4:
        return "consecutive_loss_lockdown"
    if len([trade for trade in trades if trade.get("lifecycle_status") == "closed"]) >= 20 and _recent_edge_negative(trades):
        return "recent_edge_negative"
    if _drawdown_accelerating(trades, settings.initial_balance):
        return "drawdown_accelerating"
    return ""


def _tercile_stats(settings: BacktestSettings, bars: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    size = max(1, len(bars) // 3)
    payload: dict[str, dict[str, Any]] = {}
    for name, start, end in [
        ("tercile_1", 0, size),
        ("tercile_2", size, size * 2),
        ("tercile_3", size * 2, len(bars)),
    ]:
        scoped = [trade for trade in trades if start <= int(_number(trade.get("opened_index")) or 0) < end]
        summary = _metrics(scoped, initial_balance=settings.initial_balance)
        payload[name] = {
            "closed": summary["closed"],
            "profit_factor": summary["profit_factor"],
            "expectancy": summary["expectancy"],
            "max_drawdown": summary["max_drawdown"],
            "win_rate": summary["win_rate"],
        }
    return payload


def _windows_ok(terciles: dict[str, dict[str, Any]]) -> dict[str, bool]:
    active = [item for item in terciles.values() if int(item.get("closed") or 0) >= 10]
    if not active:
        return {"pf_positive": False, "expectancy_positive": False}
    return {
        "pf_positive": all(float(item.get("profit_factor") or 0.0) >= 1.0 for item in active),
        "expectancy_positive": all(float(item.get("expectancy") or 0.0) > 0 for item in active),
    }


def _fragile_regime_dependency(metrics: dict[str, Any], terciles: dict[str, dict[str, Any]]) -> bool:
    closed = int(metrics.get("closed") or 0)
    if closed < 10:
        return True
    counts = [int(item.get("closed") or 0) for item in terciles.values()]
    if counts and max(counts) > closed * 0.7:
        return True
    for item in terciles.values():
        if int(item.get("closed") or 0) >= 10 and (float(item.get("profit_factor") or 0.0) < 1.0 or float(item.get("expectancy") or 0.0) <= 0):
            return True
    return False


def _research_gate(
    metrics: dict[str, Any],
    terciles: dict[str, dict[str, Any]],
    monte_carlo: dict[str, Any],
    fragile: bool,
    single_trade: bool,
) -> dict[str, Any]:
    reasons: list[str] = []
    if int(metrics.get("closed") or 0) < 40:
        reasons.append("sample_too_small")
    if float(metrics.get("profit_factor") or 0.0) <= 1.15:
        reasons.append("pf_below_1_15")
    if float(metrics.get("expectancy") or 0.0) <= 0:
        reasons.append("expectancy_not_positive")
    if float(metrics.get("max_drawdown") or 0.0) > 5000:
        reasons.append("drawdown_above_5000")
    if fragile:
        reasons.append("fragile_regime_dependency")
    for name, item in terciles.items():
        if int(item.get("closed") or 0) >= 10 and float(item.get("profit_factor") or 0.0) < 1.0:
            reasons.append(f"{name}_pf_below_1")
        if int(item.get("closed") or 0) >= 10 and float(item.get("expectancy") or 0.0) <= 0:
            reasons.append(f"{name}_expectancy_not_positive")
    if not monte_carlo.get("passed"):
        reasons.extend([f"monte_carlo_{reason}" for reason in monte_carlo.get("fail_reasons") or []])
    if single_trade:
        reasons.append("single_trade_dependency")
    return {"passed": not reasons, "reasons": reasons or ["passes_research_v2_gates"]}


def _research_score(metrics: dict[str, Any], monte_carlo: dict[str, Any], gate: dict[str, Any], fragile: bool, single_trade: bool) -> float:
    closed = int(metrics.get("closed") or 0)
    pf = float(metrics.get("profit_factor") or 0.0)
    expectancy = float(metrics.get("expectancy") or 0.0)
    dd = float(metrics.get("max_drawdown") or 0.0)
    win_rate = float(metrics.get("win_rate") or 0.0)
    score = 0.0
    score += min(closed, 160) * 1.2
    score += max(0.0, min(pf, 2.5) - 1.0) * 90.0
    score += max(0.0, expectancy) * 350.0
    score += max(0.0, win_rate - 45.0) * 0.7
    score -= dd / 70.0
    score -= max(0.0, 40 - closed) * 4.0
    score -= max(0.0, 1.05 - float(monte_carlo.get("profit_factor_stressed") or 0.0)) * 110.0
    score -= max(0.0, float(monte_carlo.get("max_drawdown_p95") or 0.0) - 5000.0) / 40.0
    if fragile:
        score -= 90.0
    if single_trade:
        score -= 100.0
    if not gate.get("passed"):
        score -= len(gate.get("reasons") or []) * 12.0
    return round(score, 4)


def _observation_quality(metrics: dict[str, Any]) -> bool:
    return int(metrics.get("closed") or 0) >= 10 and float(metrics.get("profit_factor") or 0.0) > 1.0 and float(metrics.get("expectancy") or 0.0) > 0


def _summary(rows: list[dict[str, Any]], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"recommendation": "no_data", **_safety()}
    most_trades = max(rows, key=lambda item: int(item.get("closed") or 0))
    quality_pool = [row for row in rows if int(row.get("closed") or 0) >= 40]
    best_quality = max(quality_pool or rows, key=_research_rank)
    top_3 = rows[:3]
    return {
        "recommendation": "research_candidate" if candidates else "observation_only" if top_3 else "reject",
        "most_trades_answer": f"{most_trades.get('timeframe')} {most_trades.get('family')} with {most_trades.get('closed')} closed trades",
        "best_quality_answer": f"{best_quality.get('timeframe')} {best_quality.get('family')} PF {best_quality.get('profit_factor')} expectancy {best_quality.get('expectancy')}",
        "timeframe_answer": _best_bucket(rows, "timeframe"),
        "side_answer": _best_bucket(rows, "side_mode"),
        "session_answer": _best_bucket(rows, "session_filter"),
        "regime_answer": _best_bucket(rows, "trend_regime"),
        "monte_carlo_answer": _fragile_families(rows, "monte_carlo"),
        "sample_answer": _fragile_families(rows, "sample_too_small"),
        "top_3_answer": "; ".join(f"{row.get('timeframe')} {row.get('family')} {row.get('side_mode')}" for row in top_3),
        "automatic_promotion": False,
        **_safety(),
    }


def _best_bucket(rows: list[dict[str, Any]], key: str) -> str:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(str(row.get(key) or "unknown"), []).append(row)
    ranked = []
    for name, items in buckets.items():
        closed = sum(int(item.get("closed") or 0) for item in items)
        top_items = sorted(items, key=_research_rank, reverse=True)[:5]
        top_avg_score = sum(float(item.get("research_score") or 0.0) for item in top_items) / max(1, len(top_items))
        ranked.append((top_avg_score + min(closed, 200) * 0.05, name, closed))
    ranked.sort(reverse=True)
    return f"{ranked[0][1]} ({ranked[0][2]} closed across variants)" if ranked else "none"


def _fragile_families(rows: list[dict[str, Any]], reason: str) -> str:
    families = sorted(
        {
            str(row.get("family"))
            for row in rows
            if any(reason in str(item) for item in row.get("reject_reasons") or [])
        }
    )
    return ", ".join(families) if families else "none"


def _candidate_profile_name(row: dict[str, Any]) -> str:
    family = str(row.get("family") or "unknown").replace("_", "-")
    timeframe = str(row.get("timeframe") or "tf").lower()
    side = str(row.get("side_mode") or "both").lower()
    return f"research_v2_{family}_{timeframe}_{side}_candidate".replace("-", "_")


def _research_rank(row: dict[str, Any]) -> float:
    return float(row.get("research_score") or 0.0)


def _csv_path_for(body: dict[str, Any], csv_dir: Path, symbol: str, timeframe: str) -> Path:
    explicit = body.get(f"csv_path_{timeframe.lower()}") or body.get("csv_path")
    if explicit:
        return Path(str(explicit))
    suffix = "30000" if timeframe == "H1" else "20000"
    return csv_dir / f"{symbol}_{timeframe}_{suffix}.csv"


def _requested_list(value: Any, default: list[str]) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [part.strip() for part in value.split(",") if part.strip()]
    return list(default)

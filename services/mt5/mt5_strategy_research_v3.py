from __future__ import annotations

import csv
import json
import math
import time
from collections import defaultdict
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
    _monte_carlo_stress,
    _recent_edge_negative,
)
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_research_v2_candidate_robustness import (
    _atr_bucket_from_trade,
    _group_stats,
    _rsi_bucket_from_trade,
    _session_name,
    _trade_hour,
    _vol_bucket_from_trade,
)
from services.mt5.mt5_strategy_research_v2 import (
    ResearchVariant,
    _features_by_index,
    _family_signal,
    _open_research_trade,
    _update_research_trade,
    _volatility_bucket,
)


STRATEGY_RESEARCH_V3_FAMILIES = [
    "trend_pullback",
    "breakout_retest",
    "volatility_expansion",
    "mean_reversion_safe",
    "liquidity_sweep_confirmed",
    "range_breakout_anti_chop",
    "momentum_continuation_filtered",
    "session_open_reversal_safe",
    "volatility_compression_breakout",
    "ema_reclaim_pullback",
    "atr_expansion_reversal",
    "failed_breakdown_reversal",
]

STRATEGY_RESEARCH_V3_TIMEFRAMES = ["M15", "M30", "H1"]
_SESSION_HOURS: dict[str, set[int] | None] = {
    "all": None,
    "asia": set(range(0, 8)),
    "london_us": set(range(7, 21)),
    "ny_core": set(range(13, 21)),
}


@dataclass(frozen=True)
class ResearchV3Variant:
    family: str
    timeframe: str
    side_mode: str
    session_name: str
    volatility_regime: str
    trend_regime: str
    rsi_regime: str
    score_threshold: float
    risk_reward: float
    time_stop_bars: int
    atr_stop_multiplier: float
    mae_exit_r: float
    momentum_loss_exit: bool

    def key(self) -> str:
        return (
            f"{self.timeframe}|{self.family}|side={self.side_mode}|session={self.session_name}|"
            f"vol={self.volatility_regime}|regime={self.trend_regime}|rsi={self.rsi_regime}|"
            f"score={self.score_threshold}|rr={self.risk_reward}|ts={self.time_stop_bars}|"
            f"atr={self.atr_stop_multiplier}|mae={self.mae_exit_r}|mom={int(self.momentum_loss_exit)}"
        )

    def research_variant(self) -> ResearchVariant:
        return ResearchVariant(
            self.family,
            self.timeframe,
            self.side_mode,
            self.session_name,
            self.volatility_regime,
            self.trend_regime,
            self.score_threshold,
            self.risk_reward,
            self.time_stop_bars,
            self.atr_stop_multiplier,
            self.mae_exit_r,
            self.momentum_loss_exit,
        )


def run_strategy_research_v3(body: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    body = body or {}
    symbol = str(body.get("symbol") or "BTCUSD").upper().strip()
    csv_dir = Path(str(body.get("csv_dir") or Path("data") / "backtests"))
    timeframes = _requested_list(body.get("timeframes"), STRATEGY_RESEARCH_V3_TIMEFRAMES)
    families = [item for item in _requested_list(body.get("families"), STRATEGY_RESEARCH_V3_FAMILIES) if item in STRATEGY_RESEARCH_V3_FAMILIES]
    max_bars = max(200, min(int(_number(body.get("max_bars")) or 60000), 65000))
    max_evaluations = max(1, int(_number(body.get("max_evaluations")) or 180))
    timeout_seconds = max(0.25, float(_number(body.get("per_evaluation_timeout_seconds")) or 5.0))
    spread_points = float(_number(body.get("spread_points")) or 25.0)
    variants = _build_variants(timeframes, families, max_evaluations=max_evaluations)
    datasets = _datasets_for(body, csv_dir, symbol, timeframes, max_bars)
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[str] = []
    csv_paths: dict[str, str] = {}

    for dataset in datasets:
        label = dataset["sample_label"]
        timeframe = dataset["timeframe"]
        csv_path = Path(dataset["csv_path"])
        csv_paths[label] = str(csv_path)
        if not csv_path.exists():
            errors.append({"sample_label": label, "timeframe": timeframe, "path": str(csv_path), "error": "csv_not_found"})
            warnings.append(f"missing_csv:{csv_path}")
            continue
        settings_body = {
            "symbol": symbol,
            "timeframe": timeframe,
            "csv_path": str(csv_path),
            "max_bars": min(max_bars, int(dataset["max_bars"])),
            "spread_points": spread_points,
            "save_results": False,
            "source": "mt5_csv",
            "timeout_seconds": timeout_seconds,
        }
        settings = replace(
            _settings(settings_body, get_mt5_config()),
            max_bars=min(max_bars, int(dataset["max_bars"])),
            timeout_seconds=timeout_seconds,
        )
        bars, load_warnings = _load_bars(settings_body, settings)
        warnings.extend([f"{label}:{warning}" for warning in load_warnings])
        bars = bars[-settings.max_bars :]
        if not bars:
            errors.append({"sample_label": label, "timeframe": timeframe, "path": str(csv_path), "error": "csv_bars_not_loaded"})
            continue
        features_by_index = _features_by_index(bars)
        for variant in [item for item in variants if item.timeframe == timeframe]:
            rows.append(
                evaluate_strategy_research_v3_variant(
                    settings,
                    bars,
                    variant,
                    sample_label=label,
                    source_csv=str(csv_path),
                    features_by_index=features_by_index,
                    timeout_seconds=timeout_seconds,
                )
            )

    _apply_cross_sample_recent_guard(rows)
    rows.sort(key=_research_v3_rank, reverse=True)
    candidates = [row for row in rows if row.get("candidate")]
    result = {
        "ok": True,
        "status": "mt5_strategy_research_v3_completed",
        "symbol": symbol,
        "mode": "paper",
        "timeframes": timeframes,
        "families": families,
        "csv_paths": csv_paths,
        "evaluations": len(rows),
        "results": rows,
        "candidates": candidates,
        "top_3_for_capital_optimizer": candidates[:3],
        "candidate_profile_names": [_candidate_profile_name(row) for row in candidates[:3]],
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


def evaluate_strategy_research_v3_variant(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    variant: ResearchV3Variant,
    *,
    sample_label: str,
    source_csv: str,
    features_by_index: dict[int, dict[str, Any]],
    timeout_seconds: float,
) -> dict[str, Any]:
    started = time.monotonic()
    trades, blocked, signals, state = _simulate_v3(settings, bars, variant, started, timeout_seconds=timeout_seconds, features_by_index=features_by_index)
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    total = _metrics(closed, initial_balance=settings.initial_balance)
    split = _split_metrics(settings, bars, closed)
    rolling = _rolling_metrics(settings, bars, closed)
    monte_carlo = _monte_carlo_stress(closed, initial_balance=settings.initial_balance, max_drawdown_limit=5000.0, simulations=400)
    fragile = _fragile_dependency(total, split, rolling)
    single_trade = _depends_on_single_trade(total)
    gate = _gate(total, split, rolling, monte_carlo, fragile, single_trade)
    score = _score_v3(total, split, rolling, monte_carlo, gate, fragile, single_trade)
    side_stats = _group_stats(settings, closed, lambda trade: str(trade.get("side") or "unknown").lower())
    session_stats = _group_stats(settings, closed, lambda trade: _session_name(_trade_hour(trade)))
    regime_stats = _group_stats(settings, closed, lambda trade: str(trade.get("regime") or "unknown"))
    hour_stats = _group_stats(settings, closed, lambda trade: str(_trade_hour(trade)))
    volatility_stats = _group_stats(settings, closed, lambda trade: _vol_bucket_from_trade(trade))
    atr_stats = _group_stats(settings, closed, lambda trade: _atr_bucket_from_trade(trade))
    rsi_stats = _group_stats(settings, closed, lambda trade: _rsi_bucket_from_trade(trade))
    return {
        "sample_label": sample_label,
        "source_csv": source_csv,
        "csv_path_used": source_csv,
        "family": variant.family,
        "profile": variant.family,
        "candidate_profile_name": _candidate_profile_name(
            {
                "family": variant.family,
                "timeframe": variant.timeframe,
                "side_mode": variant.side_mode,
                "session_filter": variant.session_name,
            }
        ),
        "variant_id": variant.key(),
        "timeframe": variant.timeframe,
        "side": variant.side_mode,
        "side_mode": variant.side_mode,
        "session": variant.session_name,
        "session_filter": variant.session_name,
        "regime": variant.trend_regime,
        "trend_regime": variant.trend_regime,
        "volatility_regime": variant.volatility_regime,
        "rsi_regime": variant.rsi_regime,
        "score_threshold": variant.score_threshold,
        "risk_reward": variant.risk_reward,
        "time_stop_bars": variant.time_stop_bars,
        "mae_exit_r": variant.mae_exit_r,
        "momentum_loss_exit": variant.momentum_loss_exit,
        "bars_loaded": len(bars),
        "bars_evaluated": max(0, len(bars) - 80),
        "first_bar_time": str(bars[0].get("time") or "") if bars else "",
        "last_bar_time": str(bars[-1].get("time") or "") if bars else "",
        "generated_signal_count": signals.get("generated", 0),
        "actionable_signal_count": signals.get("actionable", 0),
        "opened_trade_count": len(trades),
        "closed_total": total["closed"],
        "wins_total": total["wins"],
        "losses_total": total["losses"],
        "win_rate_total": total["win_rate"],
        "profit_factor_total": total["profit_factor"],
        "expectancy_total": total["expectancy"],
        "max_drawdown_total": total["max_drawdown"],
        "closed_train": split["train"]["closed"],
        "closed_validation": split["validation"]["closed"],
        "closed_recent_holdout": split["recent_holdout"]["closed"],
        "pf_train": split["train"]["profit_factor"],
        "pf_validation": split["validation"]["profit_factor"],
        "pf_recent_holdout": split["recent_holdout"]["profit_factor"],
        "expectancy_train": split["train"]["expectancy"],
        "expectancy_validation": split["validation"]["expectancy"],
        "expectancy_recent_holdout": split["recent_holdout"]["expectancy"],
        "max_drawdown_train": split["train"]["max_drawdown"],
        "max_drawdown_validation": split["validation"]["max_drawdown"],
        "max_drawdown_recent_holdout": split["recent_holdout"]["max_drawdown"],
        "win_rate_train": split["train"]["win_rate"],
        "win_rate_validation": split["validation"]["win_rate"],
        "win_rate_recent_holdout": split["recent_holdout"]["win_rate"],
        "split_stats": split,
        "rolling_window_pf_min": rolling["pf_min"],
        "rolling_window_expectancy_min": rolling["expectancy_min"],
        "rolling_window_drawdown_max": rolling["drawdown_max"],
        "rolling_windows": rolling["windows"],
        "monte_carlo_stressed_pf": monte_carlo.get("profit_factor_stressed", 0.0),
        "monte_carlo_p95_drawdown": monte_carlo.get("max_drawdown_p95", 0.0),
        "monte_carlo_stressed_expectancy": monte_carlo.get("expectancy_stressed", 0.0),
        "monte_carlo_fail_reasons": list(monte_carlo.get("fail_reasons") or []),
        "fragile_regime_dependency": fragile,
        "single_trade_dependency": single_trade,
        "edge_concentrated_old_window": _edge_concentrated_old_window(total, split),
        "side_stats": side_stats,
        "session_stats": session_stats,
        "regime_stats": regime_stats,
        "hour_stats": hour_stats,
        "volatility_stats": volatility_stats,
        "atr_regime_stats": atr_stats,
        "rsi_regime_stats": rsi_stats,
        "exit_reason_counts": total["exit_reason_counts"],
        "blocked_reason_counts": _reason_counts(blocked),
        "risk_governor_blocks": state.get("risk_governor_blocks", 0),
        "max_open_trades_observed": state.get("max_open_trades_observed", 0),
        "candidate": gate["passed"],
        "recommendation": "research_candidate" if gate["passed"] else "observation_only" if _observation_quality(total, split) else "reject",
        "rejection_reasons": gate["reasons"],
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


def write_strategy_research_v3_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "strategy_research_v3_results.csv"
    json_path = root / "strategy_research_v3_results.json"
    summary_path = root / "strategy_research_v3_summary.md"
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    headers = [
        "sample_label",
        "timeframe",
        "family",
        "profile",
        "side",
        "session",
        "regime",
        "volatility_regime",
        "rsi_regime",
        "closed_total",
        "closed_train",
        "closed_validation",
        "closed_recent_holdout",
        "profit_factor_total",
        "pf_train",
        "pf_validation",
        "pf_recent_holdout",
        "expectancy_total",
        "expectancy_train",
        "expectancy_validation",
        "expectancy_recent_holdout",
        "max_drawdown_total",
        "max_drawdown_train",
        "max_drawdown_validation",
        "max_drawdown_recent_holdout",
        "win_rate_total",
        "win_rate_train",
        "win_rate_validation",
        "win_rate_recent_holdout",
        "rolling_window_pf_min",
        "rolling_window_expectancy_min",
        "rolling_window_drawdown_max",
        "monte_carlo_stressed_pf",
        "monte_carlo_p95_drawdown",
        "fragile_regime_dependency",
        "single_trade_dependency",
        "edge_concentrated_old_window",
        "candidate",
        "recommendation",
        "rejection_reasons",
        "research_score",
        "variant_id",
        "source_csv",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({**row, "rejection_reasons": ";".join(str(item) for item in row.get("rejection_reasons") or [])})
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")
    summary_path.write_text(strategy_research_v3_summary_markdown(result), encoding="utf-8")
    return csv_path, json_path, summary_path


def strategy_research_v3_summary_markdown(result: dict[str, Any]) -> str:
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else _summary(rows, [])
    lines = [
        "# MT5 Strategy Research V3 Summary",
        "",
        "Recent-robust walk-forward signal discovery. Paper/offline only; no broker, no order execution, no automatic promotion.",
        "",
        f"Evaluations: `{result.get('evaluations', len(rows))}`.",
        f"Candidates: `{len(result.get('candidates') or [])}`.",
        "",
        "## Top 20",
    ]
    for row in rows[:20]:
        lines.append(
            f"- `{row.get('sample_label')}` `{row.get('timeframe')}` `{row.get('family')}` side `{row.get('side')}` session `{row.get('session')}` "
            f"closed `{row.get('closed_total')}` recent `{row.get('closed_recent_holdout')}`, PF `{row.get('profit_factor_total')}`, "
            f"recent PF `{row.get('pf_recent_holdout')}`, expectancy `{row.get('expectancy_total')}`, recent exp `{row.get('expectancy_recent_holdout')}`, "
            f"MC PF `{row.get('monte_carlo_stressed_pf')}`, recommendation `{row.get('recommendation')}`."
        )
    lines.extend(
        [
            "",
            "## Answers",
            f"1. Families surviving recent holdout: {summary.get('recent_survivors_answer')}",
            f"2. Historically good but recent-failing families: {summary.get('history_good_recent_bad_answer')}",
            f"3. Best timeframe balance: {summary.get('timeframe_answer')}",
            f"4. Best side: {summary.get('side_answer')}",
            f"5. Best session/hour: {summary.get('session_answer')}",
            f"6. Stable regime: {summary.get('regime_answer')}",
            f"7. Monte Carlo failures: {summary.get('monte_carlo_fail_answer')}",
            f"8. Sample failures: {summary.get('sample_fail_answer')}",
            f"9. Old-window concentration failures: {summary.get('old_window_answer')}",
            f"10. Top 3 for capital preservation optimizer: {summary.get('top_3_answer')}",
            "11. No automatic promotion.",
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


def _simulate_v3(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    variant: ResearchV3Variant,
    started: float,
    *,
    timeout_seconds: float,
    features_by_index: dict[int, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str], dict[str, int], dict[str, int]]:
    trades: list[dict[str, Any]] = []
    blocked: list[str] = []
    open_trade: dict[str, Any] | None = None
    cooldown_until = -1
    signals = {"generated": 0, "actionable": 0}
    state = {"risk_governor_blocks": 0, "max_open_trades_observed": 0}
    research_variant = variant.research_variant()
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
            open_trade, closed = _update_research_trade(settings, open_trade, bar, index, research_variant)
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
        risk_reason = _research_risk_block(settings, trades)
        if risk_reason:
            state["risk_governor_blocks"] += 1
            blocked.append(f"risk_governor_{risk_reason}")
            continue
        features = features_by_index.get(index - 1)
        if not features:
            blocked.append("insufficient_history")
            continue
        decision = _decision_for_v3(features, variant)
        if decision.get("generated"):
            signals["generated"] += 1
        if not decision.get("actionable"):
            blocked.append(str(decision.get("reason") or "no_signal"))
            continue
        signals["actionable"] += 1
        open_trade = _open_research_trade(settings, decision, bars[index], index, research_variant)
        if open_trade is None:
            blocked.append("missing_risk_parameters")
            continue
        open_trade = {
            **open_trade,
            "shadow_trade_id": f"research-v3-{variant.family}-{variant.timeframe}-{index}",
            "source": "mt5_strategy_research_v3",
            "strategy_profile": variant.family,
            "filter_profile": variant.key(),
            **_safety(),
        }
        features_snapshot = open_trade.get("features_snapshot") if isinstance(open_trade.get("features_snapshot"), dict) else {}
        open_trade["features_snapshot"] = {**features_snapshot, "research_v3": True, "rsi_regime": variant.rsi_regime}
        state["max_open_trades_observed"] = max(state["max_open_trades_observed"], 1)
    if open_trade:
        trades.append(_close(settings, open_trade, float(_number(bars[-1].get("close")) or open_trade.get("entry_price") or 0.0), "time_stop", bars[-1]))
    return trades, blocked, signals, state


def _decision_for_v3(features: dict[str, Any], variant: ResearchV3Variant) -> dict[str, Any]:
    if not _session_allowed(features, variant):
        return {"actionable": False, "generated": False, "reason": "session_filter"}
    if not _volatility_allowed(features, variant):
        return {"actionable": False, "generated": False, "reason": "volatility_regime_filter"}
    if not _trend_regime_allowed(features, variant):
        return {"actionable": False, "generated": False, "reason": "trend_regime_filter"}
    if not _rsi_allowed(features, variant):
        return {"actionable": False, "generated": False, "reason": "rsi_regime_filter"}
    side, score, reason = _family_signal_v3(features, variant.family)
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


def _family_signal_v3(features: dict[str, Any], family: str) -> tuple[str, float, str]:
    if family in {
        "trend_pullback",
        "breakout_retest",
        "volatility_expansion",
        "mean_reversion_safe",
        "liquidity_sweep_confirmed",
        "range_breakout_anti_chop",
        "momentum_continuation_filtered",
        "session_open_reversal_safe",
    }:
        return _family_signal(features, family)
    close = float(features["close"])
    prev_close = float(features["prev_close"])
    open_price = float(features["open"])
    high = float(features["high"])
    low = float(features["low"])
    ema20 = float(features["ema20"])
    ema50 = float(features["ema50"])
    rsi = float(features["rsi"])
    atr = max(float(features["atr"]), 0.000001)
    trend_score = float(features["trend_score"])
    momentum_score = float(features["momentum_score"])
    volatility_score = float(features["volatility_score"])
    body_ratio = float(features["body_ratio"])
    distance20 = float(features["distance20_atr"])
    base_trend = trend_score if close >= ema20 else 100.0 - trend_score
    base_momentum = momentum_score if close >= prev_close else 100.0 - momentum_score
    score = base_trend * 0.40 + base_momentum * 0.38 + volatility_score * 0.22
    recent_high = float(features["recent_high"])
    recent_low = float(features["recent_low"])
    previous_range = max(float(features["previous_range"]), atr)
    recent_range = float(features["recent_range"])
    compressed = recent_range <= max(previous_range * 0.78, atr * 1.05)
    if family == "volatility_compression_breakout":
        if compressed and close > recent_high and momentum_score >= 56 and rsi < 76:
            return "buy", score + 5.0, "volatility_compression_breakout_buy"
        if compressed and close < recent_low and momentum_score <= 44 and rsi > 24:
            return "sell", score + 5.0, "volatility_compression_breakout_sell"
        return "", score, "volatility_compression_not_confirmed"
    if family == "ema_reclaim_pullback":
        reclaim_buy = low <= ema20 and close > ema20 and close > open_price and ema20 >= ema50 and rsi < 72
        reclaim_sell = high >= ema20 and close < ema20 and close < open_price and ema20 <= ema50 and rsi > 28
        if reclaim_buy and distance20 <= 1.4:
            return "buy", score + 4.0, "ema_reclaim_pullback_buy"
        if reclaim_sell and distance20 <= 1.4:
            return "sell", score + 4.0, "ema_reclaim_pullback_sell"
        return "", score, "ema_reclaim_not_confirmed"
    if family == "atr_expansion_reversal":
        expanded = recent_range >= previous_range * 1.1 and float(features["body_atr"]) >= 0.55
        if expanded and low < recent_low and close > prev_close and rsi <= 45:
            return "buy", 54.0 + body_ratio * 18.0 + volatility_score * 0.15, "atr_expansion_reversal_buy"
        if expanded and high > recent_high and close < prev_close and rsi >= 55:
            return "sell", 54.0 + body_ratio * 18.0 + volatility_score * 0.15, "atr_expansion_reversal_sell"
        return "", score, "atr_expansion_reversal_not_confirmed"
    if family == "failed_breakdown_reversal":
        failed_breakdown = low < recent_low and close > recent_low and close > open_price and rsi <= 48
        failed_breakout = high > recent_high and close < recent_high and close < open_price and rsi >= 52
        reversal_score = 55.0 + body_ratio * 22.0 + min(distance20 * 3.0, 8.0)
        if failed_breakdown:
            return "buy", reversal_score, "failed_breakdown_reversal_buy"
        if failed_breakout:
            return "sell", reversal_score, "failed_breakout_reversal_sell"
        return "", reversal_score, "failed_breakout_breakdown_not_confirmed"
    return "", score, "unknown_family"


def _session_allowed(features: dict[str, Any], variant: ResearchV3Variant) -> bool:
    hours = _SESSION_HOURS.get(variant.session_name)
    if hours is None:
        return True
    hour = features.get("hour")
    return hour is not None and int(hour) in hours


def _volatility_allowed(features: dict[str, Any], variant: ResearchV3Variant) -> bool:
    bucket = _volatility_bucket(float(features.get("volatility_score") or 0.0))
    if variant.volatility_regime == "any":
        return True
    if variant.volatility_regime == "normal_high":
        return bucket in {"normal", "high"}
    return bucket == variant.volatility_regime


def _trend_regime_allowed(features: dict[str, Any], variant: ResearchV3Variant) -> bool:
    regime = str(features.get("regime") or "unknown").casefold()
    if variant.trend_regime == "any":
        return True
    if variant.trend_regime == "range":
        return regime == "chop"
    return regime == variant.trend_regime


def _rsi_allowed(features: dict[str, Any], variant: ResearchV3Variant) -> bool:
    rsi = float(features.get("rsi") or 50.0)
    if variant.rsi_regime == "any":
        return True
    if variant.rsi_regime == "neutral":
        return 35.0 <= rsi <= 65.0
    if variant.rsi_regime == "not_extreme":
        return 25.0 <= rsi <= 75.0
    if variant.rsi_regime == "low":
        return rsi <= 45.0
    if variant.rsi_regime == "high":
        return rsi >= 55.0
    return True


def _research_risk_block(settings: BacktestSettings, trades: list[dict[str, Any]]) -> str:
    if settings.spread_points > 30:
        return "spread_too_high"
    if _loss_streak(trades) >= 4:
        return "consecutive_loss_lockdown"
    if len([trade for trade in trades if trade.get("lifecycle_status") == "closed"]) >= 20 and _recent_edge_negative(trades):
        return "recent_edge_negative"
    if _drawdown_accelerating(trades, settings.initial_balance):
        return "drawdown_accelerating"
    return ""


def _split_metrics(settings: BacktestSettings, bars: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    n = len(bars)
    return {
        "train": _compact_metrics(_trades_between(trades, 0, int(n * 0.50)), settings.initial_balance),
        "validation": _compact_metrics(_trades_between(trades, int(n * 0.50), int(n * 0.75)), settings.initial_balance),
        "recent_holdout": _compact_metrics(_trades_between(trades, int(n * 0.75), n), settings.initial_balance),
    }


def _rolling_metrics(settings: BacktestSettings, bars: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, Any]:
    windows: dict[str, dict[str, dict[str, Any]]] = {}
    active: list[dict[str, Any]] = []
    for parts in (4, 6, 8):
        size = max(1, len(bars) // parts)
        group: dict[str, dict[str, Any]] = {}
        for part in range(parts):
            start = part * size
            end = len(bars) if part == parts - 1 else (part + 1) * size
            metrics = _compact_metrics(_trades_between(trades, start, end), settings.initial_balance)
            group[f"w{parts}_{part + 1}"] = metrics
            if int(metrics.get("closed") or 0) >= 5:
                active.append(metrics)
        windows[f"{parts}_windows"] = group
    if not active:
        return {"windows": windows, "pf_min": 0.0, "expectancy_min": 0.0, "drawdown_max": 0.0}
    return {
        "windows": windows,
        "pf_min": round(min(float(item.get("profit_factor") or 0.0) for item in active), 4),
        "expectancy_min": round(min(float(item.get("expectancy") or 0.0) for item in active), 4),
        "drawdown_max": round(max(float(item.get("max_drawdown") or 0.0) for item in active), 6),
    }


def _trades_between(trades: list[dict[str, Any]], start: int, end: int) -> list[dict[str, Any]]:
    return [trade for trade in trades if start <= int(_number(trade.get("opened_index")) or 0) < end]


def _compact_metrics(trades: list[dict[str, Any]], initial_balance: float) -> dict[str, Any]:
    summary = _metrics(trades, initial_balance=initial_balance)
    return {
        "closed": summary["closed"],
        "wins": summary["wins"],
        "losses": summary["losses"],
        "win_rate": summary["win_rate"],
        "profit_factor": summary["profit_factor"],
        "expectancy": summary["expectancy"],
        "max_drawdown": summary["max_drawdown"],
    }


def _gate(
    total: dict[str, Any],
    split: dict[str, dict[str, Any]],
    rolling: dict[str, Any],
    monte_carlo: dict[str, Any],
    fragile: bool,
    single_trade: bool,
) -> dict[str, Any]:
    reasons: list[str] = []
    recent = split["recent_holdout"]
    if int(total.get("closed") or 0) < 40:
        reasons.append("sample_too_small")
    if int(recent.get("closed") or 0) < 10:
        reasons.append("recent_holdout_sample_too_small")
    if float(total.get("profit_factor") or 0.0) <= 1.15:
        reasons.append("pf_below_1_15")
    if float(total.get("expectancy") or 0.0) <= 0:
        reasons.append("expectancy_not_positive")
    if float(recent.get("profit_factor") or 0.0) < 1.05:
        reasons.append("recent_holdout_pf_below_1_05")
    if float(recent.get("expectancy") or 0.0) <= 0:
        reasons.append("recent_holdout_expectancy_not_positive")
    if float(total.get("max_drawdown") or 0.0) > 5000:
        reasons.append("drawdown_above_5000")
    if float(monte_carlo.get("profit_factor_stressed") or 0.0) < 1.05:
        reasons.append("monte_carlo_stressed_pf_below_1_05")
    if float(monte_carlo.get("max_drawdown_p95") or 0.0) > 5000:
        reasons.append("monte_carlo_p95_drawdown_above_5000")
    if float(monte_carlo.get("expectancy_stressed") or 0.0) < 0:
        reasons.append("monte_carlo_stressed_expectancy_negative")
    if float(rolling.get("pf_min") or 0.0) < 1.0:
        reasons.append("rolling_window_pf_below_1")
    if float(rolling.get("expectancy_min") or 0.0) <= -0.05:
        reasons.append("rolling_window_expectancy_negative")
    if fragile:
        reasons.append("fragile_regime_dependency")
    if single_trade:
        reasons.append("single_trade_dependency")
    return {"passed": not reasons, "reasons": reasons or ["passes_research_v3_gates"]}


def _fragile_dependency(total: dict[str, Any], split: dict[str, dict[str, Any]], rolling: dict[str, Any]) -> bool:
    closed = int(total.get("closed") or 0)
    if closed < 10:
        return True
    train_closed = int(split["train"].get("closed") or 0)
    recent_closed = int(split["recent_holdout"].get("closed") or 0)
    if train_closed > closed * 0.70:
        return True
    if recent_closed < max(5, closed * 0.15):
        return True
    if int(split["recent_holdout"].get("closed") or 0) >= 5 and float(split["recent_holdout"].get("expectancy") or 0.0) <= 0:
        return True
    if float(rolling.get("pf_min") or 0.0) < 1.0 and closed >= 40:
        return True
    return False


def _edge_concentrated_old_window(total: dict[str, Any], split: dict[str, dict[str, Any]]) -> bool:
    closed = int(total.get("closed") or 0)
    return closed > 0 and int(split["train"].get("closed") or 0) > closed * 0.65


def _score_v3(
    total: dict[str, Any],
    split: dict[str, dict[str, Any]],
    rolling: dict[str, Any],
    monte_carlo: dict[str, Any],
    gate: dict[str, Any],
    fragile: bool,
    single_trade: bool,
) -> float:
    closed = int(total.get("closed") or 0)
    recent = split["recent_holdout"]
    recent_closed = int(recent.get("closed") or 0)
    pf = min(float(total.get("profit_factor") or 0.0), 3.0 if closed >= 10 else 1.5)
    recent_pf = min(float(recent.get("profit_factor") or 0.0), 3.0 if recent_closed >= 5 else 1.5)
    monte_carlo_pf = min(float(monte_carlo.get("profit_factor_stressed") or 0.0), 3.0 if closed >= 10 else 1.5)
    score = 0.0
    score += min(closed, 160) * 1.0
    score += min(recent_closed, 60) * 2.0
    score += max(0.0, pf - 1.0) * 70.0
    score += max(0.0, recent_pf - 1.0) * 90.0
    score += max(0.0, float(total.get("expectancy") or 0.0)) * 250.0
    score += max(0.0, float(recent.get("expectancy") or 0.0)) * 300.0
    score += max(0.0, monte_carlo_pf - 1.0) * 90.0
    score -= float(total.get("max_drawdown") or 0.0) / 80.0
    score -= max(0.0, 1.0 - float(rolling.get("pf_min") or 0.0)) * 90.0
    score -= max(0, 40 - closed) * 5.0
    score -= max(0, 10 - recent_closed) * 8.0
    if fragile:
        score -= 120.0
    if single_trade:
        score -= 130.0
    if not gate.get("passed"):
        score -= len(gate.get("reasons") or []) * 12.0
    return round(score, 4)


def _observation_quality(total: dict[str, Any], split: dict[str, dict[str, Any]]) -> bool:
    return (
        int(total.get("closed") or 0) >= 10
        and float(total.get("profit_factor") or 0.0) > 1.0
        and float(total.get("expectancy") or 0.0) > 0
        and float(split["recent_holdout"].get("expectancy") or 0.0) > -0.05
    )


def _build_variants(timeframes: list[str], families: list[str], *, max_evaluations: int) -> list[ResearchV3Variant]:
    variants_by_timeframe: dict[str, list[ResearchV3Variant]] = {timeframe: [] for timeframe in timeframes}
    for timeframe in timeframes:
        for family in families:
            defaults = _family_defaults(family, timeframe)
            for side in defaults["sides"]:
                for session in defaults["sessions"]:
                    for vol in defaults["volatility"]:
                        for rsi in defaults["rsi"]:
                            variants_by_timeframe[timeframe].append(
                                ResearchV3Variant(
                                    family=family,
                                    timeframe=timeframe,
                                    side_mode=side,
                                    session_name=session,
                                    volatility_regime=vol,
                                    trend_regime=defaults["trend_regime"],
                                    rsi_regime=rsi,
                                    score_threshold=defaults["score"],
                                    risk_reward=defaults["rr"],
                                    time_stop_bars=defaults["time_stop"],
                                    atr_stop_multiplier=defaults["atr_stop"],
                                    mae_exit_r=defaults["mae_exit"],
                                    momentum_loss_exit=True,
                                )
                            )
    per_timeframe = max(1, math.ceil(max_evaluations / max(1, len(timeframes))))
    selected: list[ResearchV3Variant] = []
    for timeframe in timeframes:
        unique: dict[str, ResearchV3Variant] = {}
        for variant in variants_by_timeframe.get(timeframe, []):
            unique.setdefault(variant.key(), variant)
        by_family: dict[str, list[ResearchV3Variant]] = defaultdict(list)
        for variant in unique.values():
            by_family[variant.family].append(variant)
        for bucket in by_family.values():
            bucket.sort(key=lambda item: (item.session_name, item.side_mode, item.rsi_regime, item.volatility_regime))
        timeframe_selected: list[ResearchV3Variant] = []
        while len(timeframe_selected) < per_timeframe and any(by_family.values()):
            for family in families:
                bucket = by_family.get(family) or []
                if not bucket:
                    continue
                timeframe_selected.append(bucket.pop(0))
                if len(timeframe_selected) >= per_timeframe:
                    break
        selected.extend(timeframe_selected)
    return selected[:max_evaluations]


def _family_defaults(family: str, timeframe: str) -> dict[str, Any]:
    base = {
        "sides": ["both", "buy", "sell"],
        "sessions": ["all", "london_us"],
        "volatility": ["any", "normal_high"],
        "rsi": ["any"],
        "trend_regime": "any",
        "score": 57.0,
        "rr": 1.1,
        "time_stop": 2 if timeframe != "H1" else 3,
        "atr_stop": 1.0,
        "mae_exit": 0.85,
    }
    if family in {"trend_pullback", "momentum_continuation_filtered", "ema_reclaim_pullback"}:
        base.update({"trend_regime": "trend", "score": 58.0, "rr": 1.15, "rsi": ["any", "not_extreme"]})
    elif family in {"mean_reversion_safe", "liquidity_sweep_confirmed", "session_open_reversal_safe", "atr_expansion_reversal", "failed_breakdown_reversal"}:
        base.update({"trend_regime": "chop", "score": 55.0, "rr": 1.0, "sessions": ["all", "asia", "london_us"], "mae_exit": 0.8, "rsi": ["any", "not_extreme"]})
    elif family in {"breakout_retest", "range_breakout_anti_chop", "volatility_expansion", "volatility_compression_breakout"}:
        base.update({"trend_regime": "any", "score": 57.0, "rr": 1.15, "volatility": ["normal_high"], "sessions": ["all", "london_us"]})
    return base


def _datasets_for(body: dict[str, Any], csv_dir: Path, symbol: str, timeframes: list[str], max_bars: int) -> list[dict[str, Any]]:
    datasets: list[dict[str, Any]] = []
    for timeframe in timeframes:
        if timeframe == "M30":
            for suffix in ("60000", "40000"):
                key = f"csv_path_m30_{suffix}"
                datasets.append(
                    {
                        "sample_label": f"M30_{suffix}",
                        "timeframe": "M30",
                        "csv_path": str(body.get(key) or csv_dir / f"{symbol}_M30_{suffix}.csv"),
                        "max_bars": min(max_bars, int(suffix)),
                    }
                )
            continue
        suffix = "20000" if timeframe == "M15" else "30000"
        datasets.append(
            {
                "sample_label": f"{timeframe}_{suffix}",
                "timeframe": timeframe,
                "csv_path": str(body.get(f"csv_path_{timeframe.lower()}") or csv_dir / f"{symbol}_{timeframe}_{suffix}.csv"),
                "max_bars": min(max_bars, int(suffix)),
            }
        )
    return datasets


def _apply_cross_sample_recent_guard(rows: list[dict[str, Any]]) -> None:
    failed_recent_keys = {
        _cross_sample_key(row)
        for row in rows
        if str(row.get("sample_label") or "") == "M30_40000"
        and (
            float(row.get("pf_recent_holdout") or 0.0) < 1.05
            or float(row.get("expectancy_recent_holdout") or 0.0) <= 0.0
            or float(row.get("profit_factor_total") or 0.0) <= 1.0
            or float(row.get("expectancy_total") or 0.0) <= 0.0
        )
    }
    for row in rows:
        if row.get("timeframe") == "M30" and _cross_sample_key(row) in failed_recent_keys:
            reasons = list(row.get("rejection_reasons") or row.get("reject_reasons") or [])
            if "recent_40000_failed" not in reasons:
                reasons.append("recent_40000_failed")
            row["candidate"] = False
            row["recommendation"] = "reject" if row.get("sample_label") == "M30_40000" else "observation_only"
            row["rejection_reasons"] = reasons
            row["reject_reasons"] = reasons
            row["research_score"] = round(float(row.get("research_score") or 0.0) - 80.0, 4)


def _cross_sample_key(row: dict[str, Any]) -> str:
    return "|".join(
        str(row.get(key) or "")
        for key in [
            "family",
            "timeframe",
            "side_mode",
            "session_filter",
            "volatility_regime",
            "trend_regime",
            "rsi_regime",
            "score_threshold",
            "risk_reward",
            "time_stop_bars",
            "mae_exit_r",
        ]
    )


def _summary(rows: list[dict[str, Any]], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"recommendation": "no_data", **_safety()}
    recent_survivors = [
        row
        for row in rows
        if int(row.get("closed_recent_holdout") or 0) >= 10
        and float(row.get("pf_recent_holdout") or 0.0) >= 1.05
        and float(row.get("expectancy_recent_holdout") or 0.0) > 0
    ]
    history_good_recent_bad = [
        row
        for row in rows
        if int(row.get("closed_total") or 0) >= 40
        and float(row.get("profit_factor_total") or 0.0) > 1.15
        and float(row.get("expectancy_total") or 0.0) > 0
        and (
            float(row.get("pf_recent_holdout") or 0.0) < 1.05
            or float(row.get("expectancy_recent_holdout") or 0.0) <= 0
            or "recent_40000_failed" in (row.get("rejection_reasons") or [])
        )
    ]
    return {
        "recommendation": "research_candidate" if candidates else "observation_only" if recent_survivors else "reject",
        "recent_survivors_answer": _family_list(recent_survivors),
        "history_good_recent_bad_answer": _family_list(history_good_recent_bad),
        "timeframe_answer": _best_bucket(rows, "timeframe"),
        "side_answer": _best_bucket(rows, "side_mode"),
        "session_answer": _best_bucket(rows, "session_filter"),
        "regime_answer": _best_bucket(rows, "trend_regime"),
        "monte_carlo_fail_answer": _reason_family_list(rows, "monte_carlo"),
        "sample_fail_answer": _reason_family_list(rows, "sample_too_small"),
        "old_window_answer": _family_list([row for row in rows if row.get("edge_concentrated_old_window") or "recent_40000_failed" in (row.get("rejection_reasons") or [])]),
        "top_3_answer": "; ".join(_candidate_profile_name(row) for row in candidates[:3]) if candidates else "none; no profile should pass to capital preservation optimizer",
        "automatic_promotion": False,
        **_safety(),
    }


def _family_list(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "none"
    names = []
    for row in sorted(rows, key=_research_v3_rank, reverse=True)[:8]:
        names.append(
            f"{row.get('sample_label')} {row.get('timeframe')} {row.get('family')} {row.get('side_mode')} {row.get('session_filter')} "
            f"closed={row.get('closed_total')} recent={row.get('closed_recent_holdout')}"
        )
    return "; ".join(names)


def _best_bucket(rows: list[dict[str, Any]], key: str) -> str:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(str(row.get(key) or "unknown"), []).append(row)
    ranked = []
    for name, items in buckets.items():
        closed = sum(int(item.get("closed_total") or 0) for item in items)
        recent = sum(int(item.get("closed_recent_holdout") or 0) for item in items)
        top_score = sum(float(item.get("research_score") or 0.0) for item in sorted(items, key=_research_v3_rank, reverse=True)[:5]) / max(1, min(5, len(items)))
        ranked.append((top_score + min(recent, 120) * 0.5 + min(closed, 240) * 0.05, name, closed, recent))
    ranked.sort(reverse=True)
    return f"{ranked[0][1]} ({ranked[0][2]} closed, {ranked[0][3]} recent closed across variants)" if ranked else "none"


def _reason_family_list(rows: list[dict[str, Any]], reason: str) -> str:
    families = sorted(
        {
            str(row.get("family"))
            for row in rows
            if any(reason in str(item) for item in row.get("rejection_reasons") or [])
        }
    )
    return ", ".join(families) if families else "none"


def _candidate_profile_name(row: dict[str, Any]) -> str:
    family = str(row.get("family") or "unknown").replace("_", "-")
    timeframe = str(row.get("timeframe") or "tf").lower()
    side = str(row.get("side_mode") or row.get("side") or "both").lower()
    return f"research_v3_{family}_{timeframe}_{side}_candidate".replace("-", "_")


def _research_v3_rank(row: dict[str, Any]) -> float:
    return float(row.get("research_score") or 0.0)


def _requested_list(value: Any, default: list[str]) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [part.strip() for part in value.split(",") if part.strip()]
    return list(default)

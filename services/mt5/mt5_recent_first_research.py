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
    _open_research_trade,
    _update_research_trade,
    _volatility_bucket,
)


RECENT_FIRST_FAMILIES = [
    "recent_momentum_pullback",
    "recent_range_reversion",
    "recent_volatility_breakout",
    "recent_liquidity_sweep",
    "recent_failed_breakout_reversal",
    "recent_ema_reclaim",
    "recent_session_open_continuation",
    "recent_london_us_breakout",
    "recent_atr_expansion_scalp",
    "recent_chop_avoidance_reversal",
]
RECENT_FIRST_TIMEFRAMES = ["M15", "M30", "H1"]
_SESSION_HOURS: dict[str, set[int] | None] = {
    "all": None,
    "asia": set(range(0, 8)),
    "london_us": set(range(7, 21)),
    "ny_core": set(range(13, 21)),
}


@dataclass(frozen=True)
class RecentFirstVariant:
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


def run_recent_first_research(body: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    body = body or {}
    symbol = str(body.get("symbol") or "BTCUSD").upper().strip()
    csv_dir = Path(str(body.get("csv_dir") or Path("data") / "backtests"))
    timeframes = _requested_list(body.get("timeframes"), RECENT_FIRST_TIMEFRAMES)
    families = [item for item in _requested_list(body.get("families"), RECENT_FIRST_FAMILIES) if item in RECENT_FIRST_FAMILIES]
    max_bars = max(200, min(int(_number(body.get("max_bars")) or 60000), 65000))
    max_evaluations = max(1, int(_number(body.get("max_evaluations")) or 180))
    timeout_seconds = max(0.25, float(_number(body.get("per_evaluation_timeout_seconds")) or 5.0))
    spread_points = float(_number(body.get("spread_points")) or 25.0)
    datasets = _datasets_for(body, csv_dir, symbol, timeframes, max_bars)
    dataset_counts = _dataset_counts_by_timeframe(datasets)
    variants = _build_variants(timeframes, families, max_evaluations=max_evaluations, dataset_counts=dataset_counts)
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[str] = []
    csv_paths: dict[str, str] = {}

    for dataset in datasets:
        if len(rows) >= max_evaluations:
            break
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
            if len(rows) >= max_evaluations:
                break
            rows.append(
                evaluate_recent_first_variant(
                    settings,
                    bars,
                    variant,
                    sample_label=label,
                    source_csv=str(csv_path),
                    features_by_index=features_by_index,
                    timeout_seconds=timeout_seconds,
                )
            )

    rows.sort(key=_recent_first_rank, reverse=True)
    candidates = [row for row in rows if row.get("candidate")]
    result = {
        "ok": True,
        "status": "mt5_recent_first_research_completed",
        "symbol": symbol,
        "mode": "paper",
        "timeframes": timeframes,
        "families": families,
        "csv_paths": csv_paths,
        "max_evaluations_requested": max_evaluations,
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


def evaluate_recent_first_variant(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    variant: RecentFirstVariant,
    *,
    sample_label: str,
    source_csv: str,
    features_by_index: dict[int, dict[str, Any]],
    timeout_seconds: float,
) -> dict[str, Any]:
    started = time.monotonic()
    splits = _quarter_ranges(len(bars))
    segment_order = ["recent", "previous", "middle", "oldest"]
    segment_trades: dict[str, list[dict[str, Any]]] = {}
    segment_blocked: dict[str, list[str]] = {}
    segment_signals: dict[str, dict[str, int]] = {}
    segment_state: dict[str, dict[str, int]] = {}
    for name in segment_order:
        trades, blocked, signals, state = _simulate_recent_segment(
            settings,
            bars,
            variant,
            splits[name][0],
            splits[name][1],
            started,
            timeout_seconds=timeout_seconds,
            features_by_index=features_by_index,
        )
        segment_trades[name] = trades
        segment_blocked[name] = blocked
        segment_signals[name] = signals
        segment_state[name] = state

    closed = [trade for name in ["oldest", "middle", "previous", "recent"] for trade in segment_trades.get(name, []) if trade.get("lifecycle_status") == "closed"]
    total = _metrics(closed, initial_balance=settings.initial_balance)
    split = {name: _compact_metrics(segment_trades.get(name, []), settings.initial_balance) for name in ["oldest", "middle", "previous", "recent"]}
    monte_carlo = _monte_carlo_stress(closed, initial_balance=settings.initial_balance, max_drawdown_limit=5000.0, simulations=400)
    fragile = _fragile_dependency(total, split)
    single_trade = _depends_on_single_trade(total)
    gate = _gate(total, split, monte_carlo, fragile, single_trade)
    score = _score_recent_first(total, split, monte_carlo, gate, fragile, single_trade)
    side_stats = _group_stats(settings, closed, lambda trade: str(trade.get("side") or "unknown").lower())
    session_stats = _group_stats(settings, closed, lambda trade: _session_name(_trade_hour(trade)))
    regime_stats = _group_stats(settings, closed, lambda trade: str(trade.get("regime") or "unknown"))
    hour_stats = _group_stats(settings, closed, lambda trade: str(_trade_hour(trade)))
    volatility_stats = _group_stats(settings, closed, lambda trade: _vol_bucket_from_trade(trade))
    atr_stats = _group_stats(settings, closed, lambda trade: _atr_bucket_from_trade(trade))
    rsi_stats = _group_stats(settings, closed, lambda trade: _rsi_bucket_from_trade(trade))
    blocked_all = [reason for reasons in segment_blocked.values() for reason in reasons]
    signals_all = {
        "generated": sum(int(item.get("generated") or 0) for item in segment_signals.values()),
        "actionable": sum(int(item.get("actionable") or 0) for item in segment_signals.values()),
    }
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
        "generated_signal_count": signals_all["generated"],
        "actionable_signal_count": signals_all["actionable"],
        "opened_trade_count": len(closed),
        "total_closed": total["closed"],
        "closed_total": total["closed"],
        "total_wins": total["wins"],
        "total_losses": total["losses"],
        "total_win_rate": total["win_rate"],
        "total_pf": total["profit_factor"],
        "profit_factor_total": total["profit_factor"],
        "total_expectancy": total["expectancy"],
        "expectancy_total": total["expectancy"],
        "total_max_drawdown": total["max_drawdown"],
        "max_drawdown_total": total["max_drawdown"],
        "oldest_closed": split["oldest"]["closed"],
        "middle_closed": split["middle"]["closed"],
        "previous_closed": split["previous"]["closed"],
        "recent_closed": split["recent"]["closed"],
        "oldest_win_rate": split["oldest"]["win_rate"],
        "middle_win_rate": split["middle"]["win_rate"],
        "previous_win_rate": split["previous"]["win_rate"],
        "recent_win_rate": split["recent"]["win_rate"],
        "oldest_pf": split["oldest"]["profit_factor"],
        "middle_pf": split["middle"]["profit_factor"],
        "previous_pf": split["previous"]["profit_factor"],
        "recent_pf": split["recent"]["profit_factor"],
        "oldest_expectancy": split["oldest"]["expectancy"],
        "middle_expectancy": split["middle"]["expectancy"],
        "previous_expectancy": split["previous"]["expectancy"],
        "recent_expectancy": split["recent"]["expectancy"],
        "oldest_max_drawdown": split["oldest"]["max_drawdown"],
        "middle_max_drawdown": split["middle"]["max_drawdown"],
        "previous_max_drawdown": split["previous"]["max_drawdown"],
        "recent_max_drawdown": split["recent"]["max_drawdown"],
        "split_stats": split,
        "monte_carlo_stressed_pf": monte_carlo.get("profit_factor_stressed", 0.0),
        "monte_carlo_p95_drawdown": monte_carlo.get("max_drawdown_p95", 0.0),
        "monte_carlo_stressed_expectancy": monte_carlo.get("expectancy_stressed", 0.0),
        "monte_carlo_fail_reasons": list(monte_carlo.get("fail_reasons") or []),
        "fragile_regime_dependency": fragile,
        "single_trade_dependency": single_trade,
        "recent_overfit_risk": _recent_overfit_risk(total, split),
        "side_stats": side_stats,
        "session_stats": session_stats,
        "regime_stats": regime_stats,
        "hour_stats": hour_stats,
        "volatility_stats": volatility_stats,
        "atr_regime_stats": atr_stats,
        "rsi_regime_stats": rsi_stats,
        "exit_reason_counts": total["exit_reason_counts"],
        "blocked_reason_counts": _reason_counts(blocked_all),
        "risk_governor_blocks": sum(int(item.get("risk_governor_blocks") or 0) for item in segment_state.values()),
        "max_open_trades_observed": max([int(item.get("max_open_trades_observed") or 0) for item in segment_state.values()] or [0]),
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


def write_recent_first_research_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "recent_first_research_results.csv"
    json_path = root / "recent_first_research_results.json"
    summary_path = root / "recent_first_research_summary.md"
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
        "recent_closed",
        "recent_win_rate",
        "recent_pf",
        "recent_expectancy",
        "recent_max_drawdown",
        "total_closed",
        "total_win_rate",
        "total_pf",
        "total_expectancy",
        "total_max_drawdown",
        "oldest_pf",
        "middle_pf",
        "previous_pf",
        "recent_pf",
        "oldest_expectancy",
        "middle_expectancy",
        "previous_expectancy",
        "recent_expectancy",
        "monte_carlo_stressed_pf",
        "monte_carlo_p95_drawdown",
        "fragile_regime_dependency",
        "single_trade_dependency",
        "recent_overfit_risk",
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
    summary_path.write_text(recent_first_research_summary_markdown(result), encoding="utf-8")
    return csv_path, json_path, summary_path


def recent_first_research_summary_markdown(result: dict[str, Any]) -> str:
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else _summary(rows, [])
    lines = [
        "# MT5 Recent-First Research Summary",
        "",
        "Recent-first signal discovery. Paper/offline only; no broker, no order execution, no automatic promotion.",
        "",
        f"Evaluations: `{result.get('evaluations', len(rows))}`.",
        f"Max evaluations requested: `{result.get('max_evaluations_requested')}`.",
        f"Candidates: `{len(result.get('candidates') or [])}`.",
        "",
        "## Top 20",
    ]
    for row in rows[:20]:
        lines.append(
            f"- `{row.get('sample_label')}` `{row.get('timeframe')}` `{row.get('family')}` side `{row.get('side')}` session `{row.get('session')}` "
            f"recent `{row.get('recent_closed')}` recent PF `{row.get('recent_pf')}`, recent exp `{row.get('recent_expectancy')}`, "
            f"total `{row.get('total_closed')}` total PF `{row.get('total_pf')}`, MC PF `{row.get('monte_carlo_stressed_pf')}`, "
            f"recommendation `{row.get('recommendation')}`."
        )
    lines.extend(
        [
            "",
            "## Answers",
            f"1. Families generating recent trades: {summary.get('recent_trade_families_answer')}",
            f"2. Best recent timeframe: {summary.get('timeframe_answer')}",
            f"3. Best recent side: {summary.get('side_answer')}",
            f"4. Best recent session/hour: {summary.get('session_answer')}",
            f"5. Families surviving backward validation: {summary.get('backward_survivors_answer')}",
            f"6. Recent-overfit families: {summary.get('recent_overfit_answer')}",
            f"7. Monte Carlo failures: {summary.get('monte_carlo_fail_answer')}",
            f"8. Top 3 for capital preservation optimizer: {summary.get('top_3_answer')}",
            "9. No automatic promotion.",
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


def _simulate_recent_segment(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    variant: RecentFirstVariant,
    start_index: int,
    end_index: int,
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
    loop_start = max(80, start_index)
    loop_end = min(max(loop_start, end_index), len(bars))
    max_iterations = max(1, loop_end - loop_start + 5)
    iterations = 0
    for index in range(loop_start, loop_end):
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
        if index >= loop_end - 1:
            continue
        if open_trade:
            blocked.append("max_open_trades_reached")
            continue
        if index < cooldown_until:
            blocked.append("cooldown_after_loss")
            continue
        risk_reason = _recent_research_risk_block(settings, trades)
        if risk_reason:
            state["risk_governor_blocks"] += 1
            blocked.append(f"risk_governor_{risk_reason}")
            continue
        features = features_by_index.get(index - 1)
        if not features:
            blocked.append("insufficient_history")
            continue
        decision = _decision_for_recent(features, variant)
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
            "shadow_trade_id": f"recent-first-{variant.family}-{variant.timeframe}-{index}",
            "source": "mt5_recent_first_research",
            "strategy_profile": variant.family,
            "filter_profile": variant.key(),
            **_safety(),
        }
        features_snapshot = open_trade.get("features_snapshot") if isinstance(open_trade.get("features_snapshot"), dict) else {}
        open_trade["features_snapshot"] = {**features_snapshot, "recent_first": True, "rsi_regime": variant.rsi_regime}
        state["max_open_trades_observed"] = max(state["max_open_trades_observed"], 1)
    if open_trade:
        last_bar = bars[min(loop_end - 1, len(bars) - 1)]
        trades.append(_close(settings, open_trade, float(_number(last_bar.get("close")) or open_trade.get("entry_price") or 0.0), "time_stop", last_bar))
    return trades, blocked, signals, state


def _decision_for_recent(features: dict[str, Any], variant: RecentFirstVariant) -> dict[str, Any]:
    if not _session_allowed(features, variant):
        return {"actionable": False, "generated": False, "reason": "session_filter"}
    if not _volatility_allowed(features, variant):
        return {"actionable": False, "generated": False, "reason": "volatility_regime_filter"}
    if not _trend_regime_allowed(features, variant):
        return {"actionable": False, "generated": False, "reason": "trend_regime_filter"}
    if not _rsi_allowed(features, variant):
        return {"actionable": False, "generated": False, "reason": "rsi_regime_filter"}
    side, score, reason = _family_signal_recent(features, variant.family)
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


def _family_signal_recent(features: dict[str, Any], family: str) -> tuple[str, float, str]:
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
    body_atr = float(features["body_atr"])
    distance20 = float(features["distance20_atr"])
    recent_high = float(features["recent_high"])
    recent_low = float(features["recent_low"])
    prior_high = float(features["prior_high"])
    prior_low = float(features["prior_low"])
    recent_range = float(features["recent_range"])
    previous_range = max(float(features["previous_range"]), atr)
    hour = int(features.get("hour") if features.get("hour") is not None else -1)
    trend_up = close > ema20 >= ema50
    trend_down = close < ema20 <= ema50
    base_long = trend_score * 0.36 + momentum_score * 0.42 + volatility_score * 0.22
    base_short = (100.0 - trend_score) * 0.36 + (100.0 - momentum_score) * 0.42 + volatility_score * 0.22
    if family == "recent_momentum_pullback":
        buy = trend_up and low <= ema20 + atr * 0.35 and close > prev_close and 38 <= rsi <= 72
        sell = trend_down and high >= ema20 - atr * 0.35 and close < prev_close and 28 <= rsi <= 62
        if buy and distance20 <= 1.45:
            return "buy", base_long + 4.0, "recent_momentum_pullback_buy"
        if sell and distance20 <= 1.45:
            return "sell", base_short + 4.0, "recent_momentum_pullback_sell"
        return "", max(base_long, base_short), "recent_momentum_pullback_not_confirmed"
    if family == "recent_range_reversion":
        range_like = features["regime"] == "chop" or recent_range <= previous_range * 0.95
        if range_like and low < recent_low and close > open_price and rsi <= 42:
            return "buy", 55.0 + body_ratio * 18.0 + min(distance20 * 3.0, 7.0), "recent_range_reversion_buy"
        if range_like and high > recent_high and close < open_price and rsi >= 58:
            return "sell", 55.0 + body_ratio * 18.0 + min(distance20 * 3.0, 7.0), "recent_range_reversion_sell"
        return "", 52.0 + body_ratio * 12.0, "recent_range_reversion_not_confirmed"
    if family == "recent_volatility_breakout":
        expanding = recent_range > max(previous_range * 1.04, atr * 1.1)
        if expanding and close > recent_high and momentum_score >= 54 and rsi < 76:
            return "buy", base_long + volatility_score * 0.08, "recent_volatility_breakout_buy"
        if expanding and close < recent_low and momentum_score <= 46 and rsi > 24:
            return "sell", base_short + volatility_score * 0.08, "recent_volatility_breakout_sell"
        return "", max(base_long, base_short), "recent_volatility_breakout_not_confirmed"
    if family == "recent_liquidity_sweep":
        sweep_low = low < recent_low and close > recent_low and close > open_price and rsi <= 50
        sweep_high = high > recent_high and close < recent_high and close < open_price and rsi >= 50
        if sweep_low:
            return "buy", 56.0 + body_ratio * 20.0 + volatility_score * 0.10, "recent_liquidity_sweep_buy"
        if sweep_high:
            return "sell", 56.0 + body_ratio * 20.0 + volatility_score * 0.10, "recent_liquidity_sweep_sell"
        return "", 52.0 + body_ratio * 12.0, "recent_liquidity_sweep_not_confirmed"
    if family == "recent_failed_breakout_reversal":
        failed_up = high > recent_high and close < recent_high and close < prev_close and rsi >= 52
        failed_down = low < recent_low and close > recent_low and close > prev_close and rsi <= 48
        if failed_down:
            return "buy", 57.0 + body_ratio * 18.0, "recent_failed_breakdown_reversal_buy"
        if failed_up:
            return "sell", 57.0 + body_ratio * 18.0, "recent_failed_breakout_reversal_sell"
        return "", 53.0 + body_ratio * 10.0, "recent_failed_breakout_reversal_not_confirmed"
    if family == "recent_ema_reclaim":
        if low <= ema20 and close > ema20 and close > open_price and ema20 >= ema50 and rsi < 74:
            return "buy", base_long + 4.5, "recent_ema_reclaim_buy"
        if high >= ema20 and close < ema20 and close < open_price and ema20 <= ema50 and rsi > 26:
            return "sell", base_short + 4.5, "recent_ema_reclaim_sell"
        return "", max(base_long, base_short), "recent_ema_reclaim_not_confirmed"
    if family == "recent_session_open_continuation":
        near_open = hour in {0, 1, 7, 8, 13, 14}
        if near_open and close > prev_close and close > ema20 and momentum_score >= 55 and rsi < 76:
            return "buy", base_long + 5.0, "recent_session_open_continuation_buy"
        if near_open and close < prev_close and close < ema20 and momentum_score <= 45 and rsi > 24:
            return "sell", base_short + 5.0, "recent_session_open_continuation_sell"
        return "", max(base_long, base_short), "recent_session_open_continuation_not_confirmed"
    if family == "recent_london_us_breakout":
        if hour not in _SESSION_HOURS["london_us"]:
            return "", max(base_long, base_short), "recent_london_us_outside_session"
        if close > max(recent_high, prior_high) and momentum_score >= 54 and body_atr >= 0.35:
            return "buy", base_long + 5.0, "recent_london_us_breakout_buy"
        if close < min(recent_low, prior_low) and momentum_score <= 46 and body_atr >= 0.35:
            return "sell", base_short + 5.0, "recent_london_us_breakout_sell"
        return "", max(base_long, base_short), "recent_london_us_breakout_not_confirmed"
    if family == "recent_atr_expansion_scalp":
        expanded = body_atr >= 0.55 and recent_range >= previous_range * 1.02
        if expanded and close > prev_close and momentum_score >= 53 and rsi < 75:
            return "buy", base_long + min(body_atr * 5.0, 9.0), "recent_atr_expansion_scalp_buy"
        if expanded and close < prev_close and momentum_score <= 47 and rsi > 25:
            return "sell", base_short + min(body_atr * 5.0, 9.0), "recent_atr_expansion_scalp_sell"
        return "", max(base_long, base_short), "recent_atr_expansion_scalp_not_confirmed"
    if family == "recent_chop_avoidance_reversal":
        not_chop = features["regime"] != "chop" or volatility_score >= 35
        if not_chop and low < recent_low and close > prev_close and rsi <= 46:
            return "buy", 56.0 + body_ratio * 14.0 + volatility_score * 0.08, "recent_chop_avoidance_reversal_buy"
        if not_chop and high > recent_high and close < prev_close and rsi >= 54:
            return "sell", 56.0 + body_ratio * 14.0 + volatility_score * 0.08, "recent_chop_avoidance_reversal_sell"
        return "", 52.0 + body_ratio * 10.0, "recent_chop_avoidance_reversal_not_confirmed"
    return "", max(base_long, base_short), "unknown_family"


def _session_allowed(features: dict[str, Any], variant: RecentFirstVariant) -> bool:
    hours = _SESSION_HOURS.get(variant.session_name)
    if hours is None:
        return True
    hour = features.get("hour")
    return hour is not None and int(hour) in hours


def _volatility_allowed(features: dict[str, Any], variant: RecentFirstVariant) -> bool:
    bucket = _volatility_bucket(float(features.get("volatility_score") or 0.0))
    if variant.volatility_regime == "any":
        return True
    if variant.volatility_regime == "normal_high":
        return bucket in {"normal", "high"}
    return bucket == variant.volatility_regime


def _trend_regime_allowed(features: dict[str, Any], variant: RecentFirstVariant) -> bool:
    regime = str(features.get("regime") or "unknown").casefold()
    if variant.trend_regime == "any":
        return True
    if variant.trend_regime == "range":
        return regime == "chop"
    return regime == variant.trend_regime


def _rsi_allowed(features: dict[str, Any], variant: RecentFirstVariant) -> bool:
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


def _recent_research_risk_block(settings: BacktestSettings, trades: list[dict[str, Any]]) -> str:
    if settings.spread_points > 30:
        return "spread_too_high"
    if _loss_streak(trades) >= 4:
        return "consecutive_loss_lockdown"
    if len([trade for trade in trades if trade.get("lifecycle_status") == "closed"]) >= 20 and _recent_edge_negative(trades):
        return "recent_edge_negative"
    if _drawdown_accelerating(trades, settings.initial_balance):
        return "drawdown_accelerating"
    return ""


def _quarter_ranges(length: int) -> dict[str, tuple[int, int]]:
    q1 = int(length * 0.25)
    q2 = int(length * 0.50)
    q3 = int(length * 0.75)
    return {
        "oldest": (0, q1),
        "middle": (q1, q2),
        "previous": (q2, q3),
        "recent": (q3, length),
    }


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
    monte_carlo: dict[str, Any],
    fragile: bool,
    single_trade: bool,
) -> dict[str, Any]:
    reasons: list[str] = []
    recent = split["recent"]
    if int(recent.get("closed") or 0) < 10:
        reasons.append("recent_sample_too_small")
    if float(recent.get("profit_factor") or 0.0) < 1.05:
        reasons.append("recent_pf_below_1_05")
    if float(recent.get("expectancy") or 0.0) <= 0:
        reasons.append("recent_expectancy_not_positive")
    if float(recent.get("max_drawdown") or 0.0) > 2500:
        reasons.append("recent_drawdown_above_2500")
    if int(total.get("closed") or 0) < 40:
        reasons.append("total_sample_too_small")
    if float(total.get("profit_factor") or 0.0) <= 1.15:
        reasons.append("total_pf_below_1_15")
    if float(total.get("expectancy") or 0.0) <= 0:
        reasons.append("total_expectancy_not_positive")
    if float(monte_carlo.get("profit_factor_stressed") or 0.0) < 1.05:
        reasons.append("monte_carlo_stressed_pf_below_1_05")
    if float(monte_carlo.get("max_drawdown_p95") or 0.0) > 5000:
        reasons.append("monte_carlo_p95_drawdown_above_5000")
    if float(monte_carlo.get("expectancy_stressed") or 0.0) < 0:
        reasons.append("monte_carlo_stressed_expectancy_negative")
    if fragile:
        reasons.append("fragile_regime_dependency")
    if single_trade:
        reasons.append("single_trade_dependency")
    return {"passed": not reasons, "reasons": reasons or ["passes_recent_first_gates"]}


def _fragile_dependency(total: dict[str, Any], split: dict[str, dict[str, Any]]) -> bool:
    closed = int(total.get("closed") or 0)
    if closed < 10:
        return True
    recent_closed = int(split["recent"].get("closed") or 0)
    if recent_closed < max(5, closed * 0.15):
        return True
    if recent_closed > closed * 0.72 and closed >= 20:
        return True
    active_windows = [item for item in split.values() if int(item.get("closed") or 0) >= 5]
    if not active_windows:
        return True
    weak = [
        item
        for item in active_windows
        if float(item.get("profit_factor") or 0.0) < 1.0 or float(item.get("expectancy") or 0.0) <= -0.05
    ]
    return bool(weak)


def _recent_overfit_risk(total: dict[str, Any], split: dict[str, dict[str, Any]]) -> bool:
    closed = int(total.get("closed") or 0)
    if closed <= 0:
        return False
    return int(split["recent"].get("closed") or 0) > closed * 0.70


def _score_recent_first(
    total: dict[str, Any],
    split: dict[str, dict[str, Any]],
    monte_carlo: dict[str, Any],
    gate: dict[str, Any],
    fragile: bool,
    single_trade: bool,
) -> float:
    recent = split["recent"]
    recent_closed = int(recent.get("closed") or 0)
    total_closed = int(total.get("closed") or 0)
    recent_pf = min(float(recent.get("profit_factor") or 0.0), 3.0 if recent_closed >= 10 else 1.05)
    total_pf = min(float(total.get("profit_factor") or 0.0), 3.0 if total_closed >= 40 else 1.15)
    mc_pf = min(float(monte_carlo.get("profit_factor_stressed") or 0.0), 3.0 if total_closed >= 40 else 1.15)
    score = 0.0
    score += min(recent_closed, 80) * 3.0
    score += min(total_closed, 160) * 0.8
    score += max(0.0, recent_pf - 1.0) * 120.0
    score += max(0.0, total_pf - 1.0) * 65.0
    if recent_closed >= 10:
        score += max(0.0, float(recent.get("expectancy") or 0.0)) * 350.0
    if total_closed >= 40:
        score += max(0.0, float(total.get("expectancy") or 0.0)) * 200.0
    score += max(0.0, mc_pf - 1.0) * 80.0
    score -= float(recent.get("max_drawdown") or 0.0) / 45.0
    score -= float(total.get("max_drawdown") or 0.0) / 90.0
    score -= max(0, 10 - recent_closed) * 40.0
    score -= max(0, 40 - total_closed) * 7.0
    if fragile:
        score -= 130.0
    if single_trade:
        score -= 120.0
    if not gate.get("passed"):
        score -= len(gate.get("reasons") or []) * 12.0
    return round(score, 4)


def _observation_quality(total: dict[str, Any], split: dict[str, dict[str, Any]]) -> bool:
    recent = split["recent"]
    return (
        int(recent.get("closed") or 0) >= 5
        and float(recent.get("profit_factor") or 0.0) >= 1.0
        and float(recent.get("expectancy") or 0.0) > 0
        and float(total.get("expectancy") or 0.0) > -0.05
    )


def _build_variants(
    timeframes: list[str],
    families: list[str],
    *,
    max_evaluations: int,
    dataset_counts: dict[str, int] | None = None,
) -> list[RecentFirstVariant]:
    dataset_counts = dataset_counts or {timeframe: 1 for timeframe in timeframes}
    variants_by_timeframe: dict[str, dict[str, list[RecentFirstVariant]]] = {timeframe: {family: [] for family in families} for timeframe in timeframes}
    for timeframe in timeframes:
        for family in families:
            defaults = _family_defaults(family, timeframe)
            for side in defaults["sides"]:
                for session in defaults["sessions"]:
                    for vol in defaults["volatility"]:
                        for rsi in defaults["rsi"]:
                            variants_by_timeframe[timeframe][family].append(
                                RecentFirstVariant(
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
    selected: list[RecentFirstVariant] = []
    spent = 0
    while spent < max_evaluations and any(any(bucket for bucket in by_family.values()) for by_family in variants_by_timeframe.values()):
        progressed = False
        for timeframe in timeframes:
            cost = max(1, int(dataset_counts.get(timeframe) or 1))
            if spent + cost > max_evaluations:
                continue
            by_family = variants_by_timeframe.get(timeframe) or {}
            for family in families:
                bucket = by_family.get(family) or []
                if not bucket:
                    continue
                if spent + cost > max_evaluations:
                    break
                variant = bucket.pop(0)
                selected.append(variant)
                spent += cost
                progressed = True
                if spent >= max_evaluations:
                    break
            if spent >= max_evaluations:
                break
        if not progressed:
            break
    return selected


def _family_defaults(family: str, timeframe: str) -> dict[str, Any]:
    base = {
        "sides": ["both", "buy", "sell"],
        "sessions": ["all", "london_us"],
        "volatility": ["any", "normal_high"],
        "rsi": ["any", "not_extreme"],
        "trend_regime": "any",
        "score": 55.0,
        "rr": 1.0,
        "time_stop": 2 if timeframe != "H1" else 3,
        "atr_stop": 1.0,
        "mae_exit": 0.82,
    }
    if family in {"recent_momentum_pullback", "recent_ema_reclaim", "recent_session_open_continuation"}:
        base.update({"trend_regime": "trend", "score": 55.0, "rr": 1.1, "rsi": ["any", "not_extreme"]})
    elif family in {"recent_range_reversion", "recent_liquidity_sweep", "recent_failed_breakout_reversal", "recent_chop_avoidance_reversal"}:
        base.update({"trend_regime": "chop", "score": 54.0, "rr": 0.95, "sessions": ["all", "asia", "london_us"], "mae_exit": 0.78})
    elif family in {"recent_volatility_breakout", "recent_london_us_breakout", "recent_atr_expansion_scalp"}:
        base.update({"trend_regime": "any", "score": 56.0, "rr": 1.05, "volatility": ["normal_high"], "sessions": ["all", "london_us"]})
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


def _dataset_counts_by_timeframe(datasets: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for dataset in datasets:
        counts[str(dataset.get("timeframe") or "")] += 1
    return dict(counts)


def _summary(rows: list[dict[str, Any]], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"recommendation": "no_data", **_safety()}
    recent_trade_rows = [row for row in rows if int(row.get("recent_closed") or 0) > 0]
    recent_survivors = [
        row
        for row in rows
        if int(row.get("recent_closed") or 0) >= 10
        and float(row.get("recent_pf") or 0.0) >= 1.05
        and float(row.get("recent_expectancy") or 0.0) > 0
    ]
    backward_survivors = [
        row
        for row in recent_survivors
        if int(row.get("total_closed") or 0) >= 40
        and float(row.get("total_pf") or 0.0) > 1.15
        and float(row.get("total_expectancy") or 0.0) > 0
        and not row.get("fragile_regime_dependency")
        and not row.get("single_trade_dependency")
    ]
    return {
        "recommendation": "research_candidate" if candidates else "observation_only" if recent_survivors else "reject",
        "recent_trade_families_answer": _family_list(recent_trade_rows),
        "timeframe_answer": _best_bucket(rows, "timeframe"),
        "side_answer": _best_bucket(rows, "side_mode"),
        "session_answer": _best_bucket(rows, "session_filter"),
        "backward_survivors_answer": _family_list(backward_survivors),
        "recent_overfit_answer": _family_list([row for row in rows if row.get("recent_overfit_risk")]),
        "monte_carlo_fail_answer": _reason_family_list(rows, "monte_carlo"),
        "top_3_answer": "; ".join(_candidate_profile_name(row) for row in candidates[:3]) if candidates else "none; no profile should pass to capital preservation optimizer",
        "automatic_promotion": False,
        **_safety(),
    }


def _family_list(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "none"
    names = []
    for row in sorted(rows, key=_recent_first_rank, reverse=True)[:8]:
        names.append(
            f"{row.get('sample_label')} {row.get('timeframe')} {row.get('family')} {row.get('side_mode')} {row.get('session_filter')} "
            f"recent={row.get('recent_closed')} total={row.get('total_closed')}"
        )
    return "; ".join(names)


def _best_bucket(rows: list[dict[str, Any]], key: str) -> str:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(str(row.get(key) or "unknown"), []).append(row)
    ranked = []
    for name, items in buckets.items():
        recent = sum(int(item.get("recent_closed") or 0) for item in items)
        closed = sum(int(item.get("total_closed") or 0) for item in items)
        score = sum(float(item.get("research_score") or 0.0) for item in sorted(items, key=_recent_first_rank, reverse=True)[:5]) / max(1, min(5, len(items)))
        ranked.append((score + min(recent, 160) * 0.6 + min(closed, 260) * 0.04, name, recent, closed))
    ranked.sort(reverse=True)
    return f"{ranked[0][1]} ({ranked[0][2]} recent closed, {ranked[0][3]} total closed across variants)" if ranked else "none"


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
    return f"recent_first_{family}_{timeframe}_{side}_candidate".replace("-", "_")


def _recent_first_rank(row: dict[str, Any]) -> float:
    return float(row.get("research_score") or 0.0)


def _requested_list(value: Any, default: list[str]) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [part.strip() for part in value.split(",") if part.strip()]
    return list(default)

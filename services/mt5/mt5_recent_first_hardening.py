from __future__ import annotations

import csv
import json
import time
from collections import Counter
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
from services.mt5.mt5_capital_preservation_optimizer import _depends_on_single_trade, _monte_carlo_stress
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_recent_first_research import (
    RecentFirstVariant,
    _compact_metrics,
    _decision_for_recent,
    _fragile_dependency,
    _quarter_ranges,
    _recent_research_risk_block,
)
from services.mt5.mt5_strategy_research_v2 import _features_by_index, _open_research_trade


RECENT_FIRST_HARDENING_TARGETS = [
    "recent_london_us_breakout_m30_both_hardened_v1",
    "recent_london_us_breakout_m30_sell_hardened_v1",
    "recent_london_us_breakout_m30_buy_hardened_v1",
    "recent_london_us_breakout_m30_mae_guard_v1",
    "recent_london_us_breakout_m30_fast_loss_cut_v1",
    "recent_london_us_breakout_m30_trailing_defensive_v1",
    "recent_liquidity_sweep_h1_hardened_v1",
    "recent_failed_breakout_reversal_h1_hardened_v1",
]


@dataclass(frozen=True)
class HardeningConfig:
    target_name: str
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
    hardening_actions: tuple[str, ...]
    fast_loss_cut_r: float = 0.0
    trailing_activation_r: float = 0.0
    trailing_lock_r: float = 0.0

    def recent_variant(self) -> RecentFirstVariant:
        return RecentFirstVariant(
            family=self.family,
            timeframe=self.timeframe,
            side_mode=self.side_mode,
            session_name=self.session_name,
            volatility_regime=self.volatility_regime,
            trend_regime=self.trend_regime,
            rsi_regime=self.rsi_regime,
            score_threshold=self.score_threshold,
            risk_reward=self.risk_reward,
            time_stop_bars=self.time_stop_bars,
            atr_stop_multiplier=self.atr_stop_multiplier,
            mae_exit_r=self.mae_exit_r,
            momentum_loss_exit=self.momentum_loss_exit,
        )


def run_recent_first_hardening(body: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    body = body or {}
    symbol = str(body.get("symbol") or "BTCUSD").upper().strip()
    csv_dir = Path(str(body.get("csv_dir") or Path("data") / "backtests"))
    max_bars = max(200, min(int(_number(body.get("max_bars")) or 60000), 65000))
    timeout_seconds = max(0.25, float(_number(body.get("per_evaluation_timeout_seconds")) or 2.0))
    spread_points = float(_number(body.get("spread_points")) or 25.0)
    requested_targets = _requested_list(body.get("targets"), RECENT_FIRST_HARDENING_TARGETS)
    configs = [config for config in _hardening_configs() if config.target_name in requested_targets]
    datasets = _datasets_for(body, csv_dir, symbol, max_bars)
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[str] = []
    csv_paths: dict[str, str] = {}

    for dataset in datasets:
        timeframe = dataset["timeframe"]
        csv_path = Path(dataset["csv_path"])
        sample_label = dataset["sample_label"]
        csv_paths[sample_label] = str(csv_path)
        if not csv_path.exists():
            errors.append({"sample_label": sample_label, "timeframe": timeframe, "path": str(csv_path), "error": "csv_not_found"})
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
        warnings.extend([f"{sample_label}:{warning}" for warning in load_warnings])
        bars = bars[-settings.max_bars :]
        if not bars:
            errors.append({"sample_label": sample_label, "timeframe": timeframe, "path": str(csv_path), "error": "csv_bars_not_loaded"})
            continue
        features_by_index = _features_by_index(bars)
        for config in [item for item in configs if item.timeframe == timeframe]:
            rows.append(
                evaluate_hardening_config(
                    settings,
                    bars,
                    config,
                    sample_label=sample_label,
                    source_csv=str(csv_path),
                    features_by_index=features_by_index,
                    timeout_seconds=timeout_seconds,
                )
            )

    rows.sort(key=lambda row: float(row.get("hardening_score") or 0.0), reverse=True)
    candidates = [row for row in rows if row.get("candidate")]
    result = {
        "ok": True,
        "status": "mt5_recent_first_hardening_completed",
        "symbol": symbol,
        "mode": "paper",
        "csv_paths": csv_paths,
        "evaluations": len(rows),
        "targets": [config.target_name for config in configs],
        "results": rows,
        "candidates": candidates,
        "top_3_for_capital_optimizer": candidates[:3],
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


def evaluate_hardening_config(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    config: HardeningConfig,
    *,
    sample_label: str,
    source_csv: str,
    features_by_index: dict[int, dict[str, Any]],
    timeout_seconds: float,
) -> dict[str, Any]:
    started = time.monotonic()
    trades, blocked, signals, state = _simulate_hardened(settings, bars, config, started, timeout_seconds=timeout_seconds, features_by_index=features_by_index)
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    total = _metrics(closed, initial_balance=settings.initial_balance)
    split = _split_metrics(settings, bars, closed)
    recent = split["recent"]
    monte_carlo = _monte_carlo_stress(closed, initial_balance=settings.initial_balance, max_drawdown_limit=5000.0, simulations=500)
    fragile = _fragile_dependency(total, split)
    single_trade = _depends_on_single_trade(total)
    remove_best_3 = _remove_best_metrics(settings, closed, 3)
    remove_best_5 = _remove_best_metrics(settings, closed, 5)
    worst_sequence = _worst_sequence_metrics(settings, closed)
    spread_x1_5 = _spread_stress_metrics(settings, bars, config, features_by_index, timeout_seconds, 1.5)
    spread_x2 = _spread_stress_metrics(settings, bars, config, features_by_index, timeout_seconds, 2.0)
    gate = _gate(total, recent, monte_carlo, remove_best_5, spread_x2, fragile, single_trade)
    score = _hardening_score(total, recent, monte_carlo, remove_best_5, spread_x2, gate, fragile, single_trade)
    hardening_help = _hardening_help_label(config)
    return {
        "sample_label": sample_label,
        "source_csv": source_csv,
        "csv_path_used": source_csv,
        "target_name": config.target_name,
        "family": config.family,
        "profile": config.target_name,
        "timeframe": config.timeframe,
        "side": config.side_mode,
        "side_mode": config.side_mode,
        "session": config.session_name,
        "session_filter": config.session_name,
        "hardening_actions": list(config.hardening_actions),
        "hardening_help": hardening_help,
        "risk_reward": config.risk_reward,
        "time_stop_bars": config.time_stop_bars,
        "mae_exit_r": config.mae_exit_r,
        "fast_loss_cut_r": config.fast_loss_cut_r,
        "trailing_activation_r": config.trailing_activation_r,
        "trailing_lock_r": config.trailing_lock_r,
        "bars_loaded": len(bars),
        "bars_evaluated": max(0, len(bars) - 80),
        "first_bar_time": str(bars[0].get("time") or "") if bars else "",
        "last_bar_time": str(bars[-1].get("time") or "") if bars else "",
        "generated_signal_count": signals["generated"],
        "actionable_signal_count": signals["actionable"],
        "opened_trade_count": len(closed),
        "recent_closed": recent["closed"],
        "recent_win_rate": recent["win_rate"],
        "recent_pf": recent["profit_factor"],
        "recent_expectancy": recent["expectancy"],
        "recent_max_drawdown": recent["max_drawdown"],
        "total_closed": total["closed"],
        "total_win_rate": total["win_rate"],
        "total_pf": total["profit_factor"],
        "total_expectancy": total["expectancy"],
        "total_max_drawdown": total["max_drawdown"],
        "max_drawdown": total["max_drawdown"],
        "monte_carlo_stressed_pf": monte_carlo.get("profit_factor_stressed", 0.0),
        "monte_carlo_stressed_expectancy": monte_carlo.get("expectancy_stressed", 0.0),
        "monte_carlo_p95_drawdown": monte_carlo.get("max_drawdown_p95", 0.0),
        "monte_carlo_fail_reasons": list(monte_carlo.get("fail_reasons") or []),
        "remove_best_3_pf": remove_best_3["profit_factor"],
        "remove_best_3_expectancy": remove_best_3["expectancy"],
        "remove_best_5_pf": remove_best_5["profit_factor"],
        "remove_best_5_expectancy": remove_best_5["expectancy"],
        "spread_x1_5_pf": spread_x1_5["profit_factor"],
        "spread_x1_5_expectancy": spread_x1_5["expectancy"],
        "spread_x2_pf": spread_x2["profit_factor"],
        "spread_x2_expectancy": spread_x2["expectancy"],
        "loss_sequence_stress_drawdown": worst_sequence["max_drawdown"],
        "loss_sequence_stress_pf": worst_sequence["profit_factor"],
        "fragile_regime_dependency": fragile,
        "single_trade_dependency": single_trade,
        "exit_reason_counts": total["exit_reason_counts"],
        "blocked_reason_counts": _reason_counts(blocked),
        "risk_governor_blocks": state.get("risk_governor_blocks", 0),
        "max_open_trades_observed": state.get("max_open_trades_observed", 0),
        "partial_exit_simulated": False,
        "candidate": gate["passed"],
        "recommendation": "research_candidate" if gate["passed"] else "observation_only" if _observation_quality(total, recent, monte_carlo) else "reject",
        "rejection_reasons": gate["reasons"],
        "reject_reasons": gate["reasons"],
        "hardening_score": score,
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


def write_recent_first_hardening_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "recent_first_hardening_results.csv"
    json_path = root / "recent_first_hardening_results.json"
    summary_path = root / "recent_first_hardening_summary.md"
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    headers = [
        "sample_label",
        "target_name",
        "timeframe",
        "family",
        "side",
        "session",
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
        "monte_carlo_stressed_pf",
        "monte_carlo_stressed_expectancy",
        "monte_carlo_p95_drawdown",
        "remove_best_5_pf",
        "spread_x2_pf",
        "loss_sequence_stress_drawdown",
        "fragile_regime_dependency",
        "single_trade_dependency",
        "hardening_help",
        "candidate",
        "recommendation",
        "rejection_reasons",
        "hardening_score",
        "source_csv",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({**row, "rejection_reasons": ";".join(str(item) for item in row.get("rejection_reasons") or [])})
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")
    summary_path.write_text(recent_first_hardening_summary_markdown(result), encoding="utf-8")
    return csv_path, json_path, summary_path


def recent_first_hardening_summary_markdown(result: dict[str, Any]) -> str:
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else _summary(rows, [])
    lines = [
        "# MT5 Recent-First Hardening Summary",
        "",
        "Recent-First Monte Carlo hardening. Paper/offline only; no broker, no order execution, no automatic promotion.",
        "",
        f"Evaluations: `{result.get('evaluations', len(rows))}`.",
        f"Candidates: `{len(result.get('candidates') or [])}`.",
        "",
        "## Top Results",
    ]
    for row in rows[:20]:
        lines.append(
            f"- `{row.get('sample_label')}` `{row.get('target_name')}` recent `{row.get('recent_closed')}` PF `{row.get('recent_pf')}`, "
            f"total `{row.get('total_closed')}` PF `{row.get('total_pf')}`, MC PF `{row.get('monte_carlo_stressed_pf')}`, "
            f"spread x2 PF `{row.get('spread_x2_pf')}`, remove best 5 PF `{row.get('remove_best_5_pf')}`, recommendation `{row.get('recommendation')}`."
        )
    lines.extend(
        [
            "",
            "## Answers",
            f"1. Profile improving Monte Carlo: {summary.get('best_monte_carlo_answer')}",
            f"2. Hardening that helped most: {summary.get('best_hardening_answer')}",
            f"3. M30 london_us_breakout status: {summary.get('m30_breakout_answer')}",
            f"4. Profiles failing stress/reject: {summary.get('stress_fail_answer')}",
            f"5. Capital preservation readiness: {summary.get('capital_preservation_answer')}",
            "6. No automatic promotion.",
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


def _simulate_hardened(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    config: HardeningConfig,
    started: float,
    *,
    timeout_seconds: float,
    features_by_index: dict[int, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str], dict[str, int], dict[str, int]]:
    trades: list[dict[str, Any]] = []
    blocked: list[str] = []
    signals = {"generated": 0, "actionable": 0}
    state = {"risk_governor_blocks": 0, "max_open_trades_observed": 0}
    ranges = _quarter_ranges(len(bars))
    for segment in ["recent", "previous", "middle", "oldest"]:
        segment_trades, segment_blocked, segment_signals, segment_state = _simulate_hardened_segment(
            settings,
            bars,
            config,
            ranges[segment][0],
            ranges[segment][1],
            started,
            timeout_seconds=timeout_seconds,
            features_by_index=features_by_index,
        )
        trades.extend(segment_trades)
        blocked.extend(segment_blocked)
        signals["generated"] += segment_signals["generated"]
        signals["actionable"] += segment_signals["actionable"]
        state["risk_governor_blocks"] += segment_state["risk_governor_blocks"]
        state["max_open_trades_observed"] = max(state["max_open_trades_observed"], segment_state["max_open_trades_observed"])
    return trades, blocked, signals, state


def _simulate_hardened_segment(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    config: HardeningConfig,
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
    loop_start = max(80, start_index)
    loop_end = min(max(loop_start, end_index), len(bars))
    max_iterations = max(1, loop_end - loop_start + 5)
    variant = config.recent_variant()
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
            open_trade, closed = _update_hardened_trade(settings, open_trade, bar, index, config)
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
        open_trade = _open_research_trade(settings, decision, bars[index], index, variant.research_variant())
        if open_trade is None:
            blocked.append("missing_risk_parameters")
            continue
        open_trade = {
            **open_trade,
            "shadow_trade_id": f"recent-hardening-{config.target_name}-{index}",
            "source": "mt5_recent_first_hardening",
            "strategy_profile": config.target_name,
            "filter_profile": config.target_name,
            "hardening_actions": list(config.hardening_actions),
            **_safety(),
        }
        state["max_open_trades_observed"] = max(state["max_open_trades_observed"], 1)
    if open_trade:
        last_bar = bars[min(loop_end - 1, len(bars) - 1)]
        trades.append(_close(settings, open_trade, float(_number(last_bar.get("close")) or open_trade.get("entry_price") or 0.0), "time_stop", last_bar))
    return trades, blocked, signals, state


def _update_hardened_trade(
    settings: BacktestSettings,
    trade: dict[str, Any],
    bar: dict[str, Any],
    index: int,
    config: HardeningConfig,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    high = float(_number(bar.get("high")) or _number(bar.get("close")) or 0.0)
    low = float(_number(bar.get("low")) or _number(bar.get("close")) or 0.0)
    close = float(_number(bar.get("close")) or 0.0)
    open_price = float(_number(bar.get("open")) or close)
    side = str(trade.get("side") or "").lower()
    entry = float(_number(trade.get("entry_price")) or _number(trade.get("entry")) or close)
    stop = float(_number(trade.get("stop_loss")) or entry)
    target = float(_number(trade.get("take_profit")) or entry)
    risk = abs(entry - stop) or max(entry * 0.003, 0.000001)
    bars_open = max(0, index - int(trade.get("opened_index") or index))
    if side == "buy":
        mfe = high - entry
        mae = low - entry
        stop_hit = low <= stop
        target_hit = high >= target
        adverse_now = max(0.0, entry - close)
        momentum_against = close < open_price and close < entry
    else:
        mfe = entry - low
        mae = entry - high
        stop_hit = high >= stop
        target_hit = low <= target
        adverse_now = max(0.0, close - entry)
        momentum_against = close > open_price and close > entry
    updated = {
        **trade,
        "last_price": close,
        "bars_open": bars_open,
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
    if config.fast_loss_cut_r and bars_open >= 1 and momentum_against and adverse_now >= risk * config.fast_loss_cut_r:
        return None, _close(settings, updated, close, "fast_loss_cut", bar)
    if abs(float(_number(updated.get("max_adverse_excursion")) or 0.0)) >= risk * config.mae_exit_r:
        return None, _close(settings, updated, close, "mae_guard_exit", bar)
    if config.trailing_activation_r and float(_number(updated.get("max_favorable_excursion")) or 0.0) >= risk * config.trailing_activation_r:
        trail_exit = entry + risk * config.trailing_lock_r if side == "buy" else entry - risk * config.trailing_lock_r
        if (side == "buy" and close <= trail_exit) or (side == "sell" and close >= trail_exit):
            return None, _close(settings, updated, trail_exit, "trailing_defensive_exit", bar)
    if config.momentum_loss_exit and bars_open >= 1 and momentum_against:
        return None, _close(settings, updated, close, "momentum_loss_exit", bar)
    if bars_open >= config.time_stop_bars:
        return None, _close(settings, updated, close, "time_stop", bar)
    return updated, None


def _split_metrics(settings: BacktestSettings, bars: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    ranges = _quarter_ranges(len(bars))
    return {
        name: _compact_metrics([trade for trade in trades if start <= int(_number(trade.get("opened_index")) or 0) < end], settings.initial_balance)
        for name, (start, end) in ranges.items()
    }


def _spread_stress_metrics(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    config: HardeningConfig,
    features_by_index: dict[int, dict[str, Any]],
    timeout_seconds: float,
    factor: float,
) -> dict[str, Any]:
    stressed = replace(settings, spread_points=settings.spread_points * factor)
    trades, _, _, _ = _simulate_hardened(stressed, bars, config, time.monotonic(), timeout_seconds=timeout_seconds, features_by_index=features_by_index)
    return _metrics([trade for trade in trades if trade.get("lifecycle_status") == "closed"], initial_balance=settings.initial_balance)


def _remove_best_metrics(settings: BacktestSettings, trades: list[dict[str, Any]], count: int) -> dict[str, Any]:
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    if len(closed) <= count:
        return _metrics([], initial_balance=settings.initial_balance)
    ordered = sorted(closed, key=lambda trade: float(_number(trade.get("pnl")) or 0.0), reverse=True)
    remaining = ordered[count:]
    return _metrics(remaining, initial_balance=settings.initial_balance)


def _worst_sequence_metrics(settings: BacktestSettings, trades: list[dict[str, Any]]) -> dict[str, Any]:
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    ordered = sorted(closed, key=lambda trade: float(_number(trade.get("pnl")) or 0.0))
    return _metrics(ordered, initial_balance=settings.initial_balance)


def _gate(
    total: dict[str, Any],
    recent: dict[str, Any],
    monte_carlo: dict[str, Any],
    remove_best_5: dict[str, Any],
    spread_x2: dict[str, Any],
    fragile: bool,
    single_trade: bool,
) -> dict[str, Any]:
    reasons: list[str] = []
    if int(recent.get("closed") or 0) < 25:
        reasons.append("recent_sample_below_25")
    if int(total.get("closed") or 0) < 50:
        reasons.append("total_sample_below_50")
    if float(recent.get("profit_factor") or 0.0) < 1.15:
        reasons.append("recent_pf_below_1_15")
    if float(total.get("profit_factor") or 0.0) < 1.15:
        reasons.append("total_pf_below_1_15")
    if float(recent.get("expectancy") or 0.0) <= 0:
        reasons.append("recent_expectancy_not_positive")
    if float(total.get("expectancy") or 0.0) <= 0:
        reasons.append("total_expectancy_not_positive")
    if float(total.get("max_drawdown") or 0.0) > 5000:
        reasons.append("drawdown_above_5000")
    if float(monte_carlo.get("profit_factor_stressed") or 0.0) < 1.05:
        reasons.append("monte_carlo_stressed_pf_below_1_05")
    if float(monte_carlo.get("expectancy_stressed") or 0.0) < 0:
        reasons.append("monte_carlo_stressed_expectancy_negative")
    if float(monte_carlo.get("max_drawdown_p95") or 0.0) > 5000:
        reasons.append("monte_carlo_p95_drawdown_above_5000")
    if float(remove_best_5.get("profit_factor") or 0.0) < 1.0:
        reasons.append("remove_best_5_pf_below_1")
    if float(spread_x2.get("profit_factor") or 0.0) < 1.0:
        reasons.append("spread_x2_pf_below_1")
    if fragile:
        reasons.append("fragile_regime_dependency")
    if single_trade:
        reasons.append("single_trade_dependency")
    return {"passed": not reasons, "reasons": reasons or ["passes_recent_first_hardening_gates"]}


def _hardening_score(
    total: dict[str, Any],
    recent: dict[str, Any],
    monte_carlo: dict[str, Any],
    remove_best_5: dict[str, Any],
    spread_x2: dict[str, Any],
    gate: dict[str, Any],
    fragile: bool,
    single_trade: bool,
) -> float:
    score = 0.0
    score += min(int(recent.get("closed") or 0), 80) * 2.5
    score += min(int(total.get("closed") or 0), 160) * 0.8
    score += max(0.0, float(recent.get("profit_factor") or 0.0) - 1.0) * 80.0
    score += max(0.0, float(total.get("profit_factor") or 0.0) - 1.0) * 65.0
    score += max(0.0, float(monte_carlo.get("profit_factor_stressed") or 0.0) - 1.0) * 130.0
    score += max(0.0, float(remove_best_5.get("profit_factor") or 0.0) - 1.0) * 75.0
    score += max(0.0, float(spread_x2.get("profit_factor") or 0.0) - 1.0) * 80.0
    score += max(0.0, float(recent.get("expectancy") or 0.0)) * 240.0
    score += max(0.0, float(total.get("expectancy") or 0.0)) * 180.0
    score -= max(0.0, float(total.get("max_drawdown") or 0.0) - 2500.0) / 40.0
    score -= max(0.0, float(monte_carlo.get("max_drawdown_p95") or 0.0) - 3500.0) / 35.0
    score -= max(0, 25 - int(recent.get("closed") or 0)) * 7.0
    score -= max(0, 50 - int(total.get("closed") or 0)) * 4.0
    if fragile:
        score -= 110.0
    if single_trade:
        score -= 120.0
    if not gate.get("passed"):
        score -= len(gate.get("reasons") or []) * 11.0
    return round(score, 4)


def _observation_quality(total: dict[str, Any], recent: dict[str, Any], monte_carlo: dict[str, Any]) -> bool:
    return (
        int(recent.get("closed") or 0) >= 10
        and int(total.get("closed") or 0) >= 40
        and float(recent.get("profit_factor") or 0.0) >= 1.05
        and float(total.get("profit_factor") or 0.0) >= 1.05
        and float(recent.get("expectancy") or 0.0) > 0
        and float(total.get("expectancy") or 0.0) > 0
        and float(monte_carlo.get("max_drawdown_p95") or 0.0) <= 6500
    )


def _hardening_configs() -> list[HardeningConfig]:
    return [
        _config("recent_london_us_breakout_m30_both_hardened_v1", "recent_london_us_breakout", "M30", "both", ("session_filter", "baseline_hardened"), mae=0.72, fast=0.45),
        _config("recent_london_us_breakout_m30_sell_hardened_v1", "recent_london_us_breakout", "M30", "sell", ("side_filter", "session_filter", "baseline_hardened"), mae=0.72, fast=0.45),
        _config("recent_london_us_breakout_m30_buy_hardened_v1", "recent_london_us_breakout", "M30", "buy", ("side_filter", "session_filter", "baseline_hardened"), mae=0.72, fast=0.45),
        _config("recent_london_us_breakout_m30_mae_guard_v1", "recent_london_us_breakout", "M30", "both", ("mae_guard", "session_filter"), mae=0.58, fast=0.0),
        _config("recent_london_us_breakout_m30_fast_loss_cut_v1", "recent_london_us_breakout", "M30", "both", ("fast_loss_cut", "session_filter"), mae=0.76, fast=0.32),
        _config("recent_london_us_breakout_m30_trailing_defensive_v1", "recent_london_us_breakout", "M30", "both", ("trailing_defensive", "session_filter"), mae=0.76, fast=0.45, trail=0.75, lock=0.05),
        _config("recent_liquidity_sweep_h1_hardened_v1", "recent_liquidity_sweep", "H1", "both", ("mae_guard", "fast_loss_cut"), session="all", trend="chop", rr=0.95, time_stop=3, mae=0.62, fast=0.34, score=55.0),
        _config("recent_failed_breakout_reversal_h1_hardened_v1", "recent_failed_breakout_reversal", "H1", "both", ("mae_guard", "fast_loss_cut"), session="all", trend="chop", rr=0.95, time_stop=3, mae=0.62, fast=0.34, score=55.0),
    ]


def _config(
    target: str,
    family: str,
    timeframe: str,
    side: str,
    actions: tuple[str, ...],
    *,
    session: str = "london_us",
    trend: str = "any",
    rr: float = 1.05,
    time_stop: int = 2,
    mae: float = 0.72,
    fast: float = 0.45,
    trail: float = 0.0,
    lock: float = 0.0,
    score: float = 56.0,
) -> HardeningConfig:
    return HardeningConfig(
        target_name=target,
        family=family,
        timeframe=timeframe,
        side_mode=side,
        session_name=session,
        volatility_regime="normal_high" if family == "recent_london_us_breakout" else "any",
        trend_regime=trend,
        rsi_regime="not_extreme",
        score_threshold=score,
        risk_reward=rr,
        time_stop_bars=time_stop,
        atr_stop_multiplier=1.0,
        mae_exit_r=mae,
        momentum_loss_exit=True,
        hardening_actions=actions,
        fast_loss_cut_r=fast,
        trailing_activation_r=trail,
        trailing_lock_r=lock,
    )


def _datasets_for(body: dict[str, Any], csv_dir: Path, symbol: str, max_bars: int) -> list[dict[str, Any]]:
    return [
        {
            "sample_label": "M30_60000",
            "timeframe": "M30",
            "csv_path": str(body.get("csv_path_m30_60000") or csv_dir / f"{symbol}_M30_60000.csv"),
            "max_bars": min(max_bars, 60000),
        },
        {
            "sample_label": "M30_40000",
            "timeframe": "M30",
            "csv_path": str(body.get("csv_path_m30_40000") or csv_dir / f"{symbol}_M30_40000.csv"),
            "max_bars": min(max_bars, 40000),
        },
        {
            "sample_label": "H1_30000",
            "timeframe": "H1",
            "csv_path": str(body.get("csv_path_h1") or csv_dir / f"{symbol}_H1_30000.csv"),
            "max_bars": min(max_bars, 30000),
        },
    ]


def _summary(rows: list[dict[str, Any]], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"recommendation": "no_data", **_safety()}
    best_mc = max(rows, key=lambda row: float(row.get("monte_carlo_stressed_pf") or 0.0))
    m30_rows = [row for row in rows if row.get("family") == "recent_london_us_breakout"]
    m30_best = max(m30_rows, key=lambda row: float(row.get("hardening_score") or 0.0), default={})
    stress_fail = [
        row
        for row in rows
        if row.get("recommendation") == "reject"
        or float(row.get("monte_carlo_stressed_pf") or 0.0) < 1.05
        or float(row.get("spread_x2_pf") or 0.0) < 1.0
    ]
    return {
        "recommendation": "research_candidate" if candidates else "observation_only" if any(row.get("recommendation") == "observation_only" for row in rows) else "reject",
        "best_monte_carlo_answer": f"{best_mc.get('sample_label')} {best_mc.get('target_name')} MC PF {best_mc.get('monte_carlo_stressed_pf')} total {best_mc.get('total_closed')}",
        "best_hardening_answer": _best_action_answer(rows),
        "m30_breakout_answer": (
            f"{m30_best.get('sample_label')} {m30_best.get('target_name')} recommendation {m30_best.get('recommendation')} "
            f"MC PF {m30_best.get('monte_carlo_stressed_pf')} spread x2 PF {m30_best.get('spread_x2_pf')}"
            if m30_best
            else "no M30 london_us_breakout row"
        ),
        "stress_fail_answer": _row_list(stress_fail[:8]),
        "capital_preservation_answer": "; ".join(row.get("target_name", "") for row in candidates[:3]) if candidates else "none; keep all hardening profiles in observation/research mode",
        "automatic_promotion": False,
        **_safety(),
    }


def _best_action_answer(rows: list[dict[str, Any]]) -> str:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        for action in row.get("hardening_actions") or ["none"]:
            buckets.setdefault(str(action), []).append(row)
    ranked = []
    for action, items in buckets.items():
        score = sum(float(row.get("hardening_score") or 0.0) for row in items) / max(1, len(items))
        best_mc = max(float(row.get("monte_carlo_stressed_pf") or 0.0) for row in items)
        ranked.append((score + best_mc * 20.0, action, best_mc))
    ranked.sort(reverse=True)
    return f"{ranked[0][1]} (best MC PF {round(ranked[0][2], 4)})" if ranked else "none"


def _row_list(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "none"
    return "; ".join(f"{row.get('sample_label')} {row.get('target_name')} reasons={','.join(row.get('rejection_reasons') or [])}" for row in rows)


def _hardening_help_label(config: HardeningConfig) -> str:
    if "trailing_defensive" in config.hardening_actions:
        return "trailing_defensive"
    if "fast_loss_cut" in config.hardening_actions:
        return "fast_loss_cut"
    if "mae_guard" in config.hardening_actions:
        return "mae_guard"
    if "side_filter" in config.hardening_actions:
        return "side_filter"
    if "session_filter" in config.hardening_actions:
        return "session_filter"
    return "baseline_hardened"


def _requested_list(value: Any, default: list[str]) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [part.strip() for part in value.split(",") if part.strip()]
    return list(default)

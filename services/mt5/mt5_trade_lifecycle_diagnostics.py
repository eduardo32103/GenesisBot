from __future__ import annotations

import csv
import json
import time
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median
from typing import Any

from services.mt5.mt5_backtester import _load_bars, _metrics, _number, _open_trade, _safety, _settings
from services.mt5.mt5_capital_preservation_optimizer import (
    _PROFILE_PARAMS,
    _allowed_side,
    _apply_capital_trade_risk,
    _capital_decision_from_history,
    _config,
    _depends_on_single_trade,
    _drawdown_accelerating,
    _fast_risk_block,
    _monte_carlo_stress,
    _recent_edge_negative,
    _recent_mae_bad,
    _settings_for_capital_config,
    _update_trade_capital,
)
from services.mt5.mt5_config import get_mt5_config


PRIORITY_PROFILES = [
    "low_drawdown_v5_session_filtered",
    "capital_preservation_v4_side_filtered",
    "trend_continuation_v5_defense_aware",
    "liquidity_sweep_v3_session_confirmed",
]

PRIORITY_MATRIX = [
    ("H1", "low_drawdown_v5_session_filtered"),
    ("H1", "capital_preservation_v4_side_filtered"),
    ("M30", "capital_preservation_v4_side_filtered"),
    ("M30", "trend_continuation_v5_defense_aware"),
    ("M30", "low_drawdown_v5_session_filtered"),
    ("M30", "liquidity_sweep_v3_session_confirmed"),
]


def run_trade_lifecycle_diagnostics(body: dict[str, Any] | None = None) -> dict[str, Any]:
    body = body or {}
    started = time.monotonic()
    symbol = str(body.get("symbol") or "BTCUSD").upper().strip()
    csv_dir = Path(str(body.get("csv_dir") or Path("data") / "backtests"))
    pairs = _requested_pairs(body)
    max_bars = max(100, min(int(_number(body.get("max_bars")) or 20000), 25000))
    spread_points = float(_number(body.get("spread_points")) or 25.0)
    timeout_seconds = float(_number(body.get("timeout_seconds")) or 60.0)
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[str] = []

    for timeframe, profile in pairs:
        if time.monotonic() - started > timeout_seconds:
            errors.append({"timeframe": timeframe, "profile": profile, "error": "lifecycle_timeout_guard"})
            break
        if profile not in _PROFILE_PARAMS:
            errors.append({"timeframe": timeframe, "profile": profile, "error": "unknown_profile"})
            continue
        csv_path = _csv_path_for(body, csv_dir, symbol, timeframe)
        base_body = {
            "symbol": symbol,
            "timeframe": timeframe,
            "csv_path": str(csv_path),
            "max_bars": max_bars,
            "spread_points": spread_points,
            "save_results": False,
            "source": "mt5_csv",
            "timeout_seconds": max(1.0, min(timeout_seconds, 30.0)),
        }
        settings = _settings(base_body, get_mt5_config())
        bars, load_warnings = _load_bars(base_body, settings)
        warnings.extend(load_warnings)
        bars = bars[-settings.max_bars :]
        if not bars:
            errors.append({"timeframe": timeframe, "profile": profile, "path": str(csv_path), "error": "csv_bars_not_loaded"})
            continue
        row = diagnose_trade_lifecycle(
            bars,
            settings,
            profile,
            source_csv=str(csv_path),
            started=time.monotonic(),
            timeout_seconds=max(1.0, min(timeout_seconds, 30.0)),
        )
        rows.append(row)

    rows.sort(key=lambda row: (int(row.get("closed_trade_count") or 0), float(row.get("profit_factor") or 0.0)), reverse=True)
    return {
        "ok": True,
        "status": "mt5_trade_lifecycle_diagnostics_completed",
        "symbol": symbol,
        "pairs": [{"timeframe": timeframe, "profile": profile} for timeframe, profile in pairs],
        "results": rows,
        "summary": _aggregate_lifecycle_summary(rows),
        "errors": errors,
        "warnings": warnings,
        "live_runtime_mutated": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "shadow_trades_mutated": False,
        "martingale_enabled": False,
        "grid_enabled": False,
        "averaging_down_enabled": False,
        "increase_size_after_loss_enabled": False,
        "duration_ms": int((time.monotonic() - started) * 1000),
        **_safety(),
    }


def diagnose_trade_lifecycle(
    bars: list[dict[str, Any]],
    base_settings: Any,
    profile: str,
    *,
    source_csv: str = "",
    started: float | None = None,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    started = started or time.monotonic()
    params = dict(_PROFILE_PARAMS[profile])
    config = _config(
        profile,
        1.2,
        3,
        float(params.get("min_score") or 65.0),
        float(params.get("max_spread_points") or 25.0),
        True,
        bool(params.get("avoid_chop", True)),
        2,
        2,
        True,
        True,
        True,
        True,
        bool(params.get("session_filter")),
        bool(params.get("partial_exit")),
        bool(params.get("atr_trailing")),
        True,
        0.1,
    )
    settings = _settings_for_capital_config(base_settings, config)
    counters: Counter[str] = Counter()
    no_trade_reasons: Counter[str] = Counter()
    exit_reasons: Counter[str] = Counter()
    side_signals: Counter[str] = Counter()
    hour_signals: Counter[str] = Counter()
    hour_trades: Counter[str] = Counter()
    open_trade: dict[str, Any] | None = None
    trades: list[dict[str, Any]] = []
    lost_opportunities: list[dict[str, Any]] = []
    cooldown_until = -1
    iterations = 0
    max_iterations = len(bars) + 5
    timed_out = False

    for index in range(1, len(bars)):
        iterations += 1
        if iterations > max_iterations:
            no_trade_reasons["loop_guard"] += 1
            break
        if time.monotonic() - started > timeout_seconds:
            timed_out = True
            no_trade_reasons["timeout_guard"] += 1
            break
        bar = bars[index]
        if open_trade:
            open_trade, closed = _update_trade_capital(settings, open_trade, bar, index, config)
            if closed:
                trades.append(closed)
                exit_reasons[str(closed.get("exit_reason") or "unknown")] += 1
                opened_hour = _hour_from_time(closed.get("opened_at"))
                if opened_hour is not None:
                    hour_trades[str(opened_hour)] += 1
                if closed.get("status") == "loss":
                    cooldown_until = max(cooldown_until, index + config.cooldown_after_loss_bars)
                open_trade = None
        if index >= len(bars) - 1:
            continue
        history = bars[max(0, index - 80) : index]
        decision = _capital_decision_from_history(history, settings, config)
        if not decision.get("actionable"):
            reason = str(decision.get("reason") or "no_edge")
            no_trade_reasons[reason] += 1
            if reason == "session_filter":
                counters["skipped_due_session_filter"] += 1
            continue

        counters["generated_signal_count"] += 1
        side = str(decision.get("side") or "unknown").lower()
        side_signals[side] += 1
        hour = _hour_from_time(bar.get("time"))
        if hour is not None:
            hour_signals[str(hour)] += 1
        if not _allowed_side(side, settings.filter_params or {}):
            counters["skipped_due_side_filter"] += 1
            no_trade_reasons["side_filtered"] += 1
            continue

        occupancy_reason = _occupancy_block_reason(settings, trades, config, index, cooldown_until)
        if occupancy_reason:
            counters[f"skipped_due_{occupancy_reason}"] += 1
            no_trade_reasons[occupancy_reason] += 1
            continue

        if open_trade:
            counters["skipped_due_max_open_trades"] += 1
            no_trade_reasons["max_open_trades"] += 1
            _record_lost_opportunity(lost_opportunities, bar, index, decision, open_trade)
            continue

        risk_reason = _fast_risk_block(settings, trades, config, str(decision.get("regime") or "trend"), has_open_trade=False)
        if risk_reason:
            counters["blocked_by_risk_governor"] += 1
            no_trade_reasons[f"risk_governor_{risk_reason}"] += 1
            continue

        counters["actionable_signal_count"] += 1
        candidate = _open_trade(settings, decision, bar, index, f"lifecycle-{profile}-{index}")
        if candidate is None:
            no_trade_reasons["missing_risk_parameters"] += 1
            continue
        open_trade = _apply_capital_trade_risk(candidate, decision, settings, config)
        open_trade = {
            **open_trade,
            "source": "mt5_trade_lifecycle_diagnostics",
            "filter_profile": profile,
            "strategy_profile": profile,
            "risk_governor_allowed": True,
            "risk_governor_reason": "risk_governor_pass",
            "risk_state": "normal",
            "suggested_lot_multiplier": 1.0,
            "trailing_stop_active": False,
            "virtual_stop_loss": open_trade.get("stop_loss"),
            "martingale_enabled": False,
            "grid_enabled": False,
            "averaging_down_enabled": False,
            "increase_size_after_loss_enabled": False,
            **_safety(),
        }
        counters["opened_trade_count"] += 1

    if open_trade:
        counters["open_trade_left_at_end"] += 1
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    bars_in_trade = [int(_number(trade.get("bars_open")) or 0) for trade in closed]
    metrics = _metrics(closed, initial_balance=settings.initial_balance)
    monte_carlo = _monte_carlo_stress(closed, initial_balance=settings.initial_balance, max_drawdown_limit=5000.0, simulations=250)
    fragility_reasons = _fragility_reasons(metrics, monte_carlo)
    side_stats = _side_lifecycle_stats(closed)
    hour_stats = _hour_lifecycle_stats(closed)
    exposure_pct = round((sum(bars_in_trade) / max(1, len(bars))) * 100.0, 4)
    row = {
        "timeframe": settings.timeframe,
        "profile": profile,
        "source_csv": source_csv,
        "bars_loaded": len(bars),
        "bars_evaluated": max(0, len(bars) - 1),
        "generated_signal_count": int(counters["generated_signal_count"]),
        "actionable_signal_count": int(counters["actionable_signal_count"]),
        "opened_trade_count": int(counters["opened_trade_count"]),
        "closed_trade_count": len(closed),
        "skipped_due_max_open_trades": int(counters["skipped_due_max_open_trades"]),
        "skipped_due_cooldown_after_loss": int(counters["skipped_due_cooldown_after_loss"]),
        "skipped_due_consecutive_losses": int(counters["skipped_due_consecutive_losses"]),
        "skipped_due_drawdown_accelerating": int(counters["skipped_due_drawdown_accelerating"]),
        "skipped_due_mae_filter": int(counters["skipped_due_mae_filter"]),
        "skipped_due_session_filter": int(counters["skipped_due_session_filter"]),
        "skipped_due_side_filter": int(counters["skipped_due_side_filter"]),
        "blocked_by_risk_governor": int(counters["blocked_by_risk_governor"]),
        "avg_bars_in_trade": round(sum(bars_in_trade) / max(1, len(bars_in_trade)), 4),
        "median_bars_in_trade": round(float(median(bars_in_trade)), 4) if bars_in_trade else 0.0,
        "max_bars_in_trade": max(bars_in_trade, default=0),
        "exposure_pct": exposure_pct,
        "time_in_market_pct": exposure_pct,
        "avg_MAE": _avg_trade_value(closed, "max_adverse_excursion"),
        "avg_MFE": _avg_trade_value(closed, "max_favorable_excursion"),
        "exit_reason_counts": dict(exit_reasons),
        "time_stop_exits": int(exit_reasons["time_stop"]),
        "momentum_loss_exits": int(exit_reasons["momentum_loss_exit"]),
        "stop_loss_exits": int(exit_reasons["stop_loss"]),
        "take_profit_exits": int(exit_reasons["take_profit"]),
        "trailing_exits": int(exit_reasons["trailing_stop"]),
        "side_signal_distribution": dict(side_signals),
        "session_signal_distribution": dict(sorted(hour_signals.items(), key=lambda item: int(item[0]))),
        "side_stats": side_stats,
        "session_hour_stats": hour_stats,
        "top_lost_opportunities_due_open_trade": lost_opportunities[:10],
        "top_no_trade_reasons": _top_items(no_trade_reasons, 10),
        "no_trade_reason_counts": dict(no_trade_reasons),
        "win_rate": metrics["win_rate"],
        "profit_factor": metrics["profit_factor"],
        "expectancy": metrics["expectancy"],
        "max_drawdown": metrics["max_drawdown"],
        "monte_carlo": monte_carlo,
        "monte_carlo_fail_reasons": list(monte_carlo.get("fail_reasons") or []),
        "fragility_reasons": fragility_reasons,
        "fails_only_sample_too_small": fragility_reasons == ["sample_too_small"],
        "timed_out": timed_out,
        "live_runtime_mutated": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "shadow_trades_mutated": False,
        "martingale_enabled": False,
        "grid_enabled": False,
        "averaging_down_enabled": False,
        "increase_size_after_loss_enabled": False,
        **_safety(),
    }
    return row


def write_trade_lifecycle_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "trade_lifecycle_diagnostics.csv"
    json_path = root / "trade_lifecycle_diagnostics.json"
    summary_path = root / "trade_lifecycle_summary.md"
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    headers = [
        "timeframe",
        "profile",
        "bars_loaded",
        "generated_signal_count",
        "actionable_signal_count",
        "opened_trade_count",
        "closed_trade_count",
        "skipped_due_max_open_trades",
        "skipped_due_cooldown_after_loss",
        "skipped_due_consecutive_losses",
        "skipped_due_drawdown_accelerating",
        "skipped_due_mae_filter",
        "skipped_due_session_filter",
        "skipped_due_side_filter",
        "blocked_by_risk_governor",
        "avg_bars_in_trade",
        "median_bars_in_trade",
        "max_bars_in_trade",
        "exposure_pct",
        "avg_MAE",
        "avg_MFE",
        "time_stop_exits",
        "momentum_loss_exits",
        "stop_loss_exits",
        "take_profit_exits",
        "trailing_exits",
        "win_rate",
        "profit_factor",
        "expectancy",
        "max_drawdown",
        "monte_carlo_fail_reasons",
        "fragility_reasons",
        "fails_only_sample_too_small",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    **row,
                    "monte_carlo_fail_reasons": ";".join(str(item) for item in row.get("monte_carlo_fail_reasons") or []),
                    "fragility_reasons": ";".join(str(item) for item in row.get("fragility_reasons") or []),
                }
            )
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")
    summary_path.write_text(trade_lifecycle_summary_markdown(result), encoding="utf-8")
    return csv_path, json_path, summary_path


def trade_lifecycle_summary_markdown(result: dict[str, Any]) -> str:
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    summary = _aggregate_lifecycle_summary(rows)
    lines = [
        "# MT5 Trade Lifecycle Diagnostics",
        "",
        "Paper-only lifecycle diagnostics. No broker touched, no orders, no live state mutation.",
        "",
        f"Main bottleneck: `{summary.get('main_bottleneck')}`.",
        f"Main post-signal bottleneck: `{summary.get('main_post_signal_bottleneck')}`.",
        f"Best quality/rotation profile: `{summary.get('best_quality_rotation_profile')}`.",
        "",
        "## Profiles",
    ]
    if not rows:
        lines.append("- No diagnostic rows were generated.")
    for row in rows:
        lines.append(
            f"- `{row.get('timeframe')} {row.get('profile')}` closed `{row.get('closed_trade_count')}`, "
            f"PF `{row.get('profit_factor')}`, expectancy `{row.get('expectancy')}`, DD `{row.get('max_drawdown')}`, "
            f"avg bars `{row.get('avg_bars_in_trade')}`, exposure `{row.get('exposure_pct')}%`, "
            f"max-open skips `{row.get('skipped_due_max_open_trades')}`, fragility `{row.get('fragility_reasons')}`."
        )
    lines.extend(
        [
            "",
            "## Answers",
            f"1. MaxOpenTrades=1 problem: {summary.get('max_open_answer')}",
            f"2. Trade duration problem: {summary.get('duration_answer')}",
            f"3. Best quality but worst rotation: {summary.get('best_quality_worst_rotation')}",
            f"4. Exit improvement: {summary.get('exit_recommendation')}",
            f"5. Sides/sessions to preserve: {summary.get('side_session_recommendation')}",
            f"6. Suggested variants: {', '.join(summary.get('suggested_variants') or []) or 'none'}",
            f"7. Sample-only profiles: {', '.join(summary.get('sample_only_profiles') or []) or 'none'}",
            f"8. Monte Carlo fragile profiles: {', '.join(summary.get('monte_carlo_fragile_profiles') or []) or 'none'}",
            "",
            "## Safety",
            "- No real trading.",
            "- No order_send.",
            "- No martingale, no grid, no averaging down, no size increase after loss.",
            "- MaxOpenTrades stays 1.",
            "- broker_touched=false",
            "- order_executed=false",
            "- order_policy=journal_only_no_broker",
        ]
    )
    return "\n".join(lines) + "\n"


def _occupancy_block_reason(settings: Any, trades: list[dict[str, Any]], config: Any, index: int, cooldown_until: int) -> str:
    if index < cooldown_until:
        return "cooldown_after_loss"
    if _loss_streak_local(trades) >= config.block_after_consecutive_losses:
        return "consecutive_losses"
    if config.no_trade_if_recent_edge_negative and _recent_edge_negative(trades):
        return "recent_edge_negative"
    if config.no_trade_if_drawdown_accelerating and _drawdown_accelerating(trades, settings.initial_balance):
        return "drawdown_accelerating"
    if config.max_adverse_excursion_filter and _recent_mae_bad(trades):
        return "mae_filter"
    return ""


def _record_lost_opportunity(
    items: list[dict[str, Any]],
    bar: dict[str, Any],
    index: int,
    decision: dict[str, Any],
    open_trade: dict[str, Any],
) -> None:
    if len(items) >= 25:
        return
    items.append(
        {
            "time": str(bar.get("time") or ""),
            "index": index,
            "side": decision.get("side"),
            "score": decision.get("score"),
            "reason": decision.get("reason"),
            "open_shadow_trade_id": open_trade.get("shadow_trade_id"),
            "open_side": open_trade.get("side"),
            "open_bars": int(_number(open_trade.get("bars_open")) or 0),
        }
    )


def _avg_trade_value(trades: list[dict[str, Any]], field: str) -> float:
    values = [float(_number(trade.get(field)) or 0.0) for trade in trades]
    return round(sum(values) / max(1, len(values)), 6)


def _side_lifecycle_stats(trades: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {side: _metrics([trade for trade in trades if str(trade.get("side") or "").lower() == side], initial_balance=100000.0) for side in ["buy", "sell"]}


def _hour_lifecycle_stats(trades: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    buckets: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        hour = _hour_from_time(trade.get("opened_at"))
        if hour is not None:
            buckets[str(hour)].append(trade)
    return {hour: _metrics(scoped, initial_balance=100000.0) for hour, scoped in sorted(buckets.items(), key=lambda item: int(item[0]))}


def _fragility_reasons(metrics: dict[str, Any], monte_carlo: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    closed = int(metrics.get("closed") or 0)
    if closed < 75:
        reasons.append("sample_too_small")
    if float(metrics.get("profit_factor") or 0.0) < 1.2:
        reasons.append("pf_below_1_20")
    if float(metrics.get("expectancy") or 0.0) <= 0:
        reasons.append("expectancy_not_positive")
    if float(metrics.get("win_rate") or 0.0) < 45:
        reasons.append("win_rate_below_45")
    if _depends_on_single_trade(metrics):
        reasons.append("single_trade_dependency")
    for reason in monte_carlo.get("fail_reasons") or []:
        mapped = {
            "stressed_pf_below_1_05": "monte_carlo_stressed_pf_below_1_05",
            "stressed_expectancy_negative": "monte_carlo_stressed_expectancy_negative",
        }.get(str(reason), str(reason))
        if mapped not in reasons:
            reasons.append(mapped)
    return reasons


def _aggregate_lifecycle_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "main_bottleneck": "no_data",
            "best_quality_rotation_profile": "",
            "suggested_variants": [],
            **_safety(),
        }
    totals = Counter()
    for row in rows:
        for key in [
            "skipped_due_max_open_trades",
            "skipped_due_cooldown_after_loss",
            "skipped_due_consecutive_losses",
            "skipped_due_drawdown_accelerating",
            "skipped_due_mae_filter",
            "skipped_due_session_filter",
            "skipped_due_side_filter",
        ]:
            totals[key] += int(row.get(key) or 0)
    main_bottleneck = totals.most_common(1)[0][0] if totals else "unknown"
    post_signal = Counter({key: value for key, value in totals.items() if key not in {"skipped_due_session_filter", "skipped_due_side_filter"}})
    main_post_signal = post_signal.most_common(1)[0][0] if post_signal else "none"
    best_rotation = max(rows, key=lambda row: _quality_rotation_score(row))
    quality_rows = sorted(rows, key=lambda row: (float(row.get("profit_factor") or 0.0), float(row.get("expectancy") or 0.0)), reverse=True)
    worst_rotation_among_quality = max(quality_rows[:3], key=lambda row: float(row.get("avg_bars_in_trade") or 0.0)) if quality_rows else None
    sample_only = [f"{row.get('timeframe')} {row.get('profile')}" for row in rows if row.get("fails_only_sample_too_small")]
    fragile = [
        f"{row.get('timeframe')} {row.get('profile')}"
        for row in rows
        if any(str(reason).startswith("monte_carlo") for reason in row.get("fragility_reasons") or [])
    ]
    variants = _suggested_variants(rows, main_bottleneck)
    return {
        "main_bottleneck": main_bottleneck,
        "main_post_signal_bottleneck": main_post_signal,
        "best_quality_rotation_profile": f"{best_rotation.get('timeframe')} {best_rotation.get('profile')}",
        "max_open_answer": _yes_no(totals["skipped_due_max_open_trades"] > max(5, sum(int(row.get("opened_trade_count") or 0) for row in rows))),
        "duration_answer": _yes_no(max(float(row.get("avg_bars_in_trade") or 0.0) for row in rows) >= 2.5),
        "best_quality_worst_rotation": f"{worst_rotation_among_quality.get('timeframe')} {worst_rotation_among_quality.get('profile')}" if worst_rotation_among_quality else "",
        "exit_recommendation": _exit_recommendation(rows),
        "side_session_recommendation": _side_session_recommendation(rows),
        "suggested_variants": variants,
        "sample_only_profiles": sample_only,
        "monte_carlo_fragile_profiles": fragile,
        "bottleneck_counts": dict(totals),
        **_safety(),
    }


def _suggested_variants(rows: list[dict[str, Any]], main_bottleneck: str) -> list[str]:
    variants: list[str] = []
    total_open_skips = sum(int(row.get("skipped_due_max_open_trades") or 0) for row in rows)
    total_opened = sum(int(row.get("opened_trade_count") or 0) for row in rows)
    long_duration = any(float(row.get("avg_bars_in_trade") or 0.0) >= 2.5 for row in rows)
    if (total_open_skips > max(10, total_opened * 0.5)) or long_duration:
        variants.extend(["low_drawdown_v6_faster_rotation", "capital_preservation_v5_fast_time_stop"])
    if total_open_skips > max(10, total_opened * 0.5):
        variants.append("trend_continuation_v6_active_trade_aware")
    total_time_stop = sum(int(row.get("time_stop_exits") or 0) for row in rows)
    total_closed = sum(int(row.get("closed_trade_count") or 0) for row in rows)
    if total_time_stop > max(5, total_closed * 0.35):
        variants.append("liquidity_sweep_v4_fast_reversal_exit")
    return list(dict.fromkeys(variants))


def _quality_rotation_score(row: dict[str, Any]) -> float:
    closed = float(row.get("closed_trade_count") or 0.0)
    pf = float(row.get("profit_factor") or 0.0)
    expectancy = float(row.get("expectancy") or 0.0)
    drawdown = float(row.get("max_drawdown") or 0.0)
    avg_bars = float(row.get("avg_bars_in_trade") or 0.0)
    return pf * 35.0 + expectancy * 120.0 + min(closed, 75.0) * 1.5 - drawdown / 120.0 - avg_bars * 4.0


def _exit_recommendation(rows: list[dict[str, Any]]) -> str:
    time_stops = sum(int(row.get("time_stop_exits") or 0) for row in rows)
    momentum = sum(int(row.get("momentum_loss_exits") or 0) for row in rows)
    mae = sum(int(row.get("skipped_due_mae_filter") or 0) for row in rows)
    if time_stops > max(momentum, 0):
        return "test adaptive faster time_stop and momentum_loss_exit; keep MAE defense"
    if mae:
        return "MAE defense is active; tune exits rather than adding risk"
    return "current exits are not the primary bottleneck"


def _side_session_recommendation(rows: list[dict[str, Any]]) -> str:
    best = max(rows, key=lambda row: _quality_rotation_score(row))
    side_stats = best.get("side_stats") if isinstance(best.get("side_stats"), dict) else {}
    buy_pf = float((side_stats.get("buy") or {}).get("profit_factor") or 0.0)
    sell_pf = float((side_stats.get("sell") or {}).get("profit_factor") or 0.0)
    side = "buy" if buy_pf >= sell_pf else "sell"
    hours = best.get("session_hour_stats") if isinstance(best.get("session_hour_stats"), dict) else {}
    positive_hours = [
        hour
        for hour, metrics in hours.items()
        if float(metrics.get("expectancy") or 0.0) > 0 and float(metrics.get("profit_factor") or 0.0) >= 1.2
    ]
    return f"preserve {side} bias; strongest hours: {','.join(positive_hours[:8]) or 'insufficient hour sample'}"


def _csv_path_for(body: dict[str, Any], csv_dir: Path, symbol: str, timeframe: str) -> Path:
    explicit = body.get(f"csv_path_{timeframe.lower()}") or body.get("csv_path")
    if explicit:
        return Path(str(explicit))
    extended = csv_dir / f"{symbol}_{timeframe}_{20000 if timeframe in {'M15', 'M30'} else 10000}.csv"
    if extended.exists():
        return extended
    return csv_dir / f"{symbol}_{timeframe}_5000.csv"


def _requested_pairs(body: dict[str, Any]) -> list[tuple[str, str]]:
    raw_pairs = body.get("pairs")
    if isinstance(raw_pairs, str) and raw_pairs.strip():
        return _parse_pair_tokens([part.strip() for part in raw_pairs.split(",") if part.strip()])
    if isinstance(raw_pairs, list) and raw_pairs:
        return _parse_pair_tokens(raw_pairs)
    timeframes = _requested_list(body.get("timeframes"), [])
    profiles = _requested_list(body.get("profiles"), [])
    if timeframes and profiles:
        return [(timeframe.upper(), profile) for timeframe in timeframes for profile in profiles]
    return list(PRIORITY_MATRIX)


def _parse_pair_tokens(items: list[Any]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for item in items:
        if isinstance(item, dict):
            timeframe = str(item.get("timeframe") or "").upper().strip()
            profile = str(item.get("profile") or "").strip()
        else:
            parts = str(item).split(":", 1)
            timeframe = parts[0].upper().strip() if parts else ""
            profile = parts[1].strip() if len(parts) > 1 else ""
        if timeframe and profile:
            pairs.append((timeframe, profile))
    return pairs


def _requested_list(value: Any, default: list[str]) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [part.strip() for part in value.split(",") if part.strip()]
    return list(default)


def _loss_streak_local(trades: list[dict[str, Any]]) -> int:
    streak = 0
    for trade in reversed(trades):
        if trade.get("status") == "loss":
            streak += 1
        elif trade.get("lifecycle_status") == "closed":
            break
    return streak


def _hour_from_time(value: Any) -> int | None:
    text = str(value or "")
    try:
        if "T" in text:
            return int(text.split("T", 1)[1][:2])
        if " " in text:
            return int(text.split(" ", 1)[1][:2])
    except Exception:
        return None
    return None


def _top_items(counter: Counter[str], limit: int) -> list[dict[str, Any]]:
    return [{"reason": key, "count": int(value)} for key, value in counter.most_common(limit)]


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"

from __future__ import annotations

import csv
import json
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

from services.mt5.mt5_backtester import _load_bars, _metrics, _number, _reason_counts, _safety, _settings
from services.mt5.mt5_capital_preservation_optimizer import _depends_on_single_trade, _monte_carlo_stress
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_strategy_research_v2 import (
    ResearchVariant,
    _features_by_index,
    _simulate_research,
    _volatility_bucket,
)


TARGET_VARIANTS = [
    (
        "m30_range_breakout_both_all",
        "baseline",
        ResearchVariant("range_breakout_anti_chop", "M30", "both", "all", "normal_high", "any", 57.0, 1.15, 2, 1.0, 0.9, True),
    ),
    (
        "m30_range_breakout_sell_all",
        "baseline",
        ResearchVariant("range_breakout_anti_chop", "M30", "sell", "all", "normal_high", "any", 57.0, 1.15, 2, 1.0, 0.9, True),
    ),
    (
        "m30_range_breakout_buy_all",
        "baseline",
        ResearchVariant("range_breakout_anti_chop", "M30", "buy", "all", "normal_high", "any", 57.0, 1.15, 2, 1.0, 0.9, True),
    ),
    (
        "m15_momentum_continuation_sell_london",
        "baseline",
        ResearchVariant("momentum_continuation_filtered", "M15", "sell", "london_us", "normal_high", "trend", 58.0, 1.2, 2, 1.05, 0.9, True),
    ),
    (
        "range_breakout_anti_chop_m30_sell_only_v1",
        "conservative_variant",
        ResearchVariant("range_breakout_anti_chop", "M30", "sell", "all", "any", "any", 53.0, 1.0, 1, 0.95, 0.8, True),
    ),
    (
        "range_breakout_anti_chop_m30_london_us_v1",
        "conservative_variant",
        ResearchVariant("range_breakout_anti_chop", "M30", "both", "london_us", "normal_high", "any", 57.0, 1.15, 2, 1.0, 0.85, True),
    ),
    (
        "range_breakout_anti_chop_m30_regime_filtered_v1",
        "conservative_variant",
        ResearchVariant("range_breakout_anti_chop", "M30", "both", "all", "normal_high", "trend", 56.0, 1.1, 2, 1.0, 0.85, True),
    ),
    (
        "range_breakout_anti_chop_m30_weak_tercile_guard_v1",
        "conservative_variant",
        ResearchVariant("range_breakout_anti_chop", "M30", "both", "london_us", "normal_high", "any", 59.0, 1.15, 2, 0.95, 0.75, True),
    ),
]


def run_research_v2_candidate_robustness(body: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    body = body or {}
    symbol = str(body.get("symbol") or "BTCUSD").upper().strip()
    csv_dir = Path(str(body.get("csv_dir") or Path("data") / "backtests"))
    max_bars = max(200, min(int(_number(body.get("max_bars")) or 30000), 35000))
    timeout_seconds = max(0.25, float(_number(body.get("per_evaluation_timeout_seconds")) or 4.0))
    spread_points = float(_number(body.get("spread_points")) or 25.0)
    requested = _requested_list(body.get("targets"), [name for name, _kind, _variant in TARGET_VARIANTS])
    selected = [(name, kind, variant) for name, kind, variant in TARGET_VARIANTS if name in requested]
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[str] = []
    bars_cache: dict[str, list[dict[str, Any]]] = {}
    features_cache: dict[str, dict[int, dict[str, Any]]] = {}
    settings_cache: dict[str, Any] = {}
    csv_cache: dict[str, str] = {}

    for _name, _kind, variant in selected:
        timeframe = variant.timeframe
        if timeframe not in bars_cache:
            csv_path = _csv_path_for(body, csv_dir, symbol, timeframe)
            csv_cache[timeframe] = str(csv_path)
            if not csv_path.exists():
                errors.append({"timeframe": timeframe, "path": str(csv_path), "error": "csv_not_found"})
                continue
            settings_body = {
                "symbol": symbol,
                "timeframe": timeframe,
                "csv_path": str(csv_path),
                "max_bars": max_bars,
                "spread_points": spread_points,
                "source": "mt5_csv",
                "save_results": False,
                "timeout_seconds": timeout_seconds,
            }
            settings = replace(_settings(settings_body, get_mt5_config()), max_bars=max_bars, timeout_seconds=timeout_seconds)
            bars, load_warnings = _load_bars(settings_body, settings)
            warnings.extend([f"{timeframe}:{warning}" for warning in load_warnings])
            bars = bars[-settings.max_bars :]
            if not bars:
                errors.append({"timeframe": timeframe, "path": str(csv_path), "error": "csv_bars_not_loaded"})
                continue
            bars_cache[timeframe] = bars
            features_cache[timeframe] = _features_by_index(bars)
            settings_cache[timeframe] = settings

    for name, kind, variant in selected:
        bars = bars_cache.get(variant.timeframe)
        settings = settings_cache.get(variant.timeframe)
        if not bars or settings is None:
            continue
        rows.append(
            evaluate_research_v2_candidate_robustness(
                settings,
                bars,
                variant,
                target_name=name,
                target_kind=kind,
                source_csv=csv_cache.get(variant.timeframe, ""),
                features_by_index=features_cache.get(variant.timeframe, {}),
                timeout_seconds=timeout_seconds,
            )
        )

    rows.sort(key=_robustness_rank, reverse=True)
    candidates = [row for row in rows if row.get("candidate")]
    result = {
        "ok": True,
        "status": "mt5_research_v2_candidate_robustness_completed",
        "symbol": symbol,
        "targets": [name for name, _kind, _variant in selected],
        "csv_paths": csv_cache,
        "results": rows,
        "candidates": candidates,
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


def evaluate_research_v2_candidate_robustness(
    settings: Any,
    bars: list[dict[str, Any]],
    variant: ResearchVariant,
    *,
    target_name: str,
    target_kind: str,
    source_csv: str = "",
    features_by_index: dict[int, dict[str, Any]] | None = None,
    timeout_seconds: float = 4.0,
) -> dict[str, Any]:
    started = time.monotonic()
    trades, blocked, signals, state = _simulate_research(
        settings,
        bars,
        variant,
        started,
        timeout_seconds=timeout_seconds,
        features_by_index=features_by_index or _features_by_index(bars),
    )
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    metrics = _metrics(closed, initial_balance=settings.initial_balance)
    window_stats = _window_stats(settings, bars, closed)
    side_stats = _group_stats(settings, closed, lambda trade: str(trade.get("side") or "unknown").lower())
    session_stats = _group_stats(settings, closed, lambda trade: _session_name(_trade_hour(trade)))
    hour_stats = _group_stats(settings, closed, lambda trade: str(_trade_hour(trade)))
    regime_stats = _group_stats(settings, closed, lambda trade: str(trade.get("regime") or "unknown"))
    volatility_stats = _group_stats(settings, closed, lambda trade: _vol_bucket_from_trade(trade))
    atr_stats = _group_stats(settings, closed, lambda trade: _atr_bucket_from_trade(trade))
    rsi_stats = _group_stats(settings, closed, lambda trade: _rsi_bucket_from_trade(trade))
    exit_stats = _group_stats(settings, closed, lambda trade: str(trade.get("exit_reason") or "unknown"))
    monte_carlo = _monte_carlo_stress(closed, initial_balance=settings.initial_balance, max_drawdown_limit=5000.0, simulations=500)
    weakest = _weakest_segment(window_stats, session_stats, side_stats, regime_stats, volatility_stats, atr_stats, rsi_stats, exit_stats)
    strongest = _strongest_segment(window_stats, session_stats, side_stats, regime_stats, volatility_stats, atr_stats, rsi_stats, exit_stats)
    filter_hint = _filter_hint(weakest, strongest)
    fragile = _fragile(window_stats, metrics)
    single_trade = _depends_on_single_trade(metrics)
    gate = _candidate_gate(metrics, window_stats, monte_carlo, fragile, single_trade)
    score = _score(metrics, monte_carlo, fragile, single_trade, gate)
    return {
        "target_name": target_name,
        "target_kind": target_kind,
        "timeframe": variant.timeframe,
        "family": variant.family,
        "side_mode": variant.side_mode,
        "session_filter": variant.session_name,
        "volatility_regime": variant.volatility_regime,
        "trend_regime": variant.trend_regime,
        "source_csv": source_csv,
        "bars_loaded": len(bars),
        "bars_evaluated": max(0, len(bars) - 80),
        "first_bar_time": str(bars[0].get("time") or "") if bars else "",
        "last_bar_time": str(bars[-1].get("time") or "") if bars else "",
        "generated_signal_count": signals.get("generated", 0),
        "actionable_signal_count": signals.get("actionable", 0),
        "opened_trade_count": len(trades),
        "closed": metrics["closed"],
        "wins": metrics["wins"],
        "losses": metrics["losses"],
        "win_rate": metrics["win_rate"],
        "profit_factor": metrics["profit_factor"],
        "expectancy": metrics["expectancy"],
        "max_drawdown": metrics["max_drawdown"],
        "net_pnl": metrics["net_pnl"],
        "tercile_stats": window_stats["terciles"],
        "half_stats": window_stats["halves"],
        "quarter_stats": window_stats["quarters"],
        "tercile_pf": {key: value["profit_factor"] for key, value in window_stats["terciles"].items()},
        "tercile_expectancy": {key: value["expectancy"] for key, value in window_stats["terciles"].items()},
        "tercile_drawdown": {key: value["max_drawdown"] for key, value in window_stats["terciles"].items()},
        "side_stats": side_stats,
        "session_stats": session_stats,
        "hour_stats": hour_stats,
        "regime_stats": regime_stats,
        "volatility_stats": volatility_stats,
        "atr_regime_stats": atr_stats,
        "rsi_regime_stats": rsi_stats,
        "exit_reason_stats": exit_stats,
        "exit_reason_counts": metrics["exit_reason_counts"],
        "monte_carlo_stressed_pf": monte_carlo.get("profit_factor_stressed", 0.0),
        "monte_carlo_p95_drawdown": monte_carlo.get("max_drawdown_p95", 0.0),
        "monte_carlo_fail_reasons": list(monte_carlo.get("fail_reasons") or []),
        "fragile_regime_dependency": fragile,
        "single_trade_dependency": single_trade,
        "weakest_segment": weakest,
        "strongest_segment": strongest,
        "filter_hint": filter_hint,
        "blocked_reason_counts": _reason_counts(blocked),
        "risk_governor_blocks": state.get("risk_governor_blocks", 0),
        "max_open_trades_observed": state.get("max_open_trades_observed", 0),
        "candidate": gate["passed"],
        "recommendation": "research_candidate" if gate["passed"] else "observation_only" if _observation_quality(metrics) else "reject",
        "reject_reasons": gate["reasons"],
        "robustness_score": score,
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


def write_research_v2_candidate_robustness_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "research_v2_candidate_robustness_results.csv"
    json_path = root / "research_v2_candidate_robustness_results.json"
    summary_path = root / "research_v2_candidate_robustness_summary.md"
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    headers = [
        "target_name",
        "target_kind",
        "timeframe",
        "family",
        "side_mode",
        "session_filter",
        "closed",
        "wins",
        "losses",
        "win_rate",
        "profit_factor",
        "expectancy",
        "max_drawdown",
        "monte_carlo_stressed_pf",
        "monte_carlo_p95_drawdown",
        "fragile_regime_dependency",
        "weakest_segment",
        "strongest_segment",
        "filter_hint",
        "candidate",
        "recommendation",
        "reject_reasons",
        "robustness_score",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    **row,
                    "weakest_segment": _segment_label(row.get("weakest_segment")),
                    "strongest_segment": _segment_label(row.get("strongest_segment")),
                    "reject_reasons": ";".join(str(item) for item in row.get("reject_reasons") or []),
                }
            )
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")
    summary_path.write_text(research_v2_candidate_robustness_summary_markdown(result), encoding="utf-8")
    return csv_path, json_path, summary_path


def research_v2_candidate_robustness_summary_markdown(result: dict[str, Any]) -> str:
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else _summary(rows, [])
    lines = [
        "# MT5 Research V2 Candidate Robustness Summary",
        "",
        "Focused robustness pass for Research V2 candidates. Paper/offline only; no broker, no order execution, no automatic promotion.",
        "",
        f"Candidates: `{len(result.get('candidates') or [])}`.",
        "",
        "## Rows",
    ]
    for row in rows:
        lines.append(
            f"- `{row.get('target_name')}` closed `{row.get('closed')}`, PF `{row.get('profit_factor')}`, "
            f"expectancy `{row.get('expectancy')}`, DD `{row.get('max_drawdown')}`, MC PF `{row.get('monte_carlo_stressed_pf')}`, "
            f"weakest `{_segment_label(row.get('weakest_segment'))}`, recommendation `{row.get('recommendation')}`."
        )
    lines.extend(
        [
            "",
            "## Answers",
            f"1. Block/failure source: {summary.get('failure_source_answer')}",
            f"2. Best side: {summary.get('side_answer')}",
            f"3. Best session: {summary.get('session_answer')}",
            f"4. Best regime: {summary.get('regime_answer')}",
            f"5. Best variant: {summary.get('best_variant_answer')}",
            f"6. Should continue or abandon: {summary.get('next_action_answer')}",
            "7. No automatic promotion.",
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


def _window_stats(settings: Any, bars: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
    return {
        "terciles": _indexed_windows(settings, bars, trades, 3, "tercile"),
        "halves": _indexed_windows(settings, bars, trades, 2, "half"),
        "quarters": _indexed_windows(settings, bars, trades, 4, "quarter"),
    }


def _indexed_windows(settings: Any, bars: list[dict[str, Any]], trades: list[dict[str, Any]], parts: int, prefix: str) -> dict[str, dict[str, Any]]:
    size = max(1, len(bars) // parts)
    payload: dict[str, dict[str, Any]] = {}
    for part in range(parts):
        start = part * size
        end = len(bars) if part == parts - 1 else (part + 1) * size
        scoped = [trade for trade in trades if start <= int(_number(trade.get("opened_index")) or 0) < end]
        payload[f"{prefix}_{part + 1}"] = _compact_metrics(scoped, settings.initial_balance)
    return payload


def _group_stats(settings: Any, trades: list[dict[str, Any]], key_fn: Any) -> dict[str, dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        buckets.setdefault(str(key_fn(trade) or "unknown"), []).append(trade)
    return {key: _compact_metrics(items, settings.initial_balance) for key, items in sorted(buckets.items())}


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


def _trade_hour(trade: dict[str, Any]) -> int:
    value = trade.get("hour")
    if value is not None:
        try:
            return int(value)
        except Exception:
            return -1
    opened = str(trade.get("opened_at") or "")
    if "T" in opened and len(opened.split("T", 1)[1]) >= 2:
        try:
            return int(opened.split("T", 1)[1][:2])
        except Exception:
            return -1
    return -1


def _session_name(hour: int) -> str:
    if 0 <= hour <= 7:
        return "asia"
    if 7 <= hour <= 20:
        return "london_us"
    if 13 <= hour <= 20:
        return "ny_core"
    return "off_session"


def _vol_bucket_from_trade(trade: dict[str, Any]) -> str:
    features = trade.get("features_snapshot") if isinstance(trade.get("features_snapshot"), dict) else {}
    return _volatility_bucket(float(_number(features.get("volatility_score")) or 0.0))


def _atr_bucket_from_trade(trade: dict[str, Any]) -> str:
    features = trade.get("features_snapshot") if isinstance(trade.get("features_snapshot"), dict) else {}
    atr_pct = float(_number(features.get("atr_pct")) or 0.0)
    if atr_pct < 0.25:
        return "atr_low"
    if atr_pct > 0.75:
        return "atr_high"
    return "atr_normal"


def _rsi_bucket_from_trade(trade: dict[str, Any]) -> str:
    features = trade.get("features_snapshot") if isinstance(trade.get("features_snapshot"), dict) else {}
    rsi = float(_number(features.get("rsi")) or 50.0)
    if rsi < 35:
        return "rsi_low"
    if rsi > 65:
        return "rsi_high"
    return "rsi_neutral"


def _weakest_segment(*groups: dict[str, dict[str, Any]]) -> dict[str, Any]:
    candidates = _segments(*groups)
    if not candidates:
        return {}
    return min(candidates, key=lambda item: (float(item.get("expectancy") or 0.0), float(item.get("profit_factor") or 0.0), -int(item.get("closed") or 0)))


def _strongest_segment(*groups: dict[str, dict[str, Any]]) -> dict[str, Any]:
    candidates = _segments(*groups)
    if not candidates:
        return {}
    return max(candidates, key=lambda item: (float(item.get("expectancy") or 0.0), float(item.get("profit_factor") or 0.0), int(item.get("closed") or 0)))


def _segments(*groups: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for group in groups:
        for key, metrics in group.items():
            if int(metrics.get("closed") or 0) >= 3:
                payload.append({"segment": key, **metrics})
    return payload


def _segment_label(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    return f"{value.get('segment')} closed={value.get('closed')} pf={value.get('profit_factor')} exp={value.get('expectancy')}"


def _filter_hint(weakest: dict[str, Any], strongest: dict[str, Any]) -> str:
    segment = str(weakest.get("segment") or "")
    if not segment:
        return "insufficient_segment_data"
    if segment.startswith("tercile") or segment.startswith("quarter") or segment.startswith("half"):
        return "weak_window_detected; do not promote until walk-forward variant explains this window"
    if segment == "stop_loss":
        return "stop_loss_cluster; require stronger breakout confirmation or volatility/ATR guard before entry"
    if segment == "momentum_loss_exit":
        return "momentum_loss_cluster; require continuation confirmation or avoid fading momentum after entry"
    if segment == "mae_defense_exit":
        return "mae_cluster; require tighter invalidation before opening the trade"
    if segment in {"buy", "sell"}:
        other = "sell" if segment == "buy" else "buy"
        return f"consider_{other}_only_filter"
    if segment in {"asia", "london_us", "ny_core", "off_session"}:
        return f"avoid_{segment}_session_if_sample_survives"
    if segment in {"trend", "chop", "range"}:
        return f"avoid_{segment}_regime_if_sample_survives"
    if segment.startswith("rsi_") or segment.startswith("atr_") or segment in {"low", "normal", "high"}:
        return f"avoid_{segment}_condition_if_sample_survives"
    return "segment_filter_needs_more_sample"


def _fragile(window_stats: dict[str, dict[str, dict[str, Any]]], metrics: dict[str, Any]) -> bool:
    closed = int(metrics.get("closed") or 0)
    if closed < 10:
        return True
    terciles = window_stats.get("terciles", {})
    counts = [int(item.get("closed") or 0) for item in terciles.values()]
    if counts and max(counts) > closed * 0.7:
        return True
    for group in window_stats.values():
        for item in group.values():
            if int(item.get("closed") or 0) >= 8 and float(item.get("expectancy") or 0.0) < -0.05:
                return True
    return False


def _candidate_gate(metrics: dict[str, Any], window_stats: dict[str, Any], monte_carlo: dict[str, Any], fragile: bool, single_trade: bool) -> dict[str, Any]:
    reasons: list[str] = []
    if int(metrics.get("closed") or 0) < 40:
        reasons.append("sample_too_small")
    if float(metrics.get("profit_factor") or 0.0) <= 1.15:
        reasons.append("pf_below_1_15")
    if float(metrics.get("expectancy") or 0.0) <= 0:
        reasons.append("expectancy_not_positive")
    if float(metrics.get("max_drawdown") or 0.0) > 5000:
        reasons.append("drawdown_above_5000")
    for name, metrics_item in (window_stats.get("terciles") or {}).items():
        if int(metrics_item.get("closed") or 0) >= 8 and float(metrics_item.get("expectancy") or 0.0) < -0.05:
            reasons.append(f"{name}_expectancy_negative")
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
    return {"passed": not reasons, "reasons": reasons or ["passes_candidate_robustness_rules"]}


def _score(metrics: dict[str, Any], monte_carlo: dict[str, Any], fragile: bool, single_trade: bool, gate: dict[str, Any]) -> float:
    closed = int(metrics.get("closed") or 0)
    pf = float(metrics.get("profit_factor") or 0.0)
    expectancy = float(metrics.get("expectancy") or 0.0)
    drawdown = float(metrics.get("max_drawdown") or 0.0)
    score = min(closed, 120) * 1.4
    score += max(0.0, min(pf, 3.0) - 1.0) * 70.0
    score += max(0.0, expectancy) * 260.0
    score += max(0.0, float(monte_carlo.get("profit_factor_stressed") or 0.0) - 1.0) * 75.0
    score -= drawdown / 80.0
    score -= max(0, 40 - closed) * 3.0
    if fragile:
        score -= 70.0
    if single_trade:
        score -= 90.0
    if not gate.get("passed"):
        score -= len(gate.get("reasons") or []) * 10.0
    return round(score, 4)


def _observation_quality(metrics: dict[str, Any]) -> bool:
    return int(metrics.get("closed") or 0) >= 10 and float(metrics.get("profit_factor") or 0.0) > 1.0 and float(metrics.get("expectancy") or 0.0) > 0


def _summary(rows: list[dict[str, Any]], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"recommendation": "no_data", **_safety()}
    best = max(rows, key=_robustness_rank)
    best_side = _best_group_answer(rows, "side_mode")
    best_session = _best_group_answer(rows, "session_filter")
    best_regime = _best_group_answer(rows, "trend_regime")
    failures = sorted({reason for row in rows for reason in row.get("reject_reasons", [])})
    max_closed = max(int(row.get("closed") or 0) for row in rows)
    continue_family = (
        "continue focused research on weak segments/exits; do not send to optimizer promotion yet"
        if max_closed >= 40
        else "needs more signal design before optimizer"
    )
    if max_closed < 40 and failures and all("sample_too_small" in row.get("reject_reasons", []) for row in rows):
        continue_family = "sample too small across targets; do not promote"
    return {
        "recommendation": "research_candidate" if candidates else "observation_only",
        "failure_source_answer": ", ".join(failures[:8]) if failures else "passes robustness gates",
        "side_answer": best_side,
        "session_answer": best_session,
        "regime_answer": best_regime,
        "best_variant_answer": f"{best.get('target_name')} PF {best.get('profit_factor')} expectancy {best.get('expectancy')} closed {best.get('closed')}",
        "next_action_answer": continue_family,
        "automatic_promotion": False,
        **_safety(),
    }


def _best_group_answer(rows: list[dict[str, Any]], key: str) -> str:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(str(row.get(key) or "unknown"), []).append(row)
    ranked = []
    for name, items in buckets.items():
        closed = sum(int(item.get("closed") or 0) for item in items)
        score = sum(float(item.get("robustness_score") or 0.0) for item in items) / max(1, len(items))
        ranked.append((score + min(closed, 80) * 0.2, name, closed))
    ranked.sort(reverse=True)
    return f"{ranked[0][1]} ({ranked[0][2]} closed across tested variants)" if ranked else "none"


def _robustness_rank(row: dict[str, Any]) -> float:
    return float(row.get("robustness_score") or 0.0)


def _csv_path_for(body: dict[str, Any], csv_dir: Path, symbol: str, timeframe: str) -> Path:
    explicit = body.get(f"csv_path_{timeframe.lower()}") or body.get("csv_path")
    if explicit:
        return Path(str(explicit))
    suffix = "20000" if timeframe in {"M15", "M30"} else "30000"
    return csv_dir / f"{symbol}_{timeframe}_{suffix}.csv"


def _requested_list(value: Any, default: list[str]) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [part.strip() for part in value.split(",") if part.strip()]
    return list(default)

from __future__ import annotations

import csv
import json
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

from services.mt5.mt5_backtester import BacktestSettings, _load_bars, _metrics, _number, _reason_counts, _safety, _settings
from services.mt5.mt5_capital_preservation_optimizer import _depends_on_single_trade, _drawdown_accelerating, _monte_carlo_stress
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_eth_m30_volatility_hardening import (
    EthVolatilityHardeningConfig,
    _atr_bucket,
    _feature_bucket_stats,
    _hardening_configs,
    _loss_cluster_stats,
    _session_stats,
    _volatility_bucket,
)
from services.mt5.mt5_multi_symbol_recent_first import _remove_best_metrics, _simulate_multi_symbol, _spread_stress_metrics
from services.mt5.mt5_recent_first_research import _fragile_dependency, _quarter_ranges
from services.mt5.mt5_strategy_research_v2 import _features_by_index
from services.mt5.mt5_symbol_cost_model import build_symbol_cost_model


ETH_M30_CAPITAL_TARGETS = [
    "eth_m30_vol_breakout_chop_guard_v1",
    "eth_m30_vol_breakout_regime_filtered_v1",
    "eth_m30_vol_breakout_mc_hardened_v1",
]


def run_eth_m30_capital_preservation(body: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    body = body or {}
    csv_path = Path(str(body.get("csv_path") or Path("data") / "backtests" / "multisymbol" / "ETHUSD_M30_20000.csv"))
    max_bars = max(200, min(int(_number(body.get("max_bars")) or 20000), 65000))
    timeout_seconds = max(0.25, float(_number(body.get("per_evaluation_timeout_seconds")) or 3.0))
    monte_carlo_simulations = max(100, min(int(_number(body.get("monte_carlo_simulations")) or 1200), 1500))
    requested_targets = _requested_list(body.get("targets"), ETH_M30_CAPITAL_TARGETS)
    hardening_by_name = {config.target_name: config for config in _hardening_configs()}
    configs = [hardening_by_name[name] for name in requested_targets if name in hardening_by_name]
    errors: list[dict[str, Any]] = []
    warnings: list[str] = []
    rows: list[dict[str, Any]] = []

    if not csv_path.exists():
        errors.append({"csv_path": str(csv_path), "error": "csv_not_found"})
        return _result(rows, configs, errors, warnings, started, csv_path)

    first_price = _first_csv_price(csv_path)
    cost_model = build_symbol_cost_model("ETHUSD", first_price=first_price)
    settings_body = {
        "symbol": "ETHUSD",
        "timeframe": "M30",
        "csv_path": str(csv_path),
        "max_bars": max_bars,
        "spread_points": cost_model.spread_points,
        "point": cost_model.point,
        "commission": cost_model.commission_assumption,
        "slippage_points": cost_model.slippage_assumption,
        "save_results": False,
        "source": "mt5_csv",
        "timeout_seconds": timeout_seconds,
    }
    settings = replace(
        _settings(settings_body, get_mt5_config()),
        max_bars=max_bars,
        timeout_seconds=max(1.0, min(timeout_seconds, 20.0)),
        point=cost_model.point,
        spread_points=cost_model.spread_points,
        commission=cost_model.commission_assumption,
        slippage_points=cost_model.slippage_assumption,
    )
    bars, load_warnings = _load_bars(settings_body, settings)
    warnings.extend(load_warnings)
    bars = bars[-settings.max_bars :]
    if not bars:
        errors.append({"csv_path": str(csv_path), "error": "csv_bars_not_loaded"})
        return _result(rows, configs, errors, warnings, started, csv_path)
    features_by_index = _features_by_index(bars)

    for config in configs:
        rows.append(
            evaluate_eth_m30_capital_config(
                settings,
                bars,
                config,
                source_csv=str(csv_path),
                features_by_index=features_by_index,
                timeout_seconds=timeout_seconds,
                monte_carlo_simulations=monte_carlo_simulations,
                cost_model=cost_model.as_dict(),
            )
        )

    rows.sort(key=lambda row: float(row.get("capital_preservation_score") or 0.0), reverse=True)
    return _result(rows, configs, errors, warnings, started, csv_path)


def evaluate_eth_m30_capital_config(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    hardening_config: EthVolatilityHardeningConfig,
    *,
    source_csv: str,
    features_by_index: dict[int, dict[str, Any]],
    timeout_seconds: float,
    monte_carlo_simulations: int = 1200,
    cost_model: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    trades, blocked, signals, state = _simulate_multi_symbol(
        settings,
        bars,
        hardening_config.config,
        started,
        timeout_seconds=timeout_seconds,
        features_by_index=features_by_index,
    )
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    total = _metrics(closed, initial_balance=settings.initial_balance)
    splits = _time_splits(settings, bars, closed)
    windows = _window_suite(settings, bars, closed)
    monte_carlo = _monte_carlo_stress(closed, initial_balance=settings.initial_balance, max_drawdown_limit=5000.0, simulations=monte_carlo_simulations)
    remove_best_3 = _remove_best_metrics(settings, closed, 3)
    remove_best_5 = _remove_best_metrics(settings, closed, 5)
    spread_x1_5 = _spread_stress_metrics(settings, bars, hardening_config.config, features_by_index, timeout_seconds, 1.5)
    spread_x2 = _spread_stress_metrics(settings, bars, hardening_config.config, features_by_index, timeout_seconds, 2.0)
    worst_order = _worst_order_metrics(settings, closed)
    consecutive = _loss_cluster_stats(closed)
    mae_mfe = _mae_mfe_stats(closed)
    fragile = _fragile_dependency(total, _recent_first_split(splits))
    single_trade = _depends_on_single_trade(total)
    drawdown_accel = _drawdown_accelerating(closed, settings.initial_balance)
    gate = _gate(total, splits, windows, monte_carlo, remove_best_5, spread_x2, fragile, single_trade, drawdown_accel)
    score = _score(total, splits, windows, monte_carlo, remove_best_5, spread_x2, worst_order, gate, fragile, single_trade, drawdown_accel)
    passed = bool(gate["passed"])
    experimental_record = f"{hardening_config.target_name}_capital_preservation_passed" if passed else ""
    recommendation = "paper_forward_candidate_recommended" if passed else "observation_only" if _observation_quality(total, splits, monte_carlo) else "reject"

    return {
        "target_name": hardening_config.target_name,
        "experimental_registry_record": experimental_record,
        "symbol": settings.symbol,
        "normalized_symbol": settings.normalized_symbol,
        "timeframe": settings.timeframe,
        "family": hardening_config.config.family,
        "profile": hardening_config.target_name,
        "variant_id": hardening_config.config.key(),
        "source_csv": source_csv,
        "csv_path_used": source_csv,
        "hardening_mode": hardening_config.config.hardening_mode,
        "hardening_actions": list(hardening_config.hardening_actions),
        "side": hardening_config.config.side_mode,
        "side_mode": hardening_config.config.side_mode,
        "session": hardening_config.config.session_name,
        "session_filter": hardening_config.config.session_name,
        "volatility_regime": hardening_config.config.base.volatility_regime,
        "trend_regime": hardening_config.config.base.trend_regime,
        "rsi_regime": hardening_config.config.base.rsi_regime,
        "score_threshold": hardening_config.config.base.score_threshold,
        "risk_reward": hardening_config.config.base.risk_reward,
        "time_stop_bars": hardening_config.config.base.time_stop_bars,
        "mae_exit_r": hardening_config.config.mae_exit_r,
        "fast_loss_cut_r": hardening_config.config.fast_loss_cut_r,
        "trailing_activation_r": hardening_config.config.trailing_activation_r,
        "trailing_lock_r": hardening_config.config.trailing_lock_r,
        "bars_loaded": len(bars),
        "bars_evaluated": max(0, len(bars) - 80),
        "first_bar_time": str(bars[0].get("time") or "") if bars else "",
        "last_bar_time": str(bars[-1].get("time") or "") if bars else "",
        "requested_symbol": (cost_model or {}).get("requested_symbol", settings.symbol),
        "resolved_symbol": (cost_model or {}).get("resolved_symbol", settings.symbol),
        "instrument_type": (cost_model or {}).get("instrument_type", "crypto"),
        "point": settings.point,
        "spread_points": settings.spread_points,
        "estimated_spread_price": (cost_model or {}).get("estimated_spread_price", 0.0),
        "generated_signal_count": signals["generated"],
        "actionable_signal_count": signals["actionable"],
        "opened_trade_count": len(closed),
        "recent_closed": splits["recent_holdout"]["closed"],
        "recent_pf": splits["recent_holdout"]["profit_factor"],
        "recent_expectancy": splits["recent_holdout"]["expectancy"],
        "recent_max_drawdown": splits["recent_holdout"]["max_drawdown"],
        "total_closed": total["closed"],
        "total_win_rate": total["win_rate"],
        "total_pf": total["profit_factor"],
        "total_expectancy": total["expectancy"],
        "total_max_drawdown": total["max_drawdown"],
        "max_drawdown": total["max_drawdown"],
        "train_metrics": splits["train"],
        "validation_metrics": splits["validation"],
        "recent_holdout_metrics": splits["recent_holdout"],
        "tercile_stats": splits["terciles"],
        "half_stats": splits["halves"],
        "quarter_stats": splits["quarters"],
        "rolling_windows": windows,
        "rolling_window_pf_min": windows["pf_min"],
        "rolling_window_expectancy_min": windows["expectancy_min"],
        "rolling_window_drawdown_max": windows["drawdown_max"],
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
        "worst_order_pf": worst_order["profit_factor"],
        "worst_order_drawdown": worst_order["max_drawdown"],
        "consecutive_loss_stress": consecutive,
        "max_consecutive_losses": consecutive["max_consecutive_losses"],
        "drawdown_accelerating": drawdown_accel,
        "mae_mfe_stats": mae_mfe,
        "avg_MAE_R": mae_mfe["avg_MAE_R"],
        "avg_MFE_R": mae_mfe["avg_MFE_R"],
        "buy_sell_stats": total["side_stats"],
        "buy_win_rate": total["buy_win_rate"],
        "sell_win_rate": total["sell_win_rate"],
        "buy_pf": total["buy_pf"],
        "sell_pf": total["sell_pf"],
        "session_stats": _session_stats(closed, settings.initial_balance),
        "hour_stats": total["hour_stats"],
        "volatility_regime_stats": _feature_bucket_stats(closed, settings.initial_balance, "volatility_score", _volatility_bucket),
        "atr_regime_stats": _feature_bucket_stats(closed, settings.initial_balance, "atr_pct", _atr_bucket),
        "trend_chop_range_stats": total["regime_stats"],
        "exit_reason_counts": total["exit_reason_counts"],
        "loss_cluster_stats": consecutive,
        "blocked_reason_counts": _reason_counts(blocked),
        "risk_governor_blocks": state["risk_governor_blocks"],
        "max_open_trades_observed": state["max_open_trades_observed"],
        "fragile_regime_dependency": fragile,
        "single_trade_dependency": single_trade,
        "candidate": passed,
        "capital_preservation_passed": passed,
        "paper_forward_candidate_recommended": passed,
        "recommendation": recommendation,
        "rejection_reasons": gate["reasons"],
        "reject_reasons": gate["reasons"],
        "capital_preservation_score": score,
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


def write_eth_m30_capital_preservation_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "eth_m30_capital_preservation_results.csv"
    json_path = root / "eth_m30_capital_preservation_results.json"
    summary_path = root / "eth_m30_capital_preservation_summary.md"
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    headers = [
        "target_name",
        "experimental_registry_record",
        "symbol",
        "timeframe",
        "side",
        "session",
        "trend_regime",
        "recent_closed",
        "recent_pf",
        "recent_expectancy",
        "total_closed",
        "total_pf",
        "total_expectancy",
        "total_max_drawdown",
        "rolling_window_pf_min",
        "rolling_window_expectancy_min",
        "monte_carlo_stressed_pf",
        "monte_carlo_stressed_expectancy",
        "monte_carlo_p95_drawdown",
        "spread_x1_5_pf",
        "spread_x2_pf",
        "remove_best_3_pf",
        "remove_best_5_pf",
        "worst_order_pf",
        "worst_order_drawdown",
        "max_consecutive_losses",
        "drawdown_accelerating",
        "buy_pf",
        "sell_pf",
        "avg_MAE_R",
        "avg_MFE_R",
        "fragile_regime_dependency",
        "single_trade_dependency",
        "capital_preservation_passed",
        "paper_forward_candidate_recommended",
        "recommendation",
        "rejection_reasons",
        "capital_preservation_score",
        "source_csv",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({**row, "rejection_reasons": ";".join(str(item) for item in row.get("rejection_reasons") or [])})
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")
    summary_path.write_text(eth_m30_capital_preservation_summary_markdown(result), encoding="utf-8")
    return csv_path, json_path, summary_path


def eth_m30_capital_preservation_summary_markdown(result: dict[str, Any]) -> str:
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else _summary(rows, [])
    lines = [
        "# ETHUSD M30 Capital Preservation Summary",
        "",
        "Capital preservation validation for ETHUSD M30 volatility breakout variants. Paper/offline only; no broker, no order execution, no automatic promotion.",
        "",
        f"Evaluations: `{result.get('evaluations', len(rows))}`.",
        f"Passed: `{len(result.get('passed') or [])}`.",
        "",
        "## Top Results",
    ]
    for row in rows:
        lines.append(
            f"- `{row.get('target_name')}` recent `{row.get('recent_closed')}` PF `{row.get('recent_pf')}`, "
            f"total `{row.get('total_closed')}` PF `{row.get('total_pf')}`, rolling PF min `{row.get('rolling_window_pf_min')}`, "
            f"MC PF `{row.get('monte_carlo_stressed_pf')}`, spread x2 PF `{row.get('spread_x2_pf')}`, "
            f"remove best 5 PF `{row.get('remove_best_5_pf')}`, recommendation `{row.get('recommendation')}`."
        )
    lines.extend(
        [
            "",
            "## Answers",
            f"1. chop_guard_v1: {summary.get('chop_guard_answer')}",
            f"2. regime_filtered_v1: {summary.get('regime_filtered_answer')}",
            f"3. Most robust variant: {summary.get('best_variant_answer')}",
            f"4. Buy/sell together vs one side: {summary.get('side_answer')}",
            f"5. Session to block: {summary.get('session_answer')}",
            f"6. Exit causing most losses: {summary.get('exit_answer')}",
            f"7. Paper-forward candidate status: {summary.get('paper_forward_answer')}",
            "8. No automatic promotion.",
            "9. promoted-profile mutated: false.",
            "10. forward-profile-state mutated: false.",
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


def _result(
    rows: list[dict[str, Any]],
    configs: list[EthVolatilityHardeningConfig],
    errors: list[dict[str, Any]],
    warnings: list[str],
    started: float,
    csv_path: Path,
) -> dict[str, Any]:
    rows.sort(key=lambda row: float(row.get("capital_preservation_score") or 0.0), reverse=True)
    passed = [row for row in rows if row.get("capital_preservation_passed")]
    return {
        "ok": True,
        "status": "mt5_eth_m30_capital_preservation_completed",
        "symbol": "ETHUSD",
        "timeframe": "M30",
        "mode": "paper",
        "csv_path": str(csv_path),
        "evaluations": len(rows),
        "targets": [config.target_name for config in configs],
        "results": rows,
        "passed": passed,
        "candidates": passed,
        "experimental_records": [row.get("experimental_registry_record") for row in passed if row.get("experimental_registry_record")],
        "top_3_for_paper_forward_review": passed[:3],
        "summary": _summary(rows, passed),
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


def _time_splits(settings: BacktestSettings, bars: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, Any]:
    length = len(bars)
    train_end = int(length * 0.5)
    validation_end = int(length * 0.75)
    return {
        "train": _metrics_for_range(settings, trades, 0, train_end),
        "validation": _metrics_for_range(settings, trades, train_end, validation_end),
        "recent_holdout": _metrics_for_range(settings, trades, validation_end, length),
        "terciles": _indexed_windows(settings, bars, trades, 3, "tercile"),
        "halves": _indexed_windows(settings, bars, trades, 2, "half"),
        "quarters": _indexed_windows(settings, bars, trades, 4, "quarter"),
    }


def _window_suite(settings: BacktestSettings, bars: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, Any]:
    windows: list[dict[str, Any]] = []
    for count in [4, 6, 8]:
        windows.extend(_window_list(settings, bars, trades, count, f"rolling_{count}"))
    active = [item for item in windows if int(item.get("closed") or 0) >= 5]
    return {
        "windows": windows,
        "pf_min": round(min((float(item.get("profit_factor") or 0.0) for item in active), default=0.0), 4),
        "expectancy_min": round(min((float(item.get("expectancy") or 0.0) for item in active), default=0.0), 4),
        "drawdown_max": round(max((float(item.get("max_drawdown") or 0.0) for item in active), default=0.0), 6),
        "active_window_count": len(active),
    }


def _indexed_windows(settings: BacktestSettings, bars: list[dict[str, Any]], trades: list[dict[str, Any]], count: int, prefix: str) -> dict[str, dict[str, Any]]:
    return {item["name"]: item for item in _window_list(settings, bars, trades, count, prefix)}


def _window_list(settings: BacktestSettings, bars: list[dict[str, Any]], trades: list[dict[str, Any]], count: int, prefix: str) -> list[dict[str, Any]]:
    length = len(bars)
    size = max(1, length // count)
    windows: list[dict[str, Any]] = []
    for index in range(count):
        start = index * size
        end = length if index == count - 1 else min(length, (index + 1) * size)
        metrics = _metrics_for_range(settings, trades, start, end)
        windows.append({"name": f"{prefix}_{index + 1}", "start_index": start, "end_index": end, **metrics})
    return windows


def _metrics_for_range(settings: BacktestSettings, trades: list[dict[str, Any]], start: int, end: int) -> dict[str, Any]:
    scoped = [trade for trade in trades if start <= int(_number(trade.get("opened_index")) or 0) < end]
    return _compact(_metrics(scoped, initial_balance=settings.initial_balance))


def _gate(
    total: dict[str, Any],
    splits: dict[str, Any],
    windows: dict[str, Any],
    monte_carlo: dict[str, Any],
    remove_best_5: dict[str, Any],
    spread_x2: dict[str, Any],
    fragile: bool,
    single_trade: bool,
    drawdown_accel: bool,
) -> dict[str, Any]:
    reasons: list[str] = []
    recent = splits["recent_holdout"]
    if int(recent.get("closed") or 0) < 20:
        reasons.append("recent_sample_below_20")
    if int(total.get("closed") or 0) < 75:
        reasons.append("total_sample_below_75")
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
    if float(spread_x2.get("profit_factor") or 0.0) < 0.95:
        reasons.append("spread_x2_pf_below_0_95")
    if float(remove_best_5.get("profit_factor") or 0.0) < 1.0:
        reasons.append("remove_best_5_pf_below_1")
    reasons.extend(_negative_window_reasons(splits))
    if float(windows.get("pf_min") or 0.0) < 0.95:
        reasons.append("rolling_pf_min_below_0_95")
    if float(windows.get("expectancy_min") or 0.0) < -0.10:
        reasons.append("rolling_expectancy_strong_negative")
    if fragile:
        reasons.append("fragile_regime_dependency")
    if single_trade:
        reasons.append("single_trade_dependency")
    if drawdown_accel:
        reasons.append("drawdown_accelerating")
    return {"passed": not reasons, "reasons": reasons or ["passes_eth_m30_capital_preservation_gates"]}


def _negative_window_reasons(splits: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    for group_name in ["terciles", "halves", "quarters"]:
        for name, item in (splits.get(group_name) or {}).items():
            if int(item.get("closed") or 0) >= 8 and (float(item.get("profit_factor") or 0.0) < 0.85 or float(item.get("expectancy") or 0.0) < -0.10):
                reasons.append(f"{name}_strong_negative")
    recent = splits.get("recent_holdout") or {}
    if int(recent.get("closed") or 0) >= 10 and (float(recent.get("profit_factor") or 0.0) < 1.0 or float(recent.get("expectancy") or 0.0) <= 0):
        reasons.append("recent_holdout_negative")
    return reasons


def _recent_first_split(splits: dict[str, Any]) -> dict[str, dict[str, Any]]:
    quarters = splits.get("quarters") if isinstance(splits.get("quarters"), dict) else {}
    return {
        "oldest": quarters.get("quarter_1") or {},
        "middle": quarters.get("quarter_2") or {},
        "previous": quarters.get("quarter_3") or {},
        "recent": splits.get("recent_holdout") or quarters.get("quarter_4") or {},
    }


def _score(
    total: dict[str, Any],
    splits: dict[str, Any],
    windows: dict[str, Any],
    monte_carlo: dict[str, Any],
    remove_best_5: dict[str, Any],
    spread_x2: dict[str, Any],
    worst_order: dict[str, Any],
    gate: dict[str, Any],
    fragile: bool,
    single_trade: bool,
    drawdown_accel: bool,
) -> float:
    recent = splits["recent_holdout"]
    score = 0.0
    score += min(int(recent.get("closed") or 0), 80) * 3.2
    score += min(int(total.get("closed") or 0), 180) * 1.0
    score += max(0.0, float(recent.get("profit_factor") or 0.0) - 1.0) * 100.0
    score += max(0.0, float(total.get("profit_factor") or 0.0) - 1.0) * 85.0
    score += max(0.0, float(monte_carlo.get("profit_factor_stressed") or 0.0) - 1.0) * 180.0
    score += max(0.0, float(spread_x2.get("profit_factor") or 0.0) - 1.0) * 80.0
    score += max(0.0, float(remove_best_5.get("profit_factor") or 0.0) - 1.0) * 90.0
    score += max(0.0, float(windows.get("pf_min") or 0.0) - 1.0) * 70.0
    score += max(0.0, float(recent.get("expectancy") or 0.0)) * 260.0
    score += max(0.0, float(total.get("expectancy") or 0.0)) * 220.0
    score -= max(0.0, 1.0 - float(worst_order.get("profit_factor") or 0.0)) * 50.0
    score -= max(0, 20 - int(recent.get("closed") or 0)) * 10.0
    score -= max(0, 75 - int(total.get("closed") or 0)) * 4.0
    if fragile:
        score -= 130.0
    if single_trade:
        score -= 140.0
    if drawdown_accel:
        score -= 120.0
    if not gate.get("passed"):
        score -= len(gate.get("reasons") or []) * 13.0
    return round(score, 4)


def _summary(rows: list[dict[str, Any]], passed: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"recommendation": "no_data", **_safety()}
    by_name = {row.get("target_name"): row for row in rows}
    best = max(rows, key=lambda row: float(row.get("capital_preservation_score") or 0.0), default={})
    return {
        "recommendation": "paper_forward_candidate_recommended" if passed else "observation_only" if any(row.get("recommendation") == "observation_only" for row in rows) else "reject",
        "chop_guard_answer": _row_answer(by_name.get("eth_m30_vol_breakout_chop_guard_v1")),
        "regime_filtered_answer": _row_answer(by_name.get("eth_m30_vol_breakout_regime_filtered_v1")),
        "best_variant_answer": f"{best.get('target_name')} score={best.get('capital_preservation_score')} recommendation={best.get('recommendation')}",
        "side_answer": _side_answer(best),
        "session_answer": _session_answer(best),
        "exit_answer": _exit_answer(best),
        "paper_forward_answer": (
            "; ".join(str(row.get("experimental_registry_record")) for row in passed if row.get("experimental_registry_record"))
            if passed
            else "none; remain observation_only/reject"
        ),
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        **_safety(),
    }


def _row_answer(row: dict[str, Any] | None) -> str:
    if not row:
        return "not evaluated"
    return (
        f"recommendation={row.get('recommendation')} recent={row.get('recent_closed')} total={row.get('total_closed')} "
        f"pf={row.get('total_pf')} mc_pf={row.get('monte_carlo_stressed_pf')} reasons={row.get('rejection_reasons')}"
    )


def _side_answer(row: dict[str, Any]) -> str:
    stats = row.get("buy_sell_stats") if isinstance(row.get("buy_sell_stats"), dict) else {}
    buy = stats.get("buy") if isinstance(stats.get("buy"), dict) else {}
    sell = stats.get("sell") if isinstance(stats.get("sell"), dict) else {}
    return f"both retained; buy={buy}; sell={sell}"


def _session_answer(row: dict[str, Any]) -> str:
    stats = row.get("session_stats") if isinstance(row.get("session_stats"), dict) else {}
    weak = [name for name, item in stats.items() if isinstance(item, dict) and int(item.get("closed") or 0) >= 5 and (float(item.get("profit_factor") or 0.0) < 1.0 or float(item.get("expectancy") or 0.0) <= 0)]
    return f"block_or_watch={weak or 'none'}; stats={stats}"


def _exit_answer(row: dict[str, Any]) -> str:
    counts = row.get("exit_reason_counts") if isinstance(row.get("exit_reason_counts"), dict) else {}
    loss_like = {key: value for key, value in counts.items() if key in {"stop_loss", "momentum_loss_exit", "mae_guard_exit", "fast_loss_cut"}}
    if not loss_like:
        return f"no loss exits dominant; counts={counts}"
    worst = max(loss_like, key=lambda key: int(loss_like.get(key) or 0))
    return f"{worst}; counts={counts}"


def _observation_quality(total: dict[str, Any], splits: dict[str, Any], monte_carlo: dict[str, Any]) -> bool:
    recent = splits["recent_holdout"]
    return (
        int(recent.get("closed") or 0) >= 15
        and int(total.get("closed") or 0) >= 50
        and float(total.get("profit_factor") or 0.0) >= 1.15
        and float(total.get("expectancy") or 0.0) > 0
        and float(monte_carlo.get("max_drawdown_p95") or 0.0) <= 6500
    )


def _worst_order_metrics(settings: BacktestSettings, trades: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted([trade for trade in trades if trade.get("lifecycle_status") == "closed"], key=lambda item: float(_number(item.get("pnl")) or 0.0))
    return _metrics(ordered, initial_balance=settings.initial_balance)


def _mae_mfe_stats(trades: list[dict[str, Any]]) -> dict[str, float]:
    mae_values: list[float] = []
    mfe_values: list[float] = []
    mae_r_values: list[float] = []
    mfe_r_values: list[float] = []
    for trade in trades:
        risk = abs(float(_number(trade.get("initial_risk")) or 0.0)) or 1.0
        mae = abs(min(float(_number(trade.get("max_adverse_excursion")) or 0.0), 0.0))
        mfe = max(float(_number(trade.get("max_favorable_excursion")) or 0.0), 0.0)
        mae_values.append(mae)
        mfe_values.append(mfe)
        mae_r_values.append(mae / risk)
        mfe_r_values.append(mfe / risk)
    if not trades:
        return {"avg_MAE": 0.0, "avg_MFE": 0.0, "avg_MAE_R": 0.0, "avg_MFE_R": 0.0}
    return {
        "avg_MAE": round(sum(mae_values) / len(mae_values), 6),
        "avg_MFE": round(sum(mfe_values) / len(mfe_values), 6),
        "avg_MAE_R": round(sum(mae_r_values) / len(mae_r_values), 4),
        "avg_MFE_R": round(sum(mfe_r_values) / len(mfe_r_values), 4),
    }


def _session_stats(trades: list[dict[str, Any]], initial_balance: float) -> dict[str, dict[str, Any]]:
    buckets = {"asia": set(range(0, 8)), "london_us": set(range(7, 21)), "ny_core": set(range(13, 21)), "off_session": set(range(21, 24))}
    return {name: _compact(_metrics([trade for trade in trades if _hour(trade) in hours], initial_balance=initial_balance)) for name, hours in buckets.items()}


def _feature_bucket_stats(trades: list[dict[str, Any]], initial_balance: float, feature_name: str, bucket_fn: Any) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        snapshot = trade.get("features_snapshot") if isinstance(trade.get("features_snapshot"), dict) else {}
        bucket = bucket_fn(float(_number(snapshot.get(feature_name)) or 0.0))
        grouped.setdefault(bucket, []).append(trade)
    return {name: _compact(_metrics(items, initial_balance=initial_balance)) for name, items in sorted(grouped.items())}


def _volatility_bucket(score: float) -> str:
    if score < 28:
        return "low"
    if score > 58:
        return "high"
    return "normal"


def _atr_bucket(atr_pct: float) -> str:
    if atr_pct <= 0:
        return "unknown"
    if atr_pct < 0.003:
        return "low"
    if atr_pct > 0.009:
        return "high"
    return "normal"


def _hour(trade: dict[str, Any]) -> int:
    try:
        return int(trade.get("hour"))
    except Exception:
        return -1


def _compact(metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        "closed": metrics.get("closed", 0),
        "wins": metrics.get("wins", 0),
        "losses": metrics.get("losses", 0),
        "win_rate": metrics.get("win_rate", 0.0),
        "profit_factor": metrics.get("profit_factor", 0.0),
        "expectancy": metrics.get("expectancy", 0.0),
        "max_drawdown": metrics.get("max_drawdown", 0.0),
    }


def _first_csv_price(path: Path) -> float:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                return float(_number(row.get("close")) or _number(row.get("open")) or 0.0)
    except Exception:
        return 0.0
    return 0.0


def _requested_list(value: Any, default: list[str]) -> list[str]:
    if value is None or value == "":
        return default
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return default

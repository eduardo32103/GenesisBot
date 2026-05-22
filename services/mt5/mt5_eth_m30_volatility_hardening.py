from __future__ import annotations

import csv
import json
import time
from collections import Counter
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from services.mt5.mt5_backtester import BacktestSettings, _load_bars, _metrics, _number, _reason_counts, _safety, _settings
from services.mt5.mt5_capital_preservation_optimizer import _depends_on_single_trade, _monte_carlo_stress
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_multi_symbol_recent_first import (
    MultiSymbolConfig,
    _remove_best_metrics,
    _simulate_multi_symbol,
    _split_metrics,
    _spread_stress_metrics,
)
from services.mt5.mt5_recent_first_research import RecentFirstVariant, _fragile_dependency
from services.mt5.mt5_strategy_research_v2 import _features_by_index
from services.mt5.mt5_symbol_cost_model import build_symbol_cost_model


ETH_M30_HARDENING_TARGETS = [
    "eth_m30_vol_breakout_both_baseline",
    "eth_m30_vol_breakout_buy_baseline",
    "eth_m30_vol_breakout_sell_baseline",
    "eth_m30_vol_breakout_side_filtered_v1",
    "eth_m30_vol_breakout_session_filtered_v1",
    "eth_m30_vol_breakout_ny_core_session_v1",
    "eth_m30_vol_breakout_mae_guard_v1",
    "eth_m30_vol_breakout_fast_loss_cut_v1",
    "eth_m30_vol_breakout_trailing_defensive_v1",
    "eth_m30_vol_breakout_regime_filtered_v1",
    "eth_m30_vol_breakout_high_vol_v1",
    "eth_m30_vol_breakout_chop_guard_v1",
    "eth_m30_vol_breakout_mc_hardened_v1",
]


@dataclass(frozen=True)
class EthVolatilityHardeningConfig:
    target_name: str
    config: MultiSymbolConfig
    hardening_actions: tuple[str, ...]


def run_eth_m30_volatility_hardening(body: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    body = body or {}
    csv_path = Path(str(body.get("csv_path") or Path("data") / "backtests" / "multisymbol" / "ETHUSD_M30_20000.csv"))
    output_mode = str(body.get("mode") or "paper")
    max_bars = max(200, min(int(_number(body.get("max_bars")) or 20000), 65000))
    timeout_seconds = max(0.25, float(_number(body.get("per_evaluation_timeout_seconds")) or 2.0))
    monte_carlo_simulations = max(100, min(int(_number(body.get("monte_carlo_simulations")) or 500), 1200))
    requested_targets = _requested_list(body.get("targets"), ETH_M30_HARDENING_TARGETS)
    configs = [config for config in _hardening_configs() if config.target_name in requested_targets]

    errors: list[dict[str, Any]] = []
    warnings: list[str] = []
    rows: list[dict[str, Any]] = []

    if not csv_path.exists():
        errors.append({"csv_path": str(csv_path), "error": "csv_not_found"})
        return _result(rows, configs, errors, warnings, started, csv_path, output_mode)

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
        return _result(rows, configs, errors, warnings, started, csv_path, output_mode)

    features_by_index = _features_by_index(bars)
    for hardening_config in configs:
        rows.append(
            evaluate_eth_m30_hardening_config(
                settings,
                bars,
                hardening_config,
                source_csv=str(csv_path),
                features_by_index=features_by_index,
                timeout_seconds=timeout_seconds,
                monte_carlo_simulations=monte_carlo_simulations,
                cost_model=cost_model.as_dict(),
            )
        )

    rows.sort(key=lambda row: float(row.get("eth_hardening_score") or 0.0), reverse=True)
    return _result(rows, configs, errors, warnings, started, csv_path, output_mode)


def evaluate_eth_m30_hardening_config(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    hardening_config: EthVolatilityHardeningConfig,
    *,
    source_csv: str,
    features_by_index: dict[int, dict[str, Any]],
    timeout_seconds: float,
    monte_carlo_simulations: int = 500,
    cost_model: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    config = hardening_config.config
    trades, blocked, signals, state = _simulate_multi_symbol(
        settings,
        bars,
        config,
        started,
        timeout_seconds=timeout_seconds,
        features_by_index=features_by_index,
    )
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    total = _metrics(closed, initial_balance=settings.initial_balance)
    split = _split_metrics(settings, bars, closed)
    recent = split["recent"]
    monte_carlo = _monte_carlo_stress(closed, initial_balance=settings.initial_balance, max_drawdown_limit=5000.0, simulations=monte_carlo_simulations)
    remove_best_3 = _remove_best_metrics(settings, closed, 3)
    remove_best_5 = _remove_best_metrics(settings, closed, 5)
    spread_x1_5 = _spread_stress_metrics(settings, bars, config, features_by_index, timeout_seconds, 1.5)
    spread_x2 = _spread_stress_metrics(settings, bars, config, features_by_index, timeout_seconds, 2.0)
    fragile = _fragile_dependency(total, split)
    single_trade = _depends_on_single_trade(total)
    gate = _gate(total, recent, monte_carlo, remove_best_5, spread_x2, fragile, single_trade)
    score = _score(total, recent, monte_carlo, remove_best_5, spread_x2, gate, fragile, single_trade)

    return {
        "target_name": hardening_config.target_name,
        "symbol": settings.symbol,
        "normalized_symbol": settings.normalized_symbol,
        "timeframe": settings.timeframe,
        "family": config.family,
        "profile": hardening_config.target_name,
        "source_csv": source_csv,
        "csv_path_used": source_csv,
        "variant_id": config.key(),
        "hardening_mode": config.hardening_mode,
        "hardening_actions": list(hardening_config.hardening_actions),
        "side": config.side_mode,
        "side_mode": config.side_mode,
        "session": config.session_name,
        "session_filter": config.session_name,
        "volatility_regime": config.base.volatility_regime,
        "trend_regime": config.base.trend_regime,
        "rsi_regime": config.base.rsi_regime,
        "score_threshold": config.base.score_threshold,
        "risk_reward": config.base.risk_reward,
        "time_stop_bars": config.base.time_stop_bars,
        "atr_stop_multiplier": config.base.atr_stop_multiplier,
        "mae_exit_r": config.mae_exit_r,
        "fast_loss_cut_r": config.fast_loss_cut_r,
        "trailing_activation_r": config.trailing_activation_r,
        "trailing_lock_r": config.trailing_lock_r,
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
        "spread_x1_5_pf": spread_x1_5["profit_factor"],
        "spread_x1_5_expectancy": spread_x1_5["expectancy"],
        "spread_x2_pf": spread_x2["profit_factor"],
        "spread_x2_expectancy": spread_x2["expectancy"],
        "remove_best_3_pf": remove_best_3["profit_factor"],
        "remove_best_3_expectancy": remove_best_3["expectancy"],
        "remove_best_5_pf": remove_best_5["profit_factor"],
        "remove_best_5_expectancy": remove_best_5["expectancy"],
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
        "loss_cluster_stats": _loss_cluster_stats(closed),
        "blocked_reason_counts": _reason_counts(blocked),
        "risk_governor_blocks": state["risk_governor_blocks"],
        "max_open_trades_observed": state["max_open_trades_observed"],
        "fragile_regime_dependency": fragile,
        "single_trade_dependency": single_trade,
        "candidate": gate["passed"],
        "recommendation": "capital_preservation_ready" if gate["passed"] else "observation_only" if _observation_quality(total, recent, monte_carlo) else "reject",
        "rejection_reasons": gate["reasons"],
        "reject_reasons": gate["reasons"],
        "eth_hardening_score": score,
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


def write_eth_m30_volatility_hardening_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "eth_m30_volatility_hardening_results.csv"
    json_path = root / "eth_m30_volatility_hardening_results.json"
    summary_path = root / "eth_m30_volatility_hardening_summary.md"
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    headers = [
        "target_name",
        "symbol",
        "timeframe",
        "family",
        "side",
        "session",
        "hardening_mode",
        "volatility_regime",
        "trend_regime",
        "score_threshold",
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
        "spread_x1_5_pf",
        "spread_x2_pf",
        "remove_best_3_pf",
        "remove_best_5_pf",
        "buy_win_rate",
        "sell_win_rate",
        "buy_pf",
        "sell_pf",
        "fragile_regime_dependency",
        "single_trade_dependency",
        "candidate",
        "recommendation",
        "rejection_reasons",
        "eth_hardening_score",
        "source_csv",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({**row, "rejection_reasons": ";".join(str(item) for item in row.get("rejection_reasons") or [])})
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")
    summary_path.write_text(eth_m30_volatility_hardening_summary_markdown(result), encoding="utf-8")
    return csv_path, json_path, summary_path


def eth_m30_volatility_hardening_summary_markdown(result: dict[str, Any]) -> str:
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else _summary(rows, [])
    lines = [
        "# ETHUSD M30 Volatility Breakout Hardening Summary",
        "",
        "ETHUSD M30 Recent Volatility Breakout Monte Carlo hardening. Paper/offline only; no broker, no order execution, no automatic promotion.",
        "",
        f"Evaluations: `{result.get('evaluations', len(rows))}`.",
        f"Candidates: `{len(result.get('candidates') or [])}`.",
        "",
        "## Top Results",
    ]
    for row in rows[:20]:
        lines.append(
            f"- `{row.get('target_name')}` side `{row.get('side')}` session `{row.get('session')}` recent `{row.get('recent_closed')}` "
            f"PF `{row.get('recent_pf')}`, total `{row.get('total_closed')}` PF `{row.get('total_pf')}`, "
            f"MC PF `{row.get('monte_carlo_stressed_pf')}`, spread x2 PF `{row.get('spread_x2_pf')}`, "
            f"remove best 5 PF `{row.get('remove_best_5_pf')}`, recommendation `{row.get('recommendation')}`."
        )
    lines.extend(
        [
            "",
            "## Answers",
            f"1. Filter improving Monte Carlo: {summary.get('best_filter_answer')}",
            f"2. ETHUSD M30 MC PF >= 1.05 status: {summary.get('mc_threshold_answer')}",
            f"3. Stronger side: {summary.get('side_answer')}",
            f"4. Session to disable/keep: {summary.get('session_answer')}",
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


def _result(
    rows: list[dict[str, Any]],
    configs: list[EthVolatilityHardeningConfig],
    errors: list[dict[str, Any]],
    warnings: list[str],
    started: float,
    csv_path: Path,
    output_mode: str,
) -> dict[str, Any]:
    rows.sort(key=lambda row: float(row.get("eth_hardening_score") or 0.0), reverse=True)
    candidates = [row for row in rows if row.get("candidate")]
    return {
        "ok": True,
        "status": "mt5_eth_m30_volatility_hardening_completed",
        "symbol": "ETHUSD",
        "timeframe": "M30",
        "mode": output_mode,
        "csv_path": str(csv_path),
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


def _hardening_configs() -> list[EthVolatilityHardeningConfig]:
    return [
        _cfg("eth_m30_vol_breakout_both_baseline", "both", "all", "normal_high", "any", "baseline", ("baseline",), score=56.0, mae=0.82),
        _cfg("eth_m30_vol_breakout_buy_baseline", "buy", "all", "normal_high", "any", "baseline", ("side_filter_buy",), score=56.0, mae=0.82),
        _cfg("eth_m30_vol_breakout_sell_baseline", "sell", "all", "normal_high", "any", "baseline", ("side_filter_sell",), score=56.0, mae=0.82),
        _cfg("eth_m30_vol_breakout_side_filtered_v1", "sell", "all", "normal_high", "any", "side_filtered", ("side_filter_sell", "baseline_hardened"), score=56.0, mae=0.76, fast=0.42),
        _cfg("eth_m30_vol_breakout_session_filtered_v1", "both", "london_us", "normal_high", "any", "session_filtered", ("session_filter_london_us",), score=56.0, mae=0.76, fast=0.42),
        _cfg("eth_m30_vol_breakout_ny_core_session_v1", "both", "ny_core", "normal_high", "any", "session_filtered", ("session_filter_ny_core",), score=56.0, mae=0.76, fast=0.42),
        _cfg("eth_m30_vol_breakout_mae_guard_v1", "both", "all", "normal_high", "any", "mae_guard", ("mae_guard",), score=56.0, mae=0.58),
        _cfg("eth_m30_vol_breakout_fast_loss_cut_v1", "both", "all", "normal_high", "any", "fast_loss_cut", ("fast_loss_cut",), score=56.0, mae=0.76, fast=0.30),
        _cfg("eth_m30_vol_breakout_trailing_defensive_v1", "both", "all", "normal_high", "any", "trailing_defensive", ("trailing_defensive",), score=56.0, mae=0.76, fast=0.45, trail=0.75, lock=0.05),
        _cfg("eth_m30_vol_breakout_regime_filtered_v1", "both", "all", "normal_high", "trend", "regime_filtered", ("trend_regime_filter",), score=56.0, mae=0.76, fast=0.40),
        _cfg("eth_m30_vol_breakout_high_vol_v1", "both", "all", "high", "any", "volatility_filtered", ("high_volatility_filter",), score=55.0, mae=0.76, fast=0.40),
        _cfg("eth_m30_vol_breakout_chop_guard_v1", "both", "all", "normal_high", "trend", "chop_guard", ("block_chop", "trend_regime_filter"), score=58.0, mae=0.70, fast=0.34),
        _cfg(
            "eth_m30_vol_breakout_mc_hardened_v1",
            "both",
            "london_us",
            "normal_high",
            "any",
            "mc_hardened",
            ("session_filter_london_us", "mae_guard", "fast_loss_cut", "trailing_defensive", "not_extreme_rsi"),
            score=58.0,
            rsi="not_extreme",
            rr=1.0,
            mae=0.58,
            fast=0.30,
            trail=0.70,
            lock=0.05,
        ),
    ]


def _cfg(
    target: str,
    side: str,
    session: str,
    volatility: str,
    trend: str,
    mode: str,
    actions: tuple[str, ...],
    *,
    score: float,
    rr: float = 1.05,
    time_stop: int = 2,
    atr_stop: float = 1.0,
    mae: float = 0.76,
    fast: float = 0.0,
    trail: float = 0.0,
    lock: float = 0.0,
    rsi: str = "any",
) -> EthVolatilityHardeningConfig:
    variant = RecentFirstVariant(
        family="recent_volatility_breakout",
        timeframe="M30",
        side_mode=side,
        session_name=session,
        volatility_regime=volatility,
        trend_regime=trend,
        rsi_regime=rsi,
        score_threshold=score,
        risk_reward=rr,
        time_stop_bars=time_stop,
        atr_stop_multiplier=atr_stop,
        mae_exit_r=mae,
        momentum_loss_exit=True,
    )
    return EthVolatilityHardeningConfig(
        target_name=target,
        config=MultiSymbolConfig(
            base=variant,
            hardening_mode=mode,
            mae_exit_r=mae,
            fast_loss_cut_r=fast,
            trailing_activation_r=trail,
            trailing_lock_r=lock,
        ),
        hardening_actions=actions,
    )


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
    if int(recent.get("closed") or 0) < 20:
        reasons.append("recent_sample_below_20")
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
    if float(spread_x2.get("profit_factor") or 0.0) < 0.95:
        reasons.append("spread_x2_pf_below_0_95")
    if float(remove_best_5.get("profit_factor") or 0.0) < 1.0:
        reasons.append("remove_best_5_pf_below_1")
    if fragile:
        reasons.append("fragile_regime_dependency")
    if single_trade:
        reasons.append("single_trade_dependency")
    return {"passed": not reasons, "reasons": reasons or ["passes_eth_m30_volatility_hardening_gates"]}


def _score(
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
    score += min(int(recent.get("closed") or 0), 80) * 3.0
    score += min(int(total.get("closed") or 0), 180) * 0.8
    score += max(0.0, float(recent.get("profit_factor") or 0.0) - 1.0) * 95.0
    score += max(0.0, float(total.get("profit_factor") or 0.0) - 1.0) * 75.0
    score += max(0.0, float(monte_carlo.get("profit_factor_stressed") or 0.0) - 1.0) * 180.0
    score += max(0.0, float(spread_x2.get("profit_factor") or 0.0) - 1.0) * 70.0
    score += max(0.0, float(remove_best_5.get("profit_factor") or 0.0) - 1.0) * 90.0
    score += max(0.0, float(recent.get("expectancy") or 0.0)) * 260.0
    score += max(0.0, float(total.get("expectancy") or 0.0)) * 200.0
    score -= max(0.0, float(monte_carlo.get("max_drawdown_p95") or 0.0) - 3500.0) / 30.0
    score -= max(0, 20 - int(recent.get("closed") or 0)) * 9.0
    score -= max(0, 50 - int(total.get("closed") or 0)) * 4.0
    if fragile:
        score -= 120.0
    if single_trade:
        score -= 140.0
    if not gate.get("passed"):
        score -= len(gate.get("reasons") or []) * 12.0
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


def _summary(rows: list[dict[str, Any]], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"recommendation": "no_data", **_safety()}
    best_filter = max(rows, key=lambda row: float(row.get("monte_carlo_stressed_pf") or 0.0), default={})
    best_score = max(rows, key=lambda row: float(row.get("eth_hardening_score") or 0.0), default={})
    side_answer = _best_side_answer(rows)
    session_answer = _best_session_answer(rows)
    mc_pass = [
        row
        for row in rows
        if float(row.get("monte_carlo_stressed_pf") or 0.0) >= 1.05 and float(row.get("monte_carlo_stressed_expectancy") or 0.0) >= 0
    ]
    return {
        "recommendation": "capital_preservation_ready" if candidates else "observation_only" if any(row.get("recommendation") == "observation_only" for row in rows) else "reject",
        "best_filter_answer": (
            f"{best_filter.get('target_name')} MC PF={best_filter.get('monte_carlo_stressed_pf')} "
            f"recent={best_filter.get('recent_closed')} total={best_filter.get('total_closed')}"
        ),
        "mc_threshold_answer": (
            f"{len(mc_pass)} variants reached MC PF >= 1.05; "
            f"candidate_count={len(candidates)}; best_score={best_score.get('target_name')}"
        ),
        "side_answer": side_answer,
        "session_answer": session_answer,
        "capital_preservation_answer": (
            "; ".join(str(row.get("target_name")) for row in candidates[:3])
            if candidates
            else "none; no ETHUSD M30 variant should pass to capital preservation optimizer yet"
        ),
        "automatic_promotion": False,
        **_safety(),
    }


def _session_stats(trades: list[dict[str, Any]], initial_balance: float) -> dict[str, dict[str, Any]]:
    buckets = {"asia": set(range(0, 8)), "london_us": set(range(7, 21)), "ny_core": set(range(13, 21)), "off_session": set(range(21, 24))}
    stats: dict[str, dict[str, Any]] = {}
    for name, hours in buckets.items():
        scoped = [trade for trade in trades if _hour(trade) in hours]
        stats[name] = _compact(_metrics(scoped, initial_balance=initial_balance))
    return stats


def _feature_bucket_stats(trades: list[dict[str, Any]], initial_balance: float, feature_name: str, bucket_fn: Any) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        snapshot = trade.get("features_snapshot") if isinstance(trade.get("features_snapshot"), dict) else {}
        bucket = bucket_fn(float(_number(snapshot.get(feature_name)) or 0.0))
        grouped.setdefault(bucket, []).append(trade)
    return {name: _compact(_metrics(items, initial_balance=initial_balance)) for name, items in sorted(grouped.items())}


def _loss_cluster_stats(trades: list[dict[str, Any]]) -> dict[str, Any]:
    max_streak = 0
    current = 0
    clusters = 0
    cluster_pnls: list[float] = []
    active_cluster_pnl = 0.0
    for trade in trades:
        pnl = float(_number(trade.get("pnl")) or 0.0)
        if trade.get("status") == "loss":
            current += 1
            active_cluster_pnl += pnl
            max_streak = max(max_streak, current)
        else:
            if current >= 2:
                clusters += 1
                cluster_pnls.append(active_cluster_pnl)
            current = 0
            active_cluster_pnl = 0.0
    if current >= 2:
        clusters += 1
        cluster_pnls.append(active_cluster_pnl)
    return {
        "max_consecutive_losses": max_streak,
        "loss_cluster_count": clusters,
        "worst_cluster_pnl": round(min(cluster_pnls), 6) if cluster_pnls else 0.0,
    }


def _best_side_answer(rows: list[dict[str, Any]]) -> str:
    totals: dict[str, list[dict[str, Any]]] = {"buy": [], "sell": []}
    for row in rows:
        stats = row.get("buy_sell_stats") if isinstance(row.get("buy_sell_stats"), dict) else {}
        for side in ["buy", "sell"]:
            item = stats.get(side) if isinstance(stats.get(side), dict) else {}
            totals[side].append(item)
    summaries: dict[str, dict[str, float]] = {}
    for side, items in totals.items():
        closed = sum(int(item.get("closed") or item.get("trades") or 0) for item in items)
        weighted_pf = sum(float(item.get("profit_factor") or 0.0) * int(item.get("closed") or item.get("trades") or 0) for item in items)
        weighted_exp = sum(float(item.get("expectancy") or 0.0) * int(item.get("closed") or item.get("trades") or 0) for item in items)
        summaries[side] = {
            "closed": closed,
            "pf": round(weighted_pf / closed, 4) if closed else 0.0,
            "expectancy": round(weighted_exp / closed, 4) if closed else 0.0,
        }
    best = max(summaries, key=lambda side: (summaries[side]["pf"], summaries[side]["expectancy"]))
    return f"{best} looks stronger; buy={summaries['buy']}; sell={summaries['sell']}"


def _best_session_answer(rows: list[dict[str, Any]]) -> str:
    totals: dict[str, dict[str, float]] = {}
    for row in rows:
        stats = row.get("session_stats") if isinstance(row.get("session_stats"), dict) else {}
        for session, item in stats.items():
            if not isinstance(item, dict):
                continue
            bucket = totals.setdefault(session, {"closed": 0.0, "pf_weighted": 0.0, "exp_weighted": 0.0})
            closed = float(item.get("closed") or 0.0)
            bucket["closed"] += closed
            bucket["pf_weighted"] += float(item.get("profit_factor") or 0.0) * closed
            bucket["exp_weighted"] += float(item.get("expectancy") or 0.0) * closed
    if not totals:
        return "no session data"
    compact = {
        name: {
            "closed": int(item["closed"]),
            "pf": round(item["pf_weighted"] / item["closed"], 4) if item["closed"] else 0.0,
            "expectancy": round(item["exp_weighted"] / item["closed"], 4) if item["closed"] else 0.0,
        }
        for name, item in totals.items()
    }
    best = max(compact, key=lambda name: (compact[name]["pf"], compact[name]["expectancy"]))
    weak = [name for name, item in compact.items() if item["closed"] >= 5 and (item["pf"] < 1.0 or item["expectancy"] <= 0)]
    return f"best={best} {compact[best]}; weak_or_disable={weak or 'none'}"


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

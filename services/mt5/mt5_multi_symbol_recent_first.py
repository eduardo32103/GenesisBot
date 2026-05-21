from __future__ import annotations

import csv
import json
import math
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from services.mt5.mt5_backtester import BacktestSettings, _close, _load_bars, _metrics, _number, _reason_counts, _safety, _settings
from services.mt5.mt5_capital_preservation_optimizer import (
    _depends_on_single_trade,
    _drawdown_accelerating,
    _loss_streak,
    _monte_carlo_stress,
    _recent_edge_negative,
)
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_recent_first_research import (
    RECENT_FIRST_FAMILIES,
    RECENT_FIRST_TIMEFRAMES,
    RecentFirstVariant,
    _build_variants,
    _compact_metrics,
    _decision_for_recent,
    _fragile_dependency,
    _quarter_ranges,
)
from services.mt5.mt5_strategy_research_v2 import _features_by_index, _open_research_trade
from services.mt5.mt5_symbol_cost_model import ALIAS_PATTERNS, build_symbol_cost_model, infer_instrument_type, write_cost_model_report


DEFAULT_MULTI_SYMBOLS = ["BTCUSD", "ETHUSD", "XAUUSD", "NAS100", "US500", "EURUSD", "GBPUSD"]
DEFAULT_SYMBOL_SPREAD_POINTS = {
    "BTCUSD": 25.0,
    "ETHUSD": 18.0,
    "XAUUSD": 25.0,
    "NAS100": 20.0,
    "US500": 8.0,
    "EURUSD": 2.0,
    "GBPUSD": 3.0,
}
_HARDENING_MODES = ["baseline", "mae_guard", "fast_loss_cut", "trailing_defensive"]


@dataclass(frozen=True)
class MultiSymbolConfig:
    base: RecentFirstVariant
    hardening_mode: str
    mae_exit_r: float
    fast_loss_cut_r: float = 0.0
    trailing_activation_r: float = 0.0
    trailing_lock_r: float = 0.0

    @property
    def family(self) -> str:
        return self.base.family

    @property
    def timeframe(self) -> str:
        return self.base.timeframe

    @property
    def side_mode(self) -> str:
        return self.base.side_mode

    @property
    def session_name(self) -> str:
        return self.base.session_name

    def key(self) -> str:
        return (
            f"{self.base.key()}|hardening={self.hardening_mode}|mae={self.mae_exit_r}|"
            f"fast={self.fast_loss_cut_r}|trail={self.trailing_activation_r}:{self.trailing_lock_r}"
        )

    def recent_variant(self) -> RecentFirstVariant:
        return replace(self.base, mae_exit_r=self.mae_exit_r)


def run_multi_symbol_recent_first(body: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    body = body or {}
    symbols = [symbol.upper() for symbol in _requested_list(body.get("symbols"), DEFAULT_MULTI_SYMBOLS)]
    timeframes = [timeframe.upper() for timeframe in _requested_list(body.get("timeframes"), RECENT_FIRST_TIMEFRAMES)]
    timeframes = [timeframe for timeframe in timeframes if timeframe in RECENT_FIRST_TIMEFRAMES]
    families = [family for family in _requested_list(body.get("families"), RECENT_FIRST_FAMILIES) if family in RECENT_FIRST_FAMILIES]
    csv_dir = Path(str(body.get("csv_dir") or Path("data") / "backtests" / "multisymbol"))
    fallback_csv_dir = Path(str(body.get("fallback_csv_dir") or Path("data") / "backtests"))
    bars_requested = max(200, min(int(_number(body.get("bars") or body.get("max_bars")) or 20000), 65000))
    per_eval_timeout = max(0.25, float(_number(body.get("per_evaluation_timeout_seconds")) or 1.5))
    max_per_symbol_timeframe = max(1, int(_number(body.get("max_evaluations_per_symbol_timeframe")) or 40))
    monte_carlo_simulations = max(100, min(int(_number(body.get("monte_carlo_simulations")) or 300), 1000))
    fixed_spread = _number(body.get("spread_points"))

    rows: list[dict[str, Any]] = []
    cost_rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[str] = []
    skipped_symbols: list[dict[str, Any]] = []
    csv_paths: dict[str, str] = {}

    for symbol in symbols:
        symbol_loaded = False
        for timeframe in timeframes:
            csv_path = _find_csv_path(csv_dir, fallback_csv_dir, symbol, timeframe, bars_requested)
            if csv_path is None:
                errors.append({"symbol": symbol, "timeframe": timeframe, "error": "csv_not_found"})
                continue
            symbol_loaded = True
            resolved_symbol = _symbol_from_csv_path(csv_path, symbol)
            label = f"{symbol}_{timeframe}_{_csv_size_label(csv_path)}"
            csv_paths[f"{symbol}:{timeframe}"] = str(csv_path)
            cost_model = build_symbol_cost_model(
                symbol,
                resolved_symbol=resolved_symbol,
                first_price=_first_csv_price(csv_path),
                broker_spread_points=float(fixed_spread) if fixed_spread is not None else None,
            )
            cost_rows.append({**cost_model.as_dict(), "timeframe": timeframe, "csv_path": str(csv_path), "csv_found": True, **_safety()})
            spread_points = float(cost_model.spread_points if fixed_spread is None else fixed_spread)
            settings_body = {
                "symbol": symbol,
                "timeframe": timeframe,
                "csv_path": str(csv_path),
                "max_bars": bars_requested,
                "spread_points": spread_points,
                "point": cost_model.point,
                "commission": cost_model.commission_assumption,
                "slippage_points": cost_model.slippage_assumption,
                "save_results": False,
                "source": "mt5_csv",
                "timeout_seconds": per_eval_timeout,
            }
            settings = replace(
                _settings(settings_body, get_mt5_config()),
                max_bars=bars_requested,
                timeout_seconds=max(1.0, min(per_eval_timeout, 20.0)),
                point=cost_model.point,
                spread_points=spread_points,
                commission=cost_model.commission_assumption,
                slippage_points=cost_model.slippage_assumption,
            )
            bars, load_warnings = _load_bars(settings_body, settings)
            warnings.extend([f"{label}:{warning}" for warning in load_warnings])
            bars = bars[-settings.max_bars :]
            if not bars:
                errors.append({"symbol": symbol, "timeframe": timeframe, "path": str(csv_path), "error": "csv_bars_not_loaded"})
                continue
            features_by_index = _features_by_index(bars)
            for config in _configs_for_timeframe(timeframe, families, max_per_symbol_timeframe):
                rows.append(
                    evaluate_multi_symbol_config(
                        settings,
                        bars,
                        config,
                        symbol=symbol,
                        sample_label=label,
                        source_csv=str(csv_path),
                        features_by_index=features_by_index,
                        timeout_seconds=per_eval_timeout,
                        cost_model=cost_model.as_dict(),
                        monte_carlo_simulations=monte_carlo_simulations,
                    )
                )
        if not symbol_loaded:
            skipped_symbols.append({"symbol": symbol, "reason": "no_csv_history_available"})
            cost_model = build_symbol_cost_model(symbol)
            cost_rows.append(
                {
                    **cost_model.as_dict(),
                    "timeframe": "",
                    "csv_path": "",
                    "csv_found": False,
                    "cost_model_confidence": "low",
                    "cost_model_reason": "no_local_csv_or_alias_unresolved",
                    **_safety(),
                }
            )

    rows.sort(key=lambda row: float(row.get("multi_symbol_score") or 0.0), reverse=True)
    candidates = [row for row in rows if row.get("candidate")]
    result = {
        "ok": True,
        "status": "mt5_multi_symbol_recent_first_completed",
        "mode": "paper",
        "symbols": symbols,
        "timeframes": timeframes,
        "families": families,
        "bars_requested": bars_requested,
        "csv_dir": str(csv_dir),
        "csv_paths": csv_paths,
        "cost_model_report": {"ok": True, "status": "mt5_symbol_cost_model_report_ready", "rows": cost_rows, **_safety()},
        "skipped_symbols": skipped_symbols,
        "evaluations": len(rows),
        "results": rows,
        "candidates": candidates,
        "top_3_for_capital_optimizer": candidates[:3],
        "candidate_profile_names": [_candidate_profile_name(row) for row in candidates[:3]],
        "summary": _summary(rows, candidates, symbols, skipped_symbols),
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


def evaluate_multi_symbol_config(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    config: MultiSymbolConfig,
    *,
    symbol: str,
    sample_label: str,
    source_csv: str,
    features_by_index: dict[int, dict[str, Any]],
    timeout_seconds: float,
    cost_model: dict[str, Any] | None = None,
    monte_carlo_simulations: int = 300,
) -> dict[str, Any]:
    started = time.monotonic()
    trades, blocked, signals, state = _simulate_multi_symbol(settings, bars, config, started, timeout_seconds=timeout_seconds, features_by_index=features_by_index)
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    total = _metrics(closed, initial_balance=settings.initial_balance)
    split = _split_metrics(settings, bars, closed)
    recent = split["recent"]
    monte_carlo = _monte_carlo_stress(closed, initial_balance=settings.initial_balance, max_drawdown_limit=5000.0, simulations=monte_carlo_simulations)
    remove_best_5 = _remove_best_metrics(settings, closed, 5)
    spread_x1_5 = _spread_stress_metrics(settings, bars, config, features_by_index, timeout_seconds, 1.5)
    spread_x2 = _spread_stress_metrics(settings, bars, config, features_by_index, timeout_seconds, 2.0)
    fragile = _fragile_dependency(total, split)
    single_trade = _depends_on_single_trade(total)
    gate = _gate(total, recent, monte_carlo, remove_best_5, spread_x1_5, spread_x2, fragile, single_trade, cost_model or {})
    score = _score(total, recent, monte_carlo, remove_best_5, spread_x1_5, spread_x2, gate, fragile, single_trade)
    row = {
        "symbol": symbol,
        "sample_label": sample_label,
        "source_csv": source_csv,
        "csv_path_used": source_csv,
        "family": config.family,
        "profile": config.family,
        "candidate_profile_name": _candidate_profile_name({"symbol": symbol, "family": config.family, "timeframe": config.timeframe, "side": config.side_mode}),
        "variant_id": config.key(),
        "timeframe": config.timeframe,
        "side": config.side_mode,
        "side_mode": config.side_mode,
        "session": config.session_name,
        "session_filter": config.session_name,
        "hardening_mode": config.hardening_mode,
        "risk_reward": config.base.risk_reward,
        "time_stop_bars": config.base.time_stop_bars,
        "mae_exit_r": config.mae_exit_r,
        "fast_loss_cut_r": config.fast_loss_cut_r,
        "trailing_activation_r": config.trailing_activation_r,
        "trailing_lock_r": config.trailing_lock_r,
        "bars_loaded": len(bars),
        "bars_evaluated": max(0, len(bars) - 80),
        "first_bar_time": str(bars[0].get("time") or "") if bars else "",
        "last_bar_time": str(bars[-1].get("time") or "") if bars else "",
        "spread_points": settings.spread_points,
        "requested_symbol": (cost_model or {}).get("requested_symbol", symbol),
        "resolved_symbol": (cost_model or {}).get("resolved_symbol", symbol),
        "instrument_type": (cost_model or {}).get("instrument_type", "unknown"),
        "digits": (cost_model or {}).get("digits", 0),
        "point": (cost_model or {}).get("point", settings.point),
        "tick_size": (cost_model or {}).get("tick_size", settings.point),
        "estimated_spread_price": (cost_model or {}).get("estimated_spread_price", 0.0),
        "commission_assumption": (cost_model or {}).get("commission_assumption", 0.0),
        "slippage_assumption": (cost_model or {}).get("slippage_assumption", 0.0),
        "spread_x1_5_cost": (cost_model or {}).get("spread_x1_5_cost", 0.0),
        "spread_x2_cost": (cost_model or {}).get("spread_x2_cost", 0.0),
        "cost_model_confidence": (cost_model or {}).get("cost_model_confidence", "low"),
        "cost_model_reason": (cost_model or {}).get("cost_model_reason", "missing_cost_model"),
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
        "oldest_pf": split["oldest"]["profit_factor"],
        "middle_pf": split["middle"]["profit_factor"],
        "previous_pf": split["previous"]["profit_factor"],
        "oldest_expectancy": split["oldest"]["expectancy"],
        "middle_expectancy": split["middle"]["expectancy"],
        "previous_expectancy": split["previous"]["expectancy"],
        "monte_carlo_stressed_pf": monte_carlo.get("profit_factor_stressed", 0.0),
        "monte_carlo_stressed_expectancy": monte_carlo.get("expectancy_stressed", 0.0),
        "monte_carlo_p95_drawdown": monte_carlo.get("max_drawdown_p95", 0.0),
        "monte_carlo_fail_reasons": list(monte_carlo.get("fail_reasons") or []),
        "spread_x1_5_pf": spread_x1_5["profit_factor"],
        "spread_x1_5_expectancy": spread_x1_5["expectancy"],
        "spread_x2_pf": spread_x2["profit_factor"],
        "spread_x2_expectancy": spread_x2["expectancy"],
        "remove_best_5_pf": remove_best_5["profit_factor"],
        "remove_best_5_expectancy": remove_best_5["expectancy"],
        "fragile_regime_dependency": fragile,
        "single_trade_dependency": single_trade,
        "exit_reason_counts": total["exit_reason_counts"],
        "blocked_reason_counts": _reason_counts(blocked),
        "risk_governor_blocks": state["risk_governor_blocks"],
        "max_open_trades_observed": state["max_open_trades_observed"],
        "candidate": gate["passed"],
        "recommendation": "research_candidate" if gate["passed"] else "observation_only" if _observation_quality(total, recent, monte_carlo) else "reject",
        "rejection_reasons": gate["reasons"],
        "reject_reasons": gate["reasons"],
        "multi_symbol_score": score,
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
    return row


def write_multi_symbol_recent_first_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "multi_symbol_recent_first_cost_calibrated_results.csv"
    json_path = root / "multi_symbol_recent_first_cost_calibrated_results.json"
    summary_path = root / "multi_symbol_recent_first_cost_calibrated_summary.md"
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    headers = [
        "symbol",
        "requested_symbol",
        "resolved_symbol",
        "sample_label",
        "timeframe",
        "family",
        "side",
        "session",
        "hardening_mode",
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
        "instrument_type",
        "digits",
        "point",
        "tick_size",
        "spread_points",
        "estimated_spread_price",
        "commission_assumption",
        "slippage_assumption",
        "spread_x1_5_cost",
        "spread_x2_cost",
        "cost_model_confidence",
        "cost_model_reason",
        "remove_best_5_pf",
        "fragile_regime_dependency",
        "single_trade_dependency",
        "candidate",
        "recommendation",
        "rejection_reasons",
        "multi_symbol_score",
        "source_csv",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({**row, "rejection_reasons": ";".join(str(item) for item in row.get("rejection_reasons") or [])})
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")
    summary_path.write_text(multi_symbol_recent_first_summary_markdown(result), encoding="utf-8")
    if isinstance(result.get("cost_model_report"), dict):
        write_cost_model_report(result["cost_model_report"], root)
    return csv_path, json_path, summary_path


def multi_symbol_recent_first_summary_markdown(result: dict[str, Any]) -> str:
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else _summary(rows, [], [], [])
    lines = [
        "# MT5 Multi-Symbol Recent-First Summary",
        "",
        "Multi-symbol recent-first edge discovery. Paper/offline/read-only only; no broker, no order execution, no automatic promotion.",
        "",
        f"Evaluations: `{result.get('evaluations', len(rows))}`.",
        f"Candidates: `{len(result.get('candidates') or [])}`.",
        f"Skipped symbols: `{len(result.get('skipped_symbols') or [])}`.",
        "",
        "## Top Results",
    ]
    for row in rows[:20]:
        lines.append(
            f"- `{row.get('symbol')}` `{row.get('timeframe')}` `{row.get('family')}` side `{row.get('side')}` "
            f"session `{row.get('session')}` hardening `{row.get('hardening_mode')}` recent `{row.get('recent_closed')}` "
            f"recent PF `{row.get('recent_pf')}`, total `{row.get('total_closed')}` total PF `{row.get('total_pf')}`, "
            f"MC PF `{row.get('monte_carlo_stressed_pf')}`, spread x2 PF `{row.get('spread_x2_pf')}`, recommendation `{row.get('recommendation')}`."
        )
    lines.extend(
        [
            "",
            "## Answers",
            f"1. Spread x2 zero verdict: {summary.get('spread_x2_verdict')}",
            f"2. Symbols with reliable costs: {summary.get('reliable_costs_answer')}",
            f"3. Alias/export status: {summary.get('alias_export_answer')}",
            f"4. Symbols with recent edge: {summary.get('symbols_with_edge_answer')}",
            f"5. Evaluated symbols without edge / NO_TRADE: {summary.get('symbols_without_edge_answer')}",
            f"6. Best timeframe by symbol: {summary.get('timeframe_by_symbol_answer')}",
            f"7. Best family by symbol: {summary.get('family_by_symbol_answer')}",
            f"8. Best side by symbol: {summary.get('side_by_symbol_answer')}",
            f"9. Best session by symbol: {summary.get('session_by_symbol_answer')}",
            f"10. Profiles passing Monte Carlo: {summary.get('monte_carlo_pass_answer')}",
            f"11. Profiles failing spread/slippage stress: {summary.get('spread_fail_answer')}",
            f"12. Top 3 for capital preservation optimizer: {summary.get('top_3_answer')}",
            f"13. Final recommendation: {summary.get('recommendation')}",
            "14. No automatic promotion.",
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


def _simulate_multi_symbol(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    config: MultiSymbolConfig,
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
        segment_trades, segment_blocked, segment_signals, segment_state = _simulate_multi_symbol_segment(
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


def _simulate_multi_symbol_segment(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    config: MultiSymbolConfig,
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
    recent_variant = config.recent_variant()
    research_variant = recent_variant.research_variant()
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
            open_trade, closed = _update_multi_trade(settings, open_trade, bar, index, config)
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
        risk_reason = _multi_symbol_risk_block(settings, trades)
        if risk_reason:
            state["risk_governor_blocks"] += 1
            blocked.append(f"risk_governor_{risk_reason}")
            continue
        features = features_by_index.get(index - 1)
        if not features:
            blocked.append("insufficient_history")
            continue
        decision = _decision_for_recent(features, recent_variant)
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
            "shadow_trade_id": f"multi-symbol-recent-{settings.normalized_symbol}-{config.family}-{config.timeframe}-{index}",
            "source": "mt5_multi_symbol_recent_first",
            "strategy_profile": config.family,
            "filter_profile": config.key(),
            "hardening_mode": config.hardening_mode,
            **_safety(),
        }
        state["max_open_trades_observed"] = max(state["max_open_trades_observed"], 1)
    if open_trade:
        last_bar = bars[min(loop_end - 1, len(bars) - 1)]
        trades.append(_close(settings, open_trade, float(_number(last_bar.get("close")) or open_trade.get("entry_price") or 0.0), "time_stop", last_bar))
    return trades, blocked, signals, state


def _update_multi_trade(
    settings: BacktestSettings,
    trade: dict[str, Any],
    bar: dict[str, Any],
    index: int,
    config: MultiSymbolConfig,
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
    if config.base.momentum_loss_exit and bars_open >= 1 and momentum_against:
        return None, _close(settings, updated, close, "momentum_loss_exit", bar)
    if bars_open >= config.base.time_stop_bars:
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
    config: MultiSymbolConfig,
    features_by_index: dict[int, dict[str, Any]],
    timeout_seconds: float,
    factor: float,
) -> dict[str, Any]:
    stressed = replace(settings, spread_points=settings.spread_points * factor)
    trades, _, _, _ = _simulate_multi_symbol(stressed, bars, config, time.monotonic(), timeout_seconds=timeout_seconds, features_by_index=features_by_index)
    return _metrics([trade for trade in trades if trade.get("lifecycle_status") == "closed"], initial_balance=settings.initial_balance)


def _remove_best_metrics(settings: BacktestSettings, trades: list[dict[str, Any]], count: int) -> dict[str, Any]:
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    if len(closed) <= count:
        return _metrics([], initial_balance=settings.initial_balance)
    ordered = sorted(closed, key=lambda trade: float(_number(trade.get("pnl")) or 0.0), reverse=True)
    return _metrics(ordered[count:], initial_balance=settings.initial_balance)


def _multi_symbol_risk_block(settings: BacktestSettings, trades: list[dict[str, Any]]) -> str:
    spread_price = float(settings.spread_points or 0.0) * float(settings.point or 0.0)
    instrument_type = infer_instrument_type(settings.normalized_symbol or settings.symbol)
    if spread_price > _max_spread_price_for_instrument(instrument_type, settings.symbol):
        return "spread_too_high_calibrated"
    if _loss_streak(trades) >= 4:
        return "consecutive_loss_lockdown"
    if len([trade for trade in trades if trade.get("lifecycle_status") == "closed"]) >= 20 and _recent_edge_negative(trades):
        return "recent_edge_negative"
    if _drawdown_accelerating(trades, settings.initial_balance):
        return "drawdown_accelerating"
    return ""


def _max_spread_price_for_instrument(instrument_type: str, symbol: str) -> float:
    if instrument_type == "forex":
        return 0.06 if "JPY" in symbol.upper() else 0.0006
    if instrument_type == "crypto":
        return 100.0
    if instrument_type == "metal":
        return 2.0
    if instrument_type == "index":
        return 20.0
    return 1.0


def _gate(
    total: dict[str, Any],
    recent: dict[str, Any],
    monte_carlo: dict[str, Any],
    remove_best_5: dict[str, Any],
    spread_x1_5: dict[str, Any],
    spread_x2: dict[str, Any],
    fragile: bool,
    single_trade: bool,
    cost_model: dict[str, Any] | None = None,
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
    if float(spread_x1_5.get("profit_factor") or 0.0) < 1.0:
        reasons.append("spread_x1_5_pf_below_1")
    if float(spread_x2.get("profit_factor") or 0.0) < 0.95:
        reasons.append("spread_x2_pf_below_0_95")
    if float(remove_best_5.get("profit_factor") or 0.0) < 1.0:
        reasons.append("remove_best_5_pf_below_1")
    if fragile:
        reasons.append("fragile_regime_dependency")
    if single_trade:
        reasons.append("single_trade_dependency")
    if str((cost_model or {}).get("cost_model_confidence") or "").casefold() == "low":
        reasons.append("cost_model_confidence_low")
    return {"passed": not reasons, "reasons": reasons or ["passes_multi_symbol_recent_first_gates"]}


def _score(
    total: dict[str, Any],
    recent: dict[str, Any],
    monte_carlo: dict[str, Any],
    remove_best_5: dict[str, Any],
    spread_x1_5: dict[str, Any],
    spread_x2: dict[str, Any],
    gate: dict[str, Any],
    fragile: bool,
    single_trade: bool,
) -> float:
    score = 0.0
    score += min(int(recent.get("closed") or 0), 80) * 3.0
    score += min(int(total.get("closed") or 0), 180) * 0.8
    score += max(0.0, float(recent.get("profit_factor") or 0.0) - 1.0) * 100.0
    score += max(0.0, float(total.get("profit_factor") or 0.0) - 1.0) * 70.0
    score += max(0.0, float(monte_carlo.get("profit_factor_stressed") or 0.0) - 1.0) * 140.0
    score += max(0.0, float(spread_x1_5.get("profit_factor") or 0.0) - 1.0) * 70.0
    score += max(0.0, float(spread_x2.get("profit_factor") or 0.0) - 0.95) * 90.0
    score += max(0.0, float(remove_best_5.get("profit_factor") or 0.0) - 1.0) * 90.0
    score += max(0.0, float(recent.get("expectancy") or 0.0)) * 260.0
    score += max(0.0, float(total.get("expectancy") or 0.0)) * 200.0
    score -= float(total.get("max_drawdown") or 0.0) / 75.0
    score -= float(monte_carlo.get("max_drawdown_p95") or 0.0) / 90.0
    score -= max(0, 20 - int(recent.get("closed") or 0)) * 8.0
    score -= max(0, 50 - int(total.get("closed") or 0)) * 4.0
    if fragile:
        score -= 120.0
    if single_trade:
        score -= 120.0
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


def _configs_for_timeframe(timeframe: str, families: list[str], limit: int) -> list[MultiSymbolConfig]:
    base_limit = max(1, math.ceil(limit / len(_HARDENING_MODES)))
    base_variants = _build_variants([timeframe], families, max_evaluations=base_limit, dataset_counts={timeframe: 1})
    configs: list[MultiSymbolConfig] = []
    for variant in base_variants:
        for mode in _HARDENING_MODES:
            configs.append(_config_from_variant(variant, mode))
            if len(configs) >= limit:
                return configs
    return configs


def _config_from_variant(variant: RecentFirstVariant, mode: str) -> MultiSymbolConfig:
    if mode == "mae_guard":
        return MultiSymbolConfig(variant, mode, mae_exit_r=min(variant.mae_exit_r, 0.62))
    if mode == "fast_loss_cut":
        return MultiSymbolConfig(variant, mode, mae_exit_r=min(variant.mae_exit_r, 0.76), fast_loss_cut_r=0.34)
    if mode == "trailing_defensive":
        return MultiSymbolConfig(variant, mode, mae_exit_r=min(variant.mae_exit_r, 0.76), fast_loss_cut_r=0.45, trailing_activation_r=0.75, trailing_lock_r=0.05)
    return MultiSymbolConfig(variant, "baseline", mae_exit_r=variant.mae_exit_r)


def _find_csv_path(csv_dir: Path, fallback_csv_dir: Path, symbol: str, timeframe: str, bars: int) -> Path | None:
    candidates: list[Path] = []
    suffixes = [str(bars)]
    if timeframe == "M30":
        suffixes.extend(["60000", "40000", "20000", "5000"])
    elif timeframe == "H1":
        suffixes.extend(["30000", "25000", "10000", "5000"])
    elif timeframe == "M15":
        suffixes.extend(["20000", "5000"])
    search_symbols = _csv_search_symbols(symbol)
    for root in [csv_dir, fallback_csv_dir]:
        for search_symbol in search_symbols:
            for suffix in dict.fromkeys(suffixes):
                candidates.append(root / f"{search_symbol}_{timeframe}_{suffix}.csv")
            candidates.append(root / f"{search_symbol}_{timeframe}.csv")
    for path in candidates:
        if path.exists() and path.is_file():
            return path
    return None


def _csv_search_symbols(symbol: str) -> list[str]:
    requested = str(symbol or "").upper().strip()
    values = [symbol, requested, *ALIAS_PATTERNS.get(requested, [])]
    unique: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in unique:
            unique.append(text)
    return unique


def _symbol_from_csv_path(path: Path, fallback: str) -> str:
    stem = path.stem
    parts = stem.split("_")
    return parts[0] if parts and parts[0] else fallback


def _csv_size_label(path: Path) -> str:
    stem = path.stem
    parts = stem.split("_")
    if parts and parts[-1].isdigit():
        return parts[-1]
    return "csv"


def _first_csv_price(path: Path) -> float:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                return float(_number(row.get("close")) or _number(row.get("open")) or 0.0)
    except Exception:
        return 0.0
    return 0.0


def _summary(rows: list[dict[str, Any]], candidates: list[dict[str, Any]], symbols: list[str], skipped_symbols: list[dict[str, Any]]) -> dict[str, Any]:
    rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        rows_by_symbol.setdefault(str(row.get("symbol") or "UNKNOWN"), []).append(row)
    symbols_with_edge = [
        symbol
        for symbol, items in rows_by_symbol.items()
        if any(
            int(row.get("recent_closed") or 0) >= 20
            and float(row.get("recent_pf") or 0.0) >= 1.15
            and float(row.get("recent_expectancy") or 0.0) > 0
            for row in items
        )
    ]
    evaluated_symbols = sorted(rows_by_symbol)
    no_edge_symbols = [symbol for symbol in evaluated_symbols if symbol not in symbols_with_edge]
    skipped_names = [str(item.get("symbol") or "") for item in skipped_symbols if item.get("symbol")]
    mc_pass = [
        row
        for row in rows
        if float(row.get("monte_carlo_stressed_pf") or 0.0) >= 1.05 and float(row.get("monte_carlo_stressed_expectancy") or 0.0) >= 0
    ]
    spread_fail = [
        row
        for row in rows
        if float(row.get("spread_x1_5_pf") or 0.0) < 1.0 or float(row.get("spread_x2_pf") or 0.0) < 0.95
    ]
    reliable_symbols = sorted(
        {
            str(row.get("symbol") or "")
            for row in rows
            if str(row.get("cost_model_confidence") or "").casefold() in {"high", "medium"}
        }
    )
    return {
        "recommendation": "research_candidate" if candidates else "observation_only" if symbols_with_edge else "reject",
        "spread_x2_verdict": _spread_x2_verdict(rows),
        "reliable_costs_answer": ", ".join(symbol for symbol in reliable_symbols if symbol) if reliable_symbols else "none",
        "alias_export_answer": _alias_export_answer(rows_by_symbol, skipped_symbols),
        "symbols_with_edge_answer": _symbol_list(symbols_with_edge, rows_by_symbol),
        "symbols_without_edge_answer": (
            (", ".join(no_edge_symbols) if no_edge_symbols else "none")
            + ("; skipped/no local CSV: " + ", ".join(skipped_names) if skipped_names else "")
        ),
        "timeframe_by_symbol_answer": _best_by_symbol(rows_by_symbol, "timeframe"),
        "family_by_symbol_answer": _best_by_symbol(rows_by_symbol, "family"),
        "side_by_symbol_answer": _best_by_symbol(rows_by_symbol, "side_mode"),
        "session_by_symbol_answer": _best_by_symbol(rows_by_symbol, "session_filter"),
        "monte_carlo_pass_answer": _row_list(mc_pass[:8]),
        "spread_fail_answer": _row_list(spread_fail[:8]),
        "top_3_answer": "; ".join(_candidate_profile_name(row) for row in candidates[:3]) if candidates else "none; no profile should pass to capital preservation optimizer",
        "skipped_symbols": skipped_symbols,
        "automatic_promotion": False,
        **_safety(),
}


def _spread_x2_verdict(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "no rows evaluated"
    zero_rows = [row for row in rows if float(row.get("spread_x2_pf") or 0.0) <= 0.0]
    low_conf = [row for row in zero_rows if str(row.get("cost_model_confidence") or "").casefold() == "low"]
    if zero_rows and len(zero_rows) == len(rows):
        if low_conf:
            return "inconclusive; spread x2 is zero and at least one cost model has low confidence"
        return "likely real cost sensitivity under the calibrated model, not just fixed-points artifact; verify with live broker spread snapshots before any promotion"
    if zero_rows:
        return "mixed; some profiles collapse under spread x2 while others survive"
    return "spread x2 did not collapse evaluated profiles"


def _alias_export_answer(rows_by_symbol: dict[str, list[dict[str, Any]]], skipped_symbols: list[dict[str, Any]]) -> str:
    exported = ", ".join(sorted(rows_by_symbol)) if rows_by_symbol else "none"
    skipped = ", ".join(str(item.get("symbol") or "") for item in skipped_symbols if item.get("symbol")) or "none"
    return f"exported/evaluated: {exported}; skipped/no local CSV: {skipped}"


def _symbol_list(symbols: list[str], rows_by_symbol: dict[str, list[dict[str, Any]]]) -> str:
    if not symbols:
        return "none"
    parts = []
    for symbol in symbols:
        best = max(rows_by_symbol.get(symbol) or [], key=lambda row: float(row.get("multi_symbol_score") or 0.0), default={})
        parts.append(
            f"{symbol} best={best.get('timeframe')} {best.get('family')} {best.get('side_mode')} "
            f"recent={best.get('recent_closed')} recent_pf={best.get('recent_pf')}"
        )
    return "; ".join(parts)


def _best_by_symbol(rows_by_symbol: dict[str, list[dict[str, Any]]], key: str) -> str:
    parts = []
    for symbol, rows in sorted(rows_by_symbol.items()):
        buckets: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            buckets.setdefault(str(row.get(key) or "unknown"), []).append(row)
        ranked = []
        for name, items in buckets.items():
            score = max(float(row.get("multi_symbol_score") or 0.0) for row in items)
            recent = sum(int(row.get("recent_closed") or 0) for row in items)
            total = sum(int(row.get("total_closed") or 0) for row in items)
            ranked.append((score + min(recent, 120) * 0.2 + min(total, 200) * 0.05, name))
        ranked.sort(reverse=True)
        if ranked:
            parts.append(f"{symbol}:{ranked[0][1]}")
    return "; ".join(parts) if parts else "none"


def _row_list(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "none"
    return "; ".join(
        f"{row.get('symbol')} {row.get('timeframe')} {row.get('family')} {row.get('side_mode')} "
        f"recent={row.get('recent_closed')} total={row.get('total_closed')} MC={row.get('monte_carlo_stressed_pf')} spread2={row.get('spread_x2_pf')}"
        for row in rows
    )


def _candidate_profile_name(row: dict[str, Any]) -> str:
    symbol = str(row.get("symbol") or "symbol").lower()
    family = str(row.get("family") or "family").lower()
    timeframe = str(row.get("timeframe") or "tf").lower()
    side = str(row.get("side_mode") or row.get("side") or "both").lower()
    return f"multi_symbol_recent_{symbol}_{family}_{timeframe}_{side}_candidate"


def _requested_list(value: Any, default: list[str]) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [part.strip() for part in value.split(",") if part.strip()]
    return list(default)

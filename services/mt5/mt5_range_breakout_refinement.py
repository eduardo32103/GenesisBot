from __future__ import annotations

import csv
import json
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
    _monte_carlo_stress,
    _recent_edge_negative,
)
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_research_v2_candidate_robustness import (
    _atr_bucket_from_trade,
    _candidate_gate,
    _filter_hint,
    _fragile,
    _group_stats,
    _observation_quality,
    _score,
    _segment_label,
    _session_name,
    _strongest_segment,
    _trade_hour,
    _vol_bucket_from_trade,
    _weakest_segment,
    _window_stats,
)
from services.mt5.mt5_strategy_research_v2 import (
    ResearchVariant,
    _decision_for_variant,
    _features_by_index,
    _open_research_trade,
    _update_research_trade,
)


@dataclass(frozen=True)
class RefinementCase:
    name: str
    kind: str
    variant: ResearchVariant
    allowed_sessions: tuple[str, ...] | None = None
    entry_momentum_guard: bool = False
    loss_cluster_guard: bool = False
    description: str = ""


RANGE_BREAKOUT_REFINEMENT_CASES = [
    RefinementCase(
        "m30_range_breakout_both_all",
        "baseline",
        ResearchVariant("range_breakout_anti_chop", "M30", "both", "all", "normal_high", "any", 57.0, 1.15, 2, 1.0, 0.9, True),
        description="Original Research V2 all-session both-side range breakout.",
    ),
    RefinementCase(
        "m30_range_breakout_sell_all",
        "baseline",
        ResearchVariant("range_breakout_anti_chop", "M30", "sell", "all", "normal_high", "any", 57.0, 1.15, 2, 1.0, 0.9, True),
        description="Original Research V2 sell-only all-session range breakout.",
    ),
    RefinementCase(
        "range_breakout_anti_chop_m30_london_us_v1",
        "clean_variant",
        ResearchVariant("range_breakout_anti_chop", "M30", "both", "london_us", "normal_high", "any", 57.0, 1.15, 2, 1.0, 0.85, True),
        allowed_sessions=("london_us",),
        description="Prior clean London/US-only variant.",
    ),
    RefinementCase(
        "range_breakout_anti_chop_m30_sell_london_us_v2",
        "v2_variant",
        ResearchVariant("range_breakout_anti_chop", "M30", "sell", "all", "normal_high", "any", 57.0, 1.15, 2, 1.0, 0.8, True),
        allowed_sessions=("london_us",),
        description="Sell-only London/US refinement.",
    ),
    RefinementCase(
        "range_breakout_anti_chop_m30_sell_london_asia_filtered_v2",
        "v2_variant",
        ResearchVariant("range_breakout_anti_chop", "M30", "sell", "all", "normal_high", "any", 57.0, 1.15, 2, 1.0, 0.8, True),
        allowed_sessions=("asia", "london_us"),
        entry_momentum_guard=True,
        description="Sell-only Asia+London/US with stronger continuation guard.",
    ),
    RefinementCase(
        "range_breakout_anti_chop_m30_no_offsession_v2",
        "v2_variant",
        ResearchVariant("range_breakout_anti_chop", "M30", "both", "all", "normal_high", "any", 57.0, 1.15, 2, 1.0, 0.85, True),
        allowed_sessions=("asia", "london_us"),
        description="Both-side refinement that blocks off-session trades.",
    ),
    RefinementCase(
        "range_breakout_anti_chop_m30_momentum_exit_guard_v2",
        "v2_variant",
        ResearchVariant("range_breakout_anti_chop", "M30", "both", "all", "normal_high", "any", 58.0, 1.1, 1, 0.95, 0.75, True),
        allowed_sessions=("asia", "london_us"),
        entry_momentum_guard=True,
        description="Faster exit and stricter entry momentum guard.",
    ),
    RefinementCase(
        "range_breakout_anti_chop_m30_loss_cluster_guard_v2",
        "v2_variant",
        ResearchVariant("range_breakout_anti_chop", "M30", "both", "all", "normal_high", "any", 57.0, 1.15, 2, 1.0, 0.85, True),
        allowed_sessions=("asia", "london_us"),
        loss_cluster_guard=True,
        description="Blocks fresh entries after short loss clusters.",
    ),
]


def run_range_breakout_refinement(body: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    body = body or {}
    symbol = str(body.get("symbol") or "BTCUSD").upper().strip()
    csv_dir = Path(str(body.get("csv_dir") or Path("data") / "backtests"))
    max_bars = max(200, min(int(_number(body.get("max_bars")) or 20000), 35000))
    timeout_seconds = max(0.25, float(_number(body.get("per_evaluation_timeout_seconds")) or 4.0))
    spread_points = float(_number(body.get("spread_points")) or 25.0)
    requested = _requested_list(body.get("targets"), [case.name for case in RANGE_BREAKOUT_REFINEMENT_CASES])
    selected = [case for case in RANGE_BREAKOUT_REFINEMENT_CASES if case.name in requested]
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[str] = []

    csv_path = _csv_path_for(body, csv_dir, symbol, "M30")
    if not csv_path.exists():
        errors.append({"timeframe": "M30", "path": str(csv_path), "error": "csv_not_found"})
        bars: list[dict[str, Any]] = []
        settings = None
        features_by_index: dict[int, dict[str, Any]] = {}
    else:
        settings_body = {
            "symbol": symbol,
            "timeframe": "M30",
            "csv_path": str(csv_path),
            "max_bars": max_bars,
            "spread_points": spread_points,
            "source": "mt5_csv",
            "save_results": False,
            "timeout_seconds": timeout_seconds,
        }
        settings = replace(_settings(settings_body, get_mt5_config()), max_bars=max_bars, timeout_seconds=timeout_seconds)
        bars, load_warnings = _load_bars(settings_body, settings)
        warnings.extend(load_warnings)
        bars = bars[-settings.max_bars :]
        features_by_index = _features_by_index(bars) if bars else {}
        if not bars:
            errors.append({"timeframe": "M30", "path": str(csv_path), "error": "csv_bars_not_loaded"})

    if settings is not None and bars:
        for case in selected:
            rows.append(
                evaluate_range_breakout_refinement_case(
                    settings,
                    bars,
                    case,
                    source_csv=str(csv_path),
                    features_by_index=features_by_index,
                    timeout_seconds=timeout_seconds,
                )
            )

    rows.sort(key=_refinement_rank, reverse=True)
    candidates = [row for row in rows if row.get("candidate")]
    result = {
        "ok": True,
        "status": "mt5_range_breakout_refinement_completed",
        "symbol": symbol,
        "timeframe": "M30",
        "csv_path": str(csv_path),
        "targets": [case.name for case in selected],
        "evaluations": len(rows),
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


def evaluate_range_breakout_refinement_case(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    case: RefinementCase,
    *,
    source_csv: str = "",
    features_by_index: dict[int, dict[str, Any]] | None = None,
    timeout_seconds: float = 4.0,
) -> dict[str, Any]:
    started = time.monotonic()
    trades, blocked, signals, state = _simulate_refinement(
        settings,
        bars,
        case,
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
    exit_stats = _group_stats(settings, closed, lambda trade: str(trade.get("exit_reason") or "unknown"))
    monte_carlo = _monte_carlo_stress(closed, initial_balance=settings.initial_balance, max_drawdown_limit=5000.0, simulations=500)
    weakest = _weakest_segment(window_stats, session_stats, side_stats, regime_stats, volatility_stats, atr_stats, exit_stats)
    strongest = _strongest_segment(window_stats, session_stats, side_stats, regime_stats, volatility_stats, atr_stats, exit_stats)
    fragile = _fragile(window_stats, metrics)
    single_trade = _depends_on_single_trade(metrics)
    gate = _candidate_gate(metrics, window_stats, monte_carlo, fragile, single_trade)
    score = _score(metrics, monte_carlo, fragile, single_trade, gate)
    loss_clusters = _loss_cluster_stats(closed)
    exit_clusters = {
        "stop_loss": _reason_loss_clusters(closed, "stop_loss"),
        "momentum_loss_exit": _reason_loss_clusters(closed, "momentum_loss_exit"),
        "time_stop": _reason_loss_clusters(closed, "time_stop"),
        "mae_defense_exit": _reason_loss_clusters(closed, "mae_defense_exit"),
    }
    loss_cause_counts = _loss_cause_counts(closed)
    mae_mfe = _mae_mfe_stats(closed)
    return {
        "target_name": case.name,
        "target_kind": case.kind,
        "description": case.description,
        "timeframe": case.variant.timeframe,
        "family": case.variant.family,
        "variant_id": case.variant.key(),
        "source_csv": source_csv,
        "bars_loaded": len(bars),
        "bars_evaluated": max(0, len(bars) - 80),
        "first_bar_time": str(bars[0].get("time") or "") if bars else "",
        "last_bar_time": str(bars[-1].get("time") or "") if bars else "",
        "side_mode": case.variant.side_mode,
        "session_filter": ",".join(case.allowed_sessions or (case.variant.session_name,)),
        "volatility_regime": case.variant.volatility_regime,
        "trend_regime": case.variant.trend_regime,
        "score_threshold": case.variant.score_threshold,
        "risk_reward": case.variant.risk_reward,
        "time_stop_bars": case.variant.time_stop_bars,
        "mae_exit_r": case.variant.mae_exit_r,
        "momentum_loss_exit": case.variant.momentum_loss_exit,
        "entry_momentum_guard": case.entry_momentum_guard,
        "loss_cluster_guard": case.loss_cluster_guard,
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
        "buy_win_rate": metrics["buy_win_rate"],
        "sell_win_rate": metrics["sell_win_rate"],
        "buy_pf": metrics["buy_pf"],
        "sell_pf": metrics["sell_pf"],
        "tercile_stats": window_stats["terciles"],
        "half_stats": window_stats["halves"],
        "quarter_stats": window_stats["quarters"],
        "side_stats": side_stats,
        "session_stats": session_stats,
        "hour_stats": hour_stats,
        "regime_stats": regime_stats,
        "volatility_stats": volatility_stats,
        "atr_regime_stats": atr_stats,
        "exit_reason_stats": exit_stats,
        "exit_reason_counts": metrics["exit_reason_counts"],
        "loss_cause_counts": loss_cause_counts,
        "dominant_loss_cause": _dominant_loss_cause(loss_cause_counts),
        "stop_loss_cluster_count": exit_clusters["stop_loss"]["cluster_count"],
        "momentum_loss_exit_cluster_count": exit_clusters["momentum_loss_exit"]["cluster_count"],
        "time_stop_cluster_count": exit_clusters["time_stop"]["cluster_count"],
        "exit_loss_clusters": exit_clusters,
        "loss_cluster_count": loss_clusters["cluster_count"],
        "loss_cluster_max_run": loss_clusters["max_run"],
        "loss_cluster_before_after": {
            "loss_cluster_count_after_filter": loss_clusters["cluster_count"],
            "loss_cluster_max_run_after_filter": loss_clusters["max_run"],
        },
        "avg_MAE": mae_mfe["avg_MAE"],
        "avg_MFE": mae_mfe["avg_MFE"],
        "avg_MAE_R": mae_mfe["avg_MAE_R"],
        "avg_MFE_R": mae_mfe["avg_MFE_R"],
        "monte_carlo_stressed_pf": monte_carlo.get("profit_factor_stressed", 0.0),
        "monte_carlo_p95_drawdown": monte_carlo.get("max_drawdown_p95", 0.0),
        "monte_carlo_stressed_expectancy": monte_carlo.get("expectancy_stressed", 0.0),
        "monte_carlo_fail_reasons": list(monte_carlo.get("fail_reasons") or []),
        "fragile_regime_dependency": fragile,
        "single_trade_dependency": single_trade,
        "weakest_segment": weakest,
        "strongest_segment": strongest,
        "filter_hint": _filter_hint(weakest, strongest),
        "blocked_reason_counts": _reason_counts(blocked),
        "risk_governor_blocks": state.get("risk_governor_blocks", 0),
        "max_open_trades_observed": state.get("max_open_trades_observed", 0),
        "candidate": gate["passed"],
        "deserves_capital_optimizer": gate["passed"],
        "recommendation": "capital_optimizer_candidate" if gate["passed"] else "observation_only" if _observation_quality(metrics) else "reject",
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


def write_range_breakout_refinement_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "range_breakout_refinement_results.csv"
    json_path = root / "range_breakout_refinement_results.json"
    summary_path = root / "range_breakout_refinement_summary.md"
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
        "dominant_loss_cause",
        "stop_loss_cluster_count",
        "momentum_loss_exit_cluster_count",
        "time_stop_cluster_count",
        "loss_cluster_count",
        "avg_MAE_R",
        "avg_MFE_R",
        "fragile_regime_dependency",
        "single_trade_dependency",
        "weakest_segment",
        "strongest_segment",
        "candidate",
        "deserves_capital_optimizer",
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
    summary_path.write_text(range_breakout_refinement_summary_markdown(result), encoding="utf-8")
    return csv_path, json_path, summary_path


def range_breakout_refinement_summary_markdown(result: dict[str, Any]) -> str:
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else _summary(rows, [])
    lines = [
        "# MT5 Range Breakout Anti-Chop M30 Refinement Summary",
        "",
        "Exit/cluster refinement for the Research V2 M30 range breakout family. Paper/offline only; no broker, no order execution, no automatic promotion.",
        "",
        f"Evaluations: `{result.get('evaluations', len(rows))}`.",
        f"Candidates: `{len(result.get('candidates') or [])}`.",
        "",
        "## Rows",
    ]
    for row in rows:
        lines.append(
            f"- `{row.get('target_name')}` closed `{row.get('closed')}`, PF `{row.get('profit_factor')}`, "
            f"expectancy `{row.get('expectancy')}`, DD `{row.get('max_drawdown')}`, MC PF `{row.get('monte_carlo_stressed_pf')}`, "
            f"loss cause `{row.get('dominant_loss_cause')}`, recommendation `{row.get('recommendation')}`."
        )
    lines.extend(
        [
            "",
            "## Answers",
            f"1. Segment to disable: {summary.get('segment_to_disable')}",
            f"2. Best side: {summary.get('side_answer')}",
            f"3. Best session: {summary.get('session_answer')}",
            f"4. Exit causing most losses: {summary.get('exit_loss_answer')}",
            f"5. V2 variant improving Monte Carlo: {summary.get('best_v2_monte_carlo_answer')}",
            f"6. Capital preservation optimizer handoff: {summary.get('capital_optimizer_answer')}",
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


def _simulate_refinement(
    settings: BacktestSettings,
    bars: list[dict[str, Any]],
    case: RefinementCase,
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
            open_trade, closed = _update_research_trade(settings, open_trade, bar, index, case.variant)
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
        if case.loss_cluster_guard and _loss_streak(trades) >= 2:
            blocked.append("loss_cluster_guard")
            cooldown_until = max(cooldown_until, index + 4)
            continue
        risk_reason = _risk_block(settings, trades)
        if risk_reason:
            state["risk_governor_blocks"] += 1
            blocked.append(f"risk_governor_{risk_reason}")
            continue
        features = features_by_index.get(index - 1)
        if not features:
            blocked.append("insufficient_history")
            continue
        decision = _decision_for_variant(features, case.variant)
        if decision.get("generated"):
            signals["generated"] += 1
        if not decision.get("actionable"):
            blocked.append(str(decision.get("reason") or "no_signal"))
            continue
        if not _case_session_allowed(case, features):
            blocked.append("session_filter")
            continue
        if case.entry_momentum_guard and not _momentum_guard_passes(decision):
            blocked.append("momentum_entry_guard")
            continue
        signals["actionable"] += 1
        open_trade = _open_research_trade(settings, decision, bars[index], index, case.variant)
        if open_trade is None:
            blocked.append("missing_risk_parameters")
            continue
        open_trade = {
            **open_trade,
            "shadow_trade_id": f"range-refine-{case.name}-{index}",
            "filter_profile": case.name,
            "strategy_profile": case.name,
            "refinement_case": case.name,
            "source": "mt5_range_breakout_refinement",
            **_safety(),
        }
        features_snapshot = open_trade.get("features_snapshot") if isinstance(open_trade.get("features_snapshot"), dict) else {}
        open_trade["features_snapshot"] = {
            **features_snapshot,
            "refinement_case": case.name,
            "custom_allowed_sessions": list(case.allowed_sessions or (case.variant.session_name,)),
            "entry_momentum_guard": case.entry_momentum_guard,
            "loss_cluster_guard": case.loss_cluster_guard,
        }
        state["max_open_trades_observed"] = max(state["max_open_trades_observed"], 1)
    if open_trade:
        trades.append(_close(settings, open_trade, float(_number(bars[-1].get("close")) or open_trade.get("entry_price") or 0.0), "time_stop", bars[-1]))
    return trades, blocked, signals, state


def _risk_block(settings: BacktestSettings, trades: list[dict[str, Any]]) -> str:
    if settings.spread_points > 30:
        return "spread_too_high"
    if _loss_streak(trades) >= 4:
        return "consecutive_loss_lockdown"
    if len([trade for trade in trades if trade.get("lifecycle_status") == "closed"]) >= 20 and _recent_edge_negative(trades):
        return "recent_edge_negative"
    if _drawdown_accelerating(trades, settings.initial_balance):
        return "drawdown_accelerating"
    return ""


def _case_session_allowed(case: RefinementCase, features: dict[str, Any]) -> bool:
    if not case.allowed_sessions:
        return True
    hour = features.get("hour")
    try:
        session = _session_name(int(hour))
    except Exception:
        session = "off_session"
    return session in set(case.allowed_sessions)


def _momentum_guard_passes(decision: dict[str, Any]) -> bool:
    side = str(decision.get("side") or "").lower()
    momentum = float(_number(decision.get("momentum_score")) or 50.0)
    if side == "buy":
        return momentum >= 58.0
    if side == "sell":
        return momentum <= 42.0
    return False


def _loss_cluster_stats(trades: list[dict[str, Any]]) -> dict[str, int]:
    cluster_count = 0
    current = 0
    max_run = 0
    for trade in trades:
        if trade.get("status") == "loss":
            current += 1
            max_run = max(max_run, current)
            if current == 2:
                cluster_count += 1
        else:
            current = 0
    return {"cluster_count": cluster_count, "max_run": max_run}


def _reason_loss_clusters(trades: list[dict[str, Any]], reason: str) -> dict[str, int]:
    cluster_count = 0
    current = 0
    max_run = 0
    for trade in trades:
        if trade.get("status") == "loss" and str(trade.get("exit_reason") or "") == reason:
            current += 1
            max_run = max(max_run, current)
            if current == 2:
                cluster_count += 1
        else:
            current = 0
    return {"cluster_count": cluster_count, "max_run": max_run}


def _loss_cause_counts(trades: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for trade in trades:
        if trade.get("status") != "loss":
            continue
        reason = str(trade.get("exit_reason") or "unknown")
        counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: item[1], reverse=True))


def _dominant_loss_cause(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    reason, count = next(iter(counts.items()))
    return f"{reason}:{count}"


def _mae_mfe_stats(trades: list[dict[str, Any]]) -> dict[str, float]:
    if not trades:
        return {"avg_MAE": 0.0, "avg_MFE": 0.0, "avg_MAE_R": 0.0, "avg_MFE_R": 0.0}
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
    return {
        "avg_MAE": round(sum(mae_values) / len(mae_values), 6),
        "avg_MFE": round(sum(mfe_values) / len(mfe_values), 6),
        "avg_MAE_R": round(sum(mae_r_values) / len(mae_r_values), 4),
        "avg_MFE_R": round(sum(mfe_r_values) / len(mfe_r_values), 4),
    }


def _summary(rows: list[dict[str, Any]], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"recommendation": "no_data", **_safety()}
    baseline = next((row for row in rows if row.get("target_name") == "m30_range_breakout_both_all"), rows[0])
    best = max(rows, key=_refinement_rank)
    best_v2 = max([row for row in rows if row.get("target_kind") == "v2_variant"] or rows, key=lambda row: float(row.get("monte_carlo_stressed_pf") or 0.0))
    segment_to_disable = _weak_session_answer(baseline)
    side_answer = _best_segment_from_stats(baseline.get("side_stats"), "side")
    session_answer = _best_segment_from_stats(baseline.get("session_stats"), "session")
    exit_loss_answer = str(baseline.get("dominant_loss_cause") or "none")
    candidate_answer = (
        "; ".join(str(row.get("target_name")) for row in candidates[:3])
        if candidates
        else "none; keep observation_only and send no profile to promotion"
    )
    return {
        "recommendation": "capital_optimizer_candidate" if candidates else "observation_only",
        "segment_to_disable": segment_to_disable,
        "side_answer": side_answer,
        "session_answer": session_answer,
        "exit_loss_answer": exit_loss_answer,
        "best_v2_monte_carlo_answer": (
            f"{best_v2.get('target_name')} closed {best_v2.get('closed')} PF {best_v2.get('profit_factor')} "
            f"MC PF {best_v2.get('monte_carlo_stressed_pf')}"
        ),
        "capital_optimizer_answer": candidate_answer,
        "best_variant_answer": f"{best.get('target_name')} PF {best.get('profit_factor')} expectancy {best.get('expectancy')} closed {best.get('closed')}",
        "automatic_promotion": False,
        **_safety(),
    }


def _weak_session_answer(row: dict[str, Any]) -> str:
    stats = row.get("session_stats") if isinstance(row.get("session_stats"), dict) else {}
    weak = None
    for name, metrics in stats.items():
        if int(metrics.get("closed") or 0) < 3:
            continue
        rank = (float(metrics.get("expectancy") or 0.0), float(metrics.get("profit_factor") or 0.0))
        if weak is None or rank < weak[0]:
            weak = (rank, name, metrics)
    if not weak:
        return "insufficient_session_sample"
    return f"disable_or_retest_{weak[1]} closed {weak[2].get('closed')} PF {weak[2].get('profit_factor')} expectancy {weak[2].get('expectancy')}"


def _best_segment_from_stats(stats: Any, label: str) -> str:
    if not isinstance(stats, dict) or not stats:
        return f"no_{label}_sample"
    ranked = []
    for name, metrics in stats.items():
        closed = int(metrics.get("closed") or 0)
        if closed <= 0:
            continue
        ranked.append((float(metrics.get("expectancy") or 0.0), float(metrics.get("profit_factor") or 0.0), closed, name))
    if not ranked:
        return f"no_{label}_sample"
    ranked.sort(reverse=True)
    exp, pf, closed, name = ranked[0]
    return f"{name} closed {closed} PF {pf} expectancy {exp}"


def _refinement_rank(row: dict[str, Any]) -> float:
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

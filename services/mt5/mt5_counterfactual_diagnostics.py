from __future__ import annotations

import csv
import json
import time
from collections import Counter
from dataclasses import replace
from pathlib import Path
from typing import Any

from services.mt5.mt5_backtester import _load_bars, _metrics, _number, _safety, _settings
from services.mt5.mt5_capital_preservation_optimizer import (
    _PROFILE_PARAMS,
    _config,
    _monte_carlo_stress,
    _settings_for_capital_config,
    _simulate_capital_preservation,
)
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_trade_lifecycle_diagnostics import PRIORITY_MATRIX


def run_counterfactual_diagnostics(body: dict[str, Any] | None = None) -> dict[str, Any]:
    body = body or {}
    started = time.monotonic()
    symbol = str(body.get("symbol") or "BTCUSD").upper().strip()
    csv_dir = Path(str(body.get("csv_dir") or Path("data") / "backtests"))
    pairs = _requested_pairs(body)
    max_bars = max(100, min(int(_number(body.get("max_bars")) or 20000), 25000))
    spread_points = float(_number(body.get("spread_points")) or 25.0)
    timeout_seconds = float(_number(body.get("timeout_seconds")) or 90.0)
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[str] = []

    for timeframe, profile in pairs:
        if time.monotonic() - started > timeout_seconds:
            errors.append({"timeframe": timeframe, "profile": profile, "error": "counterfactual_timeout_guard"})
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
        row = diagnose_counterfactual_profile(
            bars,
            settings,
            profile,
            source_csv=str(csv_path),
            started=time.monotonic(),
            timeout_seconds=max(1.0, min(timeout_seconds, 30.0)),
        )
        rows.append(row)

    rows.sort(key=lambda item: _counterfactual_rank(item), reverse=True)
    return {
        "ok": True,
        "status": "mt5_counterfactual_diagnostics_completed",
        "symbol": symbol,
        "pairs": [{"timeframe": timeframe, "profile": profile} for timeframe, profile in pairs],
        "results": rows,
        "summary": _counterfactual_summary(rows),
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
        "automatic_promotion": False,
        "duration_ms": int((time.monotonic() - started) * 1000),
        **_safety(),
    }


def diagnose_counterfactual_profile(
    bars: list[dict[str, Any]],
    base_settings: Any,
    profile: str,
    *,
    source_csv: str = "",
    started: float | None = None,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    started = started or time.monotonic()
    baseline_config = _default_config(profile)
    baseline = _run_scenario(
        "baseline",
        bars,
        base_settings,
        baseline_config,
        profile,
        timeout_seconds=timeout_seconds,
        overrides={},
    )
    cooldown_config = replace(
        baseline_config,
        cooldown_after_loss_bars=max(1, baseline_config.cooldown_after_loss_bars - 1),
    )
    cooldown = _run_scenario(
        "cooldown_one_bar_shorter",
        bars,
        base_settings,
        cooldown_config,
        profile,
        timeout_seconds=timeout_seconds,
        overrides={},
    )
    extra_confirm_config = replace(
        baseline_config,
        block_after_consecutive_losses=baseline_config.block_after_consecutive_losses + 1,
    )
    consecutive = _run_scenario(
        "consecutive_losses_extra_confirmation",
        bars,
        base_settings,
        extra_confirm_config,
        profile,
        timeout_seconds=timeout_seconds,
        overrides={},
    )
    session_probe_config = replace(baseline_config, session_filter=False)
    session_probe = _run_scenario(
        "session_filter_off_probe",
        bars,
        base_settings,
        session_probe_config,
        profile,
        timeout_seconds=timeout_seconds,
        overrides={"session_filter": False},
    )
    selected_hours = _positive_extra_hours(baseline, session_probe)
    session_expanded = _run_scenario(
        "session_expand_positive_hours",
        bars,
        base_settings,
        baseline_config,
        profile,
        timeout_seconds=timeout_seconds,
        overrides={"session_hours": selected_hours["allowed_hours"]} if selected_hours["extra_hours"] else {},
    )
    side_scenarios = _side_scenarios(bars, base_settings, baseline_config, profile, timeout_seconds)
    scenarios = [baseline, cooldown, consecutive, session_probe, session_expanded, *side_scenarios]
    comparisons = [_compare_scenario(baseline, scenario) for scenario in scenarios if scenario["scenario"] != "baseline"]
    permitted_comparisons = [item for item in comparisons if item.get("scenario") != "session_filter_off_probe"]
    best_counterfactual = max(permitted_comparisons, key=lambda item: _comparison_score(item), default={})
    return {
        "timeframe": base_settings.timeframe,
        "profile": profile,
        "source_csv": source_csv,
        "baseline_closed": baseline["closed"],
        "counterfactual_closed": best_counterfactual.get("counterfactual_closed", baseline["closed"]),
        "added_trades": best_counterfactual.get("added_trades", 0),
        "added_wins": best_counterfactual.get("added_wins", 0),
        "added_losses": best_counterfactual.get("added_losses", 0),
        "added_pf": best_counterfactual.get("added_pf", 0.0),
        "added_expectancy": best_counterfactual.get("added_expectancy", 0.0),
        "added_drawdown": best_counterfactual.get("added_drawdown", 0.0),
        "baseline_drawdown": baseline["max_drawdown"],
        "counterfactual_drawdown": best_counterfactual.get("counterfactual_drawdown", baseline["max_drawdown"]),
        "blocked_trades_that_would_win": best_counterfactual.get("blocked_trades_that_would_win", 0),
        "blocked_trades_that_would_lose": best_counterfactual.get("blocked_trades_that_would_lose", 0),
        "net_value_of_block": best_counterfactual.get("net_value_of_block", 0.0),
        "risk_saved_by_block": best_counterfactual.get("risk_saved_by_block", 0.0),
        "opportunity_lost_by_block": best_counterfactual.get("opportunity_lost_by_block", 0.0),
        "whether_block_is_protective": best_counterfactual.get("whether_block_is_protective", True),
        "best_counterfactual": best_counterfactual.get("scenario", ""),
        "baseline": baseline,
        "counterfactuals": scenarios,
        "comparisons": comparisons,
        "session_window_stats": selected_hours,
        "side_stats": baseline.get("side_stats", {}),
        "recommended_side_mode": _recommended_side_mode(baseline),
        "variant_warranted": _variant_warranted(best_counterfactual),
        "suggested_variant": _suggested_variant(profile, best_counterfactual),
        "do_not_change": _do_not_change(comparisons),
        "timed_out": any(item.get("timed_out") for item in scenarios),
        "live_runtime_mutated": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "shadow_trades_mutated": False,
        "automatic_promotion": False,
        **_safety(),
    }


def write_counterfactual_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "counterfactual_diagnostics.csv"
    json_path = root / "counterfactual_diagnostics.json"
    summary_path = root / "counterfactual_summary.md"
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    headers = [
        "timeframe",
        "profile",
        "baseline_closed",
        "counterfactual_closed",
        "added_trades",
        "added_wins",
        "added_losses",
        "added_pf",
        "added_expectancy",
        "added_drawdown",
        "baseline_drawdown",
        "counterfactual_drawdown",
        "blocked_trades_that_would_win",
        "blocked_trades_that_would_lose",
        "net_value_of_block",
        "risk_saved_by_block",
        "opportunity_lost_by_block",
        "whether_block_is_protective",
        "best_counterfactual",
        "recommended_side_mode",
        "variant_warranted",
        "suggested_variant",
        "do_not_change",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({**row, "do_not_change": ";".join(str(item) for item in row.get("do_not_change") or [])})
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")
    summary_path.write_text(counterfactual_summary_markdown(result), encoding="utf-8")
    return csv_path, json_path, summary_path


def counterfactual_summary_markdown(result: dict[str, Any]) -> str:
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    summary = _counterfactual_summary(rows)
    lines = [
        "# MT5 Counterfactual Diagnostics",
        "",
        "Paper-only counterfactual analysis. No broker touched, no orders, no live state mutation.",
        "",
        f"Consecutive-loss verdict: `{summary.get('consecutive_loss_verdict')}`.",
        f"Session verdict: `{summary.get('session_verdict')}`.",
        f"Side verdict: `{summary.get('side_verdict')}`.",
        f"Variant recommendation: `{summary.get('variant_recommendation')}`.",
        "",
        "## Profiles",
    ]
    if not rows:
        lines.append("- No counterfactual rows were generated.")
    for row in rows:
        lines.append(
            f"- `{row.get('timeframe')} {row.get('profile')}` best `{row.get('best_counterfactual')}`: "
            f"baseline closed `{row.get('baseline_closed')}`, counterfactual closed `{row.get('counterfactual_closed')}`, "
            f"added `{row.get('added_trades')}`, PF `{row.get('added_pf')}`, expectancy `{row.get('added_expectancy')}`, "
            f"DD delta `{row.get('added_drawdown')}`, protective `{row.get('whether_block_is_protective')}`, "
            f"variant `{row.get('suggested_variant') or 'none'}`."
        )
    lines.extend(
        [
            "",
            "## Answers",
            f"1. Consecutive losses: {summary.get('consecutive_loss_answer')}",
            f"2. Extra session window: {summary.get('session_answer')}",
            f"3. Weak side: {summary.get('side_answer')}",
            f"4. Profile deserving v6: {summary.get('variant_recommendation')}",
            f"5. Do not change: {summary.get('do_not_change')}",
            "6. No automatic promotion. All results remain observation_only/paper-offline.",
            "",
            "## Safety",
            "- No real trading.",
            "- No order_send.",
            "- No broker credentials.",
            "- No martingale, no grid, no averaging down, no size increase after loss.",
            "- RiskGovernor live behavior is unchanged.",
            "- broker_touched=false",
            "- order_executed=false",
            "- order_policy=journal_only_no_broker",
        ]
    )
    return "\n".join(lines) + "\n"


def _run_scenario(
    name: str,
    bars: list[dict[str, Any]],
    base_settings: Any,
    config: Any,
    profile: str,
    *,
    timeout_seconds: float,
    overrides: dict[str, Any],
) -> dict[str, Any]:
    settings = _settings_for_capital_config(base_settings, config)
    params = dict(settings.filter_params or {})
    params.update(overrides)
    settings = replace(settings, filter_params=params, timeout_seconds=timeout_seconds)
    started = time.monotonic()
    trades, no_trade_count, blocked, sim_state = _simulate_capital_preservation(settings, bars, config, started)
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    metrics = _metrics(closed, initial_balance=settings.initial_balance)
    monte_carlo = _monte_carlo_stress(closed, initial_balance=settings.initial_balance, max_drawdown_limit=5000.0, simulations=250)
    return {
        "scenario": name,
        "profile": profile,
        "parameters": {
            "cooldown_after_loss_bars": config.cooldown_after_loss_bars,
            "block_after_consecutive_losses": config.block_after_consecutive_losses,
            "session_filter": bool(config.session_filter or params.get("session_filter")),
            "session_hours": params.get("session_hours", []),
            "allowed_sides": params.get("allowed_sides", []),
        },
        "trades": closed,
        "closed": metrics["closed"],
        "wins": metrics["wins"],
        "losses": metrics["losses"],
        "win_rate": metrics["win_rate"],
        "profit_factor": metrics["profit_factor"],
        "expectancy": metrics["expectancy"],
        "net_pnl": metrics["net_pnl"],
        "max_drawdown": metrics["max_drawdown"],
        "side_stats": metrics.get("side_stats", {}),
        "hour_stats": metrics.get("hour_stats", {}),
        "exit_reason_counts": metrics.get("exit_reason_counts", {}),
        "no_trade_count": no_trade_count,
        "blocked_reason_counts": dict(Counter(blocked)),
        "monte_carlo": monte_carlo,
        "timed_out": "timeout_guard" in blocked,
        "sim_state": sim_state,
        **_safety(),
    }


def _compare_scenario(baseline: dict[str, Any], scenario: dict[str, Any]) -> dict[str, Any]:
    added_trades = int(scenario.get("closed") or 0) - int(baseline.get("closed") or 0)
    added_wins = int(scenario.get("wins") or 0) - int(baseline.get("wins") or 0)
    added_losses = int(scenario.get("losses") or 0) - int(baseline.get("losses") or 0)
    net_delta = float(_number(scenario.get("net_pnl")) or 0.0) - float(_number(baseline.get("net_pnl")) or 0.0)
    dd_delta = float(_number(scenario.get("max_drawdown")) or 0.0) - float(_number(baseline.get("max_drawdown")) or 0.0)
    pf = float(_number(scenario.get("profit_factor")) or 0.0)
    expectancy = float(_number(scenario.get("expectancy")) or 0.0)
    protective = net_delta <= 0 or dd_delta > 1000 or pf < float(_number(baseline.get("profit_factor")) or 0.0) * 0.85
    return {
        "scenario": scenario.get("scenario"),
        "baseline_closed": baseline.get("closed", 0),
        "counterfactual_closed": scenario.get("closed", 0),
        "added_trades": added_trades,
        "added_wins": max(0, added_wins),
        "added_losses": max(0, added_losses),
        "added_pf": pf,
        "added_expectancy": expectancy,
        "added_drawdown": round(dd_delta, 6),
        "baseline_drawdown": baseline.get("max_drawdown", 0.0),
        "counterfactual_drawdown": scenario.get("max_drawdown", 0.0),
        "blocked_trades_that_would_win": max(0, added_wins),
        "blocked_trades_that_would_lose": max(0, added_losses),
        "net_value_of_block": round(-net_delta, 6),
        "risk_saved_by_block": round(max(0.0, dd_delta), 6),
        "opportunity_lost_by_block": round(max(0.0, net_delta), 6),
        "whether_block_is_protective": protective,
        "monte_carlo_fail_reasons": scenario.get("monte_carlo", {}).get("fail_reasons", []),
        **_safety(),
    }


def _positive_extra_hours(baseline: dict[str, Any], session_probe: dict[str, Any]) -> dict[str, Any]:
    baseline_hours = set(int(hour) for hour in baseline.get("parameters", {}).get("session_hours", []) if _number(hour) is not None)
    stats = session_probe.get("hour_stats") if isinstance(session_probe.get("hour_stats"), dict) else {}
    candidates: list[dict[str, Any]] = []
    for hour_text, metrics in stats.items():
        hour = int(_number(hour_text) or 0)
        if hour in baseline_hours:
            continue
        closed = int(metrics.get("closed") or metrics.get("trades") or 0)
        pf = float(metrics.get("profit_factor") or 0.0)
        expectancy = float(metrics.get("expectancy") or 0.0)
        drawdown = float(metrics.get("max_drawdown") or 0.0)
        if closed >= 1 and expectancy > 0 and pf >= 1.2:
            candidates.append({"hour": hour, "closed": closed, "profit_factor": pf, "expectancy": expectancy, "max_drawdown": drawdown})
    candidates.sort(key=lambda item: (item["expectancy"], item["profit_factor"], -item["max_drawdown"]), reverse=True)
    extra = [item["hour"] for item in candidates[:2]]
    return {
        "baseline_hours": sorted(baseline_hours),
        "candidate_extra_hours": candidates,
        "extra_hours": extra,
        "allowed_hours": sorted(baseline_hours | set(extra)),
    }


def _side_scenarios(bars: list[dict[str, Any]], base_settings: Any, baseline_config: Any, profile: str, timeout_seconds: float) -> list[dict[str, Any]]:
    params = _PROFILE_PARAMS.get(profile, {})
    allowed = params.get("allowed_sides")
    if isinstance(allowed, list) and set(str(item).lower() for item in allowed) == {"buy"}:
        return []
    return [
        _run_scenario("side_buy_only", bars, base_settings, baseline_config, profile, timeout_seconds=timeout_seconds, overrides={"allowed_sides": ["buy"]}),
        _run_scenario("side_sell_only", bars, base_settings, baseline_config, profile, timeout_seconds=timeout_seconds, overrides={"allowed_sides": ["sell"]}),
    ]


def _default_config(profile: str) -> Any:
    params = _PROFILE_PARAMS[profile]
    return _config(
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


def _counterfactual_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "consecutive_loss_verdict": "no_data",
            "session_verdict": "no_data",
            "side_verdict": "no_data",
            "variant_recommendation": "none",
            **_safety(),
        }
    consecutive = _scenario_rows(rows, "consecutive_losses_extra_confirmation")
    session = _scenario_rows(rows, "session_expand_positive_hours")
    protective_count = sum(1 for item in consecutive if item.get("whether_block_is_protective"))
    useful_session = [item for item in session if item.get("added_trades", 0) > 0 and not item.get("whether_block_is_protective")]
    variant_rows = [row for row in rows if row.get("variant_warranted")]
    return {
        "consecutive_loss_verdict": "protective" if protective_count >= max(1, len(consecutive) // 2) else "possibly_overblocking",
        "session_verdict": "expand_positive_hours_in_offline_variant" if useful_session else "session_filter_protective_or_insufficient",
        "side_verdict": _side_answer(rows),
        "variant_recommendation": ", ".join(row.get("suggested_variant") for row in variant_rows if row.get("suggested_variant")) or "none",
        "consecutive_loss_answer": _consecutive_answer(consecutive),
        "session_answer": _session_answer(rows),
        "side_answer": _side_answer(rows),
        "do_not_change": _global_do_not_change(rows),
        "automatic_promotion": False,
        **_safety(),
    }


def _scenario_rows(rows: list[dict[str, Any]], scenario: str) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for row in rows:
        for comparison in row.get("comparisons") or []:
            if comparison.get("scenario") == scenario:
                matches.append(comparison)
    return matches


def _consecutive_answer(items: list[dict[str, Any]]) -> str:
    if not items:
        return "no counterfactual data"
    useful = [item for item in items if item.get("added_trades", 0) > 0 and not item.get("whether_block_is_protective")]
    if useful:
        labels = [str(item.get("scenario")) for item in useful]
        return f"potentially blocks some edge in {len(useful)} cases; keep offline-only and validate: {', '.join(labels)}"
    return "protective overall; do not relax live consecutive-loss defense"


def _session_answer(rows: list[dict[str, Any]]) -> str:
    hours = Counter()
    for row in rows:
        stats = row.get("session_window_stats") if isinstance(row.get("session_window_stats"), dict) else {}
        for hour in stats.get("extra_hours") or []:
            hours[str(hour)] += 1
    if not hours:
        return "no extra hour passed positive expectancy/PF filter"
    top = ", ".join(hour for hour, _count in hours.most_common(6))
    return f"candidate extra UTC hours: {top}; validate only in offline variant"


def _side_answer(rows: list[dict[str, Any]]) -> str:
    buy_better = 0
    sell_better = 0
    for row in rows:
        side = row.get("recommended_side_mode")
        if side == "buy_only":
            buy_better += 1
        elif side == "sell_only":
            sell_better += 1
    if buy_better >= sell_better:
        return "keep buy-only bias for these profiles"
    return "sell side deserves separate offline test, but not live"


def _recommended_side_mode(baseline: dict[str, Any]) -> str:
    side_stats = baseline.get("side_stats") if isinstance(baseline.get("side_stats"), dict) else {}
    buy_pf = float((side_stats.get("buy") or {}).get("profit_factor") or 0.0)
    sell_pf = float((side_stats.get("sell") or {}).get("profit_factor") or 0.0)
    if buy_pf >= sell_pf:
        return "buy_only"
    return "sell_only"


def _variant_warranted(best: dict[str, Any]) -> bool:
    if not best:
        return False
    return (
        int(best.get("added_trades") or 0) >= 3
        and float(best.get("added_pf") or 0.0) >= 1.2
        and float(best.get("added_expectancy") or 0.0) > 0
        and not bool(best.get("whether_block_is_protective"))
    )


def _suggested_variant(profile: str, best: dict[str, Any]) -> str:
    if not _variant_warranted(best):
        return ""
    if profile.startswith("low_drawdown"):
        return "low_drawdown_v6_counterfactual_safe"
    if profile.startswith("capital_preservation"):
        return "capital_preservation_v5_counterfactual_safe"
    if profile.startswith("trend_continuation"):
        return "trend_continuation_v6_counterfactual_safe"
    return f"{profile}_counterfactual_safe"


def _do_not_change(comparisons: list[dict[str, Any]]) -> list[str]:
    blocked: list[str] = []
    for item in comparisons:
        if item.get("whether_block_is_protective"):
            blocked.append(str(item.get("scenario")))
    return list(dict.fromkeys(blocked))


def _global_do_not_change(rows: list[dict[str, Any]]) -> str:
    has_session_variant = any(row.get("suggested_variant") for row in rows)
    if has_session_variant:
        return (
            "do not relax live RiskGovernor, cooldown, or consecutive-loss guards; "
            "do not disable session filter globally; only test positive-hour session expansion as offline/paper variants"
        )
    return "do not change live RiskGovernor, cooldown, consecutive-loss, or session guards"


def _comparison_score(item: dict[str, Any]) -> float:
    return (
        float(item.get("added_trades") or 0.0) * 3.0
        + max(0.0, float(item.get("added_expectancy") or 0.0)) * 100.0
        + max(0.0, float(item.get("added_pf") or 0.0) - 1.0) * 20.0
        - max(0.0, float(item.get("added_drawdown") or 0.0)) / 100.0
        - (50.0 if item.get("whether_block_is_protective") else 0.0)
    )


def _counterfactual_rank(row: dict[str, Any]) -> float:
    comparisons = row.get("comparisons") if isinstance(row.get("comparisons"), list) else []
    permitted = [item for item in comparisons if item.get("scenario") != "session_filter_off_probe"]
    best = max(permitted, key=_comparison_score, default={})
    baseline = row.get("baseline") if isinstance(row.get("baseline"), dict) else {}
    return _comparison_score(best) + float(baseline.get("profit_factor") or 0.0)


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

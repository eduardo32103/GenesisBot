from __future__ import annotations

import csv
import json
import math
import time
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


H1_MATURATION_PROFILES = [
    "low_drawdown_v5_session_filtered",
    "capital_preservation_v4_side_filtered",
    "low_drawdown_v6_counterfactual_safe",
    "capital_preservation_v5_counterfactual_safe",
]


def run_h1_candidate_maturation(body: dict[str, Any] | None = None) -> dict[str, Any]:
    body = body or {}
    started = time.monotonic()
    symbol = str(body.get("symbol") or "BTCUSD").upper().strip()
    csv_dir = Path(str(body.get("csv_dir") or Path("data") / "backtests"))
    profiles = [profile for profile in _requested_list(body.get("profiles"), H1_MATURATION_PROFILES) if profile in _PROFILE_PARAMS]
    max_bars = max(100, min(int(_number(body.get("max_bars")) or 30000), 35000))
    spread_points = float(_number(body.get("spread_points")) or 25.0)
    timeout_seconds = float(_number(body.get("timeout_seconds")) or 90.0)
    csv_path = _h1_csv_path(body, csv_dir, symbol)
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[str] = []

    base_body = {
        "symbol": symbol,
        "timeframe": "H1",
        "csv_path": str(csv_path),
        "max_bars": max_bars,
        "spread_points": spread_points,
        "save_results": False,
        "source": "mt5_csv",
        "timeout_seconds": max(1.0, min(timeout_seconds, 30.0)),
    }
    base_settings = _settings(base_body, get_mt5_config())
    bars, load_warnings = _load_bars(base_body, base_settings)
    warnings.extend(load_warnings)
    bars = bars[-base_settings.max_bars :]
    if not bars:
        errors.append({"timeframe": "H1", "path": str(csv_path), "error": "csv_bars_not_loaded"})
    else:
        for profile in profiles:
            if time.monotonic() - started > timeout_seconds:
                errors.append({"timeframe": "H1", "profile": profile, "error": "h1_maturation_timeout_guard"})
                break
            rows.append(
                evaluate_h1_profile_maturation(
                    bars,
                    base_settings,
                    profile,
                    source_csv=str(csv_path),
                    timeout_seconds=max(1.0, min(timeout_seconds, 30.0)),
                )
            )

    rows.sort(key=lambda row: _maturation_rank(row), reverse=True)
    return {
        "ok": True,
        "status": "mt5_h1_candidate_maturation_completed",
        "symbol": symbol,
        "timeframe": "H1",
        "profiles": profiles,
        "source_csv": str(csv_path),
        "results": rows,
        "summary": _summary(rows, csv_path),
        "errors": errors,
        "warnings": warnings,
        "live_runtime_mutated": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "shadow_trades_mutated": False,
        "automatic_promotion": False,
        "martingale_enabled": False,
        "grid_enabled": False,
        "averaging_down_enabled": False,
        "increase_size_after_loss_enabled": False,
        "duration_ms": int((time.monotonic() - started) * 1000),
        **_safety(),
    }


def evaluate_h1_profile_maturation(
    bars: list[dict[str, Any]],
    base_settings: Any,
    profile: str,
    *,
    source_csv: str = "",
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    config = _default_config(profile)
    settings = replace(_settings_for_capital_config(base_settings, config), timeout_seconds=timeout_seconds)
    started = time.monotonic()
    trades, no_trade_count, blocked, sim_state = _simulate_capital_preservation(settings, bars, config, started)
    closed = [trade for trade in trades if trade.get("lifecycle_status") == "closed"]
    metrics = _metrics(closed, initial_balance=settings.initial_balance)
    monte_carlo = _monte_carlo_stress(closed, initial_balance=settings.initial_balance, max_drawdown_limit=5000.0, simulations=500)
    terciles = _tercile_stats(settings, bars, closed)
    sample_required = 50
    sample_required_strict = 75
    trade_frequency = round((metrics["closed"] / max(1, len(bars))) * 1000.0, 4)
    row = {
        "timeframe": "H1",
        "profile": profile,
        "source_csv": source_csv,
        "bars_loaded": len(bars),
        "closed_actual": metrics["closed"],
        "sample_gate_required": sample_required,
        "sample_gate_required_strict": sample_required_strict,
        "missing_to_50": max(0, 50 - metrics["closed"]),
        "missing_to_75": max(0, 75 - metrics["closed"]),
        "trade_frequency_per_1000_bars": trade_frequency,
        "estimated_bars_for_50_trades": _estimate_bars_needed(50, trade_frequency),
        "estimated_bars_for_75_trades": _estimate_bars_needed(75, trade_frequency),
        "wins": metrics["wins"],
        "losses": metrics["losses"],
        "win_rate": metrics["win_rate"],
        "profit_factor": metrics["profit_factor"],
        "expectancy": metrics["expectancy"],
        "max_drawdown": metrics["max_drawdown"],
        "net_pnl": metrics["net_pnl"],
        "exit_reason_counts": metrics.get("exit_reason_counts", {}),
        "side_stats": metrics.get("side_stats", {}),
        "tercile_stats": terciles,
        "tercile_pf": {key: value["profit_factor"] for key, value in terciles.items()},
        "tercile_expectancy": {key: value["expectancy"] for key, value in terciles.items()},
        "tercile_drawdown": {key: value["max_drawdown"] for key, value in terciles.items()},
        "fragile_regime_dependency": _fragile_regime_dependency(metrics, terciles),
        "monte_carlo_stressed_pf": monte_carlo.get("profit_factor_stressed", 0.0),
        "monte_carlo_stressed_expectancy": monte_carlo.get("expectancy_stressed", 0.0),
        "monte_carlo_p95_drawdown": monte_carlo.get("max_drawdown_p95", 0.0),
        "monte_carlo_fail_reasons": list(monte_carlo.get("fail_reasons") or []),
        "monte_carlo_passed": bool(monte_carlo.get("passed")),
        "sample_maturation_status": _maturation_status(metrics, terciles, monte_carlo),
        "candidate": _candidate(metrics, terciles, monte_carlo),
        "recommended_next_step": _recommended_next_step(metrics, terciles, monte_carlo),
        "no_trade_count": no_trade_count,
        "blocked_reason_counts": _counts(blocked),
        "risk_governor_blocks": sim_state.get("risk_governor_blocks", 0),
        "live_runtime_mutated": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "shadow_trades_mutated": False,
        "automatic_promotion": False,
        **_safety(),
    }
    return row


def write_h1_candidate_maturation_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "h1_candidate_maturation_results.csv"
    json_path = root / "h1_candidate_maturation_results.json"
    summary_path = root / "h1_candidate_maturation_summary.md"
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    headers = [
        "timeframe",
        "profile",
        "bars_loaded",
        "closed_actual",
        "sample_gate_required",
        "sample_gate_required_strict",
        "missing_to_50",
        "missing_to_75",
        "trade_frequency_per_1000_bars",
        "estimated_bars_for_50_trades",
        "estimated_bars_for_75_trades",
        "win_rate",
        "profit_factor",
        "expectancy",
        "max_drawdown",
        "monte_carlo_stressed_pf",
        "monte_carlo_stressed_expectancy",
        "monte_carlo_p95_drawdown",
        "monte_carlo_fail_reasons",
        "fragile_regime_dependency",
        "sample_maturation_status",
        "candidate",
        "recommended_next_step",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    **row,
                    "monte_carlo_fail_reasons": ";".join(str(item) for item in row.get("monte_carlo_fail_reasons") or []),
                }
            )
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")
    summary_path.write_text(h1_candidate_maturation_summary_markdown(result), encoding="utf-8")
    return csv_path, json_path, summary_path


def h1_candidate_maturation_summary_markdown(result: dict[str, Any]) -> str:
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else _summary(rows, Path(result.get("source_csv") or ""))
    lines = [
        "# MT5 H1 Candidate Maturation Summary",
        "",
        "Paper-only H1 maturation diagnostics. No broker touched, no orders, no live state mutation.",
        "",
        f"Best profile: `{summary.get('best_profile') or 'none'}`.",
        f"Recommendation: `{summary.get('recommendation')}`.",
        "",
        "## Profiles",
    ]
    if not rows:
        lines.append("- No H1 rows were generated.")
    for row in rows:
        lines.append(
            f"- `{row.get('profile')}` closed `{row.get('closed_actual')}`, PF `{row.get('profit_factor')}`, "
            f"expectancy `{row.get('expectancy')}`, DD `{row.get('max_drawdown')}`, "
            f"freq/1000 `{row.get('trade_frequency_per_1000_bars')}`, "
            f"bars for 50/75 `{row.get('estimated_bars_for_50_trades')}`/`{row.get('estimated_bars_for_75_trades')}`, "
            f"MC stressed PF `{row.get('monte_carlo_stressed_pf')}`, p95 DD `{row.get('monte_carlo_p95_drawdown')}`, "
            f"status `{row.get('sample_maturation_status')}`."
        )
    lines.extend(
        [
            "",
            "## Answers",
            f"1. Edge real or pretty sample: {summary.get('edge_answer')}",
            f"2. Bars needed for 50/75 trades: {summary.get('bars_needed_answer')}",
            f"3. Most stable H1 profile: {summary.get('stable_profile_answer')}",
            f"4. Monte Carlo failures: {summary.get('monte_carlo_answer')}",
            f"5. Next action: {summary.get('next_action_answer')}",
            "6. No broker and no real trading. No automatic promotion.",
            "",
            "## Export More H1 History",
            "- `python scripts/export_mt5_history.py --symbol BTCUSD --timeframe H1 --bars 25000 --output data/backtests/BTCUSD_H1_25000.csv`",
            "- `python scripts/export_mt5_history.py --symbol BTCUSD --timeframe H1 --bars 30000 --output data/backtests/BTCUSD_H1_30000.csv`",
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


def _tercile_stats(settings: Any, bars: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    size = max(1, len(bars) // 3)
    windows = {
        "tercile_1": (0, size),
        "tercile_2": (size, size * 2),
        "tercile_3": (size * 2, len(bars)),
    }
    payload: dict[str, dict[str, Any]] = {}
    for name, (start, end) in windows.items():
        scoped = [
            trade
            for trade in trades
            if start <= int(_number(trade.get("opened_index")) or 0) < end
        ]
        metrics = _metrics(scoped, initial_balance=settings.initial_balance)
        payload[name] = {
            "closed": metrics["closed"],
            "win_rate": metrics["win_rate"],
            "profit_factor": metrics["profit_factor"],
            "expectancy": metrics["expectancy"],
            "max_drawdown": metrics["max_drawdown"],
        }
    return payload


def _estimate_bars_needed(target: int, trade_frequency_per_1000: float) -> int:
    if trade_frequency_per_1000 <= 0:
        return 0
    return int(math.ceil((target / trade_frequency_per_1000) * 1000.0))


def _fragile_regime_dependency(metrics: dict[str, Any], terciles: dict[str, dict[str, Any]]) -> bool:
    total_closed = int(metrics.get("closed") or 0)
    if total_closed < 6:
        return True
    tercile_closed = [int(item.get("closed") or 0) for item in terciles.values()]
    if max(tercile_closed or [0]) >= total_closed * 0.7:
        return True
    for item in terciles.values():
        if int(item.get("closed") or 0) >= 3 and (float(item.get("profit_factor") or 0.0) < 1.0 or float(item.get("expectancy") or 0.0) < -0.05):
            return True
    return False


def _maturation_status(metrics: dict[str, Any], terciles: dict[str, dict[str, Any]], monte_carlo: dict[str, Any]) -> str:
    closed = int(metrics.get("closed") or 0)
    if closed < 50:
        return "sample_too_small"
    if _fragile_regime_dependency(metrics, terciles):
        return "fragile_regime_dependency"
    if not monte_carlo.get("passed"):
        return "monte_carlo_fragile"
    if float(metrics.get("profit_factor") or 0.0) >= 1.2 and float(metrics.get("expectancy") or 0.0) > 0:
        return "paper_forward_candidate_possible"
    return "observation_only"


def _candidate(metrics: dict[str, Any], terciles: dict[str, dict[str, Any]], monte_carlo: dict[str, Any]) -> bool:
    return (
        int(metrics.get("closed") or 0) >= 50
        and float(metrics.get("profit_factor") or 0.0) >= 1.2
        and float(metrics.get("expectancy") or 0.0) > 0
        and float(metrics.get("win_rate") or 0.0) >= 45
        and float(metrics.get("max_drawdown") or 0.0) <= 5000
        and not _fragile_regime_dependency(metrics, terciles)
        and bool(monte_carlo.get("passed"))
    )


def _recommended_next_step(metrics: dict[str, Any], terciles: dict[str, dict[str, Any]], monte_carlo: dict[str, Any]) -> str:
    if int(metrics.get("closed") or 0) < 50:
        return "export_more_h1_history_and_rerun"
    if _fragile_regime_dependency(metrics, terciles):
        return "do_not_promote_investigate_regime_dependency"
    if not monte_carlo.get("passed"):
        return "do_not_promote_monte_carlo_fragile"
    return "continue_paper_forward_validation_only"


def _summary(rows: list[dict[str, Any]], csv_path: Path) -> dict[str, Any]:
    if not rows:
        return {
            "best_profile": "",
            "recommendation": "no_data",
            "edge_answer": "no H1 rows were generated",
            **_safety(),
        }
    best = max(rows, key=_maturation_rank)
    mc_failures = [
        f"{row.get('profile')}: {','.join(row.get('monte_carlo_fail_reasons') or [])}"
        for row in rows
        if row.get("monte_carlo_fail_reasons")
    ]
    needs = [
        f"{row.get('profile')} needs {row.get('estimated_bars_for_50_trades')}/{row.get('estimated_bars_for_75_trades')} bars"
        for row in rows
    ]
    recommendation = "paper_forward_candidate_possible" if any(row.get("candidate") for row in rows) else "keep_observation_only"
    best_is_fragile = bool(best.get("fragile_regime_dependency"))
    return {
        "best_profile": best.get("profile"),
        "recommendation": recommendation,
        "source_csv": str(csv_path),
        "edge_answer": _edge_answer(rows),
        "bars_needed_answer": "; ".join(needs),
        "stable_profile_answer": f"{best.get('profile')} has the best combined PF/expectancy/drawdown score, but remains {best.get('sample_maturation_status')}",
        "monte_carlo_answer": "; ".join(mc_failures) if mc_failures else "no Monte Carlo failure in this H1 run",
        "next_action_answer": (
            "keep H1 observation_only; export 25k/30k H1 bars and diagnose regime dependency before any paper-forward promotion"
            if best_is_fragile
            else "keep H1 observation_only; export 25k/30k H1 bars and rerun before any paper-forward promotion"
        ),
        "automatic_promotion": False,
        **_safety(),
    }


def _edge_answer(rows: list[dict[str, Any]]) -> str:
    strong = [row for row in rows if float(row.get("profit_factor") or 0.0) >= 1.2 and float(row.get("expectancy") or 0.0) > 0]
    if not strong:
        return "no robust H1 edge yet; metrics fail PF/expectancy gates"
    fragile = [row for row in strong if row.get("fragile_regime_dependency")]
    if fragile:
        return "edge is promising but concentrated in one tercile/regime; keep observation_only and require deeper H1 history"
    sample_small = [row for row in strong if int(row.get("closed_actual") or 0) < 50]
    if sample_small:
        return "edge is promising but still sample-small; do not promote"
    return "edge may be real enough for further paper-forward review, not real trading"


def _maturation_rank(row: dict[str, Any]) -> float:
    return (
        float(row.get("profit_factor") or 0.0) * 30.0
        + float(row.get("expectancy") or 0.0) * 120.0
        + min(float(row.get("closed_actual") or 0.0), 75.0)
        - float(row.get("max_drawdown") or 0.0) / 100.0
        - (40.0 if row.get("fragile_regime_dependency") else 0.0)
        - (30.0 if row.get("monte_carlo_fail_reasons") else 0.0)
    )


def _counts(items: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        counts[str(item)] = counts.get(str(item), 0) + 1
    return counts


def _h1_csv_path(body: dict[str, Any], csv_dir: Path, symbol: str) -> Path:
    explicit = body.get("csv_path_h1") or body.get("csv_path")
    if explicit:
        return Path(str(explicit))
    for suffix in ["30000", "25000", "10000", "5000"]:
        path = csv_dir / f"{symbol}_H1_{suffix}.csv"
        if path.exists():
            return path
    return csv_dir / f"{symbol}_H1_5000.csv"


def _requested_list(value: Any, default: list[str]) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [part.strip() for part in value.split(",") if part.strip()]
    return list(default)

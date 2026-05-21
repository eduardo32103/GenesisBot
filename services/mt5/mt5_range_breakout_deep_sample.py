from __future__ import annotations

import csv
import json
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

from services.mt5.mt5_backtester import _load_bars, _number, _safety, _settings
from services.mt5.mt5_config import get_mt5_config
from services.mt5.mt5_range_breakout_refinement import (
    RANGE_BREAKOUT_REFINEMENT_CASES,
    RefinementCase,
    _refinement_rank,
    evaluate_range_breakout_refinement_case,
)
from services.mt5.mt5_strategy_research_v2 import _features_by_index


DEEP_SAMPLE_TARGETS = [
    "m30_range_breakout_both_all",
    "range_breakout_anti_chop_m30_no_offsession_v2",
    "range_breakout_anti_chop_m30_london_us_v1",
    "range_breakout_anti_chop_m30_sell_london_us_v2",
    "range_breakout_anti_chop_m30_sell_london_asia_filtered_v2",
    "range_breakout_anti_chop_m30_momentum_exit_guard_v2",
]


def run_range_breakout_deep_sample(body: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    body = body or {}
    symbol = str(body.get("symbol") or "BTCUSD").upper().strip()
    csv_dir = Path(str(body.get("csv_dir") or Path("data") / "backtests"))
    max_bars_requested = max(200, min(int(_number(body.get("max_bars")) or 60000), 65000))
    timeout_seconds = max(0.25, float(_number(body.get("per_evaluation_timeout_seconds")) or 8.0))
    spread_points = float(_number(body.get("spread_points")) or 25.0)
    requested_targets = _requested_list(body.get("targets"), DEEP_SAMPLE_TARGETS)
    selected_cases = [case for case in RANGE_BREAKOUT_REFINEMENT_CASES if case.name in requested_targets]
    requested_csvs = _requested_csv_paths(body, csv_dir, symbol)

    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    warnings: list[str] = []
    csvs_evaluated: list[str] = []
    csvs_missing: list[str] = []

    for sample_label, csv_path, sample_max_bars in requested_csvs:
        if not csv_path.exists():
            csvs_missing.append(str(csv_path))
            warnings.append(f"missing_csv:{csv_path}")
            errors.append({"sample_label": sample_label, "path": str(csv_path), "error": "csv_not_found"})
            continue
        settings_body = {
            "symbol": symbol,
            "timeframe": "M30",
            "csv_path": str(csv_path),
            "max_bars": min(sample_max_bars, max_bars_requested),
            "spread_points": spread_points,
            "source": "mt5_csv",
            "save_results": False,
            "timeout_seconds": timeout_seconds,
        }
        settings = replace(
            _settings(settings_body, get_mt5_config()),
            max_bars=min(sample_max_bars, max_bars_requested),
            timeout_seconds=timeout_seconds,
        )
        bars, load_warnings = _load_bars(settings_body, settings)
        warnings.extend([f"{sample_label}:{warning}" for warning in load_warnings])
        bars = bars[-settings.max_bars :]
        if not bars:
            errors.append({"sample_label": sample_label, "path": str(csv_path), "error": "csv_bars_not_loaded"})
            continue
        csvs_evaluated.append(str(csv_path))
        features_by_index = _features_by_index(bars)
        for case in selected_cases:
            row = evaluate_range_breakout_refinement_case(
                settings,
                bars,
                case,
                source_csv=str(csv_path),
                features_by_index=features_by_index,
                timeout_seconds=timeout_seconds,
            )
            row.update(
                {
                    "sample_label": sample_label,
                    "csv_path_used": str(csv_path),
                    "max_bars_requested": settings.max_bars,
                    "bars_loaded": len(bars),
                    "bars_evaluated": max(0, len(bars) - 80),
                    "first_bar_time": str(bars[0].get("time") or "") if bars else "",
                    "last_bar_time": str(bars[-1].get("time") or "") if bars else "",
                }
            )
            row.update(_readiness(row))
            rows.append(row)

    rows.sort(key=_deep_sample_rank, reverse=True)
    candidates = [row for row in rows if row.get("candidate")]
    result = {
        "ok": True,
        "status": "mt5_range_breakout_deep_sample_completed",
        "symbol": symbol,
        "timeframe": "M30",
        "requested_csvs": [str(path) for _label, path, _max_bars in requested_csvs],
        "csvs_evaluated": csvs_evaluated,
        "csvs_missing": csvs_missing,
        "max_bars_requested": max_bars_requested,
        "targets": [case.name for case in selected_cases],
        "evaluations": len(rows),
        "results": rows,
        "candidates": candidates,
        "summary": _summary(rows, candidates, csvs_missing),
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


def write_range_breakout_deep_sample_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "range_breakout_deep_sample_results.csv"
    json_path = root / "range_breakout_deep_sample_results.json"
    summary_path = root / "range_breakout_deep_sample_summary.md"
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    headers = [
        "sample_label",
        "csv_path_used",
        "max_bars_requested",
        "bars_loaded",
        "bars_evaluated",
        "first_bar_time",
        "last_bar_time",
        "target_name",
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
        "single_trade_dependency",
        "dominant_loss_cause",
        "momentum_loss_exit_cluster_count",
        "candidate",
        "readiness",
        "capital_preservation_ready",
        "paper_forward_candidate_recommended",
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
                    "reject_reasons": ";".join(str(item) for item in row.get("reject_reasons") or []),
                }
            )
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True), encoding="utf-8")
    summary_path.write_text(range_breakout_deep_sample_summary_markdown(result), encoding="utf-8")
    return csv_path, json_path, summary_path


def range_breakout_deep_sample_summary_markdown(result: dict[str, Any]) -> str:
    rows = result.get("results") if isinstance(result.get("results"), list) else []
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else _summary(rows, [], [])
    lines = [
        "# MT5 Range Breakout Deep-Sample Validation Summary",
        "",
        "M30 range breakout deep-sample validation. Paper/offline only; no broker, no order execution, no automatic promotion.",
        "",
        f"Evaluations: `{result.get('evaluations', len(rows))}`.",
        f"CSV evaluated: `{len(result.get('csvs_evaluated') or [])}`.",
        f"CSV missing: `{len(result.get('csvs_missing') or [])}`.",
        f"Candidates: `{len(result.get('candidates') or [])}`.",
        "",
        "## Top Rows",
    ]
    for row in rows[:20]:
        lines.append(
            f"- `{row.get('sample_label')}` `{row.get('target_name')}` closed `{row.get('closed')}`, PF `{row.get('profit_factor')}`, "
            f"expectancy `{row.get('expectancy')}`, DD `{row.get('max_drawdown')}`, MC PF `{row.get('monte_carlo_stressed_pf')}`, "
            f"readiness `{row.get('readiness')}`."
        )
    lines.extend(
        [
            "",
            "## Answers",
            f"1. no_offsession_v2 >=40 trades: {summary.get('no_offsession_40_answer')}",
            f"2. PF/expectancy retained: {summary.get('edge_answer')}",
            f"3. Monte Carlo: {summary.get('monte_carlo_answer')}",
            f"4. Off-session toxicity: {summary.get('offsession_answer')}",
            f"5. Sell side strength: {summary.get('side_answer')}",
            f"6. Exit causing losses: {summary.get('exit_answer')}",
            f"7. Capital optimizer handoff: {summary.get('capital_optimizer_answer')}",
            "8. No automatic promotion.",
            "",
            "## Export Read-Only Commands",
            "```powershell",
            "python scripts\\export_mt5_history.py --symbol BTCUSD --timeframe M30 --bars 40000 --output data\\backtests\\BTCUSD_M30_40000.csv",
            "python scripts\\export_mt5_history.py --symbol BTCUSD --timeframe M30 --bars 60000 --output data\\backtests\\BTCUSD_M30_60000.csv",
            "```",
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


def _readiness(row: dict[str, Any]) -> dict[str, Any]:
    closed = int(row.get("closed") or 0)
    gate_passed = bool(row.get("candidate"))
    if gate_passed and closed >= 75:
        readiness = "paper_forward_candidate_recommended"
    elif gate_passed and closed >= 40:
        readiness = "capital_preservation_ready"
    elif closed >= 40:
        readiness = "reject_or_observation_only_failed_robustness"
    else:
        readiness = "sample_too_small"
    return {
        "readiness": readiness,
        "capital_preservation_ready": readiness in {"capital_preservation_ready", "paper_forward_candidate_recommended"},
        "paper_forward_candidate_recommended": readiness == "paper_forward_candidate_recommended",
    }


def _summary(rows: list[dict[str, Any]], candidates: list[dict[str, Any]], missing: list[str]) -> dict[str, Any]:
    if not rows:
        return {
            "recommendation": "no_data",
            "no_offsession_40_answer": "not evaluated; extended CSV missing",
            "edge_answer": "no rows evaluated",
            "monte_carlo_answer": "no rows evaluated",
            "offsession_answer": "baseline unavailable",
            "side_answer": "baseline unavailable",
            "exit_answer": "baseline unavailable",
            "capital_optimizer_answer": "none",
            "missing_csvs": missing,
            **_safety(),
        }
    no_off = [row for row in rows if row.get("target_name") == "range_breakout_anti_chop_m30_no_offsession_v2"]
    best_no_off = max(no_off, key=lambda row: int(row.get("closed") or 0), default={})
    best = max(rows, key=_deep_sample_rank)
    baseline = next((row for row in rows if row.get("target_name") == "m30_range_breakout_both_all"), None)
    no_off_40 = (
        f"yes, closed {best_no_off.get('closed')} on {best_no_off.get('sample_label')} readiness {best_no_off.get('readiness')}"
        if int(best_no_off.get("closed") or 0) >= 40
        else f"no, best closed {best_no_off.get('closed', 0)} on {best_no_off.get('sample_label', 'none')}"
    )
    handoff = (
        "; ".join(f"{row.get('sample_label')} {row.get('target_name')}" for row in rows if row.get("capital_preservation_ready"))
        or "none; keep observation_only/no promotion"
    )
    recent_failed = _recent_validation_failed(rows)
    return {
        "recommendation": (
            "capital_preservation_ready"
            if any(row.get("capital_preservation_ready") for row in rows) and not recent_failed
            else "reject"
            if recent_failed
            else "observation_only"
        ),
        "no_offsession_40_answer": no_off_40,
        "edge_answer": f"best row {best.get('target_name')} PF {best.get('profit_factor')} expectancy {best.get('expectancy')} closed {best.get('closed')}",
        "monte_carlo_answer": f"best row MC PF {best.get('monte_carlo_stressed_pf')} p95 DD {best.get('monte_carlo_p95_drawdown')}",
        "offsession_answer": _offsession_answer(baseline),
        "side_answer": _side_answer(baseline or best),
        "exit_answer": str((baseline or best).get("dominant_loss_cause") or "none"),
        "capital_optimizer_answer": "none; recent 40k validation failed, keep reject/no promotion" if recent_failed else handoff,
        "recent_validation_failed": recent_failed,
        "missing_csvs": missing,
        "automatic_promotion": False,
        **_safety(),
    }


def _offsession_answer(row: dict[str, Any] | None) -> str:
    if not row:
        return "baseline not evaluated; prior refinement marked off_session toxic"
    sessions = row.get("session_stats") if isinstance(row.get("session_stats"), dict) else {}
    off = sessions.get("off_session")
    if not off:
        return "no off_session trades in evaluated row"
    return f"off_session closed {off.get('closed')} PF {off.get('profit_factor')} expectancy {off.get('expectancy')}; compare against recent-sample stability before re-enabling"


def _side_answer(row: dict[str, Any]) -> str:
    sides = row.get("side_stats") if isinstance(row.get("side_stats"), dict) else {}
    if not sides:
        return "no side stats"
    parts = []
    for side in sorted(sides):
        metrics = sides.get(side) or {}
        parts.append(f"{side} closed {metrics.get('closed')} PF {metrics.get('profit_factor')} expectancy {metrics.get('expectancy')}")
    return "; ".join(parts)


def _recent_validation_failed(rows: list[dict[str, Any]]) -> bool:
    recent_rows = [
        row
        for row in rows
        if str(row.get("sample_label") or "") == "40000"
        and row.get("target_name")
        in {
            "m30_range_breakout_both_all",
            "range_breakout_anti_chop_m30_no_offsession_v2",
            "range_breakout_anti_chop_m30_london_us_v1",
        }
    ]
    if not recent_rows:
        return False
    return all(float(row.get("profit_factor") or 0.0) <= 1.0 or float(row.get("expectancy") or 0.0) <= 0.0 for row in recent_rows)


def _deep_sample_rank(row: dict[str, Any]) -> float:
    readiness_bonus = 120.0 if row.get("paper_forward_candidate_recommended") else 70.0 if row.get("capital_preservation_ready") else 0.0
    return _refinement_rank(row) + readiness_bonus


def _requested_csv_paths(body: dict[str, Any], csv_dir: Path, symbol: str) -> list[tuple[str, Path, int]]:
    paths: list[tuple[str, Path, int]] = []
    explicit = body.get("csv_paths")
    if isinstance(explicit, list) and explicit:
        for item in explicit:
            path = Path(str(item))
            label = path.stem.replace(f"{symbol}_M30_", "")
            paths.append((label, path, _max_bars_from_path(path)))
        return paths
    for bars in (40000, 60000):
        key = f"csv_path_m30_{bars}"
        path = Path(str(body.get(key) or csv_dir / f"{symbol}_M30_{bars}.csv"))
        paths.append((f"{bars}", path, bars))
    fallback = body.get("include_baseline_20000")
    if str(fallback or "").casefold() in {"1", "true", "yes", "on"}:
        path = Path(str(body.get("csv_path_m30_20000") or csv_dir / f"{symbol}_M30_20000.csv"))
        paths.insert(0, ("20000", path, 20000))
    return paths


def _max_bars_from_path(path: Path) -> int:
    digits = "".join(ch for ch in path.stem.split("_")[-1] if ch.isdigit())
    return int(digits or 60000)


def _requested_list(value: Any, default: list[str]) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [part.strip() for part in value.split(",") if part.strip()]
    return list(default)

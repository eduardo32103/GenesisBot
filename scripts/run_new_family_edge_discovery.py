from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_new_family_edge_discovery import run_new_family_edge_discovery


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    search_root = Path(args.results_dir) if args.results_dir else ROOT / "data" / "backtests" / "multisymbol"
    result = run_new_family_edge_discovery(
        result_paths=[Path(path) for path in args.result_path],
        search_root=search_root,
        include_offline_backtests=args.run_offline_backtests,
        max_offline_evaluations=args.max_offline_evaluations,
        max_bars=args.max_bars,
        monte_carlo_simulations=args.monte_carlo_simulations,
        per_evaluation_timeout_seconds=args.per_evaluation_timeout_seconds,
        max_runtime_seconds=args.max_runtime_seconds,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "MT5 new family edge discovery",
        f"mode={result.get('mode')}",
        f"recommendation={result.get('recommendation')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"applies_to_real_trading={result.get('applies_to_real_trading')}",
        f"useful_rows={result.get('useful_rows')}",
        f"offline_backtests_run={result.get('offline_backtests_run')}",
        f"offline_evaluations={result.get('offline_evaluations')}",
        f"interrupted_or_timed_out={result.get('interrupted_or_timed_out')}",
        f"interruption_reason={result.get('interruption_reason') or ''}",
        f"max_runtime_seconds={result.get('max_runtime_seconds')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Loaded sources:",
    ]
    loaded_sources = result.get("loaded_sources") or []
    lines.extend(f"- {source}" for source in loaded_sources) if loaded_sources else lines.append("- none")

    lines.extend(["", "Missing sources:"])
    missing_sources = result.get("missing_sources") or []
    lines.extend(f"- {source}" for source in missing_sources) if missing_sources else lines.append("- none")

    skipped_sources = result.get("skipped_sources") or []
    if skipped_sources:
        lines.extend(["", "Skipped sources:"])
        for item in skipped_sources:
            lines.append(f"- {item.get('path')} reason={item.get('reason')}")

    offline_errors = result.get("offline_errors") or []
    if offline_errors:
        lines.extend(["", "Offline backtest errors:"])
        for item in offline_errors:
            lines.append(f"- {item}")

    lines.extend(["", "Families evaluated:"])
    families = result.get("families_evaluated") or []
    lines.extend(f"- {family}" for family in families) if families else lines.append("- none")

    lines.extend(["", "Symbol/timeframes evaluated:"])
    symbol_timeframes = result.get("symbol_timeframes_evaluated") or []
    lines.extend(f"- {item}" for item in symbol_timeframes) if symbol_timeframes else lines.append("- none")

    lines.extend(["", "Candidate ranking:"])
    lines.extend(_ranking_lines(result.get("ranking") or [], limit=25))

    lines.extend(["", "Top near misses:"])
    near_misses = result.get("top_near_misses") or []
    if near_misses:
        for row in near_misses:
            lines.append(
                f"- {row.get('symbol')} {row.get('timeframe')} {row.get('profile')} "
                f"concept={row.get('conceptual_family')} failed={','.join(row.get('rejection_reasons') or [])}"
            )
    else:
        lines.append("- none")

    lines.extend(["", "Excluded by registry or sibling risk:"])
    excluded = result.get("excluded_by_registry_or_sibling_risk") or []
    if excluded:
        for row in excluded[:20]:
            lines.append(
                f"- {row.get('symbol')} {row.get('timeframe')} {row.get('profile')} "
                f"status={row.get('candidate_status')} "
                f"reason={row.get('research_rejection_reason') or row.get('sibling_risk_reason') or row.get('degradation_reason')}"
            )
    else:
        lines.append("- none")

    lines.extend(["", "Skipped family ideas:"])
    skipped_ideas = result.get("skipped_family_ideas") or []
    if skipped_ideas:
        for item in skipped_ideas:
            lines.append(f"- {item.get('family')} reason={item.get('reason')} next={item.get('next_step')}")
    else:
        lines.append("- none")

    lines.extend(["", "Next expansion:"])
    for item in result.get("next_expansion") or []:
        lines.append(f"- {item.get('action')} reason={item.get('reason')}")

    recommended = result.get("recommended_candidate")
    lines.append("")
    lines.append(
        "recommended_candidate="
        + (
            f"{recommended.get('symbol')} {recommended.get('timeframe')} {recommended.get('profile')}"
            if isinstance(recommended, dict)
            else "none"
        )
    )
    return "\n".join(lines)


def _ranking_lines(rows: list[dict[str, Any]], *, limit: int) -> list[str]:
    header = (
        "symbol | timeframe | profile | concept | recent_closed | total_closed | recent_pf | total_pf | expectancy | "
        "mc_pf | mc_exp | spread_x2_pf | remove_best_5_pf | fragile | single_trade | status | action"
    )
    lines = [header, "-" * len(header)]
    if not rows:
        lines.append("none")
        return lines
    for row in rows[:limit]:
        lines.append(
            " | ".join(
                [
                    str(row.get("symbol") or ""),
                    str(row.get("timeframe") or ""),
                    str(row.get("profile") or ""),
                    str(row.get("conceptual_family") or ""),
                    str(row.get("recent_closed") or 0),
                    str(row.get("total_closed") or 0),
                    str(row.get("recent_pf") or 0.0),
                    str(row.get("total_pf") or 0.0),
                    str(row.get("expectancy") or 0.0),
                    str(row.get("monte_carlo_stressed_pf") or 0.0),
                    str(row.get("monte_carlo_stressed_expectancy") or 0.0),
                    str(row.get("spread_x2_pf") or 0.0),
                    str(row.get("remove_best_5_pf") or 0.0),
                    str(row.get("fragile_regime_dependency") or False),
                    str(row.get("single_trade_dependency") or False),
                    str(row.get("candidate_status") or ""),
                    str(row.get("recommended_next_action") or ""),
                ]
            )
        )
    return lines


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover non-correlated MT5 paper-forward families without activation.")
    parser.add_argument("--results-dir", default="", help="Optional directory containing small processed result CSV/JSON files.")
    parser.add_argument("--result-path", action="append", default=[], help="Optional processed result CSV/JSON path. Can be repeated.")
    parser.add_argument("--run-offline-backtests", action="store_true", help="Explicitly run slower local OHLC offline evaluations.")
    parser.add_argument("--max-offline-evaluations", type=int, default=80)
    parser.add_argument("--max-bars", type=int, default=20000)
    parser.add_argument("--monte-carlo-simulations", type=int, default=150)
    parser.add_argument("--per-evaluation-timeout-seconds", type=float, default=1.5)
    parser.add_argument("--max-runtime-seconds", type=float, default=15.0)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())

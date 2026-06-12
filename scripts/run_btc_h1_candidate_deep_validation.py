from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_btc_h1_candidate_deep_validation import (  # noqa: E402
    DEFAULT_CSV_PATHS,
    PROCESSED_SOURCE_PATHS,
    run_btc_h1_candidate_deep_validation,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    body: dict[str, Any] = {
        "csv_paths": args.csv_paths or ",".join(str(ROOT / path) for path in DEFAULT_CSV_PATHS),
        "processed_source_paths": args.processed_source_paths or ",".join(str(ROOT / path) for path in PROCESSED_SOURCE_PATHS),
        "max_bars": args.max_bars,
        "max_configs": args.max_configs,
        "monte_carlo_simulations": args.monte_carlo_simulations,
        "per_evaluation_timeout_seconds": args.per_evaluation_timeout_seconds,
        "max_runtime_seconds": args.max_runtime_seconds,
        "load_persistent_memory": not args.no_persistent_memory,
        "persist_research_lesson": not args.no_persist_research_lesson,
    }
    if args.targets:
        body["targets"] = args.targets
    if args.spread_points is not None:
        body["spread_points"] = args.spread_points

    result = run_btc_h1_candidate_deep_validation(body)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    best = result.get("best_variant") if isinstance(result.get("best_variant"), dict) else {}
    recommended = result.get("recommended_candidate") if isinstance(result.get("recommended_candidate"), dict) else None
    lesson_write = result.get("research_lesson_write") if isinstance(result.get("research_lesson_write"), dict) else {}
    lines = [
        "BTCUSD H1 candidate deep validation",
        f"status={result.get('status')}",
        f"mode={result.get('mode')}",
        f"candidate_profile_name={result.get('candidate_profile_name')}",
        f"candidate_profile_before={result.get('candidate_profile_before')}",
        f"source_family={result.get('source_family')}",
        f"source_profile={result.get('source_profile')}",
        f"source_identity_status={result.get('source_identity_status')}",
        f"source_identity_resolved={result.get('source_identity_resolved')}",
        f"csv_used={_comma(result.get('csv_used') or [])}",
        f"missing_csvs={_comma(result.get('missing_csvs') or [])}",
        f"processed_sources_loaded={_comma(result.get('processed_sources_loaded') or [])}",
        f"processed_sources_missing={_comma(result.get('processed_sources_missing') or [])}",
        f"useful_processed_rows={result.get('useful_processed_rows')}",
        f"windows_evaluated={_comma(result.get('windows_evaluated') or [])}",
        f"variants_evaluated={result.get('variants_evaluated')}",
        f"recommendation={result.get('recommendation')}",
        f"paper_observation_ready={result.get('paper_observation_ready')}",
        f"requires_human_approval={result.get('requires_human_approval')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"paper_rotation_applied={result.get('paper_rotation_applied')}",
        f"applies_to_real_trading={result.get('applies_to_real_trading')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        f"research_lesson_persisted={result.get('research_lesson_persisted')}",
        f"research_lesson_write_reason={lesson_write.get('reason') or ''}",
        "",
        "Observed paper-review metrics:",
        json.dumps(result.get("observed_paper_review_metrics") or {}, sort_keys=True),
        "",
        "best_variant="
        + (
            f"{best.get('target_name')} csv={best.get('csv_label')} window={best.get('validation_window')} "
            f"recent_closed={best.get('recent_closed')} total_closed={best.get('total_closed')} "
            f"recent_pf={best.get('recent_profit_factor')} total_pf={best.get('profit_factor')} "
            f"mc_pf={best.get('monte_carlo_stressed_pf')} spread_x2_pf={best.get('spread_x2_pf')} "
            f"remove_best_5_pf={best.get('remove_best_5_pf')} status={best.get('candidate_status')} "
            f"rejections={','.join(best.get('rejection_reasons') or []) or 'none'}"
            if best
            else "none"
        ),
        "recommended_candidate="
        + (
            f"{recommended.get('symbol')} {recommended.get('timeframe')} {recommended.get('target_name')}"
            if recommended
            else "none"
        ),
        "",
        "Deep validation ranking:",
        *_ranking_lines(result.get("results") or []),
    ]
    errors = result.get("errors") or []
    warnings = result.get("warnings") or []
    if errors:
        lines.extend(["", "Errors:"])
        lines.extend(f"- {item}" for item in errors)
    if warnings:
        lines.extend(["", "Warnings:"])
        lines.extend(f"- {item}" for item in warnings)
    return "\n".join(lines)


def _ranking_lines(rows: list[dict[str, Any]], *, limit: int = 30) -> list[str]:
    header = (
        "target | csv | family | window | recent_closed | total_closed | win_rate | recent_win_rate | "
        "recent_pf | total_pf | expectancy | recent_expectancy | mc_pf | mc_exp | mc_p95_dd | "
        "spread_x1_5_pf | spread_x2_pf | remove_best_1_pf | remove_best_5_pf | single_trade | "
        "fragile | stability | cost_confidence | status | rejection_reasons"
    )
    lines = [header, "-" * len(header)]
    if not rows:
        lines.append("none")
        return lines
    for row in rows[:limit]:
        lines.append(
            " | ".join(
                [
                    str(row.get("target_name") or ""),
                    str(row.get("csv_label") or ""),
                    str(row.get("family") or ""),
                    str(row.get("validation_window") or ""),
                    str(row.get("recent_closed") or 0),
                    str(row.get("total_closed") or 0),
                    str(row.get("win_rate") or 0.0),
                    str(row.get("recent_win_rate") or 0.0),
                    str(row.get("recent_profit_factor") or 0.0),
                    str(row.get("profit_factor") or 0.0),
                    str(row.get("expectancy") or 0.0),
                    str(row.get("recent_expectancy") or 0.0),
                    str(row.get("monte_carlo_stressed_pf") or 0.0),
                    str(row.get("monte_carlo_stressed_expectancy") or 0.0),
                    str(row.get("monte_carlo_p95_drawdown") or 0.0),
                    str(row.get("spread_x1_5_pf") or 0.0),
                    str(row.get("spread_x2_pf") or 0.0),
                    str(row.get("remove_best_1_pf") or 0.0),
                    str(row.get("remove_best_5_pf") or 0.0),
                    str(row.get("single_trade_dependency") or False),
                    str(row.get("fragile_regime_dependency") or False),
                    str(row.get("sample_stability_score") or 0.0),
                    str(row.get("cost_model_confidence") or ""),
                    str(row.get("candidate_status") or ""),
                    ",".join(row.get("rejection_reasons") or []),
                ]
            )
        )
    return lines


def _comma(values: list[Any]) -> str:
    return ",".join(str(value) for value in values) if values else "none"


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run offline BTCUSD H1 paper candidate deep validation.")
    parser.add_argument("--csv-paths", default="", help="Comma-separated BTCUSD H1 CSV paths.")
    parser.add_argument("--processed-source-paths", default="", help="Comma-separated processed result CSV/JSON paths.")
    parser.add_argument("--targets", default="", help="Comma-separated target names.")
    parser.add_argument("--max-bars", type=int, default=20000)
    parser.add_argument("--max-configs", type=int, default=4)
    parser.add_argument("--monte-carlo-simulations", type=int, default=200)
    parser.add_argument("--per-evaluation-timeout-seconds", type=float, default=2.0)
    parser.add_argument("--max-runtime-seconds", type=float, default=60.0)
    parser.add_argument("--spread-points", type=float)
    parser.add_argument("--no-persistent-memory", action="store_true")
    parser.add_argument("--no-persist-research-lesson", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())

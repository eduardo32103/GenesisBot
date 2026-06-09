from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_btc_m30_london_us_breakout_deep_validation import (
    DEFAULT_CSV_PATHS,
    run_btc_m30_london_us_breakout_deep_validation,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    body: dict[str, Any] = {
        "csv_paths": args.csv_paths or ",".join(str(ROOT / path) for path in DEFAULT_CSV_PATHS),
        "max_bars": args.max_bars,
        "monte_carlo_simulations": args.monte_carlo_simulations,
        "per_evaluation_timeout_seconds": args.per_evaluation_timeout_seconds,
    }
    if args.targets:
        body["targets"] = args.targets
    if args.spread_points is not None:
        body["spread_points"] = args.spread_points

    result = run_btc_m30_london_us_breakout_deep_validation(body)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "BTCUSD M30 London-US breakout deep validation",
        f"csv_used={_comma(result.get('csv_used') or [])}",
        f"missing_csvs={_comma(result.get('missing_csvs') or [])}",
        f"windows_evaluated={_comma(result.get('windows_evaluated') or [])}",
        f"variants_evaluated={result.get('variants_evaluated')}",
        f"recommendation={result.get('recommendation')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"applies_to_real_trading={result.get('applies_to_real_trading')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
    ]
    export_readiness = result.get("export_readiness") or {}
    if export_readiness:
        lines.extend(
            [
                f"export_readiness.needed={export_readiness.get('needed')}",
                f"export_readiness.prepared_read_only={export_readiness.get('prepared_read_only')}",
                f"export_readiness.missing_depths={_comma(export_readiness.get('missing_depths') or [])}",
            ]
        )

    errors = result.get("errors") or []
    warnings = result.get("warnings") or []
    if errors:
        lines.extend(["", "Errors:"])
        lines.extend(f"- {item}" for item in errors)
    if warnings:
        lines.extend(["", "Warnings:"])
        lines.extend(f"- {item}" for item in warnings)

    best = result.get("best_variant")
    lines.append("")
    lines.append(
        "best_variant="
        + (
            f"{best.get('target_name')} csv={best.get('csv_label')} window={best.get('validation_window')} "
            f"recent_closed={best.get('recent_closed')} status={best.get('candidate_status')} "
            f"rejections={','.join(best.get('rejection_reasons') or []) or 'none'}"
            if isinstance(best, dict)
            else "none"
        )
    )

    recommended = result.get("recommended_candidate")
    lines.append(
        "recommended_candidate="
        + (
            f"{recommended.get('symbol')} {recommended.get('timeframe')} {recommended.get('target_name')}"
            if isinstance(recommended, dict)
            else "none"
        )
    )

    lines.extend(["", "Deep validation ranking:"])
    lines.extend(_ranking_lines(result.get("results") or []))
    return "\n".join(lines)


def _ranking_lines(rows: list[dict[str, Any]], *, limit: int = 40) -> list[str]:
    header = (
        "target | csv | window | recent_closed | total_closed | recent_pf | total_pf | expectancy | mc_pf | "
        "mc_exp | spread_x2_pf | remove_best_5_pf | max_drawdown | fragile | single_trade | stability | "
        "status | rejection_reasons"
    )
    lines = [header, "-" * len(header)]
    if not rows:
        lines.append("none")
        return lines
    for row in rows[:limit]:
        stability = row.get("window_stability") if isinstance(row.get("window_stability"), dict) else {}
        lines.append(
            " | ".join(
                [
                    str(row.get("target_name") or ""),
                    str(row.get("csv_label") or ""),
                    str(row.get("validation_window") or ""),
                    str(row.get("recent_closed") or 0),
                    str(row.get("total_closed") or 0),
                    str(row.get("recent_pf") or 0.0),
                    str(row.get("total_pf") or 0.0),
                    str(row.get("expectancy") or 0.0),
                    str(row.get("monte_carlo_stressed_pf") or 0.0),
                    str(row.get("monte_carlo_stressed_expectancy") or 0.0),
                    str(row.get("spread_x2_pf") or 0.0),
                    str(row.get("remove_best_5_pf") or 0.0),
                    str(row.get("max_drawdown") or 0.0),
                    str(row.get("fragile_regime_dependency") or False),
                    str(row.get("single_trade_dependency") or False),
                    f"{stability.get('passing_recent_windows', 0)}/{stability.get('validation_windows_evaluated', 0)}",
                    str(row.get("candidate_status") or ""),
                    ",".join(row.get("rejection_reasons") or []),
                ]
            )
        )
    return lines


def _comma(values: list[Any]) -> str:
    return ",".join(str(value) for value in values) if values else "none"


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run offline BTCUSD M30 London-US breakout deep validation.")
    parser.add_argument("--csv-paths", default="", help="Comma-separated BTCUSD M30 CSV paths.")
    parser.add_argument("--targets", default="", help="Comma-separated deep validation target names.")
    parser.add_argument("--max-bars", type=int, default=60000)
    parser.add_argument("--monte-carlo-simulations", type=int, default=250)
    parser.add_argument("--per-evaluation-timeout-seconds", type=float, default=6.0)
    parser.add_argument("--spread-points", type=float)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())

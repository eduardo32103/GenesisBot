from __future__ import annotations

import argparse
import json
import sys
from typing import Any
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_ustec_m30_h1_trend_pullback_hardening import run_ustec_m30_h1_trend_pullback_hardening


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    body: dict[str, Any] = {
        "max_bars": args.max_bars,
        "monte_carlo_simulations": args.monte_carlo_simulations,
    }
    if args.m30_csv_paths:
        body["m30_csv_paths"] = args.m30_csv_paths
    if args.h1_csv_paths:
        body["h1_csv_paths"] = args.h1_csv_paths
    if args.targets:
        body["targets"] = args.targets

    result = run_ustec_m30_h1_trend_pullback_hardening(body)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "USTEC M30/H1 multi-timeframe trend pullback hardening",
        f"csv_used={','.join(result.get('csv_used') or []) or 'none'}",
        f"missing_csvs={','.join(result.get('missing_csvs') or []) or 'none'}",
        f"variants_evaluated={result.get('variants_evaluated')}",
        f"recommendation={result.get('recommendation')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"applies_to_real_trading={result.get('applies_to_real_trading')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
    ]
    errors = result.get("errors") or []
    if errors:
        lines.extend(["", "Errors:"])
        lines.extend(f"- {item}" for item in errors)

    best = result.get("best_variant")
    lines.append("")
    lines.append(
        "best_variant="
        + (
            f"{best.get('profile')} status={best.get('candidate_status')} "
            f"recent_closed={best.get('recent_closed')} total_closed={best.get('total_closed')} "
            f"recent_pf={best.get('recent_pf')} total_pf={best.get('total_pf')} "
            f"mc_pf={best.get('monte_carlo_stressed_pf')} spread_x2_pf={best.get('spread_x2_pf')} "
            f"remove_best_5_pf={best.get('remove_best_5_pf')} "
            f"rejections={','.join(best.get('rejection_reasons') or []) or 'none'}"
            if isinstance(best, dict)
            else "none"
        )
    )
    recommended = result.get("recommended_candidate")
    lines.append(
        "recommended_candidate="
        + (
            f"{recommended.get('symbol')} {recommended.get('timeframe')}/{recommended.get('higher_timeframe')} {recommended.get('profile')}"
            if isinstance(recommended, dict)
            else "none"
        )
    )
    lines.extend(["", "Variant ranking:"])
    lines.extend(_ranking_lines(result.get("results") or []))
    return "\n".join(lines)


def _ranking_lines(rows: list[dict[str, Any]], *, limit: int = 40) -> list[str]:
    header = (
        "profile | actions | recent_closed | total_closed | recent_pf | total_pf | expectancy | mc_pf | "
        "mc_exp | spread_x2_pf | remove_best_5_pf | max_drawdown | fragile | single_trade | data_quality | status | rejection_reasons"
    )
    lines = [header, "-" * len(header)]
    if not rows:
        lines.append("none")
        return lines
    for row in rows[:limit]:
        lines.append(
            " | ".join(
                [
                    str(row.get("profile") or ""),
                    ",".join(row.get("hardening_actions") or []),
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
                    str(row.get("data_quality") or ""),
                    str(row.get("candidate_status") or ""),
                    ",".join(row.get("rejection_reasons") or []),
                ]
            )
        )
    return lines


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run offline USTEC M30/H1 trend pullback hardening without activation.")
    parser.add_argument("--m30-csv-paths", default="", help="Comma-separated M30 CSV paths. Defaults to known USTEC/NAS100 local aliases.")
    parser.add_argument("--h1-csv-paths", default="", help="Comma-separated H1 CSV paths. Defaults to known USTEC/NAS100 local aliases.")
    parser.add_argument("--targets", default="", help="Comma-separated variant mode names.")
    parser.add_argument("--max-bars", type=int, default=20000)
    parser.add_argument("--monte-carlo-simulations", type=int, default=300)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())

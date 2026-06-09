from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_xau_m15_session_hardening import DEFAULT_CSV_PATH, run_xau_m15_session_hardening


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    body: dict[str, Any] = {
        "csv_path": str(Path(args.csv_path)) if args.csv_path else str(ROOT / DEFAULT_CSV_PATH),
        "max_bars": args.max_bars,
        "monte_carlo_simulations": args.monte_carlo_simulations,
        "per_evaluation_timeout_seconds": args.per_evaluation_timeout_seconds,
    }
    if args.targets:
        body["targets"] = args.targets
    if args.spread_points is not None:
        body["spread_points"] = args.spread_points

    result = run_xau_m15_session_hardening(body)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "XAUUSD M15 session-open continuation hardening",
        f"csv_path={result.get('csv_path')}",
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
            f"{best.get('target_name')} status={best.get('candidate_status')} "
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

    lines.extend(["", "Variant ranking:"])
    lines.extend(_ranking_lines(result.get("results") or []))
    return "\n".join(lines)


def _ranking_lines(rows: list[dict[str, Any]], *, limit: int = 30) -> list[str]:
    header = (
        "target | actions | recent_closed | total_closed | recent_pf | total_pf | expectancy | mc_pf | mc_exp | "
        "spread_x2_pf | remove_best_5_pf | max_drawdown | fragile | single_trade | degraded | sibling_risk | "
        "status | rejection_reasons"
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
                    str(row.get("degraded_by_registry") or False),
                    str(row.get("sibling_risk") or False),
                    str(row.get("candidate_status") or ""),
                    ",".join(row.get("rejection_reasons") or []),
                ]
            )
        )
    return lines


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run offline XAUUSD M15 session hardening without activating candidates.")
    parser.add_argument("--csv-path", default="", help="Optional XAUUSD M15 CSV path.")
    parser.add_argument("--targets", default="", help="Comma-separated hardening target names.")
    parser.add_argument("--max-bars", type=int, default=20000)
    parser.add_argument("--monte-carlo-simulations", type=int, default=300)
    parser.add_argument("--per-evaluation-timeout-seconds", type=float, default=2.0)
    parser.add_argument("--spread-points", type=float)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.mt5.mt5_strategy_research_v3 import (  # noqa: E402
    run_strategy_research_v3,
    write_strategy_research_v3_outputs,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MT5 Strategy Research V3 recent-robust walk-forward discovery from local CSV files.")
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--csv-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--csv-path-m15", default="")
    parser.add_argument("--csv-path-m30-60000", default="")
    parser.add_argument("--csv-path-m30-40000", default="")
    parser.add_argument("--csv-path-h1", default="")
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--timeframes", default="")
    parser.add_argument("--families", default="")
    parser.add_argument("--max-bars", type=int, default=60000)
    parser.add_argument("--max-evaluations", type=int, default=180)
    parser.add_argument("--spread-points", type=float, default=25.0)
    parser.add_argument("--per-evaluation-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--smoke", action="store_true", help="Run a small bounded research pass.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.smoke:
        args.timeframes = args.timeframes or "M15,M30"
        args.families = args.families or "range_breakout_anti_chop,ema_reclaim_pullback"
        args.max_bars = min(args.max_bars, 1200)
        args.max_evaluations = min(args.max_evaluations, 12)
        args.per_evaluation_timeout_seconds = min(args.per_evaluation_timeout_seconds, 1.0)
    started = time.monotonic()
    result = run_strategy_research_v3(
        {
            "symbol": args.symbol,
            "csv_dir": args.csv_dir,
            "csv_path_m15": args.csv_path_m15,
            "csv_path_m30_60000": args.csv_path_m30_60000,
            "csv_path_m30_40000": args.csv_path_m30_40000,
            "csv_path_h1": args.csv_path_h1,
            "timeframes": args.timeframes,
            "families": args.families,
            "max_bars": args.max_bars,
            "max_evaluations": args.max_evaluations,
            "spread_points": args.spread_points,
            "per_evaluation_timeout_seconds": args.per_evaluation_timeout_seconds,
        }
    )
    csv_path, json_path, summary_path = write_strategy_research_v3_outputs(result, args.output_dir)
    print_table(result.get("results") or [])
    print()
    print(f"Saved CSV:     {csv_path}")
    print(f"Saved JSON:    {json_path}")
    print(f"Saved summary: {summary_path}")
    print(f"Candidates: {len(result.get('candidates') or [])}")
    print(f"Recommendation: {(result.get('summary') or {}).get('recommendation')}")
    print(f"Duration seconds: {round(time.monotonic() - started, 2)}")
    print("broker_touched=false")
    print("order_executed=false")
    print("order_policy=journal_only_no_broker")
    return 0 if result.get("ok") else 1


def print_table(rows: list[dict[str, Any]], limit: int = 20) -> None:
    headers = [
        "sample_label",
        "timeframe",
        "family",
        "side",
        "session",
        "closed_total",
        "closed_recent_holdout",
        "profit_factor_total",
        "pf_recent_holdout",
        "expectancy_total",
        "expectancy_recent_holdout",
        "monte_carlo_stressed_pf",
        "candidate",
        "recommendation",
        "research_score",
    ]
    visible = rows[:limit]
    widths = {header: len(header) for header in headers}
    for row in visible:
        for header in headers:
            widths[header] = max(widths[header], len(str(row.get(header, ""))))
    print(" | ".join(header.ljust(widths[header]) for header in headers))
    print("-+-".join("-" * widths[header] for header in headers))
    for row in visible:
        print(" | ".join(str(row.get(header, "")).ljust(widths[header]) for header in headers))


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.mt5.mt5_h1_candidate_maturation import (  # noqa: E402
    H1_MATURATION_PROFILES,
    run_h1_candidate_maturation,
    write_h1_candidate_maturation_outputs,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MT5 H1 candidate maturation diagnostics from BTCUSD CSV files.")
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--csv-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--csv-path-h1", default="")
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--profiles", default=",".join(H1_MATURATION_PROFILES))
    parser.add_argument("--max-bars", type=int, default=30000)
    parser.add_argument("--spread-points", type=float, default=25.0)
    parser.add_argument("--timeout-seconds", type=float, default=90.0)
    parser.add_argument("--smoke", action="store_true", help="Run a bounded smoke pass that should finish quickly.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.smoke:
        args.profiles = "low_drawdown_v5_session_filtered"
        args.max_bars = min(args.max_bars, 800)
        args.timeout_seconds = min(args.timeout_seconds, 20.0)
    started = time.monotonic()
    result = run_h1_candidate_maturation(
        {
            "symbol": args.symbol,
            "csv_dir": args.csv_dir,
            "csv_path_h1": args.csv_path_h1,
            "profiles": args.profiles,
            "max_bars": args.max_bars,
            "spread_points": args.spread_points,
            "timeout_seconds": args.timeout_seconds,
        }
    )
    csv_path, json_path, summary_path = write_h1_candidate_maturation_outputs(result, args.output_dir)
    print_table(result.get("results") or [])
    print()
    print(f"Saved CSV:     {csv_path}")
    print(f"Saved JSON:    {json_path}")
    print(f"Saved summary: {summary_path}")
    print(f"Duration seconds: {round(time.monotonic() - started, 2)}")
    print("broker_touched=false")
    print("order_executed=false")
    print("order_policy=journal_only_no_broker")
    return 0 if result.get("ok") else 1


def print_table(rows: list[dict[str, Any]], limit: int = 20) -> None:
    headers = [
        "profile",
        "closed_actual",
        "missing_to_50",
        "trade_frequency_per_1000_bars",
        "estimated_bars_for_50_trades",
        "profit_factor",
        "expectancy",
        "max_drawdown",
        "monte_carlo_stressed_pf",
        "monte_carlo_p95_drawdown",
        "sample_maturation_status",
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

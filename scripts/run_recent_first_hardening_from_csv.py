from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.mt5.mt5_recent_first_hardening import (  # noqa: E402
    RECENT_FIRST_HARDENING_TARGETS,
    run_recent_first_hardening,
    write_recent_first_hardening_outputs,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MT5 Recent-First Monte Carlo hardening from local CSV files.")
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--csv-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--csv-path-m30-60000", default="")
    parser.add_argument("--csv-path-m30-40000", default="")
    parser.add_argument("--csv-path-h1", default="")
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--targets", default="")
    parser.add_argument("--max-bars", type=int, default=60000)
    parser.add_argument("--spread-points", type=float, default=25.0)
    parser.add_argument("--per-evaluation-timeout-seconds", type=float, default=2.0)
    parser.add_argument("--smoke", action="store_true", help="Run a small bounded hardening pass.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.smoke:
        args.max_bars = min(args.max_bars, 1200)
        args.per_evaluation_timeout_seconds = min(args.per_evaluation_timeout_seconds, 1.0)
        args.targets = args.targets or ",".join(
            [
                "recent_london_us_breakout_m30_both_hardened_v1",
                "recent_london_us_breakout_m30_mae_guard_v1",
                "recent_liquidity_sweep_h1_hardened_v1",
            ]
        )
    elif not args.targets:
        args.targets = ",".join(RECENT_FIRST_HARDENING_TARGETS)

    started = time.monotonic()
    result = run_recent_first_hardening(
        {
            "symbol": args.symbol,
            "csv_dir": args.csv_dir,
            "csv_path_m30_60000": args.csv_path_m30_60000,
            "csv_path_m30_40000": args.csv_path_m30_40000,
            "csv_path_h1": args.csv_path_h1,
            "targets": args.targets,
            "max_bars": args.max_bars,
            "spread_points": args.spread_points,
            "per_evaluation_timeout_seconds": args.per_evaluation_timeout_seconds,
        }
    )
    csv_path, json_path, summary_path = write_recent_first_hardening_outputs(result, args.output_dir)
    print_table(result.get("results") or [])
    print()
    print(f"Saved CSV:     {csv_path}")
    print(f"Saved JSON:    {json_path}")
    print(f"Saved summary: {summary_path}")
    print(f"Evaluations: {result.get('evaluations')}")
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
        "target_name",
        "recent_closed",
        "recent_pf",
        "total_closed",
        "total_pf",
        "monte_carlo_stressed_pf",
        "spread_x2_pf",
        "remove_best_5_pf",
        "candidate",
        "recommendation",
        "hardening_score",
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

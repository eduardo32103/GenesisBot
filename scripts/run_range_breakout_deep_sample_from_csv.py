from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.mt5.mt5_range_breakout_deep_sample import (  # noqa: E402
    run_range_breakout_deep_sample,
    write_range_breakout_deep_sample_outputs,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MT5 M30 range breakout deep-sample validation from local CSV files.")
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--csv-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--csv-path-m30-40000", default="")
    parser.add_argument("--csv-path-m30-60000", default="")
    parser.add_argument("--csv-path-m30-20000", default="")
    parser.add_argument("--include-baseline-20000", action="store_true")
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--targets", default="")
    parser.add_argument("--max-bars", type=int, default=60000)
    parser.add_argument("--spread-points", type=float, default=25.0)
    parser.add_argument("--per-evaluation-timeout-seconds", type=float, default=8.0)
    parser.add_argument("--smoke", action="store_true", help="Run a small bounded validation pass.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.smoke:
        args.targets = "range_breakout_anti_chop_m30_no_offsession_v2"
        args.max_bars = min(args.max_bars, 1200)
        args.per_evaluation_timeout_seconds = min(args.per_evaluation_timeout_seconds, 1.0)
        args.include_baseline_20000 = True
    started = time.monotonic()
    result = run_range_breakout_deep_sample(
        {
            "symbol": args.symbol,
            "csv_dir": args.csv_dir,
            "csv_path_m30_40000": args.csv_path_m30_40000,
            "csv_path_m30_60000": args.csv_path_m30_60000,
            "csv_path_m30_20000": args.csv_path_m30_20000,
            "include_baseline_20000": args.include_baseline_20000,
            "targets": args.targets,
            "max_bars": args.max_bars,
            "spread_points": args.spread_points,
            "per_evaluation_timeout_seconds": args.per_evaluation_timeout_seconds,
        }
    )
    csv_path, json_path, summary_path = write_range_breakout_deep_sample_outputs(result, args.output_dir)
    print_table(result.get("results") or [])
    print()
    print(f"Saved CSV:     {csv_path}")
    print(f"Saved JSON:    {json_path}")
    print(f"Saved summary: {summary_path}")
    print(f"CSV evaluated: {len(result.get('csvs_evaluated') or [])}")
    print(f"CSV missing: {len(result.get('csvs_missing') or [])}")
    for path in result.get("csvs_missing") or []:
        print(f"Missing CSV: {path}")
    print(f"Candidates: {len(result.get('candidates') or [])}")
    print(f"Duration seconds: {round(time.monotonic() - started, 2)}")
    print("broker_touched=false")
    print("order_executed=false")
    print("order_policy=journal_only_no_broker")
    return 0 if result.get("ok") else 1


def print_table(rows: list[dict[str, Any]], limit: int = 20) -> None:
    headers = [
        "sample_label",
        "target_name",
        "bars_loaded",
        "closed",
        "win_rate",
        "profit_factor",
        "expectancy",
        "max_drawdown",
        "monte_carlo_stressed_pf",
        "readiness",
        "candidate",
        "recommendation",
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

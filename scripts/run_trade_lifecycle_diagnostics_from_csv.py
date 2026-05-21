from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.mt5.mt5_trade_lifecycle_diagnostics import (  # noqa: E402
    PRIORITY_MATRIX,
    run_trade_lifecycle_diagnostics,
    write_trade_lifecycle_outputs,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MT5 paper-only trade lifecycle diagnostics from BTCUSD CSV files.")
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--csv-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--pairs", default=",".join(f"{timeframe}:{profile}" for timeframe, profile in PRIORITY_MATRIX))
    parser.add_argument("--max-bars", type=int, default=20000)
    parser.add_argument("--spread-points", type=float, default=25.0)
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    parser.add_argument("--smoke", action="store_true", help="Run a bounded smoke pass that should finish quickly.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.smoke:
        args.pairs = "M30:capital_preservation_v4_side_filtered"
        args.max_bars = min(args.max_bars, 800)
        args.timeout_seconds = min(args.timeout_seconds, 15.0)
    started = time.monotonic()
    result = run_trade_lifecycle_diagnostics(
        {
            "symbol": args.symbol,
            "csv_dir": args.csv_dir,
            "pairs": args.pairs,
            "max_bars": args.max_bars,
            "spread_points": args.spread_points,
            "timeout_seconds": args.timeout_seconds,
        }
    )
    csv_path, json_path, summary_path = write_trade_lifecycle_outputs(result, args.output_dir)
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
        "timeframe",
        "profile",
        "generated_signal_count",
        "actionable_signal_count",
        "opened_trade_count",
        "closed_trade_count",
        "skipped_due_max_open_trades",
        "avg_bars_in_trade",
        "profit_factor",
        "expectancy",
        "max_drawdown",
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

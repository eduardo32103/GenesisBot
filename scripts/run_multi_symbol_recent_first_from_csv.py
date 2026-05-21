from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.mt5.mt5_multi_symbol_recent_first import (  # noqa: E402
    DEFAULT_MULTI_SYMBOLS,
    run_multi_symbol_recent_first,
    write_multi_symbol_recent_first_outputs,
)
from services.mt5.mt5_recent_first_research import RECENT_FIRST_FAMILIES, RECENT_FIRST_TIMEFRAMES  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MT5 multi-symbol Recent-First edge discovery from local CSV files.")
    parser.add_argument("--symbols", default=",".join(DEFAULT_MULTI_SYMBOLS))
    parser.add_argument("--timeframes", default=",".join(RECENT_FIRST_TIMEFRAMES))
    parser.add_argument("--families", default=",".join(RECENT_FIRST_FAMILIES))
    parser.add_argument("--csv-dir", default=str(REPO_ROOT / "data" / "backtests" / "multisymbol"))
    parser.add_argument("--fallback-csv-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "data" / "backtests" / "multisymbol"))
    parser.add_argument("--bars", type=int, default=20000)
    parser.add_argument("--spread-points", type=float, default=None)
    parser.add_argument("--max-evaluations-per-symbol-timeframe", type=int, default=40)
    parser.add_argument("--per-evaluation-timeout-seconds", type=float, default=1.5)
    parser.add_argument("--smoke", action="store_true", help="Run a small bounded multi-symbol pass.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.smoke:
        args.symbols = args.symbols or "BTCUSD,ETHUSD"
        args.timeframes = "M15"
        args.families = "recent_momentum_pullback,recent_ema_reclaim"
        args.bars = min(args.bars, 1200)
        args.max_evaluations_per_symbol_timeframe = min(args.max_evaluations_per_symbol_timeframe, 8)
        args.per_evaluation_timeout_seconds = min(args.per_evaluation_timeout_seconds, 1.0)
    started = time.monotonic()
    payload: dict[str, Any] = {
        "symbols": args.symbols,
        "timeframes": args.timeframes,
        "families": args.families,
        "csv_dir": args.csv_dir,
        "fallback_csv_dir": args.fallback_csv_dir,
        "bars": args.bars,
        "max_evaluations_per_symbol_timeframe": args.max_evaluations_per_symbol_timeframe,
        "per_evaluation_timeout_seconds": args.per_evaluation_timeout_seconds,
    }
    if args.spread_points is not None:
        payload["spread_points"] = args.spread_points
    result = run_multi_symbol_recent_first(payload)
    csv_path, json_path, summary_path = write_multi_symbol_recent_first_outputs(result, args.output_dir)
    print_table(result.get("results") or [])
    print()
    print(f"Saved CSV:     {csv_path}")
    print(f"Saved JSON:    {json_path}")
    print(f"Saved summary: {summary_path}")
    print(f"Evaluations: {result.get('evaluations')}")
    print(f"Candidates: {len(result.get('candidates') or [])}")
    print(f"Skipped symbols: {len(result.get('skipped_symbols') or [])}")
    print(f"Recommendation: {(result.get('summary') or {}).get('recommendation')}")
    print(f"Duration seconds: {round(time.monotonic() - started, 2)}")
    print("broker_touched=false")
    print("order_executed=false")
    print("order_policy=journal_only_no_broker")
    return 0 if result.get("ok") else 1


def print_table(rows: list[dict[str, Any]], limit: int = 20) -> None:
    headers = [
        "symbol",
        "timeframe",
        "family",
        "side",
        "session",
        "hardening_mode",
        "recent_closed",
        "recent_pf",
        "total_closed",
        "total_pf",
        "monte_carlo_stressed_pf",
        "spread_x2_pf",
        "candidate",
        "recommendation",
        "multi_symbol_score",
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

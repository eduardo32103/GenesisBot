from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.mt5.mt5_capital_preservation_optimizer import CAPITAL_PRESERVATION_PROFILES, CAPITAL_PRESERVATION_TIMEFRAMES
from services.mt5.mt5_entry_funnel_diagnostics import run_entry_funnel_diagnostics, write_entry_funnel_outputs


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MT5 entry funnel diagnostics from BTCUSD CSV files.")
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--csv-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--timeframes", default=",".join(CAPITAL_PRESERVATION_TIMEFRAMES))
    parser.add_argument("--profiles", default=",".join(CAPITAL_PRESERVATION_PROFILES))
    parser.add_argument("--max-bars", type=int, default=5000)
    parser.add_argument("--spread-points", type=float, default=25.0)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--smoke", action="store_true", help="Run a small diagnostic pass that should finish quickly.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.smoke:
        args.timeframes = "M15"
        args.profiles = "trend_continuation_v3_balanced,breakout_pullback_v3_balanced,low_drawdown_v3_more_trades"
        args.max_bars = min(args.max_bars, 800)
        args.timeout_seconds = min(args.timeout_seconds, 15.0)
    started = time.monotonic()
    payload = {
        "symbol": args.symbol,
        "csv_dir": args.csv_dir,
        "timeframes": args.timeframes,
        "profiles": args.profiles,
        "max_bars": args.max_bars,
        "spread_points": args.spread_points,
        "timeout_seconds": args.timeout_seconds,
    }
    result = run_entry_funnel_diagnostics(payload)
    csv_path, json_path, summary_path = write_entry_funnel_outputs(result, args.output_dir)
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
        "bars_evaluated",
        "generated_signal_count",
        "actionable_signal_count",
        "opened_shadow_trade_count",
        "closed_trade_count",
        "blocked_by_risk_governor",
        "restrictiveness_score",
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

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.mt5.mt5_capital_preservation_optimizer import (
    CAPITAL_PRESERVATION_PROFILES,
    CAPITAL_PRESERVATION_TIMEFRAMES,
    MT5CapitalPreservationOptimizer,
    write_capital_preservation_outputs,
)


def print_table(rows: list[dict[str, Any]], limit: int = 25) -> None:
    headers = [
        "timeframe",
        "profile",
        "closed",
        "win_rate",
        "profit_factor",
        "expectancy",
        "max_drawdown",
        "capital_preservation_score",
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MT5 capital-preservation strategy search from local BTCUSD CSV files.")
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--csv-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--timeframes", default=",".join(CAPITAL_PRESERVATION_TIMEFRAMES))
    parser.add_argument("--profiles", default=",".join(CAPITAL_PRESERVATION_PROFILES))
    parser.add_argument("--max-bars", type=int, default=5000)
    parser.add_argument("--max-evaluations", type=int, default=180, help="Budgeted total profile/parameter configs per timeframe set.")
    parser.add_argument("--risk-reward-values", default="0.8,1.0,1.2,1.5")
    parser.add_argument("--time-stop-bars", default="1,2,3,4,6")
    parser.add_argument("--score-min-values", default="55,60,65,70,75")
    parser.add_argument("--spread-max-values", default="20,25,30")
    parser.add_argument("--cooldown-after-loss-values", default="1,2,3,4")
    parser.add_argument("--block-after-consecutive-losses-values", default="2,3")
    parser.add_argument("--timeout-seconds", type=float, default=4.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = {
        "symbol": args.symbol,
        "csv_dir": args.csv_dir,
        "timeframes": args.timeframes,
        "profiles": args.profiles,
        "max_bars": args.max_bars,
        "max_evaluations": args.max_evaluations,
        "risk_reward_values": args.risk_reward_values,
        "time_stop_bars": args.time_stop_bars,
        "score_min_values": args.score_min_values,
        "spread_max_values": args.spread_max_values,
        "cooldown_after_loss_values": args.cooldown_after_loss_values,
        "block_after_consecutive_losses_values": args.block_after_consecutive_losses_values,
        "timeout_seconds": args.timeout_seconds,
    }
    optimizer = MT5CapitalPreservationOptimizer()
    result = optimizer.run(payload)
    csv_path, json_path, summary_path = write_capital_preservation_outputs(result, args.output_dir)
    print_table(result.get("results") or [])
    print()
    print(f"Saved CSV:     {csv_path}")
    print(f"Saved JSON:    {json_path}")
    print(f"Saved summary: {summary_path}")
    print(f"Candidates: {len(result.get('candidates') or [])}")
    print(f"Recommendation: {result.get('recommendation')}")
    print("broker_touched=false")
    print("order_executed=false")
    print("order_policy=journal_only_no_broker")
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.mt5.mt5_eth_m30_volatility_hardening import (  # noqa: E402
    ETH_M30_HARDENING_TARGETS,
    run_eth_m30_volatility_hardening,
    write_eth_m30_volatility_hardening_outputs,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ETHUSD M30 volatility breakout Monte Carlo hardening from local CSV.")
    parser.add_argument("--csv-path", default=str(REPO_ROOT / "data" / "backtests" / "multisymbol" / "ETHUSD_M30_20000.csv"))
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "data" / "backtests" / "multisymbol"))
    parser.add_argument("--targets", default="")
    parser.add_argument("--max-bars", type=int, default=20000)
    parser.add_argument("--monte-carlo-simulations", type=int, default=500)
    parser.add_argument("--per-evaluation-timeout-seconds", type=float, default=2.0)
    parser.add_argument("--smoke", action="store_true", help="Run a small bounded hardening pass.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.smoke:
        args.max_bars = min(args.max_bars, 1200)
        args.monte_carlo_simulations = min(args.monte_carlo_simulations, 120)
        args.per_evaluation_timeout_seconds = min(args.per_evaluation_timeout_seconds, 1.0)
        args.targets = args.targets or ",".join(
            [
                "eth_m30_vol_breakout_both_baseline",
                "eth_m30_vol_breakout_mae_guard_v1",
                "eth_m30_vol_breakout_mc_hardened_v1",
            ]
        )
    elif not args.targets:
        args.targets = ",".join(ETH_M30_HARDENING_TARGETS)

    started = time.monotonic()
    result = run_eth_m30_volatility_hardening(
        {
            "csv_path": args.csv_path,
            "targets": args.targets,
            "max_bars": args.max_bars,
            "monte_carlo_simulations": args.monte_carlo_simulations,
            "per_evaluation_timeout_seconds": args.per_evaluation_timeout_seconds,
        }
    )
    csv_path, json_path, summary_path = write_eth_m30_volatility_hardening_outputs(result, args.output_dir)
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
        "target_name",
        "side",
        "session",
        "recent_closed",
        "recent_pf",
        "total_closed",
        "total_pf",
        "monte_carlo_stressed_pf",
        "spread_x2_pf",
        "remove_best_5_pf",
        "candidate",
        "recommendation",
        "eth_hardening_score",
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

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.mt5.mt5_robust_optimizer import MT5RobustOptimizer, ROBUST_PROFILES, ROBUST_TIMEFRAMES


CSV_COLUMNS = [
    "timeframe",
    "profile",
    "rr",
    "time_stop_min",
    "closed",
    "wins",
    "losses",
    "win_rate",
    "profit_factor",
    "expectancy",
    "max_drawdown",
    "test_pf",
    "test_expectancy",
    "monte_carlo_risk_of_ruin",
    "monte_carlo_drawdown_p95",
    "institutional_score",
    "recommendation",
    "candidate",
    "pass_fail_reasons",
    "broker_touched",
    "order_executed",
]


def write_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "robust_optimizer_results.csv"
    json_path = root / "robust_optimizer_results.json"
    summary_path = root / "robust_optimizer_summary.md"
    rows = list(result.get("results") or [])
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            mc = row.get("monte_carlo") if isinstance(row.get("monte_carlo"), dict) else {}
            writer.writerow(
                {
                    "timeframe": row.get("timeframe", ""),
                    "profile": row.get("profile", ""),
                    "rr": row.get("rr", ""),
                    "time_stop_min": row.get("time_stop_min", ""),
                    "closed": row.get("closed", 0),
                    "wins": row.get("wins", 0),
                    "losses": row.get("losses", 0),
                    "win_rate": row.get("win_rate", 0),
                    "profit_factor": row.get("profit_factor", 0),
                    "expectancy": row.get("expectancy", 0),
                    "max_drawdown": row.get("max_drawdown", 0),
                    "test_pf": row.get("test_pf", 0),
                    "test_expectancy": row.get("test_expectancy", 0),
                    "monte_carlo_risk_of_ruin": mc.get("risk_of_ruin", 0),
                    "monte_carlo_drawdown_p95": mc.get("max_drawdown_p95", 0),
                    "institutional_score": row.get("institutional_score", 0),
                    "recommendation": row.get("recommendation", ""),
                    "candidate": row.get("candidate", False),
                    "pass_fail_reasons": ";".join(str(reason) for reason in row.get("pass_fail_reasons", [])),
                    "broker_touched": row.get("broker_touched", False),
                    "order_executed": row.get("order_executed", False),
                }
            )
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    summary_path.write_text(_summary_markdown(result), encoding="utf-8")
    return csv_path, json_path, summary_path


def print_table(rows: list[dict[str, Any]], limit: int = 20) -> None:
    headers = ["timeframe", "profile", "closed", "win_rate", "profit_factor", "expectancy", "max_drawdown", "institutional_score", "recommendation"]
    visible = rows[:limit]
    widths = {header: len(header) for header in headers}
    for row in visible:
        for header in headers:
            widths[header] = max(widths[header], len(str(row.get(header, ""))))
    print(" | ".join(header.ljust(widths[header]) for header in headers))
    print("-+-".join("-" * widths[header] for header in headers))
    for row in visible:
        print(" | ".join(str(row.get(header, "")).ljust(widths[header]) for header in headers))


def _summary_markdown(result: dict[str, Any]) -> str:
    rows = list(result.get("results") or [])
    candidates = list(result.get("candidates") or [])
    lines = [
        "# MT5 Robust Optimizer Summary",
        "",
        "Safety: `broker_touched=false`, `order_executed=false`, `order_policy=journal_only_no_broker`.",
        "",
        f"Recommendation: **{result.get('recommendation', 'reject')}**",
        "",
        "This report never recommends real trading. Passing profiles are paper-forward candidates only.",
        "",
    ]
    if candidates:
        lines.append("## Best Paper Candidates")
        for row in candidates[:5]:
            lines.append(
                f"- `{row['timeframe']} {row['profile']}`: PF `{row['profit_factor']}`, "
                f"expectancy `{row['expectancy']}`, DD `{row['max_drawdown']}`, score `{row['institutional_score']}`."
            )
    else:
        lines.extend(["## Candidates", "No profile passed the institutional robustness gates. Keep `observation_only`."])
    lines.extend(["", "## Top Results"])
    for row in rows[:10]:
        reasons = ", ".join(str(reason) for reason in row.get("pass_fail_reasons", [])[:4])
        lines.append(
            f"- `{row.get('timeframe')} {row.get('profile')}` PF `{row.get('profit_factor')}`, "
            f"WR `{row.get('win_rate')}`, DD `{row.get('max_drawdown')}`, "
            f"recommendation `{row.get('recommendation')}`. Reasons: {reasons}."
        )
    lines.extend(
        [
            "",
            "## Risk Position",
            "- Real trading remains disabled.",
            "- No martingale, no grid escalation, no averaging losses.",
            "- If there is doubt, Genesis must return `NO_TRADE`.",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run institutional MT5 robust profile optimizer from local BTCUSD CSV files.")
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--csv-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--timeframes", default=",".join(ROBUST_TIMEFRAMES))
    parser.add_argument("--profiles", default=",".join(ROBUST_PROFILES))
    parser.add_argument("--max-bars", type=int, default=5000)
    parser.add_argument("--rr-values", default="1.2", help="Comma-separated RR values. Use 1.0,1.2,1.5 for a broad grid.")
    parser.add_argument("--time-stop-minutes", default="15", help="Comma-separated time-stop minutes. Use 15,30,60 for a broad grid.")
    parser.add_argument("--timeout-seconds", type=float, default=2.0, help="Per-simulation guard to prevent long local runs.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    optimizer = MT5RobustOptimizer()
    result = optimizer.run(
        {
            "symbol": args.symbol,
            "csv_dir": args.csv_dir,
            "timeframes": args.timeframes,
            "profiles": args.profiles,
            "max_bars": args.max_bars,
            "rr_values": args.rr_values,
            "time_stop_minutes": args.time_stop_minutes,
            "timeout_seconds": args.timeout_seconds,
        }
    )
    csv_path, json_path, summary_path = write_outputs(result, args.output_dir)
    print_table(result.get("results") or [])
    print()
    print(f"Saved CSV:     {csv_path}")
    print(f"Saved JSON:    {json_path}")
    print(f"Saved summary: {summary_path}")
    print(f"Candidates: {len(result.get('candidates') or [])}")
    print("broker_touched=false")
    print("order_executed=false")
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

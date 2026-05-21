from __future__ import annotations

import argparse
import json
import sys
import time
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
        "config_id",
        "timeframe",
        "profile",
        "duplicate_count",
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
    parser.add_argument("--csv-path-m15", default="")
    parser.add_argument("--csv-path-m30", default="")
    parser.add_argument("--csv-path-h1", default="")
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
    parser.add_argument("--per-evaluation-timeout-seconds", type=float, default=None)
    parser.add_argument("--progress-every", type=int, default=25)
    parser.add_argument("--max-runtime-seconds", type=float, default=0.0)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--limit-profiles", type=int, default=0)
    parser.add_argument("--smoke", action="store_true", help="Run a bounded smoke search that should finish in under 30 seconds.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.smoke:
        args.timeframes = "M15"
        args.profiles = "breakout_pullback_v1,trend_continuation_v1,capital_preservation_v2"
        args.max_bars = min(args.max_bars, 600)
        args.max_evaluations = min(args.max_evaluations, 6)
        args.timeout_seconds = min(args.timeout_seconds, 1.5)
        args.per_evaluation_timeout_seconds = 1.5
        args.max_runtime_seconds = 30.0
        args.progress_every = 1
    profiles = [part.strip() for part in str(args.profiles or "").split(",") if part.strip()]
    if args.limit_profiles and args.limit_profiles > 0:
        profiles = profiles[: args.limit_profiles]
    output_dir = Path(args.output_dir)
    existing_results = _load_existing_results(output_dir) if args.resume else []
    collected_rows = list(existing_results)
    interrupted = False
    started = time.monotonic()

    def progress(payload: dict[str, Any]) -> None:
        params = payload.get("parameters") if isinstance(payload.get("parameters"), dict) else {}
        elapsed = payload.get("elapsed_seconds", 0)
        print(
            f"[capital-search] {payload.get('current')}/{payload.get('total')} "
            f"{payload.get('timeframe')} {payload.get('profile')} "
            f"rr={params.get('risk_reward')} ts={params.get('time_stop_bars')} "
            f"score={params.get('score_min')} spread={params.get('spread_max')} "
            f"elapsed={elapsed}s best={payload.get('best_score')}",
            flush=True,
        )

    def incremental(payload: dict[str, Any]) -> None:
        nonlocal collected_rows
        rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
        collected_rows = rows
        _write_partial(args, rows, interrupted=bool(payload.get("interrupted")), errors=payload.get("errors") if isinstance(payload.get("errors"), list) else [])

    payload = {
        "symbol": args.symbol,
        "csv_dir": args.csv_dir,
        "csv_path_m15": args.csv_path_m15,
        "csv_path_m30": args.csv_path_m30,
        "csv_path_h1": args.csv_path_h1,
        "timeframes": args.timeframes,
        "profiles": profiles,
        "max_bars": args.max_bars,
        "max_evaluations": args.max_evaluations,
        "risk_reward_values": args.risk_reward_values,
        "time_stop_bars": args.time_stop_bars,
        "score_min_values": args.score_min_values,
        "spread_max_values": args.spread_max_values,
        "cooldown_after_loss_values": args.cooldown_after_loss_values,
        "block_after_consecutive_losses_values": args.block_after_consecutive_losses_values,
        "timeout_seconds": args.timeout_seconds,
        "per_evaluation_timeout_seconds": args.per_evaluation_timeout_seconds or args.timeout_seconds,
        "progress_every": args.progress_every,
        "max_runtime_seconds": args.max_runtime_seconds,
        "existing_results": existing_results,
        "progress_callback": progress,
        "incremental_callback": incremental,
    }
    optimizer = MT5CapitalPreservationOptimizer()
    try:
        result = optimizer.run(payload)
        collected_rows = list(result.get("results") or collected_rows)
    except KeyboardInterrupt:
        interrupted = True
        result = _partial_result(args, collected_rows, interrupted=True, started=started, errors=[{"error": "keyboard_interrupt"}])
        print("interrupted=true", flush=True)
    finally:
        if collected_rows:
            _write_partial(args, collected_rows, interrupted=interrupted, errors=result.get("errors", []) if "result" in locals() and isinstance(result.get("errors"), list) else [])
    csv_path, json_path, summary_path = write_capital_preservation_outputs(result, args.output_dir)
    print_table(result.get("results") or [])
    print()
    print(f"Saved CSV:     {csv_path}")
    print(f"Saved JSON:    {json_path}")
    print(f"Saved summary: {summary_path}")
    print(f"Candidates: {len(result.get('candidates') or [])}")
    print(f"Recommendation: {result.get('recommendation')}")
    print(f"Interrupted: {bool(result.get('interrupted'))}")
    print(f"Duration seconds: {round(time.monotonic() - started, 2)}")
    print("broker_touched=false")
    print("order_executed=false")
    print("order_policy=journal_only_no_broker")
    return 0 if result.get("ok") else 1


def _load_existing_results(output_dir: Path) -> list[dict[str, Any]]:
    path = output_dir / "capital_preservation_optimizer_results.json"
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("results") if isinstance(payload, dict) else None
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _partial_result(args: argparse.Namespace, rows: list[dict[str, Any]], *, interrupted: bool, started: float, errors: list[dict[str, Any]]) -> dict[str, Any]:
    sorted_rows = sorted(rows, key=lambda item: (item.get("recommendation") != "paper_forward_candidate", -float(_number(item.get("capital_preservation_score")) or 0.0)))
    candidates = [row for row in sorted_rows if row.get("recommendation") == "paper_forward_candidate"]
    return {
        "ok": True,
        "status": "mt5_capital_preservation_optimizer_interrupted" if interrupted else "mt5_capital_preservation_optimizer_partial",
        "symbol": args.symbol,
        "timeframes": [part.strip() for part in str(args.timeframes).split(",") if part.strip()],
        "profiles": [part.strip() for part in str(args.profiles).split(",") if part.strip()],
        "results": sorted_rows,
        "candidates": candidates,
        "best_profile": candidates[0] if candidates else (sorted_rows[0] if sorted_rows else None),
        "recommendation": "paper_forward_candidate" if candidates else "reject",
        "interrupted": interrupted,
        "errors": errors,
        "live_runtime_mutated": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "shadow_trades_mutated": False,
        "martingale_enabled": False,
        "grid_enabled": False,
        "averaging_down_enabled": False,
        "increase_size_after_loss_enabled": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "duration_ms": int((time.monotonic() - started) * 1000),
    }


def _write_partial(args: argparse.Namespace, rows: list[dict[str, Any]], *, interrupted: bool, errors: list[dict[str, Any]]) -> None:
    result = _partial_result(args, rows, interrupted=interrupted, started=time.monotonic(), errors=errors)
    write_capital_preservation_outputs(result, args.output_dir)


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    raise SystemExit(main())

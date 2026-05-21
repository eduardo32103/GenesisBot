from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path
from typing import Any, Callable


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.mt5.mt5_backtester import _FILTER_PROFILES
from services.mt5.mt5_forward_replay import MT5ForwardReplay


DEFAULT_TIMEFRAMES = ["M15", "M30", "H1"]
BASE_SWEEP_PROFILES = [
    "baseline",
    "quality_v2",
    "quality_strict",
    "momentum_v1",
    "trend_v1",
    "anti_chop_v1",
    "rsi_reversal_safe",
]
CSV_COLUMNS = [
    "timeframe",
    "profile",
    "closed",
    "wins",
    "losses",
    "win_rate",
    "profit_factor",
    "expectancy",
    "max_drawdown",
    "degraded",
    "degradation_reason",
    "score",
    "candidate",
    "bars_loaded",
    "broker_touched",
    "order_executed",
]


def default_profiles() -> list[str]:
    profiles = list(BASE_SWEEP_PROFILES)
    for profile in sorted(_FILTER_PROFILES):
        if profile not in profiles and profile != "quality_loose":
            profiles.append(profile)
    return profiles


def is_candidate(result: dict[str, Any]) -> bool:
    return (
        int(_number(result.get("closed")) or 0) >= 25
        and float(_number(result.get("profit_factor")) or 0.0) >= 1.15
        and float(_number(result.get("expectancy")) or 0.0) > 0.0
        and float(_number(result.get("win_rate")) or 0.0) >= 40.0
        and not bool(result.get("degraded"))
        and float(_number(result.get("max_drawdown")) or 0.0) <= 5000.0
        and result.get("broker_touched") is False
        and result.get("order_executed") is False
    )


def score_result(result: dict[str, Any]) -> float:
    closed = int(_number(result.get("closed")) or 0)
    pf = float(_number(result.get("profit_factor")) or 0.0)
    expectancy = float(_number(result.get("expectancy")) or 0.0)
    win_rate = float(_number(result.get("win_rate")) or 0.0)
    drawdown = float(_number(result.get("max_drawdown")) or 0.0)
    score = (pf * 100.0) + (expectancy * 1000.0) + (win_rate * 0.35) - (drawdown / 100.0)
    if closed < 25:
        score -= 500.0
    if result.get("degraded"):
        score -= 10000.0
    if result.get("broker_touched") is not False or result.get("order_executed") is not False:
        score -= 100000.0
    return round(score, 4)


def summarize_result(result: dict[str, Any]) -> dict[str, Any]:
    row = {
        "timeframe": str(result.get("timeframe") or "").upper(),
        "profile": str(result.get("profile") or ""),
        "closed": int(_number(result.get("closed")) or 0),
        "wins": int(_number(result.get("wins")) or 0),
        "losses": int(_number(result.get("losses")) or 0),
        "win_rate": round(float(_number(result.get("win_rate")) or 0.0), 4),
        "profit_factor": round(float(_number(result.get("profit_factor")) or 0.0), 4),
        "expectancy": round(float(_number(result.get("expectancy")) or 0.0), 6),
        "max_drawdown": round(float(_number(result.get("max_drawdown")) or 0.0), 4),
        "degraded": bool(result.get("degraded")),
        "degradation_reason": str(result.get("degradation_reason") or ""),
        "bars_loaded": int(_number(result.get("bars_loaded")) or 0),
        "broker_touched": bool(result.get("broker_touched")),
        "order_executed": bool(result.get("order_executed")),
    }
    row["score"] = score_result(row)
    row["candidate"] = is_candidate(row)
    return row


def run_sweep(
    *,
    symbol: str = "BTCUSD",
    csv_dir: Path | str = REPO_ROOT / "data" / "backtests",
    timeframes: list[str] | None = None,
    profiles: list[str] | None = None,
    max_bars: int = 5000,
    initial_balance: float = 100000.0,
    spread_points: float = 30.0,
    slippage_points: float = 5.0,
    commission: float = 0.0,
    checkpoints: list[int] | None = None,
    runner_factory: Callable[[], Any] | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    clean_symbol = str(symbol or "BTCUSD").upper().strip()
    clean_timeframes = [str(tf).upper().strip() for tf in (timeframes or DEFAULT_TIMEFRAMES) if str(tf).strip()]
    clean_profiles = [str(profile).strip().casefold() for profile in (profiles or default_profiles()) if str(profile).strip()]
    root = Path(csv_dir)
    checkpoints = checkpoints or [10, 25, 50, 100]
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    runner_factory = runner_factory or (lambda: MT5ForwardReplay(memory=None))

    for timeframe in clean_timeframes:
        csv_path = root / f"{clean_symbol}_{timeframe}_5000.csv"
        if not csv_path.exists():
            errors.append({"timeframe": timeframe, "error": "csv_not_found", "path": str(csv_path)})
            continue
        csv_text = csv_path.read_text(encoding="utf-8-sig")
        for profile in clean_profiles:
            runner = runner_factory()
            payload = {
                "symbol": clean_symbol,
                "timeframe": timeframe,
                "profile": profile,
                "filter_profile": profile,
                "csv_text": csv_text,
                "initial_balance": initial_balance,
                "spread_points": spread_points,
                "slippage_points": slippage_points,
                "commission": commission,
                "max_bars": max_bars,
                "checkpoints": checkpoints,
                "persist": False,
            }
            try:
                result = runner.run(payload)
            except Exception as exc:
                errors.append({"timeframe": timeframe, "profile": profile, "error": str(exc)[:300]})
                continue
            row = summarize_result(result)
            row["csv_path"] = str(csv_path)
            row["ok"] = bool(result.get("ok"))
            row["status"] = str(result.get("status") or "")
            row["order_policy"] = str(result.get("order_policy") or "journal_only_no_broker")
            rows.append(row)

    rows.sort(key=lambda item: (bool(item["degraded"]), not bool(item["candidate"]), -float(item["score"])))
    return {
        "ok": True,
        "status": "forward_replay_sweep_completed",
        "symbol": clean_symbol,
        "timeframes": clean_timeframes,
        "profiles": clean_profiles,
        "max_bars": max_bars,
        "results": rows,
        "candidates": [row for row in rows if row.get("candidate")],
        "errors": errors,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "duration_ms": int((time.monotonic() - started) * 1000),
    }


def write_outputs(result: dict[str, Any], output_dir: Path | str) -> tuple[Path, Path]:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / "forward_replay_sweep_results.csv"
    json_path = root / "forward_replay_sweep_results.json"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in result.get("results") or []:
            writer.writerow({key: row.get(key, "") for key in CSV_COLUMNS})
    json_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    return csv_path, json_path


def print_table(rows: list[dict[str, Any]]) -> None:
    headers = ["timeframe", "profile", "closed", "wins", "losses", "win_rate", "profit_factor", "expectancy", "max_drawdown", "degraded", "candidate", "score"]
    widths = {header: len(header) for header in headers}
    for row in rows:
        for header in headers:
            widths[header] = max(widths[header], len(str(row.get(header, ""))))
    print(" | ".join(header.ljust(widths[header]) for header in headers))
    print("-+-".join("-" * widths[header] for header in headers))
    for row in rows:
        print(" | ".join(str(row.get(header, "")).ljust(widths[header]) for header in headers))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run isolated MT5 accelerated forward replay sweep from local BTCUSD CSV files.")
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--csv-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "data" / "backtests"))
    parser.add_argument("--timeframes", default=",".join(DEFAULT_TIMEFRAMES), help="Comma-separated timeframes, default M15,M30,H1")
    parser.add_argument("--profiles", default=",".join(default_profiles()), help="Comma-separated filter profiles")
    parser.add_argument("--max-bars", type=int, default=5000)
    parser.add_argument("--initial-balance", type=float, default=100000.0)
    parser.add_argument("--spread-points", type=float, default=30.0)
    parser.add_argument("--slippage-points", type=float, default=5.0)
    parser.add_argument("--commission", type=float, default=0.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    timeframes = [part.strip() for part in args.timeframes.split(",") if part.strip()]
    profiles = [part.strip() for part in args.profiles.split(",") if part.strip()]
    result = run_sweep(
        symbol=args.symbol,
        csv_dir=args.csv_dir,
        timeframes=timeframes,
        profiles=profiles,
        max_bars=args.max_bars,
        initial_balance=args.initial_balance,
        spread_points=args.spread_points,
        slippage_points=args.slippage_points,
        commission=args.commission,
    )
    csv_path, json_path = write_outputs(result, args.output_dir)
    print_table(result["results"])
    print()
    print(f"Saved CSV:  {csv_path}")
    print(f"Saved JSON: {json_path}")
    print(f"Candidates: {len(result['candidates'])}")
    print("broker_touched=false")
    print("order_executed=false")
    return 0 if result["ok"] else 1


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_runtime_context_diagnostics import run_runtime_context_diagnostics  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_runtime_context_diagnostics(symbol=args.symbol, timeframe=args.timeframe, max_age_minutes=args.max_age_minutes)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok") else 1


def _human_summary(result: dict[str, Any]) -> str:
    fields = {
        "runtime_context_status": result.get("runtime_context_status"),
        "symbol": result.get("symbol"),
        "timeframe": result.get("timeframe"),
        "runtime_snapshot_available": result.get("runtime_snapshot_available"),
        "runtime_snapshot_recent": result.get("runtime_snapshot_recent"),
        "runtime_snapshot_complete": result.get("runtime_snapshot_complete"),
        "runtime_snapshot_context": result.get("runtime_snapshot_context"),
        "last_tick_at": result.get("last_tick_at"),
        "bars_last_at": result.get("bars_last_at"),
        "bars_count": result.get("bars_count"),
        "tick_merged_into_bar_context": result.get("tick_merged_into_bar_context"),
        "runtime_context_missing_fields": result.get("runtime_context_missing_fields") or [],
        "data_invented": result.get("data_invented"),
        "forced_context": result.get("forced_context"),
        "broker_touched": result.get("broker_touched"),
        "order_executed": result.get("order_executed"),
        "order_policy": result.get("order_policy"),
    }
    return "\n".join(f"{key}={json.dumps(value, sort_keys=True, default=str)}" for key, value in fields.items())


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnose MT5 runtime context completeness without inventing data.")
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--timeframe", default="M30")
    parser.add_argument("--max-age-minutes", type=float, default=90.0)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())

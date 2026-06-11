from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_controlled_learning_loop_supervisor import (  # noqa: E402
    DEFAULT_LOCK_PATH,
    run_controlled_learning_loop_supervisor,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the safe MT5 controlled paper learning loop supervisor.")
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--timeframe", default="")
    parser.add_argument("--cycles", type=int, default=0)
    parser.add_argument("--interval-seconds", type=int, default=300)
    parser.add_argument("--lock-file", default=str(DEFAULT_LOCK_PATH))
    parser.add_argument("--max-queue-depth", type=int, default=25)
    parser.add_argument("--max-open-shadow-trades", type=int, default=3)
    parser.add_argument("--dry-run-cycles", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    result = run_controlled_learning_loop_supervisor(
        symbol=args.symbol,
        timeframe=args.timeframe,
        cycles=args.cycles,
        interval_seconds=args.interval_seconds,
        lock_path=args.lock_file,
        max_queue_depth=args.max_queue_depth,
        max_open_shadow_trades=args.max_open_shadow_trades,
        dry_run_cycles=args.dry_run_cycles,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
    else:
        _print_summary(result)
    return 0 if result.get("ok", True) else 1


def _print_summary(result: dict[str, Any]) -> None:
    fields = {
        "supervisor_state": result.get("supervisor_state") or "",
        "cycles_requested": result.get("cycles_requested"),
        "cycles_completed": result.get("cycles_completed"),
        "stop_reason": result.get("stop_reason") or "",
        "db_state": result.get("db_state") or {},
        "capital_state": result.get("capital_state") or "",
        "adaptive_state": result.get("adaptive_state") or "",
        "tournament_top_candidate": result.get("tournament_top_candidate"),
        "paper_rotation_recommendation": result.get("paper_rotation_recommendation") or "",
        "paper_rotation_applied": bool(result.get("paper_rotation_applied")),
        "candidate_activated": bool(result.get("candidate_activated")),
        "paper_forward_onboarding_started": bool(result.get("paper_forward_onboarding_started")),
        "gates": _compact_gates(result.get("gates") or []),
        "broker_touched": bool(result.get("broker_touched")),
        "order_executed": bool(result.get("order_executed")),
        "order_policy": result.get("order_policy") or "journal_only_no_broker",
    }
    for key, value in fields.items():
        print(f"{key}={json.dumps(value, sort_keys=True, default=str)}")


def _compact_gates(rows: list[Any]) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        compact.append(
            {
                "name": row.get("name") or "",
                "passed": bool(row.get("passed")),
                "stop_reason": row.get("stop_reason") or "",
            }
        )
    return compact


if __name__ == "__main__":
    raise SystemExit(main())

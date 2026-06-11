from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_autonomous_learning_orchestrator import (  # noqa: E402
    DEFAULT_LOCK_PATH,
    run_autonomous_learning_loop,
    run_autonomous_learning_orchestrator,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the MT5 autonomous paper learning orchestrator safely.")
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--timeframe", default="")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply-paper-rotation", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval-seconds", type=int, default=300)
    parser.add_argument("--max-cycles", type=int)
    parser.add_argument("--lock-file", default=str(DEFAULT_LOCK_PATH))
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-persistent", action="store_true")
    parser.add_argument("--no-shadow-snapshot", action="store_true")
    parser.add_argument("--no-rotation", action="store_true")
    parser.add_argument("--no-trade-learning", action="store_true")
    parser.add_argument("--no-persist-events", action="store_true")
    args = parser.parse_args()

    cycle_kwargs = {
        "symbol": args.symbol,
        "timeframe": args.timeframe,
        "dry_run": args.dry_run,
        "apply_paper_rotation": args.apply_paper_rotation,
        "load_persistent": not args.no_persistent,
        "load_shadow_snapshot": not args.no_shadow_snapshot,
        "load_rotation": not args.no_rotation,
        "run_trade_learning": not args.no_trade_learning,
        "persist_events": not args.no_persist_events,
    }
    if args.loop:
        result = run_autonomous_learning_loop(
            interval_seconds=args.interval_seconds,
            max_cycles=args.max_cycles,
            lock_path=args.lock_file,
            **cycle_kwargs,
        )
    else:
        result = run_autonomous_learning_orchestrator(**cycle_kwargs)

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
    else:
        _print_summary(result)
    return 0 if result.get("ok", True) else 1


def _print_summary(result: dict[str, Any]) -> None:
    cycle = result.get("last_cycle") if isinstance(result.get("last_cycle"), dict) else result
    db_state = cycle.get("db_state") if isinstance(cycle.get("db_state"), dict) else result.get("db_state") or {}
    top = cycle.get("tournament_top_candidate") if isinstance(cycle.get("tournament_top_candidate"), dict) else None
    if top is None and isinstance(result.get("tournament_top_candidate"), dict):
        top = result.get("tournament_top_candidate")
    fields = {
        "learning_state": cycle.get("learning_state") or result.get("learning_state") or "",
        "db_state": db_state,
        "capital_state": cycle.get("capital_state") or result.get("capital_state") or "",
        "adaptive_state": cycle.get("adaptive_state") or result.get("adaptive_state") or "",
        "safe_to_learn": cycle.get("safe_to_learn") if "safe_to_learn" in cycle else result.get("safe_to_learn"),
        "safe_to_open_new_shadow": cycle.get("safe_to_open_new_shadow") if "safe_to_open_new_shadow" in cycle else result.get("safe_to_open_new_shadow"),
        "active_profiles": _compact_profiles(result.get("active_profiles") or []),
        "paused_profiles": _compact_profiles(result.get("paused_profiles") or []),
        "degraded_profiles": _compact_profiles(result.get("degraded_profiles") or []),
        "tournament_top_candidate": _compact_candidate(top),
        "paper_rotation_recommendation": cycle.get("paper_rotation_recommendation") or result.get("paper_rotation_recommendation") or "",
        "paper_rotation_applied": bool(cycle.get("paper_rotation_applied") if "paper_rotation_applied" in cycle else result.get("paper_rotation_applied")),
        "rejected_candidates": _compact_profiles(result.get("rejected_candidates") or []),
        "circuit_breakers": _compact_breakers(result.get("circuit_breakers") or []),
        "recommended_next_action": cycle.get("recommended_next_action") or result.get("recommended_next_action") or "",
        "candidate_activated": bool(result.get("candidate_activated")),
        "paper_forward_onboarding_started": bool(result.get("paper_forward_onboarding_started")),
        "broker_touched": bool(result.get("broker_touched")),
        "order_executed": bool(result.get("order_executed")),
        "order_policy": result.get("order_policy") or "journal_only_no_broker",
    }
    if "cycles_completed" in result:
        fields["loop_status"] = {
            "status": result.get("status"),
            "cycles_completed": result.get("cycles_completed"),
            "lock_active": result.get("lock_active"),
            "lock_path": result.get("lock_path"),
        }
    for key, value in fields.items():
        print(f"{key}={json.dumps(value, sort_keys=True, default=str)}")


def _compact_candidate(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "symbol": row.get("symbol") or "",
        "timeframe": row.get("timeframe") or "",
        "profile": row.get("profile") or row.get("family") or "",
        "tournament_score": row.get("tournament_score"),
        "recommended_action": row.get("recommended_action") or "",
    }


def _compact_profiles(rows: list[Any]) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for row in rows[:8]:
        if not isinstance(row, dict):
            continue
        compact.append(
            {
                "symbol": row.get("symbol") or "",
                "timeframe": row.get("timeframe") or "",
                "profile": row.get("profile") or row.get("family") or "",
                "recommended_action": row.get("recommended_action") or row.get("candidate_status") or "",
            }
        )
    return compact


def _compact_breakers(rows: list[Any]) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for row in rows[:8]:
        if not isinstance(row, dict):
            continue
        compact.append(
            {
                "name": row.get("name") or "",
                "active": bool(row.get("active")),
                "critical": bool(row.get("critical")),
                "reason": row.get("reason") or "",
            }
        )
    return compact


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_autonomous_learning_status import run_autonomous_learning_status  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Show the read-only MT5 autonomous learning status.")
    parser.add_argument("--symbol", default="BTCUSD")
    parser.add_argument("--timeframe", default="")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    result = run_autonomous_learning_status(symbol=args.symbol, timeframe=args.timeframe)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
    else:
        _print_summary(result)
    return 0 if result.get("ok", True) else 1


def _print_summary(result: dict[str, Any]) -> None:
    fields = {
        "status": result.get("status") or "",
        "provider": result.get("provider") or "",
        "db_available": bool(result.get("db_available")),
        "db_degraded": bool(result.get("db_degraded")),
        "tables_ready": bool(result.get("tables_ready")),
        "learning_state": result.get("learning_state") or "",
        "capital_state": result.get("capital_state") or "",
        "adaptive_state": result.get("adaptive_state") or "",
        "safe_to_learn": bool(result.get("safe_to_learn")),
        "safe_to_open_new_shadow": bool(result.get("safe_to_open_new_shadow")),
        "tournament_top_candidate": result.get("tournament_top_candidate"),
        "paper_rotation_recommendation": result.get("paper_rotation_recommendation") or "",
        "paper_rotation_applied": bool(result.get("paper_rotation_applied")),
        "candidate_activated": bool(result.get("candidate_activated")),
        "paper_forward_onboarding_started": bool(result.get("paper_forward_onboarding_started")),
        "recommended_next_action": result.get("recommended_next_action") or "",
        "broker_touched": bool(result.get("broker_touched")),
        "order_executed": bool(result.get("order_executed")),
        "order_policy": result.get("order_policy") or "journal_only_no_broker",
    }
    for key, value in fields.items():
        print(f"{key}={json.dumps(value, sort_keys=True, default=str)}")


if __name__ == "__main__":
    raise SystemExit(main())

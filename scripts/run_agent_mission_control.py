from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_agent_mission_control import run_agent_mission_control  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_agent_mission_control(load_db_state=not args.no_db_probe)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_summary(result))
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report Genesis subagent mission control state. Read-only.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-db-probe", action="store_true", help="Skip live Persistent Intelligence health probe.")
    return parser.parse_args(argv)


def _summary(result: dict[str, Any]) -> str:
    db = result.get("db_state") if isinstance(result.get("db_state"), dict) else {}
    safety = result.get("safety_state") if isinstance(result.get("safety_state"), dict) else {}
    prompts = result.get("next_codex_prompts") or []
    prompt_files = ", ".join(str(item.get("file")) for item in prompts[:5] if isinstance(item, dict))
    return "\n".join(
        [
            "Genesis Agent Mission Control",
            f"status={result.get('status')}",
            f"mission_state={result.get('mission_state')}",
            f"current_phase={result.get('current_phase')}",
            f"urgent_tasks={len(result.get('urgent_tasks') or [])}",
            f"blocked_tasks={len(result.get('blocked_tasks') or [])}",
            f"active_blockers={len(result.get('active_blockers') or [])}",
            f"db_available={db.get('db_available')}",
            f"db_degraded={db.get('db_degraded')}",
            f"tables_ready={db.get('tables_ready')}",
            f"safety_state={safety.get('safety_state')}",
            f"next_prompt_files={prompt_files}",
            f"recommended_next_action={result.get('recommended_next_action')}",
            f"candidate_activated={result.get('candidate_activated')}",
            f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
            f"broker_touched={result.get('broker_touched')}",
            f"order_executed={result.get('order_executed')}",
            f"order_policy={result.get('order_policy')}",
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())

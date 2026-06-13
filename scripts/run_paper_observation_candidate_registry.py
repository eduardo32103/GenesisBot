from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_paper_observation_candidate_registry import (  # noqa: E402
    DEFAULT_PAYLOAD_PATH,
    run_paper_observation_candidate_registry,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_paper_observation_candidate_registry(
        payload_path=args.payload or DEFAULT_PAYLOAD_PATH,
        apply=args.apply,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0 if result.get("payload_valid") else 1


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "MT5 Paper Observation Candidate Registry",
        f"status={result.get('status')}",
        f"payload_valid={result.get('payload_valid')}",
        f"payload_path={result.get('payload_path')}",
        f"candidate_profile={result.get('candidate_profile')}",
        f"symbol={result.get('symbol')}",
        f"timeframe={result.get('timeframe')}",
        f"dry_run={result.get('dry_run')}",
        f"applied={result.get('applied')}",
        f"rows_to_write={len(result.get('rows_to_write') or [])}",
        f"rows_written={result.get('rows_written')}",
        f"research_lesson_prepared={result.get('research_lesson_prepared')}",
        f"profile_state_prepared={result.get('profile_state_prepared')}",
        f"strategy_registry_prepared={result.get('strategy_registry_prepared')}",
        f"candidate_rotation_review_prepared={result.get('candidate_rotation_review_prepared')}",
        f"candidate_activated={result.get('candidate_activated')}",
        f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
        f"applies_to_real_trading={result.get('applies_to_real_trading')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Rows to write:",
    ]
    lines.extend(_row_lines(result.get("rows_to_write") or []))
    lines.extend(["", f"validation_errors={_comma(result.get('validation_errors') or [])}"])
    return "\n".join(lines)


def _row_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    return [f"- {row.get('table')} operation={row.get('operation')}" for row in rows]


def _comma(values: list[Any]) -> str:
    return ",".join(str(value) for value in values) if values else "none"


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register a compact paper observation candidate payload.")
    parser.add_argument("--payload", default=str(DEFAULT_PAYLOAD_PATH), help="Path to compact candidate payload JSON.")
    parser.add_argument("--apply", action="store_true", help="Persist compact rows to Persistent Intelligence.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())

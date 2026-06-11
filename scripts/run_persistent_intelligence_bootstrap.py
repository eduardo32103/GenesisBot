from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_persistent_intelligence_bootstrap import (  # noqa: E402
    persistent_intelligence_bootstrap_status,
    run_persistent_intelligence_bootstrap,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = persistent_intelligence_bootstrap_status() if args.status_only else run_persistent_intelligence_bootstrap()
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0 if result.get("ok", True) else 1


def _human_summary(result: dict[str, Any]) -> str:
    return "\n".join(
        [
            "MT5 Persistent Intelligence Bootstrap",
            f"status={result.get('status')}",
            f"db_available={result.get('db_available')}",
            f"tables_ready={result.get('tables_ready')}",
            f"db_degraded={result.get('db_degraded')}",
            f"seeded_degradation_rows={result.get('seeded_degradation_rows', 0)}",
            f"seeded_rejection_rows={result.get('seeded_rejection_rows', 0)}",
            f"seeded_strategy_rows={result.get('seeded_strategy_rows', 0)}",
            f"seeded_profile_state_rows={result.get('seeded_profile_state_rows', 0)}",
            f"seeded_research_lesson_rows={result.get('seeded_research_lesson_rows', 0)}",
            f"seeded_adaptive_governor_state_rows={result.get('seeded_adaptive_governor_state_rows', 0)}",
            f"seeded_candidate_rotation_rows={result.get('seeded_candidate_rotation_rows', 0)}",
            f"skipped_existing_rows={result.get('skipped_existing_rows', 0)}",
            f"errors={json.dumps(result.get('errors') or [], ensure_ascii=True, sort_keys=True)}",
            f"recommendation={result.get('recommendation')}",
            f"decision={result.get('decision', '')}",
            f"reason={result.get('reason', '')}",
            f"candidate_activated={result.get('candidate_activated')}",
            f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
            f"broker_touched={result.get('broker_touched')}",
            f"order_executed={result.get('order_executed')}",
            f"order_policy={result.get('order_policy')}",
        ]
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed Genesis Persistent Intelligence with stable paper-only knowledge.")
    parser.add_argument("--status-only", action="store_true", help="Report bootstrap readiness without writes.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())

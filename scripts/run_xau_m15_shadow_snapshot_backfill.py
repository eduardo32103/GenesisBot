from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_xau_m15_shadow_snapshot_backfill import (  # noqa: E402
    DEFAULT_SNAPSHOT_PATH,
    run_xau_m15_shadow_snapshot_backfill,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_xau_m15_shadow_snapshot_backfill(snapshot_path=args.snapshot_path, apply=args.apply)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True, default=str))
    else:
        print(_human_summary(result))
    return 0


def _human_summary(result: dict[str, Any]) -> str:
    return "\n".join(
        [
            "MT5 XAUUSD M15 Shadow Snapshot Backfill",
            f"status={result.get('status')}",
            f"reason={result.get('reason')}",
            f"payload_valid={result.get('payload_valid')}",
            f"validation_errors={','.join(result.get('validation_errors') or [])}",
            f"dry_run={result.get('dry_run')}",
            f"applied={result.get('applied')}",
            f"shadow_trade_id={result.get('shadow_trade_id')}",
            f"existing_shadow_found={result.get('existing_shadow_found')}",
            f"existing_open_shadow_count={result.get('existing_open_shadow_count')}",
            f"rows_written={result.get('rows_written')}",
            f"shadow_source={result.get('shadow_source')}",
            f"duplicate_prevented={result.get('duplicate_prevented')}",
            f"candidate_activated={result.get('candidate_activated')}",
            f"paper_forward_onboarding_started={result.get('paper_forward_onboarding_started')}",
            f"broker_touched={result.get('broker_touched')}",
            f"order_executed={result.get('order_executed')}",
            f"order_policy={result.get('order_policy')}",
        ]
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dry-run or apply XAUUSD M15 paper shadow snapshot backfill.")
    parser.add_argument("--snapshot-path", default=str(DEFAULT_SNAPSHOT_PATH))
    parser.add_argument("--apply", action="store_true", help="Apply the validated paper-only snapshot to Persistent Intelligence.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())

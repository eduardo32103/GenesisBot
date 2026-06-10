from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = MT5PersistentIntelligenceStore().compact_old_decision_events(
        older_than_days=args.older_than_days,
        limit=args.limit,
        dry_run=not args.execute,
        confirm_delete_detail=args.confirm_delete_detail,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0


def _human_summary(result: dict[str, Any]) -> str:
    return "\n".join(
        [
            "MT5 Persistent Intelligence Compaction",
            f"dry_run={result.get('dry_run')}",
            f"older_than_days={result.get('older_than_days')}",
            f"rows_scanned={result.get('rows_scanned')}",
            f"rows_summarized={result.get('rows_summarized')}",
            f"rows_deleted={result.get('rows_deleted')}",
            f"critical_data_deleted={result.get('critical_data_deleted')}",
            f"broker_touched={result.get('broker_touched')}",
            f"order_executed={result.get('order_executed')}",
            f"order_policy={result.get('order_policy')}",
            f"retention_plan={json.dumps(result.get('retention_plan') or {}, ensure_ascii=True, sort_keys=True)}",
            f"summary={json.dumps(result.get('summary') or {}, ensure_ascii=True, sort_keys=True)}",
        ]
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize old MT5 decision events without deleting critical data.")
    parser.add_argument("--older-than-days", type=int, default=30)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--execute", action="store_true", help="Record the compaction summary as a research lesson.")
    parser.add_argument("--confirm-delete-detail", action="store_true", help="Reserved for a future explicit delete phase; no deletion happens now.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())

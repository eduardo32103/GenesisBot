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
    result = MT5PersistentIntelligenceStore().healthcheck(write_test_event=args.write_test_event)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0


def _human_summary(result: dict[str, Any]) -> str:
    lines = [
        "MT5 Persistent Intelligence Healthcheck",
        f"db_available={result.get('db_available')}",
        f"db_degraded={result.get('db_degraded')}",
        f"tables_ready={result.get('tables_ready')}",
        f"last_write_at={result.get('last_write_at')}",
        f"failed_writes={result.get('failed_writes')}",
        f"queued_writes={result.get('queued_writes')}",
        f"estimated_storage_mode={result.get('estimated_storage_mode')}",
        f"recommendation={result.get('recommendation')}",
        f"secrets_printed={result.get('secrets_printed')}",
        f"broker_touched={result.get('broker_touched')}",
        f"order_executed={result.get('order_executed')}",
        f"order_policy={result.get('order_policy')}",
        "",
        "Tables:",
    ]
    table_status = result.get("table_status") if isinstance(result.get("table_status"), dict) else {}
    if table_status:
        lines.extend(f"- {table}: {ready}" for table, ready in sorted(table_status.items()))
    else:
        lines.append("- none")
    return "\n".join(lines)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Supabase persistent intelligence readiness without touching broker.")
    parser.add_argument("--write-test-event", action="store_true", help="Write one safe research lesson if all tables are ready.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())

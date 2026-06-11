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
        f"provider={result.get('provider')}",
        f"db_available={result.get('db_available')}",
        f"db_degraded={result.get('db_degraded')}",
        f"tables_ready={result.get('tables_ready')}",
        f"current_probe_ok={result.get('current_probe_ok')}",
        f"db_health_source={result.get('db_health_source')}",
        f"table_count={result.get('table_count')}",
        f"missing_tables={','.join(result.get('missing_tables') or [])}",
        f"missing_tables_count={len(result.get('missing_tables') or [])}",
        f"database_env_ready={((result.get('env') if isinstance(result.get('env'), dict) else {}) or {}).get('database_env_ready')}",
        f"postgres_driver_available={((result.get('env') if isinstance(result.get('env'), dict) else {}) or {}).get('postgres_driver_available')}",
        f"supabase_env_ready={((result.get('env') if isinstance(result.get('env'), dict) else {}) or {}).get('supabase_env_ready')}",
        f"permission_select={((result.get('permission_checks') if isinstance(result.get('permission_checks'), dict) else {}) or {}).get('select')}",
        f"permission_insert={((result.get('permission_checks') if isinstance(result.get('permission_checks'), dict) else {}) or {}).get('insert')}",
        f"permission_upsert={((result.get('permission_checks') if isinstance(result.get('permission_checks'), dict) else {}) or {}).get('upsert')}",
        f"last_write_at={result.get('last_write_at')}",
        f"schema_missing_write_freeze={result.get('schema_missing_write_freeze')}",
        f"writes_frozen={result.get('writes_frozen')}",
        f"schema_check_cooldown_sec={result.get('schema_check_cooldown_sec')}",
        f"last_schema_check_at={result.get('last_schema_check_at')}",
        f"schema_check_cooldown_active={result.get('schema_check_cooldown_active')}",
        f"pool_enabled={result.get('pool_enabled')}",
        f"pool_max_size={result.get('pool_max_size')}",
        f"pool_in_use={result.get('pool_in_use')}",
        f"queue_depth={result.get('queue_depth')}",
        f"queue_max_size={result.get('queue_max_size')}",
        f"failed_writes={result.get('failed_writes')}",
        f"queued_writes={result.get('queued_writes')}",
        f"dropped_noncritical_writes={result.get('dropped_noncritical_writes')}",
        f"suppressed_duplicate_events={result.get('suppressed_duplicate_events')}",
        f"last_db_error_category={result.get('last_db_error_category')}",
        f"last_db_error_at={result.get('last_db_error_at')}",
        f"last_db_error_age_seconds={result.get('last_db_error_age_seconds')}",
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
    parser.add_argument("--write-test-event", action="store_true", help="Write safe HEALTHCHECK rows if all tables are ready.")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())

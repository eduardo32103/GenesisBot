from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.mt5.mt5_persistent_db_doctor import run_persistent_db_doctor  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_persistent_db_doctor(
        apply_schema=args.apply_schema,
        repair=args.repair,
        wait_for_connection=args.wait_for_connection,
        max_connect_attempts=args.max_connect_attempts,
        connect_backoff_seconds=args.connect_backoff_seconds,
        prefer_public_url=args.prefer_public_url,
        use_public_url=args.use_public_url,
        include_rls=args.include_rls,
        statement_timeout_ms=args.statement_timeout_ms,
        verbose_sanitized=args.verbose_sanitized,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0


def _human_summary(result: dict[str, object]) -> str:
    diagnostics = result.get("diagnostics") if isinstance(result.get("diagnostics"), dict) else {}
    apply_result = result.get("apply_result") if isinstance(result.get("apply_result"), dict) else {}
    return "\n".join(
        [
            "MT5 Persistent DB Doctor",
            f"provider={result.get('provider')}",
            f"db_available={result.get('db_available')}",
            f"db_degraded={result.get('db_degraded')}",
            f"tables_ready={result.get('tables_ready')}",
            f"missing_tables={','.join(result.get('missing_tables') or [])}",
            f"schema_missing_write_freeze={result.get('schema_missing_write_freeze')}",
            f"writes_frozen={result.get('writes_frozen')}",
            f"queue_depth={result.get('queue_depth')}",
            f"failed_writes={result.get('failed_writes')}",
            f"failed_writes_total={result.get('failed_writes_total')}",
            f"failed_writes_active={result.get('failed_writes_active')}",
            f"failed_writes_unresolved={result.get('failed_writes_unresolved')}",
            f"failed_writes_critical={result.get('failed_writes_critical')}",
            f"failed_write_semantics_known={result.get('failed_write_semantics_known')}",
            f"dropped_noncritical_writes_total={result.get('dropped_noncritical_writes_total')}",
            f"last_db_error_category={result.get('last_db_error_category')}",
            f"db_readiness_blocking_reason={result.get('db_readiness_blocking_reason')}",
            f"DATABASE_URL_PRESENT={diagnostics.get('DATABASE_URL_PRESENT')}",
            f"DATABASE_PUBLIC_URL_PRESENT={diagnostics.get('DATABASE_PUBLIC_URL_PRESENT')}",
            f"PGHOST_PRESENT={diagnostics.get('PGHOST_PRESENT')}",
            f"can_connect={diagnostics.get('can_connect')}",
            f"current_database_available={diagnostics.get('current_database_available')}",
            f"current_schema_available={diagnostics.get('current_schema_available')}",
            f"current_user_available={diagnostics.get('current_user_available')}",
            f"has_public_schema_privilege={diagnostics.get('has_public_schema_privilege')}",
            f"can_create_table_probe={diagnostics.get('can_create_table_probe')}",
            f"apply_attempted={apply_result.get('attempted')}",
            f"apply_applied={apply_result.get('applied')}",
            f"statement_count={apply_result.get('statement_count')}",
            f"statements_applied={apply_result.get('statements_applied')}",
            f"statements_failed={apply_result.get('statements_failed')}",
            f"first_failed_statement_kind={apply_result.get('first_failed_statement_kind')}",
            f"first_failed_error_sanitized={apply_result.get('first_failed_error_sanitized')}",
            f"apply_failed_reason={apply_result.get('apply_failed_reason')}",
            f"auto_apply_schema_enabled={result.get('auto_apply_schema_enabled')}",
            f"recommendation={result.get('recommendation')}",
            f"decision={result.get('decision')}",
            f"reason={result.get('reason')}",
            f"secrets_printed={result.get('secrets_printed')}",
            f"broker_touched={result.get('broker_touched')}",
            f"order_executed={result.get('order_executed')}",
            f"order_policy={result.get('order_policy')}",
        ]
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Genesis Persistent DB Doctor safely.")
    parser.add_argument("--apply-schema", action="store_true")
    parser.add_argument("--repair", action="store_true")
    parser.add_argument("--no-rls", dest="include_rls", action="store_false", help="Do not include RLS statements. Default.")
    parser.add_argument("--include-rls", dest="include_rls", action="store_true", help="Include optional RLS statements.")
    parser.add_argument("--wait-for-connection", action="store_true")
    parser.add_argument("--max-connect-attempts", type=int, default=10)
    parser.add_argument("--connect-backoff-seconds", type=float, default=5.0)
    parser.add_argument("--prefer-public-url", action="store_true", default=True)
    parser.add_argument("--use-public-url", action="store_true")
    parser.add_argument("--statement-timeout-ms", type=int, default=30000)
    parser.add_argument("--verbose-sanitized", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.set_defaults(include_rls=False)
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_persistent_intelligence_apply_schema import run_apply_schema  # noqa: E402
from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore  # noqa: E402


APPLY_COMMAND = "python scripts/run_persistent_intelligence_apply_schema.py --apply --no-rls"


def run_repair_check(*, apply: bool = False, include_rls: bool = False) -> dict[str, Any]:
    healthcheck = MT5PersistentIntelligenceStore().healthcheck(write_test_event=False)
    missing_tables = list(healthcheck.get("missing_tables") or [])
    last_category = str(healthcheck.get("last_db_error_category") or "")
    database_url_present = bool(os.getenv("DATABASE_URL"))
    recommendations: list[str] = []
    if not database_url_present:
        recommendations.append("configure_DATABASE_URL")
    if missing_tables:
        recommendations.append(APPLY_COMMAND)
    if last_category == "max_connections":
        recommendations.append("reduce PERSISTENT_DB_POOL_MAX_SIZE to 1, keep queue bounded, and stop duplicate learning loops before applying schema")
    if not recommendations:
        recommendations.append("persistent_intelligence_ready" if not healthcheck.get("db_degraded") else "review_healthcheck")

    apply_result: dict[str, Any] = {"attempted": False, "applied": False}
    if apply:
        apply_result = run_apply_schema(apply=True, include_rls=include_rls)
        apply_result["attempted"] = True
    return {
        "ok": True,
        "status": "persistent_intelligence_repair_check_ready",
        "provider": healthcheck.get("provider"),
        "db_available": healthcheck.get("db_available"),
        "db_degraded": healthcheck.get("db_degraded"),
        "tables_ready": healthcheck.get("tables_ready"),
        "missing_tables": missing_tables,
        "schema_missing_write_freeze": healthcheck.get("schema_missing_write_freeze"),
        "pool_enabled": healthcheck.get("pool_enabled"),
        "pool_max_size": healthcheck.get("pool_max_size"),
        "queue_depth": healthcheck.get("queue_depth"),
        "queue_max_size": healthcheck.get("queue_max_size"),
        "failed_writes": healthcheck.get("failed_writes"),
        "queued_writes": healthcheck.get("queued_writes"),
        "dropped_noncritical_writes": healthcheck.get("dropped_noncritical_writes"),
        "suppressed_duplicate_events": healthcheck.get("suppressed_duplicate_events"),
        "last_db_error_category": last_category,
        "recommended_actions": recommendations,
        "apply_command": APPLY_COMMAND,
        "apply_result": apply_result,
        "secrets_printed": False,
        **_safety(),
    }


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = run_repair_check(apply=args.apply, include_rls=args.include_rls)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=True))
    else:
        print(_human_summary(result))
    return 0


def _human_summary(result: dict[str, Any]) -> str:
    return "\n".join(
        [
            "MT5 Persistent Intelligence Repair Check",
            f"provider={result.get('provider')}",
            f"db_available={result.get('db_available')}",
            f"db_degraded={result.get('db_degraded')}",
            f"tables_ready={result.get('tables_ready')}",
            f"missing_tables={','.join(result.get('missing_tables') or [])}",
            f"schema_missing_write_freeze={result.get('schema_missing_write_freeze')}",
            f"pool_enabled={result.get('pool_enabled')}",
            f"pool_max_size={result.get('pool_max_size')}",
            f"queue_depth={result.get('queue_depth')}",
            f"queue_max_size={result.get('queue_max_size')}",
            f"failed_writes={result.get('failed_writes')}",
            f"queued_writes={result.get('queued_writes')}",
            f"dropped_noncritical_writes={result.get('dropped_noncritical_writes')}",
            f"suppressed_duplicate_events={result.get('suppressed_duplicate_events')}",
            f"last_db_error_category={result.get('last_db_error_category')}",
            f"recommended_actions={json.dumps(result.get('recommended_actions') or [], ensure_ascii=True)}",
            f"apply_command={result.get('apply_command')}",
            f"apply_attempted={(result.get('apply_result') or {}).get('attempted')}",
            f"apply_applied={(result.get('apply_result') or {}).get('applied')}",
            f"secrets_printed={result.get('secrets_printed')}",
            f"broker_touched={result.get('broker_touched')}",
            f"order_executed={result.get('order_executed')}",
            f"order_policy={result.get('order_policy')}",
        ]
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Diagnose and optionally repair Genesis MT5 Persistent Intelligence schema readiness.")
    parser.add_argument("--apply", action="store_true", help="Apply schema repair. Default only recommends.")
    parser.add_argument("--no-rls", dest="include_rls", action="store_false", help="Do not include RLS statements. Default.")
    parser.add_argument("--include-rls", dest="include_rls", action="store_true", help="Include optional RLS statements.")
    parser.add_argument("--json", action="store_true")
    parser.set_defaults(include_rls=False)
    return parser.parse_args(argv)


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


if __name__ == "__main__":
    raise SystemExit(main())

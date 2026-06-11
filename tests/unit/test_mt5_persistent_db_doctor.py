from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from api.main import create_app
from api.routes.genesis import get_genesis_mt5_persistent_db_doctor_status
from services.mt5.mt5_persistent_db_doctor import (
    maybe_auto_apply_persistent_schema,
    reset_persistent_db_doctor_for_tests,
    run_persistent_db_doctor,
)
from services.mt5.mt5_persistent_intelligence_store import (
    MT5PersistentIntelligenceStore,
    _reset_persistent_intelligence_counters_for_tests,
)


class MT5PersistentDbDoctorTests(unittest.TestCase):
    def setUp(self) -> None:
        _reset_persistent_intelligence_counters_for_tests()
        reset_persistent_db_doctor_for_tests()

    def test_doctor_diagnoses_missing_schema_without_secrets_or_broker(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_MissingTablesClient())

        with patch("services.mt5.mt5_persistent_db_doctor.run_connection_diagnostics", return_value=_diagnostics(can_connect=True)):
            result = run_persistent_db_doctor(store=store)

        self.assertTrue(result["ok"])
        self.assertFalse(result["tables_ready"])
        self.assertEqual(len(result["missing_tables"]), 11)
        self.assertTrue(result["writes_frozen"])
        self.assertEqual(result["queue_depth"], 0)
        self.assertEqual(result["recommendation"], "apply_schema_sql")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "persistent_intelligence_schema_missing")
        self.assertFalse(result["secrets_printed"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_doctor_apply_schema_uses_direct_apply_and_compacts_output(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_MissingTablesClient())
        apply_payload = {
            "ok": True,
            "provider": "railway_postgres",
            "db_available": True,
            "dry_run": False,
            "applied": True,
            "include_rls": False,
            "connection_source": "DATABASE_PUBLIC_URL",
            "connect_attempts": 1,
            "statement_count": 29,
            "statements_applied": 29,
            "statements_failed": 0,
            "tables_ready": True,
            "missing_tables_after": [],
            "recommendation": "persistent_intelligence_ready",
            "secrets_printed": False,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

        with patch("services.mt5.mt5_persistent_db_doctor.run_connection_diagnostics", return_value=_diagnostics(can_connect=True)), patch(
            "services.mt5.mt5_persistent_db_doctor.run_apply_schema", return_value=apply_payload
        ):
            result = run_persistent_db_doctor(store=store, repair=True)

        self.assertTrue(result["apply_result"]["attempted"])
        self.assertTrue(result["apply_result"]["applied"])
        self.assertEqual(result["apply_result"]["connection_source"], "DATABASE_PUBLIC_URL")
        self.assertEqual(result["apply_result"]["statement_count"], 29)
        self.assertEqual(result["apply_result"]["statements_applied"], 29)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_doctor_preserves_sanitized_schema_apply_failure_details(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_MissingTablesClient())
        apply_payload = {
            "ok": False,
            "provider": "railway_postgres",
            "db_available": True,
            "dry_run": False,
            "applied": False,
            "include_rls": False,
            "connection_source": "DATABASE_URL",
            "connect_attempts": 1,
            "statement_count": 29,
            "statements_applied": 0,
            "statements_failed": 1,
            "first_failed_statement_index": 1,
            "first_failed_statement_kind": "create_extension",
            "first_failed_error_sanitized": "permission denied to create extension pgcrypto",
            "apply_failed_reason": "permission denied to create extension pgcrypto",
            "error_category": "extension_permission_error",
            "tables_ready": False,
            "missing_tables_after": ["mt5_profile_state"],
            "recommendation": "review_pgcrypto_extension_permissions",
            "secrets_printed": False,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

        with patch("services.mt5.mt5_persistent_db_doctor.run_connection_diagnostics", return_value=_diagnostics(can_connect=True)), patch(
            "services.mt5.mt5_persistent_db_doctor.run_apply_schema", return_value=apply_payload
        ):
            result = run_persistent_db_doctor(store=store, repair=True, verbose_sanitized=True)

        self.assertTrue(result["apply_result"]["attempted"])
        self.assertFalse(result["apply_result"]["applied"])
        self.assertEqual(result["apply_result"]["first_failed_statement_kind"], "create_extension")
        self.assertEqual(result["apply_result"]["error_category"], "extension_permission_error")
        self.assertEqual(result["apply_result"]["apply_failed_reason"], "permission denied to create extension pgcrypto")
        self.assertEqual(result["recommendation"], "apply_schema_failed_review_statement_error")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_doctor_recommends_restart_when_max_connections(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_MaxConnectionsClient())

        result = run_persistent_db_doctor(store=store)

        self.assertEqual(result["last_db_error_category"], "max_connections")
        self.assertEqual(result["recommendation"], "restart_db_and_app_then_apply_schema")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_auto_apply_disabled_by_default(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            result = maybe_auto_apply_persistent_schema()

        self.assertEqual(result["status"], "persistent_db_doctor_auto_apply_disabled")
        self.assertFalse(result["auto_apply_schema_enabled"])
        self.assertFalse(result["attempted"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_auto_apply_runs_once_when_enabled_and_then_cools_down(self) -> None:
        payload = {
            "ok": True,
            "provider": "railway_postgres",
            "db_available": True,
            "db_degraded": False,
            "tables_ready": True,
            "missing_tables": [],
            "writes_frozen": False,
            "recommendation": "persistent_intelligence_ready",
            "secrets_printed": False,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

        with patch.dict("os.environ", {"GENESIS_DB_AUTO_APPLY_SCHEMA": "true"}, clear=True), patch(
            "services.mt5.mt5_persistent_db_doctor.run_persistent_db_doctor", return_value=payload
        ) as doctor:
            first = maybe_auto_apply_persistent_schema()
            second = maybe_auto_apply_persistent_schema()

        self.assertEqual(first["recommendation"], "persistent_intelligence_ready")
        self.assertEqual(second["status"], "persistent_db_doctor_auto_apply_cooldown")
        self.assertEqual(doctor.call_count, 1)
        self.assertFalse(second["broker_touched"])
        self.assertFalse(second["order_executed"])
        self.assertEqual(second["order_policy"], "journal_only_no_broker")

    def test_doctor_output_does_not_leak_env_urls(self) -> None:
        secret_url = "postgresql://user:DOCTORSECRET@example.test:5432/db"
        with patch.dict("os.environ", {"DATABASE_URL": secret_url}, clear=True):
            result = run_persistent_db_doctor(store=MT5PersistentIntelligenceStore(client=_MissingTablesClient()))

        serialized = json.dumps(result, sort_keys=True)
        self.assertNotIn(secret_url, serialized)
        self.assertNotIn("DOCTORSECRET", serialized)
        self.assertFalse(result["secrets_printed"])

    def test_db_doctor_endpoint_is_exposed_and_safe(self) -> None:
        app = create_app()
        self.assertEqual(
            app["genesis_mt5_persistent_db_doctor_endpoint"],
            "/api/genesis/mt5/persistent-intelligence/db-doctor",
        )

        with patch("api.routes.genesis.mt5_persistent_db_doctor_status", return_value={"ok": True, "broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}):
            payload = get_genesis_mt5_persistent_db_doctor_status()

        self.assertTrue(payload["ok"])
        self.assertFalse(payload["broker_touched"])
        self.assertFalse(payload["order_executed"])
        self.assertEqual(payload["order_policy"], "journal_only_no_broker")

    def test_db_doctor_endpoint_accepts_explicit_repair_flags(self) -> None:
        payload = {
            "ok": True,
            "repair_requested": True,
            "apply_schema_requested": True,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }
        with patch("api.routes.genesis.mt5_persistent_db_doctor_status", return_value=payload) as doctor:
            result = get_genesis_mt5_persistent_db_doctor_status(
                repair=True,
                apply_schema=True,
                wait_for_connection=True,
                max_connect_attempts=7,
                verbose_sanitized=True,
            )

        self.assertTrue(result["repair_requested"])
        doctor.assert_called_once_with(
            repair=True,
            apply_schema=True,
            wait_for_connection=True,
            max_connect_attempts=7,
            verbose_sanitized=True,
        )
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")


class _MissingTablesClient:
    available = True
    url_configured = True
    key_configured = True

    def table_ready(self, table: str) -> bool:
        raise RuntimeError("relation does not exist")

    def insert(self, table: str, payload: dict[str, object]) -> dict[str, object]:
        raise RuntimeError("relation does not exist")

    def upsert(self, table: str, payload: dict[str, object], *, on_conflict: tuple[str, ...]) -> dict[str, object]:
        raise RuntimeError("relation does not exist")

    def select(self, table: str, *, params: dict[str, str] | None = None) -> list[dict[str, object]]:
        raise RuntimeError("relation does not exist")


class _MaxConnectionsClient(_MissingTablesClient):
    def table_ready(self, table: str) -> bool:
        raise RuntimeError("max clients reached in session mode - max clients are limited to pool_size: 15")


def _diagnostics(*, can_connect: bool) -> dict[str, object]:
    return {
        "provider": "railway_postgres",
        "connection_source": "DATABASE_URL",
        "DATABASE_URL_PRESENT": True,
        "DATABASE_PUBLIC_URL_PRESENT": False,
        "PGHOST_PRESENT": False,
        "PGUSER_PRESENT": False,
        "PGPASSWORD_PRESENT": False,
        "can_parse_url": True,
        "can_connect": can_connect,
        "connect_attempts": 1,
        "error_category": "" if can_connect else "connection_or_schema_error",
        "error_message_sanitized": "",
        "secrets_printed": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


if __name__ == "__main__":
    unittest.main()

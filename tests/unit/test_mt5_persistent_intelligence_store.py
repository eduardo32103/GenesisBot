from __future__ import annotations

import unittest
import json
from unittest.mock import patch

from api.main import create_app
from api.routes.genesis import (
    get_genesis_mt5_persistent_intelligence_failed_write_summary,
    get_genesis_mt5_persistent_intelligence_status,
)
from scripts.run_persistent_db_connection_diagnostics import run_connection_diagnostics
from scripts.run_persistent_intelligence_apply_schema import _set_statement_timeout, _sql_statements, run_apply_schema
from services.mt5.mt5_bridge import mt5_capital_protection_status, mt5_learning_status, mt5_strategy_tournament_status
from services.mt5.mt5_persistent_schema import CREATE_SCHEMA_SQL, REQUIRED_TABLES, get_persistent_intelligence_schema_sql
from services.mt5.mt5_persistent_intelligence_store import (
    MT5PersistentIntelligenceStore,
    RailwayPostgresClient,
    SupabaseRestClient,
    _reset_persistent_intelligence_counters_for_tests,
    persistent_intelligence_schema_freeze_status,
    persist_adaptive_governor_state,
    persist_candidate_rotation_run,
    persist_decision_event,
    persist_research_lesson,
    persist_risk_event,
    persist_shadow_trade,
)
from services.mt5.mt5_persistent_connection_manager import persistent_write_backpressure


class MT5PersistentIntelligenceStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        _reset_persistent_intelligence_counters_for_tests()

    def test_missing_database_config_degrades_safely_and_keeps_no_trade(self) -> None:
        store = MT5PersistentIntelligenceStore(client=SupabaseRestClient(url="", key=""))

        result = store.healthcheck()

        self.assertTrue(result["ok"])
        self.assertFalse(result["db_available"])
        self.assertTrue(result["db_degraded"])
        self.assertFalse(result["tables_ready"])
        self.assertEqual(result["recommendation"], "configure_database_env")
        self.assertEqual(result["provider"], "supabase_rest")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "persistent_intelligence_db_degraded")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_detects_database_url_provider_without_printing_secret(self) -> None:
        database_url = "postgresql://genesis:SUPERPASS@example.railway.internal:5432/railway?sslmode=require"
        with patch.dict(
            "os.environ",
            {
                "DATABASE_URL": database_url,
                "SUPABASE_URL": "https://example.supabase.co",
                "SUPABASE_SERVICE_ROLE_KEY": "SUPASECRET",
            },
            clear=True,
        ), patch.object(RailwayPostgresClient, "table_ready", side_effect=RuntimeError("connection refused")):
            store = MT5PersistentIntelligenceStore()
            result = store.healthcheck()

        serialized = json.dumps(result, sort_keys=True)
        self.assertEqual(result["provider"], "railway_postgres")
        self.assertTrue(result["env"]["database_url_present"])
        self.assertNotIn("SUPERPASS", serialized)
        self.assertNotIn(database_url, serialized)
        self.assertFalse(result["secrets_printed"])

    def test_detects_supabase_provider_when_forced_without_printing_key(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "DATABASE_URL": "postgresql://genesis:SUPERPASS@example.railway.internal:5432/railway",
                "SUPABASE_URL": "https://example.supabase.co",
                "SUPABASE_SERVICE_ROLE_KEY": "SUPASECRET",
                "PERSISTENT_DB_PROVIDER": "supabase_rest",
            },
            clear=True,
        ), patch.object(SupabaseRestClient, "table_ready", side_effect=RuntimeError("connection refused")):
            store = MT5PersistentIntelligenceStore()
            result = store.healthcheck()

        serialized = json.dumps(result, sort_keys=True)
        self.assertEqual(result["provider"], "supabase_rest")
        self.assertTrue(result["env"]["supabase_url_present"])
        self.assertTrue(result["env"]["supabase_secret_key_present"])
        self.assertNotIn("SUPASECRET", serialized)
        self.assertNotIn("SUPERPASS", serialized)

    def test_missing_postgres_driver_recommends_install_driver(self) -> None:
        client = RailwayPostgresClient(database_url="postgresql://user:pass@localhost:5432/db", driver_available=False)
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.healthcheck()

        self.assertEqual(result["provider"], "railway_postgres")
        self.assertFalse(result["db_available"])
        self.assertTrue(result["db_degraded"])
        self.assertEqual(result["recommendation"], "install_postgres_driver")
        self.assertFalse(result["env"]["postgres_driver_available"])

    def test_railway_postgres_client_uses_parameterized_sql(self) -> None:
        client = RailwayPostgresClient(database_url="postgresql://user:pass@localhost:5432/db", driver_available=True)
        captured: list[tuple[str, list[object]]] = []

        with patch.object(client, "_execute", side_effect=lambda sql, values: captured.append((sql, values))):
            client.insert(
                "mt5_decision_events",
                {
                    "symbol": "EURUSD",
                    "timeframe": "H1",
                    "decision": "NO_TRADE",
                    "reason": "risk_governor_block:recent_edge_negative",
                    "broker_touched": False,
                    "order_executed": False,
                    "order_policy": "journal_only_no_broker",
                },
            )

        sql, values = captured[0]
        self.assertIn("%s", sql)
        self.assertNotIn("EURUSD", sql)
        self.assertIn("EURUSD", values)

    def test_railway_postgres_client_reuses_pooled_connection(self) -> None:
        client = _PooledRailwayClient()

        client.insert("mt5_decision_events", _decision_payload("EURUSD", "H1", "first"))
        client.insert("mt5_decision_events", _decision_payload("EURUSD", "H1", "second"))

        self.assertEqual(client.connection_count, 1)
        self.assertEqual(len(client.connections), 1)
        self.assertEqual(client.connections[0].close_count, 0)
        status = client.pool_status()
        self.assertTrue(status["pool_enabled"])
        self.assertEqual(status["pool_max_size"], 2)
        self.assertEqual(status["pool_idle"], 1)

    def test_repeated_preflight_does_not_create_unbounded_db_connections(self) -> None:
        with patch.dict("os.environ", {"PERSISTENT_DB_SCHEMA_CHECK_COOLDOWN_SEC": "60"}):
            _reset_persistent_intelligence_counters_for_tests()
            first_client = _ReadyPooledRailwayClient()
            first = MT5PersistentIntelligenceStore(client=first_client).healthcheck()
            first_execute_count = first_client.connections[0].cursor_obj.execute_count
            second_client = _ReadyPooledRailwayClient()
            second = MT5PersistentIntelligenceStore(client=second_client).healthcheck()

        self.assertTrue(first["db_available"])
        self.assertFalse(first["db_degraded"])
        self.assertTrue(first["tables_ready"])
        self.assertEqual(first["db_health_source"], "current_probe")
        self.assertEqual(first["db_connection_opened"], 1)
        self.assertEqual(first["db_connection_closed"], 0)
        self.assertEqual(first["pool_idle"], 1)
        self.assertTrue(second["db_available"])
        self.assertFalse(second["db_degraded"])
        self.assertTrue(second["tables_ready"])
        self.assertEqual(second["db_health_source"], "schema_cache")
        self.assertTrue(second["schema_check_cooldown_active"])
        self.assertEqual(second["db_connection_opened"], 1)
        self.assertEqual(second["db_connection_closed"], 0)
        self.assertEqual(first_client.connection_count, 1)
        self.assertEqual(second_client.connection_count, 0)
        self.assertEqual(first_client.connections[0].cursor_obj.execute_count, first_execute_count)

    def test_db_connection_closed_on_status_failure(self) -> None:
        client = _FailingPooledRailwayClient()
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.healthcheck()
        status = client.pool_status()

        self.assertTrue(result["db_degraded"])
        self.assertEqual(result["last_db_error_category"], "max_connections")
        self.assertTrue(result["db_pool_exhaustion_detected"])
        self.assertEqual(status["db_connection_opened"], 1)
        self.assertEqual(status["db_connection_closed"], 1)
        self.assertEqual(client.connections[0].close_count, 1)
        self.assertEqual(status["pool_idle"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_missing_tables_degrades_safely_with_apply_schema_recommendation(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_MissingTablesClient())

        result = store.healthcheck()

        self.assertTrue(result["db_available"])
        self.assertTrue(result["db_degraded"])
        self.assertFalse(result["tables_ready"])
        self.assertEqual(len(result["missing_tables"]), len(REQUIRED_TABLES))
        self.assertEqual(result["recommendation"], "apply_schema_sql")
        self.assertTrue(result["env"]["supabase_env_ready"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_schema_sql_contains_all_tables_indexes_and_no_destructive_ops(self) -> None:
        lowered = get_persistent_intelligence_schema_sql().casefold()

        for table in REQUIRED_TABLES:
            self.assertIn(f"create table if not exists public.{table}", lowered)
        self.assertIn("create index if not exists", lowered)
        self.assertNotIn("enable row level security", lowered)
        self.assertIn("enable row level security", get_persistent_intelligence_schema_sql(include_rls=True).casefold())
        self.assertIn("create extension if not exists pgcrypto", lowered)
        for forbidden in ("drop table", "truncate", "delete from"):
            self.assertNotIn(forbidden, lowered)

    def test_apply_schema_dry_run_does_not_execute_schema_writes(self) -> None:
        connection = _FakeSchemaConnection(existing_tables=[])

        result = run_apply_schema(
            apply=False,
            database_url="postgresql://user:SUPERSECRET@example.test:5432/db",
            connect_factory=lambda _: connection,
        )

        serialized = json.dumps(result, sort_keys=True)
        self.assertTrue(result["ok"])
        self.assertTrue(result["db_available"])
        self.assertTrue(result["schema_sql_ready"])
        self.assertTrue(result["dry_run"])
        self.assertFalse(result["applied"])
        self.assertEqual(connection.schema_execute_count, 0)
        self.assertNotIn("SUPERSECRET", serialized)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_sql_splitter_ignores_comment_only_and_preserves_semicolons_in_strings(self) -> None:
        statements = _sql_statements(
            """
            -- leading comment only;
            create table if not exists public.example (value text default 'a;b');
            /* block comment ; */
            create index if not exists idx_example on public.example(value);
            """
        )

        self.assertEqual(len(statements), 2)
        self.assertIn("'a;b'", statements[0])
        self.assertTrue(statements[0].casefold().startswith("create table"))
        self.assertTrue(statements[1].casefold().startswith("create index"))

    def test_apply_schema_executes_statement_by_statement_and_commits_each_statement(self) -> None:
        connection = _FakeSchemaConnection(existing_tables=[])

        result = run_apply_schema(
            apply=True,
            database_url="postgresql://user:SUPERSECRET@example.test:5432/db",
            connect_factory=lambda _: connection,
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["applied"])
        self.assertTrue(result["tables_ready"])
        self.assertGreater(result["statement_count"], len(REQUIRED_TABLES))
        self.assertEqual(connection.schema_execute_count, result["statement_count"])
        self.assertEqual(connection.commit_count, result["statement_count"])
        self.assertEqual(result["statements_applied"], result["statement_count"])
        self.assertEqual(result["statements_failed"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_apply_schema_failure_reports_first_failed_statement_kind(self) -> None:
        connection = _FailingSchemaConnection(
            existing_tables=[],
            fail_on="create extension",
            error=RuntimeError("permission denied to create extension pgcrypto for SUPERSECRET"),
        )

        result = run_apply_schema(
            apply=True,
            database_url="postgresql://user:SUPERSECRET@example.test:5432/db",
            connect_factory=lambda _: connection,
            verbose_sanitized=True,
        )

        serialized = json.dumps(result, sort_keys=True)
        self.assertTrue(result["ok"])
        self.assertTrue(result["db_available"])
        self.assertTrue(result["can_connect"])
        self.assertFalse(result["applied"])
        self.assertEqual(result["statements_applied"], 0)
        self.assertEqual(result["statements_failed"], 1)
        self.assertEqual(result["first_failed_statement_index"], 1)
        self.assertEqual(result["first_failed_statement_kind"], "create_extension")
        self.assertEqual(result["error_category"], "extension_permission_error")
        self.assertIn("permission denied", result["first_failed_error_sanitized"])
        self.assertNotIn("SUPERSECRET", serialized)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_statement_timeout_failure_rolls_back_aborted_transaction(self) -> None:
        connection = _FailingSchemaConnection(
            existing_tables=[],
            fail_on="set statement_timeout",
            error=RuntimeError("syntax error at or near timeout"),
        )

        _set_statement_timeout(connection, 30000)

        self.assertEqual(connection.rollback_count, 1)
        self.assertEqual(connection.commit_count, 0)

    def test_apply_schema_reports_all_connection_attempts_when_waiting(self) -> None:
        attempts = 0

        def failing_connect(_: str) -> object:
            nonlocal attempts
            attempts += 1
            raise RuntimeError("max clients reached in session mode")

        with patch("scripts.run_persistent_intelligence_apply_schema.time.sleep", return_value=None):
            result = run_apply_schema(
                apply=True,
                database_url="postgresql://user:SUPERSECRET@example.test:5432/db",
                connect_factory=failing_connect,
                wait_for_connection=True,
                max_connect_attempts=3,
                connect_backoff_seconds=0.01,
            )

        serialized = json.dumps(result, sort_keys=True)
        self.assertTrue(result["ok"])
        self.assertFalse(result["db_available"])
        self.assertEqual(result["connect_attempts"], 3)
        self.assertEqual(attempts, 3)
        self.assertEqual(result["error_category"], "max_connections")
        self.assertNotIn("SUPERSECRET", serialized)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_apply_schema_can_prefer_database_public_url_without_printing_secret(self) -> None:
        connection = _FakeSchemaConnection(existing_tables=[])
        public_url = "postgresql://public_user:PUBLICSECRET@public.example.test:5432/railway"
        private_url = "postgresql://private_user:PRIVATESECRET@private.example.test:5432/railway"
        captured: list[str] = []

        with patch.dict("os.environ", {"DATABASE_URL": private_url, "DATABASE_PUBLIC_URL": public_url}, clear=True):
            result = run_apply_schema(
                apply=False,
                prefer_public_url=True,
                connect_factory=lambda target: captured.append(target) or connection,
            )

        serialized = json.dumps(result, sort_keys=True)
        self.assertTrue(result["ok"])
        self.assertTrue(result["DATABASE_URL_PRESENT"])
        self.assertTrue(result["DATABASE_PUBLIC_URL_PRESENT"])
        self.assertEqual(result["connection_source"], "DATABASE_PUBLIC_URL")
        self.assertEqual(captured, [public_url])
        self.assertNotIn(public_url, serialized)
        self.assertNotIn(private_url, serialized)
        self.assertNotIn("PUBLICSECRET", serialized)
        self.assertNotIn("PRIVATESECRET", serialized)
        self.assertFalse(result["secrets_printed"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_connection_diagnostics_reports_env_presence_without_printing_secrets(self) -> None:
        public_url = "postgresql://public_user:PUBLICSECRET@public.example.test:5432/railway"
        private_url = "postgresql://private_user:PRIVATESECRET@private.example.test:5432/railway"

        with patch.dict(
            "os.environ",
            {
                "DATABASE_URL": private_url,
                "DATABASE_PUBLIC_URL": public_url,
                "PGHOST": "db.example.test",
                "PGUSER": "pg_user",
                "PGPASSWORD": "PGSECRET",
                "PGDATABASE": "railway",
            },
            clear=True,
        ):
            result = run_connection_diagnostics(
                prefer_public_url=True,
                connect_factory=lambda _: (_ for _ in ()).throw(RuntimeError(f"could not connect with {public_url} PGSECRET")),
            )

        serialized = json.dumps(result, sort_keys=True)
        self.assertFalse(result["ok"])
        self.assertTrue(result["DATABASE_URL_PRESENT"])
        self.assertTrue(result["DATABASE_PUBLIC_URL_PRESENT"])
        self.assertTrue(result["PGHOST_PRESENT"])
        self.assertTrue(result["PGUSER_PRESENT"])
        self.assertTrue(result["PGPASSWORD_PRESENT"])
        self.assertTrue(result["can_parse_url"])
        self.assertFalse(result["can_connect"])
        self.assertNotIn(public_url, serialized)
        self.assertNotIn(private_url, serialized)
        self.assertNotIn("PUBLICSECRET", serialized)
        self.assertNotIn("PRIVATESECRET", serialized)
        self.assertNotIn("PGSECRET", serialized)
        self.assertFalse(result["secrets_printed"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_connection_diagnostics_reports_privilege_probes_without_printing_secrets(self) -> None:
        private_url = "postgresql://private_user:PRIVATESECRET@private.example.test:5432/railway"

        with patch.dict("os.environ", {"DATABASE_URL": private_url}, clear=True):
            result = run_connection_diagnostics(connect_factory=lambda _: _FakeDiagnosticsConnection())

        serialized = json.dumps(result, sort_keys=True)
        self.assertTrue(result["ok"])
        self.assertTrue(result["DATABASE_URL_PRESENT"])
        self.assertTrue(result["can_connect"])
        self.assertTrue(result["current_database_available"])
        self.assertTrue(result["current_schema_available"])
        self.assertTrue(result["current_user_available"])
        self.assertTrue(result["has_public_schema_privilege"])
        self.assertTrue(result["can_create_table_probe"])
        self.assertNotIn(private_url, serialized)
        self.assertNotIn("PRIVATESECRET", serialized)
        self.assertFalse(result["secrets_printed"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_healthcheck_checks_tables_and_can_write_safe_test_event(self) -> None:
        client = _FakeClient()
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.healthcheck(write_test_event=True)

        self.assertTrue(result["db_available"])
        self.assertFalse(result["db_degraded"])
        self.assertTrue(result["tables_ready"])
        self.assertTrue(result["test_write"]["attempted"])
        self.assertTrue(result["test_write"]["ok"])
        self.assertTrue(result["permission_checks"]["insert"])
        self.assertTrue(result["permission_checks"]["upsert"])
        self.assertEqual(client.inserted[-1]["table"], "mt5_decision_events")
        inserted = client.inserted[-1]["payload"]
        self.assertEqual(inserted["symbol"], "HEALTHCHECK")
        self.assertEqual(inserted["timeframe"], "NA")
        self.assertFalse(inserted["broker_touched"])
        self.assertFalse(inserted["order_executed"])
        self.assertEqual(inserted["order_policy"], "journal_only_no_broker")
        self.assertEqual(client.upserted[-1]["table"], "mt5_profile_state")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")
        self.assertTrue(result["pool_enabled"])
        self.assertEqual(result["queue_max_size"], 100)
        self.assertEqual(result["queue_depth"], 0)

    def test_healthcheck_status_mode_does_not_write_without_explicit_test_event(self) -> None:
        client = _FakeClient()
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.healthcheck(write_test_event=False)

        self.assertTrue(result["db_available"])
        self.assertFalse(result["db_degraded"])
        self.assertTrue(result["tables_ready"])
        self.assertTrue(result["status_endpoints_write_free"])
        self.assertFalse(result["test_write"]["attempted"])
        self.assertEqual(client.inserted, [])
        self.assertEqual(client.upserted, [])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_healthcheck_does_not_print_secret_values(self) -> None:
        result = MT5PersistentIntelligenceStore(client=_MissingTablesClient(secret="SUPERSECRET")).healthcheck()
        serialized = json.dumps(result, sort_keys=True)

        self.assertNotIn("SUPERSECRET", serialized)
        self.assertFalse(result["secrets_printed"])
        self.assertFalse(result["env"]["secret_values_printed"])

    def test_upsert_profile_state_forces_real_trading_false(self) -> None:
        client = _FakeClient()
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.upsert_profile_state(
            {
                "symbol": "ETHUSD",
                "timeframe": "M30",
                "profile": "eth_m30_vol_breakout_chop_guard_v1",
                "status": "observation_only",
                "active": True,
                "applies_to_paper_shadow": False,
                "applies_to_real_trading": True,
                "degradation_reason": "early_forward_edge_failed",
            }
        )

        self.assertTrue(result["ok"])
        row = client.upserted[-1]["payload"]
        self.assertEqual(row["symbol"], "ETHUSD")
        self.assertEqual(row["timeframe"], "M30")
        self.assertFalse(row["applies_to_real_trading"])
        self.assertEqual(row["degradation_reason"], "early_forward_edge_failed")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_record_shadow_trade_stores_only_compact_trade_fields(self) -> None:
        client = _FakeClient()
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.record_shadow_trade(
            {
                "shadow_trade_id": "paper-test-1",
                "symbol": "BTCUSD",
                "broker_symbol": "BTCUSD.raw",
                "timeframe": "M30",
                "strategy_profile": "BTCUSD_PAPER_EXPLORATION_V1",
                "source": "paper_observation_shadow_once",
                "side": "buy",
                "entry_price": 100000,
                "stop_loss": 99000,
                "take_profit": 102000,
                "payload": {"raw_ticks": list(range(1000))},
                "broker_touched": True,
                "order_executed": True,
                "order_policy": "unsafe",
            }
        )

        self.assertTrue(result["ok"])
        row = client.upserted[-1]["payload"]
        self.assertEqual(row["shadow_trade_id"], "paper-test-1")
        self.assertEqual(row["broker_symbol"], "BTCUSD.RAW")
        self.assertEqual(row["strategy_profile"], "BTCUSD_PAPER_EXPLORATION_V1")
        self.assertEqual(row["source"], "paper_observation_shadow_once")
        self.assertEqual(row["stop_loss"], 99000.0)
        self.assertEqual(row["take_profit"], 102000.0)
        self.assertNotIn("payload", row)
        self.assertFalse(row["broker_touched"])
        self.assertFalse(row["order_executed"])
        self.assertEqual(row["order_policy"], "journal_only_no_broker")

    def test_record_shadow_trade_missing_optional_column_uses_minimal_fallback_without_queue(self) -> None:
        client = _MissingOptionalShadowTradeColumnClient("bars_since_entry")
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.record_shadow_trade(
            {
                "shadow_trade_id": "xau-open",
                "symbol": "XAUUSD",
                "broker_symbol": "XAUUSD.b",
                "timeframe": "M15",
                "source": "paper_observation_shadow_once",
                "side": "buy",
                "entry_price": 4270.63,
                "bars_since_entry": 0,
                "status": "open",
                "opened_at": "2026-06-18T09:22:47+00:00",
            },
            critical=True,
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["shadow_trade_schema_fallback_used"])
        self.assertIn("bars_since_entry", result["omitted_optional_columns"])
        self.assertEqual(len(client.upserted), 1)
        self.assertNotIn("bars_since_entry", client.upserted[-1]["payload"])
        self.assertEqual(persistent_write_backpressure().status()["queue_depth"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_record_shadow_trade_sample_validity_explicit_column_missing_keeps_metadata(self) -> None:
        client = _MissingOptionalShadowTradeColumnClient("sample_valid")
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.record_shadow_trade(
            {
                "shadow_trade_id": "xau-flat-invalid",
                "symbol": "XAUUSD",
                "broker_symbol": "XAUUSD.b",
                "timeframe": "M15",
                "status": "closed",
                "entry_price": 100.0,
                "exit_price": 100.0,
                "pnl": 0.0,
                "r_multiple": 0.0,
                "exit_reason": "paper_timebox_exit",
                "sample_valid": False,
                "invalid_reason": "market_inactive_or_frozen",
                "metric_exclusion_reason": "excluded_from_winrate_frozen_market",
                "market_active_at_entry": False,
                "market_active_at_exit": False,
                "frozen_market_detected": True,
                "price_movement_observed": False,
            },
            critical=True,
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["shadow_trade_schema_fallback_used"])
        self.assertIn("sample_valid", result["omitted_optional_columns"])
        payload = client.upserted[-1]["payload"]
        self.assertNotIn("sample_valid", payload)
        self.assertIn("sample_validity_metadata", payload)
        metadata = payload["sample_validity_metadata"]
        self.assertEqual(metadata["sample_valid"], False)
        self.assertEqual(metadata["invalid_reason"], "market_inactive_or_frozen")
        self.assertEqual(metadata["metric_exclusion_reason"], "excluded_from_winrate_frozen_market")
        self.assertEqual(persistent_write_backpressure().status()["queue_depth"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_runtime_persistence_helpers_record_compact_events(self) -> None:
        client = _FakeClient()
        store = MT5PersistentIntelligenceStore(client=client)

        decision = persist_decision_event(
            {
                "symbol": "EURUSD",
                "timeframe": "H1",
                "decision": "NO_TRADE",
                "reason": "risk_governor_block:recent_edge_negative",
                "profile": "test_profile",
                "strategy_score": 61.5,
                "risk_state": "blocked",
                "risk_allowed": False,
                "risk_reason": "recent_edge_negative",
                "bars_data": [{"open": 1, "high": 2, "low": 0, "close": 1}],
                "raw_ticks": list(range(100)),
            },
            store=store,
        )
        risk = persist_risk_event(
            {
                "symbol": "EURUSD",
                "timeframe": "H1",
                "risk_state": "blocked",
                "allowed": False,
                "reason": "recent_edge_negative",
                "circuit_breaker": "risk_governor",
                "open_shadow_count": 0,
                "recommended_action": "NO_TRADE",
            },
            store=store,
        )
        shadow = persist_shadow_trade(
            {
                "shadow_trade_id": "paper-test-2",
                "symbol": "EURUSD",
                "timeframe": "H1",
                "profile": "test_profile",
                "side": "sell",
                "status": "closed",
                "pnl": 1.2,
                "r_multiple": 0.8,
                "exit_reason": "take_profit",
            },
            store=store,
        )
        governor = persist_adaptive_governor_state(
            {
                "global_state": "watch",
                "recommended_next_action": "continue_research",
                "active_profiles": [],
                "paused_profiles": [],
                "degraded_profiles": [],
                "circuit_breakers": [],
                "open_shadow_trades": 0,
            },
            store=store,
        )
        lesson = persist_research_lesson(
            {
                "family": "session_vwap_reclaim",
                "symbol": "EURUSD",
                "timeframe": "H1",
                "lesson_type": "rejected_after_real_hardening",
                "failure_pattern": "proxy_false_positive_after_costs",
                "summary": "Hardening failed after costs.",
                "avoid_next": ["eurusd_h1_session_vwap_reclaim"],
                "recommended_next_research_phase": "continue_research",
            },
            store=store,
        )

        self.assertTrue(decision["ok"])
        self.assertTrue(risk["ok"])
        self.assertTrue(shadow["ok"])
        self.assertTrue(governor["ok"])
        self.assertTrue(lesson["ok"])
        decision_row = client.inserted[0]["payload"]
        self.assertNotIn("bars_data", decision_row)
        self.assertNotIn("raw_ticks", decision_row)
        self.assertFalse(decision_row["broker_touched"])
        self.assertFalse(decision_row["order_executed"])
        self.assertEqual(decision_row["order_policy"], "journal_only_no_broker")

    def test_get_degraded_and_rejected_fallback_to_local_registries_when_db_missing(self) -> None:
        store = MT5PersistentIntelligenceStore(client=SupabaseRestClient(url="", key=""))

        degraded = store.get_degraded_profiles()
        rejected = store.get_rejected_research_families()

        self.assertEqual(degraded["source"], "local_forward_profile_degradation_registry")
        self.assertTrue(degraded["degraded_profiles"])
        self.assertEqual(rejected["source"], "local_research_rejection_registry")
        self.assertTrue(rejected["research_rejections"])
        self.assertFalse(degraded["broker_touched"])
        self.assertFalse(rejected["order_executed"])

    def test_compaction_summarizes_old_decisions_without_deleting_details(self) -> None:
        client = _FakeClient(
            selected={
                "mt5_decision_events": [
                    {"decision": "NO_TRADE", "reason": "adaptive_governor:kill_switch"},
                    {"decision": "NO_TRADE", "reason": "adaptive_governor:kill_switch"},
                    {"decision": "NO_TRADE", "reason": "risk_governor_block:recent_edge_negative"},
                ]
            }
        )
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.compact_old_decision_events(dry_run=True)

        self.assertTrue(result["dry_run"])
        self.assertEqual(result["rows_summarized"], 3)
        self.assertEqual(result["rows_deleted"], 0)
        self.assertFalse(result["critical_data_deleted"])
        self.assertIn("decision_events", result["retention_plan"])
        self.assertEqual(result["summary"]["by_decision"]["NO_TRADE"], 3)
        self.assertEqual(len(client.inserted), 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_db_write_failure_degrades_safely_and_marks_critical_no_trade(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_FailingWriteClient())

        result = persist_decision_event(
            {
                "symbol": "BTCUSD",
                "timeframe": "M30",
                "decision": "BUY",
                "reason": "unit_test",
                "profile": "test_profile",
            },
            critical=True,
            store=store,
        )

        self.assertFalse(result["ok"])
        self.assertTrue(result["db_degraded"])
        self.assertTrue(result["queued"])
        self.assertEqual(result["failed_writes"], 1)
        self.assertEqual(result["queued_writes"], 1)
        self.assertEqual(result["queue_depth"], 1)
        self.assertTrue(result["critical_persistence_failed"])
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "persistent_intelligence_db_degraded")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_status_exposes_failed_write_active_total_fields(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_FailingWriteClient())

        persist_decision_event(_decision_payload("BTCUSD", "M30", "summary_fields"), store=store)
        result = store.healthcheck(write_test_event=False)
        summary = get_genesis_mt5_persistent_intelligence_failed_write_summary()

        self.assertEqual(result["failed_writes"], 1)
        self.assertEqual(result["failed_writes_total"], 1)
        self.assertEqual(result["failed_writes_active"], 1)
        self.assertEqual(result["failed_writes_unresolved"], 1)
        self.assertEqual(result["failed_writes_critical"], 0)
        self.assertTrue(result["failed_write_semantics_known"])
        self.assertEqual(result["db_readiness_blocking_reason"], "queue_depth_high")
        self.assertEqual(summary["failed_writes_total"], 1)
        self.assertEqual(summary["failed_writes_active"], 1)
        self.assertEqual(summary["counts_by_criticality"]["noncritical"], 1)
        self.assertTrue(summary["payloads_redacted"])
        self.assertNotIn("summary_fields", json.dumps(summary, sort_keys=True))
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_max_connections_error_marks_db_degraded_and_backoff(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_MaxConnectionsClient())

        result = persist_decision_event(_decision_payload("BTCUSD", "M30", "max_clients"), store=store)
        healthcheck = store.healthcheck()

        self.assertFalse(result["ok"])
        self.assertTrue(result["db_degraded"])
        self.assertEqual(result["last_db_error_category"], "max_connections")
        self.assertTrue(healthcheck["db_degraded"])
        self.assertEqual(healthcheck["last_db_error_category"], "max_connections")
        self.assertEqual(healthcheck["recommendation"], "backoff_persistent_db_writes")
        self.assertFalse(healthcheck["broker_touched"])
        self.assertFalse(healthcheck["order_executed"])
        self.assertEqual(healthcheck["order_policy"], "journal_only_no_broker")

    def test_queue_max_size_limits_noncritical_writes_and_drops_overflow(self) -> None:
        with patch.dict("os.environ", {"PERSISTENT_DB_QUEUE_MAX_SIZE": "2"}):
            _reset_persistent_intelligence_counters_for_tests()
            store = MT5PersistentIntelligenceStore(client=SupabaseRestClient(url="", key=""))

            persist_decision_event(_decision_payload("EURUSD", "H1", "one"), store=store)
            persist_decision_event(_decision_payload("EURUSD", "H1", "two"), store=store)
            third = persist_decision_event(_decision_payload("EURUSD", "H1", "three"), store=store)
            stats = persistent_write_backpressure().status()

        self.assertFalse(third["ok"])
        self.assertFalse(third["queued"])
        self.assertEqual(stats["queue_depth"], 2)
        self.assertEqual(stats["queued_writes"], 2)
        self.assertEqual(stats["dropped_noncritical_writes"], 1)
        self.assertEqual(third["dropped_noncritical_writes"], 1)
        self.assertFalse(third["broker_touched"])
        self.assertFalse(third["order_executed"])
        self.assertEqual(third["order_policy"], "journal_only_no_broker")

    def test_queue_drain_flushes_queued_noncritical_writes_when_db_recovers(self) -> None:
        failing = MT5PersistentIntelligenceStore(client=_FailingWriteClient())
        queued = persist_decision_event(_decision_payload("EURUSD", "H1", "queued_for_drain"), store=failing)
        self.assertTrue(queued["queued"])
        self.assertEqual(queued["queue_depth"], 1)

        client = _FakeClient()
        recovered = MT5PersistentIntelligenceStore(client=client)
        result = recovered.drain_queued_writes()
        healthcheck = recovered.healthcheck()

        self.assertTrue(result["ok"])
        self.assertTrue(result["drain_attempted"])
        self.assertEqual(result["drain"]["before_queue_depth"], 1)
        self.assertEqual(result["drain"]["after_queue_depth"], 0)
        self.assertEqual(result["queue_depth"], 0)
        self.assertEqual(result["queued_writes"], 0)
        self.assertEqual(result["queued_writes_total"], 1)
        self.assertEqual(len(client.inserted), 1)
        self.assertEqual(client.inserted[0]["table"], "mt5_decision_events")
        self.assertTrue(result["queue_drain_succeeded"])
        self.assertEqual(healthcheck["queue_depth"], 0)
        self.assertTrue(healthcheck["queue_drain_succeeded"])
        self.assertTrue(healthcheck["last_queue_drain_attempt_at"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_queue_drain_drops_failed_noncritical_write_without_blocking_queue(self) -> None:
        unavailable = MT5PersistentIntelligenceStore(client=SupabaseRestClient(url="", key=""))
        queued = persist_decision_event(_decision_payload("EURUSD", "H1", "drop_failed_noncritical"), store=unavailable)
        self.assertTrue(queued["queued"])

        failing = MT5PersistentIntelligenceStore(client=_FailingWriteClient())
        result = failing.drain_queued_writes()

        self.assertTrue(result["ok"])
        self.assertTrue(result["drain_attempted"])
        self.assertEqual(result["drain"]["before_queue_depth"], 1)
        self.assertEqual(result["drain"]["after_queue_depth"], 0)
        self.assertEqual(result["drain"]["failed"], 1)
        self.assertEqual(result["drain"]["dropped_noncritical_writes"], 1)
        self.assertEqual(result["queue_depth"], 0)
        self.assertEqual(result["queued_writes"], 0)
        self.assertEqual(result["queued_writes_total"], 1)
        self.assertGreaterEqual(result["dropped_noncritical_writes"], 1)
        self.assertEqual(result["reason"], "persistent_intelligence_queue_drained")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_queue_drain_retains_failed_critical_write_and_returns_no_trade(self) -> None:
        unavailable = MT5PersistentIntelligenceStore(client=SupabaseRestClient(url="", key=""))
        queued = persist_shadow_trade(
            {"shadow_trade_id": "critical-queued", "symbol": "XAUUSD", "timeframe": "M15", "status": "open"},
            critical=True,
            store=unavailable,
        )
        self.assertTrue(queued["queued"])

        failing = MT5PersistentIntelligenceStore(client=_FailingWriteClient())
        result = failing.drain_queued_writes()

        self.assertFalse(result["ok"])
        self.assertTrue(result["drain_attempted"])
        self.assertEqual(result["drain"]["before_queue_depth"], 1)
        self.assertEqual(result["drain"]["after_queue_depth"], 1)
        self.assertEqual(result["drain"]["critical_writes_retained"], 1)
        self.assertEqual(result["queue_depth"], 1)
        self.assertEqual(result["queued_writes"], 1)
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "critical_persistence_queue_retained")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_missing_tables_activate_schema_missing_write_freeze(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_MissingTablesClient())

        healthcheck = store.healthcheck()

        self.assertTrue(healthcheck["schema_missing_write_freeze"])
        self.assertTrue(healthcheck["writes_frozen"])
        self.assertEqual(healthcheck["schema_check_cooldown_sec"], 60.0)
        self.assertTrue(healthcheck["last_schema_check_at"])
        self.assertEqual(set(healthcheck["schema_missing_tables"]), set(REQUIRED_TABLES))
        self.assertEqual(healthcheck["recommendation"], "apply_schema_sql")
        self.assertEqual(healthcheck["queue_depth"], 0)
        self.assertFalse(healthcheck["broker_touched"])
        self.assertFalse(healthcheck["order_executed"])
        self.assertEqual(healthcheck["order_policy"], "journal_only_no_broker")

    def test_schema_missing_write_freeze_reuses_schema_check_cache_during_cooldown(self) -> None:
        with patch.dict("os.environ", {"PERSISTENT_DB_SCHEMA_CHECK_COOLDOWN_SEC": "60"}):
            _reset_persistent_intelligence_counters_for_tests()
            client = _CountingMissingTablesClient()
            store = MT5PersistentIntelligenceStore(client=client)

            first = store.healthcheck()
            first_calls = client.table_ready_calls
            second = store.healthcheck()

        self.assertTrue(first["schema_missing_write_freeze"])
        self.assertTrue(first["writes_frozen"])
        self.assertTrue(second["schema_missing_write_freeze"])
        self.assertTrue(second["writes_frozen"])
        self.assertTrue(second["schema_check_cooldown_active"])
        self.assertEqual(second["last_schema_check_at"], first["last_schema_check_at"])
        self.assertEqual(client.table_ready_calls, first_calls)
        self.assertIn("schema_check_cooldown", second["table_errors"])
        self.assertEqual(second["queue_depth"], 0)
        self.assertFalse(second["broker_touched"])
        self.assertFalse(second["order_executed"])
        self.assertEqual(second["order_policy"], "journal_only_no_broker")

    def test_healthy_schema_check_reuses_cache_during_cooldown(self) -> None:
        with patch.dict("os.environ", {"PERSISTENT_DB_SCHEMA_CHECK_COOLDOWN_SEC": "60"}):
            _reset_persistent_intelligence_counters_for_tests()
            client = _CountingHealthyClient()
            first = MT5PersistentIntelligenceStore(client=client).healthcheck()
            first_calls = client.table_ready_calls
            second = MT5PersistentIntelligenceStore(client=client).healthcheck()

        self.assertTrue(first["db_available"])
        self.assertFalse(first["db_degraded"])
        self.assertTrue(first["tables_ready"])
        self.assertEqual(first["db_health_source"], "current_probe")
        self.assertEqual(second["db_health_source"], "schema_cache")
        self.assertTrue(second["schema_check_cooldown_active"])
        self.assertEqual(second["last_schema_check_at"], first["last_schema_check_at"])
        self.assertEqual(second["table_errors"].get("schema_check_cooldown"), "healthy_schema_cache")
        self.assertEqual(client.table_ready_calls, first_calls)
        self.assertFalse(second["broker_touched"])
        self.assertFalse(second["order_executed"])
        self.assertEqual(second["order_policy"], "journal_only_no_broker")

    def test_schema_missing_write_freeze_drops_noncritical_without_queue_growth(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_MissingTablesClient())
        store.healthcheck()

        result = persist_decision_event(_decision_payload("EURUSD", "H1", "schema_missing"), store=store)

        self.assertFalse(result["ok"])
        self.assertFalse(result["queued"])
        self.assertTrue(result["write"]["schema_missing_write_freeze"])
        self.assertEqual(result["write"]["reason"], "schema_missing_write_freeze")
        self.assertEqual(result["queue_depth"], 0)
        self.assertEqual(result["dropped_noncritical_writes"], 1)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_schema_missing_write_freeze_keeps_queue_depth_zero_across_repeated_noncritical_writes(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_MissingTablesClient())
        store.healthcheck()

        results = [
            persist_decision_event(_decision_payload("EURUSD", "H1", f"schema_missing_{idx}"), store=store)
            for idx in range(5)
        ]
        stats = persistent_write_backpressure().status()

        self.assertTrue(all(not result["queued"] for result in results))
        self.assertEqual(stats["queue_depth"], 0)
        self.assertEqual(stats["last_db_error_category"], "missing_schema")
        self.assertGreaterEqual(stats["dropped_noncritical_writes"], 5)
        self.assertFalse(results[-1]["broker_touched"])
        self.assertFalse(results[-1]["order_executed"])
        self.assertEqual(results[-1]["order_policy"], "journal_only_no_broker")

    def test_preflight_dry_run_missing_schema_does_not_enqueue_mt5_risk_events(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_MissingTablesClient())
        store.healthcheck()

        result = persist_risk_event(
            {
                "symbol": "BTCUSD",
                "timeframe": "M15",
                "risk_state": "preflight_diagnostic",
                "allowed": True,
                "reason": "readiness_preflight_diagnostic",
                "recommended_action": "review_preflight",
                "preflight_only": True,
                "dry_run_no_persist": True,
            },
            store=store,
        )
        stats = persistent_write_backpressure().status()

        self.assertTrue(result["ok"])
        self.assertTrue(result["suppressed_noncritical_risk_event"])
        self.assertTrue(result["write"]["risk_event_persistence_suppressed"])
        self.assertFalse(result["queued"])
        self.assertFalse(result["db_degraded"])
        self.assertEqual(result["queue_depth"], 0)
        self.assertEqual(stats["queue_depth"], 0)
        self.assertEqual(stats["failed_writes_active"], 0)
        self.assertEqual(stats["failed_writes_unresolved"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_preflight_dry_run_db_degraded_suppresses_noncritical_risk_event_persistence(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_FailingWriteClient())

        result = store.record_risk_event(
            {
                "symbol": "BTCUSD",
                "timeframe": "M15",
                "risk_state": "blocked",
                "allowed": False,
                "reason": "db_degraded_preflight",
                "circuit_breaker": "persistent_db_degraded",
                "recommended_action": "NO_TRADE",
                "preflight_only": True,
                "dry_run_no_persist": True,
            }
        )
        stats = persistent_write_backpressure().status()

        self.assertTrue(result["ok"])
        self.assertTrue(result["suppressed_noncritical_risk_event"])
        self.assertTrue(result["risk_event_persistence_suppressed"])
        self.assertFalse(result["queued"])
        self.assertFalse(result["db_degraded"])
        self.assertEqual(stats["queue_depth"], 0)
        self.assertEqual(stats["failed_writes_active"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_preflight_dry_run_returns_suppressed_noncritical_risk_events(self) -> None:
        client = _FakeClient()
        store = MT5PersistentIntelligenceStore(client=client)

        result = persist_risk_event(
            {
                "symbol": "ETHUSD",
                "timeframe": "M15",
                "risk_state": "preflight_diagnostic",
                "allowed": True,
                "reason": "dry_run_preflight",
                "recommended_action": "review_preflight",
                "dry_run": True,
            },
            store=store,
        )

        self.assertEqual(result["event_type"], "risk_event")
        self.assertTrue(result["suppressed_noncritical_risk_event"])
        self.assertTrue(result["write"]["risk_event_persistence_suppressed"])
        self.assertEqual(result["write"]["table"], "mt5_risk_events")
        self.assertEqual(result["write"]["suppression_reason"], "preflight_dry_run")
        self.assertEqual(client.inserted, [])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_repeated_preflight_under_missing_schema_does_not_create_failed_writes_active(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_MissingTablesClient())
        store.healthcheck()

        results = [
            persist_risk_event(
                {
                    "symbol": "BTCUSD",
                    "timeframe": "M15",
                    "risk_state": "preflight_diagnostic",
                    "allowed": True,
                    "reason": f"readiness_preflight_diagnostic_{idx}",
                    "recommended_action": "review_preflight",
                    "preflight_only": True,
                    "dry_run_no_persist": True,
                },
                store=store,
            )
            for idx in range(7)
        ]
        stats = persistent_write_backpressure().status()

        self.assertTrue(all(result["suppressed_noncritical_risk_event"] for result in results))
        self.assertTrue(all(not result["queued"] for result in results))
        self.assertEqual(stats["queue_depth"], 0)
        self.assertEqual(stats["queued_writes"], 0)
        self.assertEqual(stats["failed_writes_active"], 0)
        self.assertEqual(stats["failed_writes_unresolved"], 0)
        self.assertFalse(results[-1]["broker_touched"])
        self.assertFalse(results[-1]["order_executed"])
        self.assertEqual(results[-1]["order_policy"], "journal_only_no_broker")

    def test_repeated_preflight_under_missing_schema_does_not_increase_queue_depth(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_MissingTablesClient())
        store.healthcheck()
        before = persistent_write_backpressure().status()

        for idx in range(5):
            persist_risk_event(
                {
                    "symbol": "ETHUSD",
                    "timeframe": "M15",
                    "risk_state": "preflight_diagnostic",
                    "allowed": True,
                    "reason": f"dry_run_preflight_{idx}",
                    "recommended_action": "review_preflight",
                    "preflight_only": True,
                },
                store=store,
            )
        after = persistent_write_backpressure().status()

        self.assertEqual(before["queue_depth"], 0)
        self.assertEqual(after["queue_depth"], 0)
        self.assertEqual(after["queued_writes"], 0)
        self.assertEqual(after["failed_writes_active"], 0)
        self.assertEqual(after["failed_writes_unresolved"], 0)

    def test_noncritical_risk_event_writer_respects_persist_events_false(self) -> None:
        client = _FakeClient()
        store = MT5PersistentIntelligenceStore(client=client)

        result = persist_risk_event(
            {
                "symbol": "ETHUSD",
                "timeframe": "M15",
                "risk_state": "preflight_diagnostic",
                "allowed": True,
                "reason": "persist_events_false_preflight",
                "recommended_action": "review_preflight",
                "persist_events": False,
            },
            store=store,
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["suppressed_noncritical_risk_event"])
        self.assertTrue(result["write"]["risk_event_persistence_suppressed"])
        self.assertEqual(result["write"]["suppression_reason"], "persist_events_false")
        self.assertEqual(client.inserted, [])
        self.assertEqual(result["queue_depth"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_schema_missing_write_freeze_returns_no_trade_for_critical_write(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_MissingTablesClient())
        store.healthcheck()

        result = persist_shadow_trade(
            {"shadow_trade_id": "paper-schema-missing", "symbol": "BTCUSD", "timeframe": "M30", "status": "open"},
            critical=True,
            store=store,
        )

        self.assertFalse(result["ok"])
        self.assertFalse(result["queued"])
        self.assertTrue(result["critical_persistence_failed"])
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "persistent_intelligence_schema_missing")
        self.assertTrue(result["write"]["schema_missing_write_freeze"])
        self.assertEqual(result["queue_depth"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_critical_queue_full_returns_no_trade_without_unbounded_queue(self) -> None:
        with patch.dict("os.environ", {"PERSISTENT_DB_QUEUE_MAX_SIZE": "0"}):
            _reset_persistent_intelligence_counters_for_tests()
            store = MT5PersistentIntelligenceStore(client=SupabaseRestClient(url="", key=""))

            result = persist_shadow_trade(
                {"shadow_trade_id": "paper-critical-1", "symbol": "BTCUSD", "timeframe": "M30", "status": "open"},
                critical=True,
                store=store,
            )

        self.assertFalse(result["ok"])
        self.assertFalse(result["queued"])
        self.assertTrue(result["critical_persistence_failed"])
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["queue_depth"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_duplicate_no_trade_events_are_coalesced(self) -> None:
        client = _FakeClient()
        store = MT5PersistentIntelligenceStore(client=client)
        payload = _decision_payload("US500", "H1", "risk_governor_block:recent_edge_negative")

        first = persist_decision_event(payload, store=store)
        second = persist_decision_event(payload, store=store)

        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertEqual(len(client.inserted), 1)
        self.assertTrue(second["write"]["suppressed_duplicate"])
        self.assertEqual(second["suppressed_duplicate_events"], 1)
        self.assertFalse(second["broker_touched"])
        self.assertFalse(second["order_executed"])
        self.assertEqual(second["order_policy"], "journal_only_no_broker")

    def test_duplicate_risk_events_are_coalesced(self) -> None:
        client = _FakeClient()
        store = MT5PersistentIntelligenceStore(client=client)
        payload = {
            "symbol": "BTCUSD",
            "timeframe": "H1",
            "risk_state": "blocked",
            "allowed": False,
            "reason": "risk_governor_block",
            "circuit_breaker": "risk_governor",
            "recommended_action": "NO_TRADE",
        }

        first = persist_risk_event(payload, store=store)
        second = persist_risk_event(payload, store=store)

        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertEqual(len(client.inserted), 1)
        self.assertTrue(second["write"]["suppressed_duplicate"])
        self.assertEqual(second["suppressed_duplicate_events"], 1)
        self.assertFalse(second["broker_touched"])
        self.assertFalse(second["order_executed"])
        self.assertEqual(second["order_policy"], "journal_only_no_broker")

    def test_real_paper_observation_can_persist_critical_risk_events_when_not_dry_run(self) -> None:
        client = _FakeClient()
        store = MT5PersistentIntelligenceStore(client=client)

        result = persist_risk_event(
            {
                "symbol": "BTCUSD",
                "timeframe": "M15",
                "risk_state": "critical_safety_exit",
                "allowed": False,
                "reason": "critical_safety_exit",
                "circuit_breaker": "critical_safety_exit",
                "recommended_action": "NO_TRADE",
            },
            critical=True,
            store=store,
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["critical"])
        self.assertFalse(result["suppressed_noncritical_risk_event"])
        self.assertEqual(len(client.inserted), 1)
        self.assertEqual(client.inserted[0]["table"], "mt5_risk_events")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_duplicate_candidate_rotation_runs_are_coalesced(self) -> None:
        client = _FakeClient()
        store = MT5PersistentIntelligenceStore(client=client)
        payload = {
            "recommendation": "continue_research",
            "recommended_candidate": {"symbol": "BTCUSD", "timeframe": "H1", "profile": "btc_h1_review"},
            "candidate_activated": False,
            "paper_forward_onboarding_started": False,
        }

        first = persist_candidate_rotation_run(payload, store=store)
        second = persist_candidate_rotation_run(payload, store=store)

        self.assertTrue(first["ok"])
        self.assertTrue(second["ok"])
        self.assertEqual(len(client.upserted), 1)
        self.assertTrue(second["write"]["suppressed_duplicate"])
        self.assertEqual(second["suppressed_duplicate_events"], 1)
        self.assertFalse(second["broker_touched"])
        self.assertFalse(second["order_executed"])
        self.assertEqual(second["order_policy"], "journal_only_no_broker")

    def test_recent_events_returns_compact_safety_summary(self) -> None:
        client = _FakeClient(
            selected={
                "mt5_decision_events": [
                    {"symbol": "BTCUSD", "timeframe": "M30", "decision": "NO_TRADE", "reason": "blocked"}
                ],
                "mt5_risk_events": [{"symbol": "BTCUSD", "timeframe": "M30", "reason": "blocked"}],
                "mt5_shadow_trades": [{"shadow_trade_id": "paper-1", "symbol": "BTCUSD", "timeframe": "M30"}],
                "mt5_research_lessons": [{"family": "test", "summary": "small"}],
            }
        )
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.recent_events(limit=5)

        self.assertTrue(result["ok"])
        self.assertFalse(result["db_degraded"])
        self.assertEqual(len(result["recent_decisions"]), 1)
        self.assertEqual(len(result["recent_risk_events"]), 1)
        self.assertEqual(len(result["recent_shadow_events"]), 1)
        self.assertEqual(len(result["recent_research_lessons"]), 1)
        self.assertFalse(result["recent_decisions"][0]["broker_touched"])
        self.assertTrue(result["status_endpoints_write_free"])
        self.assertEqual(client.inserted, [])
        self.assertEqual(client.upserted, [])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_shadow_trade_history_is_read_only_and_splits_open_closed(self) -> None:
        client = _FakeClient(
            selected={
                "mt5_shadow_trades": [
                    {"shadow_trade_id": "open-1", "symbol": "XAUUSD", "timeframe": "M15", "status": "open"},
                    {"shadow_trade_id": "closed-1", "symbol": "XAUUSD", "timeframe": "M15", "status": "closed", "closed_at": "2026-06-16T09:30:00+00:00", "pnl": 1.5},
                ]
            }
        )
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.shadow_trade_history(symbol="XAUUSD", timeframe="M15", limit=20)

        self.assertTrue(result["ok"])
        self.assertTrue(result["status_endpoints_write_free"])
        self.assertEqual(result["open_count"], 1)
        self.assertEqual(result["closed_count"], 1)
        self.assertEqual(result["closed_trades"][0]["shadow_trade_id"], "closed-1")
        self.assertEqual(client.inserted, [])
        self.assertEqual(client.upserted, [])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_shadow_trade_history_does_not_select_age_minutes_and_derives_age(self) -> None:
        client = _FakeClient(
            selected={
                "mt5_shadow_trades": [
                    {
                        "shadow_trade_id": "closed-age",
                        "symbol": "XAUUSD",
                        "timeframe": "M15",
                        "status": "closed",
                        "opened_at": "2026-06-16T09:00:00+00:00",
                        "closed_at": "2026-06-16T09:30:00+00:00",
                    }
                ]
            }
        )
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.shadow_trade_history(symbol="XAUUSD", timeframe="M15", limit=20)

        self.assertTrue(result["ok"])
        self.assertTrue(result["age_minutes_derived"])
        self.assertNotIn("age_minutes", client.last_select)
        self.assertEqual(result["closed_trades"][0]["age_minutes"], 30.0)
        self.assertEqual(result["queue_depth"], 0)
        self.assertEqual(result["queued_writes"], 0)
        self.assertTrue(result["status_endpoints_write_free"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_shadow_trade_history_optional_missing_column_falls_back_without_queue_or_db_degrade(self) -> None:
        client = _OptionalColumnMissingHistoryClient(
            missing_column="safety_exit_category",
            selected={
                "mt5_shadow_trades": [
                    {"shadow_trade_id": "closed-fallback", "symbol": "XAUUSD", "timeframe": "M15", "status": "closed"}
                ]
            },
        )
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.shadow_trade_history(symbol="XAUUSD", timeframe="M15", limit=20)

        self.assertTrue(result["ok"])
        self.assertTrue(result["history_available"])
        self.assertTrue(result["history_schema_fallback_used"])
        self.assertIn("safety_exit_category", result["omitted_history_columns"])
        self.assertFalse(result["db_degraded"])
        self.assertEqual(result["queue_depth"], 0)
        self.assertEqual(result["queued_writes"], 0)
        self.assertEqual(persistent_write_backpressure().status()["queue_depth"], 0)
        self.assertEqual(client.inserted, [])
        self.assertEqual(client.upserted, [])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_history_exposes_sample_validity_fields(self) -> None:
        client = _FakeClient(
            selected={
                "mt5_shadow_trades": [
                    {
                        "shadow_trade_id": "closed-sample-fields",
                        "symbol": "BTCUSD",
                        "timeframe": "M15",
                        "status": "closed",
                        "closed_at": "2026-07-07T00:00:00+00:00",
                        "sample_valid": False,
                        "invalid_reason": "market_inactive_or_frozen",
                        "metric_exclusion_reason": "excluded_from_winrate_frozen_market",
                        "market_active_at_entry": False,
                        "market_active_at_exit": False,
                        "frozen_market_detected": True,
                        "price_movement_observed": False,
                    }
                ]
            }
        )
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.shadow_trade_history(symbol="BTCUSD", timeframe="M15", limit=20)

        self.assertTrue(result["ok"])
        self.assertTrue(result["sample_validity_contract_ready"])
        closed = result["closed_trades"][0]
        self.assertEqual(closed["sample_valid"], False)
        self.assertEqual(closed["invalid_reason"], "market_inactive_or_frozen")
        self.assertEqual(closed["metric_exclusion_reason"], "excluded_from_winrate_frozen_market")
        self.assertEqual(closed["market_active_at_entry"], False)
        self.assertEqual(closed["market_active_at_exit"], False)
        self.assertEqual(closed["frozen_market_detected"], True)
        self.assertEqual(closed["price_movement_observed"], False)
        self.assertIn("sample_validity_metadata", closed)

    def test_history_exposes_sample_validity_metadata(self) -> None:
        client = _FakeClient(
            selected={
                "mt5_shadow_trades": [
                    {
                        "shadow_trade_id": "closed-sample-metadata",
                        "symbol": "ETHUSD",
                        "timeframe": "M15",
                        "status": "closed",
                        "closed_at": "2026-07-07T00:00:00+00:00",
                        "sample_validity_metadata": {
                            "sample_valid": False,
                            "invalid_reason": "no_price_movement",
                            "metric_exclusion_reason": "excluded_from_winrate_no_price_movement",
                            "market_active_at_entry": True,
                            "market_active_at_exit": False,
                            "frozen_market_detected": True,
                            "price_movement_observed": False,
                        },
                    }
                ]
            }
        )
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.shadow_trade_history(symbol="ETHUSD", timeframe="M15", limit=20)

        self.assertTrue(result["ok"])
        closed = result["closed_trades"][0]
        metadata = closed["sample_validity_metadata"]
        self.assertEqual(metadata["sample_valid"], False)
        self.assertEqual(closed["sample_valid"], False)
        self.assertEqual(closed["invalid_reason"], "no_price_movement")
        self.assertEqual(closed["metric_exclusion_reason"], "excluded_from_winrate_no_price_movement")
        self.assertEqual(closed["market_active_at_entry"], True)
        self.assertEqual(closed["market_active_at_exit"], False)
        self.assertEqual(closed["frozen_market_detected"], True)
        self.assertEqual(closed["price_movement_observed"], False)

    def test_history_legacy_record_sample_validity_defaults(self) -> None:
        client = _OptionalColumnMissingHistoryClient(
            missing_columns=[
                "sample_valid",
                "invalid_reason",
                "metric_exclusion_reason",
                "market_active_at_entry",
                "market_active_at_exit",
                "frozen_market_detected",
                "price_movement_observed",
                "sample_validity_metadata",
            ],
            selected={
                "mt5_shadow_trades": [
                    {
                        "shadow_trade_id": "legacy-closed",
                        "symbol": "BTCUSD",
                        "timeframe": "M15",
                        "status": "closed",
                        "closed_at": "2026-07-07T00:00:00+00:00",
                    }
                ]
            }
        )
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.shadow_trade_history(symbol="BTCUSD", timeframe="M15", limit=20)

        self.assertTrue(result["ok"])
        self.assertTrue(result["history_schema_fallback_used"])
        self.assertTrue(result["sample_validity_contract_ready"])
        self.assertEqual(result["sample_validity_contract_mode"], "normalized_defaults")
        self.assertIn("sample_valid", result["sample_validity_omitted_columns"])
        self.assertIn("sample_validity_metadata", result["sample_validity_omitted_columns"])
        closed = result["closed_trades"][0]
        self.assertIn("sample_valid", closed)
        self.assertIn("invalid_reason", closed)
        self.assertIn("metric_exclusion_reason", closed)
        self.assertIn("market_active_at_entry", closed)
        self.assertIn("market_active_at_exit", closed)
        self.assertIn("frozen_market_detected", closed)
        self.assertIn("price_movement_observed", closed)
        self.assertIn("sample_validity_metadata", closed)
        self.assertIsNone(closed["sample_valid"])
        self.assertEqual(closed["invalid_reason"], "")
        self.assertEqual(closed["metric_exclusion_reason"], "")
        self.assertIsNone(closed["market_active_at_entry"])
        self.assertIsNone(closed["market_active_at_exit"])
        self.assertIsNone(closed["frozen_market_detected"])
        self.assertIsNone(closed["price_movement_observed"])
        self.assertEqual(closed["sample_validity_metadata"], {})

    def test_history_json_fallback_sample_validity(self) -> None:
        client = _FakeClient(
            selected={
                "mt5_shadow_trades": [
                    {
                        "shadow_trade_id": "details-json-closed",
                        "symbol": "ETHUSD",
                        "timeframe": "M15",
                        "status": "closed",
                        "closed_at": "2026-07-07T00:00:00+00:00",
                        "details": json.dumps(
                            {
                                "sample_validity": {
                                    "sample_valid": False,
                                    "invalid_reason": "market_inactive_or_frozen",
                                    "metric_exclusion_reason": "excluded_from_winrate_frozen_market",
                                    "market_active_at_entry": False,
                                    "market_active_at_exit": False,
                                    "frozen_market_detected": True,
                                    "price_movement_observed": False,
                                }
                            }
                        ),
                    }
                ]
            }
        )
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.shadow_trade_history(symbol="ETHUSD", timeframe="M15", limit=20)

        self.assertTrue(result["ok"])
        closed = result["closed_trades"][0]
        self.assertEqual(closed["sample_valid"], False)
        self.assertEqual(closed["invalid_reason"], "market_inactive_or_frozen")
        self.assertEqual(closed["metric_exclusion_reason"], "excluded_from_winrate_frozen_market")
        self.assertEqual(closed["market_active_at_entry"], False)
        self.assertEqual(closed["market_active_at_exit"], False)
        self.assertEqual(closed["frozen_market_detected"], True)
        self.assertEqual(closed["price_movement_observed"], False)

    def test_frozen_sample_history_roundtrip_preserves_exclusion_reason(self) -> None:
        client = _FakeClient()
        store = MT5PersistentIntelligenceStore(client=client)

        write = store.record_shadow_trade(
            {
                "shadow_trade_id": "frozen-roundtrip",
                "symbol": "XAUUSD",
                "broker_symbol": "XAUUSD.b",
                "timeframe": "M15",
                "status": "closed",
                "entry_price": 100.0,
                "exit_price": 100.0,
                "pnl": 0.0,
                "r_multiple": 0.0,
                "closed_at": "2026-07-07T00:00:00+00:00",
                "exit_reason": "paper_timebox_exit",
                "sample_valid": False,
                "invalid_reason": "market_inactive_or_frozen",
                "metric_exclusion_reason": "excluded_from_winrate_frozen_market",
                "market_active_at_entry": False,
                "market_active_at_exit": False,
                "frozen_market_detected": True,
                "price_movement_observed": False,
            },
            critical=True,
        )
        client.selected["mt5_shadow_trades"] = [client.upserted[-1]["payload"]]

        result = store.shadow_trade_history(symbol="XAUUSD", timeframe="M15", limit=20)

        self.assertTrue(write["ok"])
        self.assertTrue(result["ok"])
        closed = result["closed_trades"][0]
        self.assertEqual(closed["sample_valid"], False)
        self.assertEqual(closed["invalid_reason"], "market_inactive_or_frozen")
        self.assertEqual(closed["metric_exclusion_reason"], "excluded_from_winrate_frozen_market")
        self.assertEqual(closed["sample_validity_metadata"]["metric_exclusion_reason"], "excluded_from_winrate_frozen_market")

    def test_sample_valid_false_available_to_metric_filters(self) -> None:
        client = _FakeClient(
            selected={
                "mt5_shadow_trades": [
                    {
                        "shadow_trade_id": "metric-filter-invalid",
                        "symbol": "BTCUSD",
                        "timeframe": "M15",
                        "status": "closed",
                        "closed_at": "2026-07-07T00:00:00+00:00",
                        "pnl": 0.0,
                        "sample_valid": False,
                        "metric_exclusion_reason": "excluded_from_winrate_frozen_market",
                    }
                ]
            }
        )
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.shadow_trade_history(symbol="BTCUSD", timeframe="M15", limit=20)

        closed = result["closed_trades"][0]
        metric_eligible = [row for row in result["closed_trades"] if row.get("sample_valid") is not False]
        self.assertEqual(closed["sample_valid"], False)
        self.assertEqual(closed["metric_exclusion_reason"], "excluded_from_winrate_frozen_market")
        self.assertEqual(metric_eligible, [])

    def test_recent_events_returns_empty_safe_summary_during_db_backoff(self) -> None:
        client = _FakeClient(selected={"mt5_decision_events": [{"symbol": "BTCUSD"}]})
        store = MT5PersistentIntelligenceStore(client=client)
        persistent_write_backpressure().record_failure(
            "mt5_decision_events",
            {"symbol": "BTCUSD"},
            critical=False,
            reason="max clients reached in session mode",
            duration_ms=1,
        )

        result = store.recent_events(limit=5)

        self.assertTrue(result["ok"])
        self.assertTrue(result["db_degraded"])
        self.assertEqual(result["reason"], "persistent_db_backoff_active")
        self.assertEqual(result["recent_decisions"], [])
        self.assertEqual(client.select_calls, 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_stale_max_connections_error_does_not_block_current_green_probe(self) -> None:
        client = _FakeClient()
        store = MT5PersistentIntelligenceStore(client=client)
        backpressure = persistent_write_backpressure()
        backpressure.record_failure(
            "mt5_decision_events",
            {"symbol": "BTCUSD"},
            critical=False,
            reason="too many clients already",
            duration_ms=1,
        )
        backpressure._backoff_until = 0.0  # simulate expired backoff while preserving diagnostic history

        result = store.healthcheck(write_test_event=False)

        self.assertTrue(result["current_probe_ok"])
        self.assertEqual(result["db_health_source"], "current_probe")
        self.assertTrue(result["stale_error_ignored"])
        self.assertEqual(result["last_db_error_category"], "max_connections")
        self.assertIn("last_db_error_at", result)
        self.assertIn("last_db_error_age_seconds", result)
        self.assertTrue(result["db_available"])
        self.assertFalse(result["db_degraded"])
        self.assertTrue(result["tables_ready"])
        self.assertTrue(result["status_endpoints_write_free"])
        self.assertGreater(len(client.checked_tables), 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_healthcheck_probe_max_connections_does_not_queue_status_write(self) -> None:
        client = _MaxConnectionProbeClient()
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.healthcheck(write_test_event=False)

        self.assertTrue(result["status_endpoints_write_free"])
        self.assertFalse(result["db_available"])
        self.assertTrue(result["db_degraded"])
        self.assertEqual(result["last_db_error_category"], "max_connections")
        self.assertTrue(result["backoff_active"])
        self.assertEqual(result["queue_depth"], 0)
        self.assertEqual(result["queued_writes"], 0)
        self.assertEqual(result["failed_writes"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_recent_events_select_max_connections_does_not_queue_status_write(self) -> None:
        client = _MaxConnectionSelectClient()
        store = MT5PersistentIntelligenceStore(client=client)

        result = store.recent_events(limit=5)

        self.assertTrue(result["ok"])
        self.assertTrue(result["status_endpoints_write_free"])
        self.assertTrue(result["db_degraded"])
        self.assertEqual(result["last_db_error_category"], "max_connections")
        self.assertEqual(result["queue_depth"], 0)
        self.assertEqual(result["queued_writes"], 0)
        self.assertEqual(result["failed_writes"], 0)
        self.assertEqual(client.select_calls, 1)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_stale_missing_schema_error_does_not_block_current_green_probe(self) -> None:
        client = _FakeClient()
        store = MT5PersistentIntelligenceStore(client=client)
        persistent_write_backpressure().record_schema_missing_freeze(
            "mt5_profile_state",
            {"symbol": "BTCUSD"},
            critical=False,
        )

        result = store.healthcheck(write_test_event=False)

        self.assertTrue(result["current_probe_ok"])
        self.assertEqual(result["db_health_source"], "current_probe")
        self.assertTrue(result["stale_error_ignored"])
        self.assertEqual(result["last_db_error_category"], "")
        self.assertTrue(result["db_available"])
        self.assertFalse(result["db_degraded"])
        self.assertTrue(result["tables_ready"])
        self.assertEqual(result["missing_tables"], [])
        self.assertEqual(result["recommendation"], "persistent_intelligence_ready")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_recent_events_short_circuits_during_schema_missing_freeze(self) -> None:
        client = _CountingMissingTablesClient()
        store = MT5PersistentIntelligenceStore(client=client)
        store.healthcheck()

        result = store.recent_events(limit=5)

        self.assertTrue(result["ok"])
        self.assertTrue(result["db_degraded"])
        self.assertEqual(result["reason"], "schema_missing_write_freeze")
        self.assertTrue(result["schema_missing_write_freeze"])
        self.assertTrue(result["writes_frozen"])
        self.assertEqual(result["recent_decisions"], [])
        self.assertEqual(result["recent_risk_events"], [])
        self.assertEqual(result["recent_shadow_events"], [])
        self.assertEqual(result["recent_research_lessons"], [])
        self.assertEqual(client.select_calls, 0)
        self.assertEqual(result["queue_depth"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_recent_events_recovers_when_schema_freeze_is_stale_but_tables_are_ready(self) -> None:
        MT5PersistentIntelligenceStore(client=_MissingTablesClient()).healthcheck()
        self.assertEqual(persistent_write_backpressure().status()["last_db_error_category"], "missing_schema")
        store = MT5PersistentIntelligenceStore(
            client=_FakeClient(selected={"mt5_decision_events": [{"symbol": "BTCUSD", "decision": "NO_TRADE"}]})
        )

        result = store.recent_events(limit=5)
        freeze = persistent_intelligence_schema_freeze_status()

        self.assertFalse(result["db_degraded"])
        self.assertEqual(result.get("reason", ""), "")
        self.assertEqual(len(result["recent_decisions"]), 1)
        self.assertFalse(freeze["writes_frozen"])
        self.assertFalse(freeze["db_degraded"])
        self.assertEqual(persistent_write_backpressure().status()["last_db_error_category"], "")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_learning_and_tournament_endpoints_fast_fail_during_schema_missing_freeze(self) -> None:
        MT5PersistentIntelligenceStore(client=_MissingTablesClient()).healthcheck()

        learning = mt5_learning_status(symbol="BTCUSD")
        tournament = mt5_strategy_tournament_status()
        capital = mt5_capital_protection_status()

        self.assertEqual(learning["learning_state"], "paused_by_db_schema_missing")
        self.assertEqual(learning["decision"], "NO_TRADE")
        self.assertEqual(learning["reason"], "persistent_intelligence_schema_missing")
        self.assertEqual(tournament["status"], "mt5_strategy_tournament_paused_by_db_schema_missing")
        self.assertEqual(tournament["ranked_candidates"], [])
        self.assertEqual(capital["capital_state"], "paused_by_db_schema_missing")
        self.assertFalse(capital["safe_to_trade"])
        for result in (learning, tournament, capital):
            self.assertTrue(result["writes_frozen"])
            self.assertFalse(result["broker_touched"])
            self.assertFalse(result["order_executed"])
            self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_endpoint_is_exposed_and_route_returns_safe_status(self) -> None:
        app = create_app()
        payload = get_genesis_mt5_persistent_intelligence_status()

        self.assertEqual(
            app["genesis_mt5_persistent_intelligence_status_endpoint"],
            "/api/genesis/mt5/persistent-intelligence/status",
        )
        self.assertEqual(
            app["genesis_mt5_persistent_intelligence_recent_events_endpoint"],
            "/api/genesis/mt5/persistent-intelligence/recent-events?limit=10",
        )
        self.assertEqual(
            app["genesis_mt5_persistent_intelligence_failed_write_summary_endpoint"],
            "/api/genesis/mt5/persistent-intelligence/failed-write-summary",
        )
        self.assertEqual(
            app["genesis_mt5_persistent_intelligence_queue_drain_endpoint"],
            "/api/genesis/mt5/persistent-intelligence/queue-drain",
        )
        self.assertTrue(payload["ok"])
        self.assertIn("db_available", payload)
        self.assertFalse(payload["broker_touched"])
        self.assertFalse(payload["order_executed"])
        self.assertEqual(payload["order_policy"], "journal_only_no_broker")


class _FakeClient:
    available = True
    url_configured = True
    key_configured = True

    def __init__(self, *, selected: dict[str, list[dict[str, object]]] | None = None) -> None:
        self.selected = selected or {}
        self.inserted: list[dict[str, object]] = []
        self.upserted: list[dict[str, object]] = []
        self.checked_tables: list[str] = []
        self.select_calls = 0
        self.last_select = ""

    def table_ready(self, table: str) -> bool:
        self.checked_tables.append(table)
        return True

    def insert(self, table: str, payload: dict[str, object]) -> dict[str, object]:
        self.inserted.append({"table": table, "payload": dict(payload)})
        return {"ok": True}

    def upsert(self, table: str, payload: dict[str, object], *, on_conflict: tuple[str, ...]) -> dict[str, object]:
        self.upserted.append({"table": table, "payload": dict(payload), "on_conflict": on_conflict})
        return {"ok": True}

    def select(self, table: str, *, params: dict[str, str] | None = None) -> list[dict[str, object]]:
        self.select_calls += 1
        self.last_select = str((params or {}).get("select") or "")
        return [dict(row) for row in self.selected.get(table, [])]


class _OptionalColumnMissingHistoryClient(_FakeClient):
    def __init__(
        self,
        *,
        missing_column: str = "",
        missing_columns: list[str] | tuple[str, ...] | set[str] | None = None,
        selected: dict[str, list[dict[str, object]]] | None = None,
    ) -> None:
        super().__init__(selected=selected)
        self.missing_column = missing_column
        self.missing_columns = {str(column) for column in (missing_columns or [missing_column]) if str(column)}

    def select(self, table: str, *, params: dict[str, str] | None = None) -> list[dict[str, object]]:
        selected = str((params or {}).get("select") or "")
        selected_columns = {column.strip() for column in selected.split(",")}
        for missing_column in sorted(self.missing_columns):
            if missing_column in selected_columns:
                self.select_calls += 1
                self.last_select = selected
                raise RuntimeError(f'ERROR: column "{missing_column}" does not exist')
        return super().select(table, params=params)


class _MissingOptionalShadowTradeColumnClient(_FakeClient):
    def __init__(self, missing_column: str) -> None:
        super().__init__()
        self.missing_column = missing_column

    def upsert(self, table: str, payload: dict[str, object], *, on_conflict: tuple[str, ...]) -> dict[str, object]:
        if table == "mt5_shadow_trades" and self.missing_column in payload:
            raise RuntimeError(f'ERROR: column "{self.missing_column}" of relation "mt5_shadow_trades" does not exist')
        return super().upsert(table, payload, on_conflict=on_conflict)


class _MissingTablesClient:
    available = True
    url_configured = True
    key_configured = True

    def __init__(self, *, secret: str = "") -> None:
        self.secret = secret

    def table_ready(self, table: str) -> bool:
        raise RuntimeError("relation does not exist")

    def insert(self, table: str, payload: dict[str, object]) -> dict[str, object]:
        raise RuntimeError("relation does not exist")

    def upsert(self, table: str, payload: dict[str, object], *, on_conflict: tuple[str, ...]) -> dict[str, object]:
        raise RuntimeError("relation does not exist")

    def select(self, table: str, *, params: dict[str, str] | None = None) -> list[dict[str, object]]:
        raise RuntimeError("relation does not exist")


class _CountingMissingTablesClient(_MissingTablesClient):
    def __init__(self) -> None:
        super().__init__()
        self.table_ready_calls = 0
        self.select_calls = 0

    def table_ready(self, table: str) -> bool:
        self.table_ready_calls += 1
        return super().table_ready(table)

    def select(self, table: str, *, params: dict[str, str] | None = None) -> list[dict[str, object]]:
        self.select_calls += 1
        return super().select(table, params=params)


class _CountingHealthyClient(_FakeClient):
    def __init__(self) -> None:
        super().__init__()
        self.table_ready_calls = 0

    def table_ready(self, table: str) -> bool:
        self.table_ready_calls += 1
        return super().table_ready(table)


class _FailingWriteClient:
    available = True
    url_configured = True
    key_configured = True

    def table_ready(self, table: str) -> bool:
        return True

    def insert(self, table: str, payload: dict[str, object]) -> dict[str, object]:
        raise RuntimeError("supabase write timeout")

    def upsert(self, table: str, payload: dict[str, object], *, on_conflict: tuple[str, ...]) -> dict[str, object]:
        raise RuntimeError("supabase write timeout")

    def select(self, table: str, *, params: dict[str, str] | None = None) -> list[dict[str, object]]:
        return []


class _MaxConnectionsClient(_FailingWriteClient):
    def insert(self, table: str, payload: dict[str, object]) -> dict[str, object]:
        raise RuntimeError("max clients reached in session mode / max clients limited to pool_size: 15")

    def upsert(self, table: str, payload: dict[str, object], *, on_conflict: tuple[str, ...]) -> dict[str, object]:
        raise RuntimeError("max clients reached in session mode / max clients limited to pool_size: 15")


class _MaxConnectionProbeClient(_FakeClient):
    def table_ready(self, table: str) -> bool:
        self.checked_tables.append(table)
        raise RuntimeError("max clients reached in session mode")


class _MaxConnectionSelectClient(_FakeClient):
    def select(self, table: str, *, params: dict[str, str] | None = None) -> list[dict[str, object]]:
        self.select_calls += 1
        raise RuntimeError("max clients reached in session mode")


class _PooledRailwayClient(RailwayPostgresClient):
    def __init__(self) -> None:
        self.connection_count = 0
        self.connections: list[_FakePgConnection] = []
        super().__init__(
            database_url="postgresql://user:pass@example.test:5432/db",
            driver_available=True,
            pool_max_size=2,
        )

    def _connect(self) -> "_FakePgConnection":
        self.connection_count += 1
        connection = _FakePgConnection()
        self.connections.append(connection)
        return connection


class _ReadyPooledRailwayClient(RailwayPostgresClient):
    def __init__(self) -> None:
        self.connection_count = 0
        self.connections: list[_ReadyPgConnection] = []
        super().__init__(
            database_url="postgresql://user:pass@example.test:5432/readydb",
            driver_available=True,
            pool_max_size=1,
        )

    def _connect(self) -> "_ReadyPgConnection":
        self.connection_count += 1
        connection = _ReadyPgConnection()
        self.connections.append(connection)
        return connection


class _FailingPooledRailwayClient(RailwayPostgresClient):
    def __init__(self) -> None:
        self.connection_count = 0
        self.connections: list[_FailingPgConnection] = []
        super().__init__(
            database_url="postgresql://user:pass@example.test:5432/failingdb",
            driver_available=True,
            pool_max_size=1,
        )

    def _connect(self) -> "_FailingPgConnection":
        self.connection_count += 1
        connection = _FailingPgConnection()
        self.connections.append(connection)
        return connection


class _FakePgConnection:
    def __init__(self) -> None:
        self.close_count = 0
        self.commit_count = 0
        self.rollback_count = 0
        self.cursor_obj = _FakePgCursor()

    def cursor(self) -> "_FakePgCursor":
        return self.cursor_obj

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.close_count += 1


class _ReadyPgConnection(_FakePgConnection):
    def __init__(self) -> None:
        super().__init__()
        self.cursor_obj = _ReadyPgCursor()


class _FailingPgConnection(_FakePgConnection):
    def __init__(self) -> None:
        super().__init__()
        self.cursor_obj = _FailingPgCursor()


class _FakePgCursor:
    description: list[tuple[str]] = []

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, values: tuple[object, ...]) -> None:
        self.executed.append((sql, values))

    def fetchall(self) -> list[tuple[object, ...]]:
        return []


class _ReadyPgCursor(_FakePgCursor):
    description = [("table_name",)]

    def __init__(self) -> None:
        super().__init__()
        self.execute_count = 0

    def execute(self, sql: str, values: tuple[object, ...]) -> None:
        self.execute_count += 1
        super().execute(sql, values)

    def fetchall(self) -> list[tuple[object, ...]]:
        return [("ready",)]


class _FailingPgCursor(_FakePgCursor):
    def execute(self, sql: str, values: tuple[object, ...]) -> None:
        super().execute(sql, values)
        raise RuntimeError("max clients reached in session mode / max clients limited to pool_size: 15")


class _FakeSchemaConnection:
    def __init__(self, *, existing_tables: list[str]) -> None:
        self.existing_tables = existing_tables
        self.cursor_obj = _FakeSchemaCursor(self)
        self.commit_count = 0
        self.rollback_count = 0
        self.close_count = 0
        self.schema_execute_count = 0

    def cursor(self) -> "_FakeSchemaCursor":
        return self.cursor_obj

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.close_count += 1


class _FakeSchemaCursor:
    def __init__(self, connection: _FakeSchemaConnection) -> None:
        self.connection = connection
        self.last_select = False

    def execute(self, sql: str, values: tuple[object, ...] | None = None) -> None:
        del values
        self.last_select = "information_schema.tables" in sql
        if not self.last_select:
            self.connection.schema_execute_count += 1
            lowered = sql.casefold()
            if "create table if not exists public." in lowered:
                table = lowered.split("create table if not exists public.", 1)[1].split(" ", 1)[0].strip()
                if table not in self.connection.existing_tables:
                    self.connection.existing_tables.append(table)

    def fetchall(self) -> list[tuple[object]]:
        if not self.last_select:
            return []
        return [(table,) for table in self.connection.existing_tables]


class _FailingSchemaConnection(_FakeSchemaConnection):
    def __init__(self, *, existing_tables: list[str], fail_on: str, error: Exception) -> None:
        super().__init__(existing_tables=existing_tables)
        self.fail_on = fail_on.casefold()
        self.error = error
        self.cursor_obj = _FailingSchemaCursor(self)


class _FailingSchemaCursor(_FakeSchemaCursor):
    def execute(self, sql: str, values: tuple[object, ...] | None = None) -> None:
        if self.connection.fail_on in sql.casefold():
            raise self.connection.error
        super().execute(sql, values)


class _FakeDiagnosticsConnection:
    def __init__(self) -> None:
        self.cursor_obj = _FakeDiagnosticsCursor()
        self.rollback_count = 0
        self.close_count = 0

    def cursor(self) -> "_FakeDiagnosticsCursor":
        return self.cursor_obj

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.close_count += 1


class _FakeDiagnosticsCursor:
    def __init__(self) -> None:
        self.last_row: tuple[object, ...] | None = None

    def execute(self, sql: str, values: tuple[object, ...] | None = None) -> None:
        del values
        lowered = sql.casefold()
        if "current_database" in lowered:
            self.last_row = ("railway",)
        elif "current_schema" in lowered:
            self.last_row = ("public",)
        elif "current_user" in lowered:
            self.last_row = ("genesis",)
        elif "has_schema_privilege" in lowered:
            self.last_row = (True,)
        elif "create temp table" in lowered:
            self.last_row = None
        else:
            self.last_row = None

    def fetchone(self) -> tuple[object, ...] | None:
        return self.last_row

    def fetchall(self) -> list[tuple[object, ...]]:
        return [self.last_row] if self.last_row else []


def _decision_payload(symbol: str, timeframe: str, reason: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "decision": "NO_TRADE",
        "reason": reason,
        "profile": "unit_profile",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


if __name__ == "__main__":
    unittest.main()

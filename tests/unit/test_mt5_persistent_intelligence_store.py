from __future__ import annotations

import unittest
import json
from unittest.mock import patch

from api.main import create_app
from api.routes.genesis import get_genesis_mt5_persistent_intelligence_status
from services.mt5.mt5_persistent_schema import CREATE_SCHEMA_SQL, REQUIRED_TABLES
from services.mt5.mt5_persistent_intelligence_store import (
    MT5PersistentIntelligenceStore,
    RailwayPostgresClient,
    SupabaseRestClient,
    _reset_persistent_intelligence_counters_for_tests,
    persist_adaptive_governor_state,
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
        lowered = CREATE_SCHEMA_SQL.casefold()

        for table in REQUIRED_TABLES:
            self.assertIn(f"create table if not exists public.{table}", lowered)
        self.assertIn("create index if not exists", lowered)
        self.assertIn("enable row level security", lowered)
        self.assertIn("create extension if not exists pgcrypto", lowered)
        for forbidden in ("drop table", "truncate", "delete from"):
            self.assertNotIn(forbidden, lowered)

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
        self.assertEqual(result["queue_max_size"], 500)
        self.assertEqual(result["queue_depth"], 0)

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
                "timeframe": "M30",
                "strategy_profile": "BTCUSD_PAPER_EXPLORATION_V1",
                "side": "buy",
                "entry_price": 100000,
                "payload": {"raw_ticks": list(range(1000))},
                "broker_touched": True,
                "order_executed": True,
                "order_policy": "unsafe",
            }
        )

        self.assertTrue(result["ok"])
        row = client.upserted[-1]["payload"]
        self.assertEqual(row["shadow_trade_id"], "paper-test-1")
        self.assertNotIn("payload", row)
        self.assertFalse(row["broker_touched"])
        self.assertFalse(row["order_executed"])
        self.assertEqual(row["order_policy"], "journal_only_no_broker")

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
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

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
        return [dict(row) for row in self.selected.get(table, [])]


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


class _FakePgCursor:
    description: list[tuple[str]] = []

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql: str, values: tuple[object, ...]) -> None:
        self.executed.append((sql, values))

    def fetchall(self) -> list[tuple[object, ...]]:
        return []


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

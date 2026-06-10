from __future__ import annotations

import unittest
import json

from api.main import create_app
from api.routes.genesis import get_genesis_mt5_persistent_intelligence_status
from services.mt5.mt5_persistent_schema import CREATE_SCHEMA_SQL, REQUIRED_TABLES
from services.mt5.mt5_persistent_intelligence_store import (
    MT5PersistentIntelligenceStore,
    SupabaseRestClient,
)


class MT5PersistentIntelligenceStoreTests(unittest.TestCase):
    def test_missing_supabase_config_degrades_safely_and_keeps_no_trade(self) -> None:
        store = MT5PersistentIntelligenceStore(client=SupabaseRestClient(url="", key=""))

        result = store.healthcheck()

        self.assertTrue(result["ok"])
        self.assertFalse(result["db_available"])
        self.assertTrue(result["db_degraded"])
        self.assertFalse(result["tables_ready"])
        self.assertEqual(result["recommendation"], "configure_supabase_env")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "persistent_intelligence_db_degraded")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_missing_tables_degrades_safely_with_apply_schema_recommendation(self) -> None:
        store = MT5PersistentIntelligenceStore(client=_MissingTablesClient())

        result = store.healthcheck()

        self.assertTrue(result["db_available"])
        self.assertTrue(result["db_degraded"])
        self.assertFalse(result["tables_ready"])
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

    def test_endpoint_is_exposed_and_route_returns_safe_status(self) -> None:
        app = create_app()
        payload = get_genesis_mt5_persistent_intelligence_status()

        self.assertEqual(
            app["genesis_mt5_persistent_intelligence_status_endpoint"],
            "/api/genesis/mt5/persistent-intelligence/status",
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


if __name__ == "__main__":
    unittest.main()

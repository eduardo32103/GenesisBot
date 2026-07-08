from __future__ import annotations

import inspect
import os
import sys
import types
import unittest
from contextlib import contextmanager
from importlib.machinery import ModuleSpec
from unittest.mock import patch

from services.genesis.memory_store import MemoryStore, memory_store_postgres_resolver_used, safe_postgres_db_fingerprint
from services.mt5 import mt5_legacy_shadow_inspector as inspector
from services.mt5.mt5_legacy_shadow_inspector import inspect_legacy_open_shadows
from services.mt5.mt5_persistent_intelligence_store import RailwayPostgresClient
from services.mt5.mt5_shadow_trading import MT5ShadowTrading


class MT5LegacyShadowInspectorTests(unittest.TestCase):
    def test_memory_store_postgres_resolver_matches_persistent_intelligence(self) -> None:
        calls: list[dict[str, object]] = []
        database_url = "postgresql://genesis%40bot:SUPER%3APASS@db.railway.internal:5432/railway?sslmode=require"

        with _patched_pg8000(lambda **kwargs: calls.append(dict(kwargs)) or FakePgConnection()), patch.dict(
            os.environ,
            {"PERSISTENT_DB_CONNECT_TIMEOUT_SEC": "7"},
            clear=False,
        ):
            store = MemoryStore(database_url=database_url, require_postgres=True, ensure_schema=False)
            RailwayPostgresClient(database_url=database_url, timeout_seconds=7)._connect()

        self.assertEqual(store.backend, "postgres")
        self.assertEqual(store.resolver_used, memory_store_postgres_resolver_used())
        memory_call = calls[0]
        persistent_call = calls[1]
        for key in ("user", "password", "host", "port", "database", "timeout"):
            self.assertEqual(memory_call[key], persistent_call[key])
        self.assertIsNotNone(memory_call["ssl_context"])
        self.assertIsNotNone(persistent_call["ssl_context"])
        self.assertEqual(memory_call["application_name"], "GenesisMemoryStore")
        self.assertEqual(persistent_call["application_name"], "GenesisPersistentIntelligence")

    def test_memory_store_database_url_railway_format_supported(self) -> None:
        calls: list[dict[str, object]] = []
        database_url = "postgresql://genesis:SUPERPASS@db.railway.internal:5432/railway?sslmode=require"

        with _patched_pg8000(lambda **kwargs: calls.append(dict(kwargs)) or FakePgConnection()):
            store = MemoryStore(database_url=database_url, require_postgres=True, ensure_schema=False)

        self.assertEqual(store.backend, "postgres")
        self.assertTrue(store.db_fingerprint.startswith("railway_postgres:"))
        self.assertEqual(calls[0]["host"], "db.railway.internal")
        self.assertEqual(calls[0]["database"], "railway")
        self.assertEqual(calls[0]["port"], 5432)
        self.assertIsNotNone(calls[0]["ssl_context"])

    def test_memory_store_postgres_resolver_does_not_log_secret_url(self) -> None:
        secret_url = "postgresql://user:SUPERSECRET@example.railway.internal:5432/railway?sslmode=require"

        with patch.object(MemoryStore, "_connect_postgres", side_effect=RuntimeError(f"bad url {secret_url}")):
            with self.assertRaises(RuntimeError) as raised:
                MemoryStore(database_url=secret_url, require_postgres=True, ensure_schema=False)

        self.assertEqual(str(raised.exception), "source_unavailable_require_live_db")
        self.assertIsNone(raised.exception.__cause__)
        self.assertNotIn("SUPERSECRET", str(raised.exception))
        self.assertNotIn(secret_url, str(raised.exception))

    def test_legacy_open_shadow_inspector_uses_capital_source_limit_500(self) -> None:
        memory = FakeMemory(
            [
                _event(_trade("shadow-1", "BTCUSD")),
                _event(_trade("shadow-2", "ETHUSD")),
            ]
        )

        result = inspect_legacy_open_shadows(memory=memory, limit=500, status="open")

        self.assertTrue(result["ok"])
        self.assertEqual(result["limit_used"], 500)
        self.assertEqual(result["capital_snapshot_limit"], 500)
        self.assertEqual(result["effective_fetch_limit"], 500)
        self.assertEqual(result["open_shadow_trades_count"], 2)
        self.assertTrue(result["source_matches_capital_protection"])
        self.assertTrue(memory.calls)
        self.assertTrue(all(call == ("mt5_shadow_trades", None, 500) for call in memory.calls))

    def test_inspector_require_live_db_blocks_sqlite_fallback(self) -> None:
        with patch.dict(os.environ, {"DATABASE_URL": ""}), patch.object(
            inspector,
            "MemoryStore",
            side_effect=AssertionError("sqlite fallback must not be opened"),
        ):
            result = inspect_legacy_open_shadows(require_live_db=True)

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "source_unavailable_require_live_db")
        self.assertFalse(result["live_db_detected"])
        self.assertFalse(result["source_matches_capital_protection"])

    def test_inspector_require_live_db_uses_memory_store_postgres_resolver(self) -> None:
        memory = FakeMemory(
            [_event(_trade("shadow-live", "BTCUSD"))],
            backend="postgres",
            database_url="postgresql://user:pass@db.railway.internal:5432/railway?sslmode=require",
        )

        with patch.object(inspector, "MemoryStore", return_value=memory) as memory_cls:
            result = inspect_legacy_open_shadows(require_live_db=True, limit=500, status="open")

        self.assertTrue(result["ok"])
        self.assertEqual(memory_cls.call_args.kwargs["require_postgres"], True)
        self.assertEqual(memory_cls.call_args.kwargs["ensure_schema"], False)
        self.assertEqual(result["resolver_used"], memory_store_postgres_resolver_used())
        self.assertTrue(result["db_fingerprint"].startswith("railway_postgres:"))

    def test_inspector_require_live_db_blocks_when_postgres_connect_fails(self) -> None:
        secret_url = "postgresql://user:SUPERSECRET@example.railway.internal:5432/railway?sslmode=require"

        with patch.dict(os.environ, {"DATABASE_URL": secret_url}, clear=False), patch.object(
            MemoryStore,
            "_connect_postgres",
            side_effect=RuntimeError(f"failed {secret_url}"),
        ):
            result = inspect_legacy_open_shadows(require_live_db=True)

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "source_unavailable_require_live_db")
        self.assertFalse(result["live_db_detected"])
        self.assertNotIn("SUPERSECRET", str(result))
        self.assertNotIn(secret_url, str(result))

    def test_inspector_require_live_db_never_falls_back_to_sqlite(self) -> None:
        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://user:pass@example.railway.internal:5432/railway"}, clear=False), patch.object(
            MemoryStore,
            "_connect_postgres",
            side_effect=RuntimeError("postgres unavailable"),
        ), patch.object(MemoryStore, "_sqlite", side_effect=AssertionError("sqlite fallback must not be used")):
            result = inspect_legacy_open_shadows(require_live_db=True)

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "source_unavailable_require_live_db")
        self.assertEqual(result["backend_type"], "unavailable")

    def test_inspector_default_does_not_ensure_schema(self) -> None:
        memory = FakeMemory([_event(_trade("shadow-readonly", "BTCUSD"))])

        with patch.object(inspector, "MemoryStore", return_value=memory) as memory_cls:
            result = inspect_legacy_open_shadows(limit=500, status="open")

        self.assertTrue(result["ok"])
        self.assertEqual(memory_cls.call_args.kwargs["ensure_schema"], False)
        self.assertEqual(memory_cls.call_args.kwargs["require_postgres"], False)

    def test_inspector_still_uses_ensure_schema_false(self) -> None:
        memory = FakeMemory([], backend="postgres", database_url="postgresql://user:pass@db.railway.internal:5432/railway")

        with patch.object(inspector, "MemoryStore", return_value=memory) as memory_cls:
            inspect_legacy_open_shadows(limit=500, status="open", require_live_db=True)

        self.assertEqual(memory_cls.call_args.kwargs["ensure_schema"], False)
        self.assertEqual(memory_cls.call_args.kwargs["require_postgres"], True)

    def test_inspector_default_is_read_only_no_schema_init(self) -> None:
        memory = FakeMemory([])

        with patch.object(inspector, "MemoryStore", return_value=memory) as memory_cls, patch.object(
            MT5ShadowTrading,
            "snapshot",
            side_effect=RuntimeError("missing schema"),
        ):
            result = inspect_legacy_open_shadows(limit=500, status="open")

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "inspector_source_unavailable_read_only")
        self.assertEqual(memory_cls.call_args.kwargs["ensure_schema"], False)
        self.assertTrue(result["read_only"])
        self.assertFalse(result["mutations_executed"])

    def test_records_sample_redacted_by_default(self) -> None:
        raw_id = "shadow-sensitive-id-123"
        memory = FakeMemory([_event(_trade(raw_id, "BTCUSD"))])

        result = inspect_legacy_open_shadows(memory=memory, limit=500, status="open")

        self.assertTrue(result["ok"])
        record = result["records_sample"][0]
        self.assertNotEqual(record["shadow_trade_id"], raw_id)
        self.assertTrue(str(record["shadow_trade_id"]).startswith("redacted:"))
        self.assertEqual(len(record["shadow_trade_id_hash"]), 16)
        self.assertNotIn(raw_id, str(result))

    def test_include_sensitive_ids_not_allowed_with_require_live_db(self) -> None:
        memory = FakeMemory(
            [_event(_trade("shadow-sensitive-live", "BTCUSD"))],
            backend="postgres",
            database_url="postgresql://user:pass@example/db",
        )

        result = inspect_legacy_open_shadows(
            memory=memory,
            require_live_db=True,
            include_sensitive_ids=True,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "include_sensitive_ids_not_allowed_with_require_live_db")
        self.assertEqual(result["recommendation"], "rerun_without_include_sensitive_ids_for_live_db")
        self.assertEqual(result["records_sample"], [])

    def test_inspector_json_contract_keeps_redacted_records_sample(self) -> None:
        raw_id = "shadow-contract-id-456"
        memory = FakeMemory([_event(_trade(raw_id, "XAUUSD"))])

        result = inspect_legacy_open_shadows(memory=memory, limit=500, status="open")

        expected_keys = {
            "source_name",
            "backend_type",
            "live_db_required",
            "live_db_detected",
            "source_matches_capital_protection",
            "query_description",
            "limit_used",
            "status_filter",
            "scope",
            "open_shadow_trades_count",
            "records_sample",
            "symbols_included",
            "oldest_open_at",
            "newest_open_at",
            "recommendation",
        }
        self.assertTrue(expected_keys.issubset(result.keys()))
        self.assertTrue(result["records_sample"])
        self.assertTrue(str(result["records_sample"][0]["shadow_trade_id"]).startswith("redacted:"))
        self.assertNotIn(raw_id, str(result))

    def test_inspector_reports_source_matches_capital_protection(self) -> None:
        memory = FakeMemory(
            [
                _event(_trade("shadow-live-1", "BTCUSD")),
                _event(_trade("shadow-live-2", "XAUUSD")),
            ],
            backend="postgres",
            database_url="postgresql://user:pass@example/db",
        )

        result = inspect_legacy_open_shadows(memory=memory, limit=500, status="open", require_live_db=True)

        self.assertTrue(result["ok"])
        self.assertTrue(result["live_db_detected"])
        self.assertTrue(result["source_matches_capital_protection"])
        self.assertEqual(result["source_name"], "legacy_memory_store_mt5_shadow_trades")
        self.assertEqual(result["open_shadow_trades_count"], 2)
        self.assertEqual(result["symbols_included"], {"BTCUSD": 1, "XAUUSD": 1})

    def test_inspector_db_fingerprint_is_redacted(self) -> None:
        secret_url = "postgresql://user:SUPERSECRET@example.railway.internal:5432/railway?sslmode=require"
        memory = FakeMemory([_event(_trade("shadow-fp", "BTCUSD"))], backend="postgres", database_url=secret_url)

        result = inspect_legacy_open_shadows(memory=memory, limit=500, status="open", require_live_db=True)

        self.assertTrue(result["ok"])
        self.assertTrue(result["db_fingerprint"].startswith("railway_postgres:"))
        self.assertNotIn("SUPERSECRET", str(result))
        self.assertNotIn("example.railway.internal", str(result))
        self.assertNotIn(secret_url, str(result))
        self.assertIn(result["db_fingerprint"], str(result["source_fingerprint"]) + result["db_fingerprint"])

    def test_inspector_is_read_only_no_mutation(self) -> None:
        memory = FakeMemory([_event(_trade("shadow-ro", "BTCUSD"))])

        result = inspect_legacy_open_shadows(memory=memory, limit=500, status="open")

        self.assertTrue(result["read_only"])
        self.assertFalse(result["mutations_executed"])
        self.assertEqual(memory.mutations, [])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_inspector_does_not_call_order_send(self) -> None:
        source = inspect.getsource(inspector)

        self.assertNotIn("order_send", source)

    def test_inspector_does_not_open_or_close_shadows(self) -> None:
        memory = FakeMemory([_event(_trade("shadow-safe", "ETHUSD"))])
        with patch.object(MT5ShadowTrading, "create_shadow_trade", side_effect=AssertionError("must not open")), patch.object(
            MT5ShadowTrading,
            "close_shadow_trade",
            side_effect=AssertionError("must not close"),
        ):
            result = inspect_legacy_open_shadows(memory=memory, limit=500, status="open")

        self.assertTrue(result["ok"])
        self.assertFalse(result["shadow_opened"])
        self.assertFalse(result["shadow_closed"])


class FakeMemory:
    def __init__(
        self,
        events: list[dict[str, object]],
        *,
        backend: str = "sqlite",
        database_url: str = "",
    ) -> None:
        self.events = events
        self.backend = backend
        self.database_url = database_url
        self.resolver_used = memory_store_postgres_resolver_used()
        self.postgres_resolver_used = memory_store_postgres_resolver_used()
        self.db_fingerprint = safe_postgres_db_fingerprint(database_url)
        self.calls: list[tuple[str | None, str | None, int]] = []
        self.mutations: list[str] = []

    def get_mt5_events(self, collection: str | None = None, symbol: str | None = None, limit: int = 30) -> list[dict[str, object]]:
        self.calls.append((collection, symbol, limit))
        return self.events[:limit]

    def save_event(self, *_args: object, **_kwargs: object) -> dict[str, object]:
        self.mutations.append("save_event")
        raise AssertionError("inspector must not write")


def _event(payload: dict[str, object]) -> dict[str, object]:
    return {
        "event_type": "mt5_shadow_trade",
        "payload": payload,
        "source": "test",
        "confidence": "media",
        "created_at": payload["opened_at"],
    }


def _trade(trade_id: str, symbol: str, *, status: str = "open") -> dict[str, object]:
    return {
        "shadow_trade_id": trade_id,
        "symbol": symbol,
        "normalized_symbol": symbol,
        "timeframe": "M15",
        "strategy_profile": "paper_profile",
        "action": "BUY",
        "status": status,
        "opened_at": "2026-07-07T00:00:00+00:00",
        "updated_at": "2026-07-07T00:00:00+00:00",
        "source": "mt5_bridge",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


class FakePgConnection:
    pass


@contextmanager
def _patched_pg8000(connect: object):
    pg8000 = types.ModuleType("pg8000")
    dbapi = types.ModuleType("pg8000.dbapi")
    dbapi.connect = connect  # type: ignore[attr-defined]
    pg8000.dbapi = dbapi  # type: ignore[attr-defined]
    pg8000.__path__ = []  # type: ignore[attr-defined]
    pg8000.__spec__ = ModuleSpec("pg8000", loader=None, is_package=True)
    dbapi.__spec__ = ModuleSpec("pg8000.dbapi", loader=None)
    with patch.dict(sys.modules, {"pg8000": pg8000, "pg8000.dbapi": dbapi}):
        yield


if __name__ == "__main__":
    unittest.main()

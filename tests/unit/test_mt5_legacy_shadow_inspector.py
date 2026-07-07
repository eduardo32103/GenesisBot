from __future__ import annotations

import inspect
import os
import unittest
from unittest.mock import patch

from services.mt5 import mt5_legacy_shadow_inspector as inspector
from services.mt5.mt5_legacy_shadow_inspector import inspect_legacy_open_shadows
from services.mt5.mt5_shadow_trading import MT5ShadowTrading


class MT5LegacyShadowInspectorTests(unittest.TestCase):
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
        self.assertEqual(result["effective_fetch_limit"], 100)
        self.assertEqual(result["open_shadow_trades_count"], 2)
        self.assertTrue(result["source_matches_capital_protection"])
        self.assertTrue(memory.calls)
        self.assertTrue(all(call == ("mt5_shadow_trades", None, 100) for call in memory.calls))

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

    def test_inspector_default_does_not_ensure_schema(self) -> None:
        memory = FakeMemory([_event(_trade("shadow-readonly", "BTCUSD"))])

        with patch.object(inspector, "MemoryStore", return_value=memory) as memory_cls:
            result = inspect_legacy_open_shadows(limit=500, status="open")

        self.assertTrue(result["ok"])
        self.assertEqual(memory_cls.call_args.kwargs["ensure_schema"], False)
        self.assertEqual(memory_cls.call_args.kwargs["require_postgres"], False)

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


if __name__ == "__main__":
    unittest.main()

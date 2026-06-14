from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from services.mt5.mt5_persistent_schema import CREATE_SCHEMA_SQL
from services.mt5.mt5_xau_m15_paper_observation_readiness import CANDIDATE_PROFILE
from services.mt5.mt5_xau_m15_shadow_snapshot_backfill import run_xau_m15_shadow_snapshot_backfill, validate_xau_m15_shadow_snapshot


class MT5XauM15ShadowSnapshotBackfillTests(unittest.TestCase):
    def test_invalid_empty_snapshot_is_rejected(self) -> None:
        result = validate_xau_m15_shadow_snapshot({"open_count": 0, "trades": []})

        self.assertFalse(result["payload_valid"])
        self.assertIn("open_count_must_equal_1", result["validation_errors"])
        self.assertIn("shadow_trade_missing", result["validation_errors"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_dry_run_valid_snapshot_writes_nothing(self) -> None:
        store = _FakeStore()
        path = _write_temp_snapshot(_valid_snapshot())

        result = run_xau_m15_shadow_snapshot_backfill(snapshot_path=path, apply=False, store=store)

        self.assertTrue(result["payload_valid"])
        self.assertTrue(result["dry_run"])
        self.assertFalse(result["applied"])
        self.assertEqual(result["rows_written"], 0)
        self.assertEqual(result["shadow_trade_id"], "xau-m15-backfill-test")
        self.assertFalse(store.recorded)
        self.assertEqual(result["shadow_source"], "persistent_intelligence_backfill")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_apply_valid_snapshot_upserts_compact_shadow_trade(self) -> None:
        store = _FakeStore()
        path = _write_temp_snapshot(_valid_snapshot())

        result = run_xau_m15_shadow_snapshot_backfill(snapshot_path=path, apply=True, store=store)

        self.assertTrue(result["payload_valid"])
        self.assertFalse(result["dry_run"])
        self.assertTrue(result["applied"])
        self.assertEqual(result["rows_written"], 1)
        self.assertEqual(len(store.recorded), 1)
        row = store.recorded[0]
        self.assertEqual(row["shadow_trade_id"], "xau-m15-backfill-test")
        self.assertEqual(row["symbol"], "XAUUSD")
        self.assertEqual(row["broker_symbol"], "XAUUSD.b")
        self.assertEqual(row["timeframe"], "M15")
        self.assertEqual(row["status"], "open")
        self.assertEqual(row["source"], "paper_observation_shadow_once")
        self.assertEqual(row["strategy_profile"], CANDIDATE_PROFILE)
        self.assertFalse(row["broker_touched"])
        self.assertFalse(row["order_executed"])
        self.assertEqual(row["order_policy"], "journal_only_no_broker")

    def test_duplicate_shadow_id_is_prevented(self) -> None:
        existing = {"shadow_trade_id": "xau-m15-backfill-test", "symbol": "XAUUSD", "timeframe": "M15", "status": "open"}
        store = _FakeStore(existing=[existing])
        path = _write_temp_snapshot(_valid_snapshot())

        result = run_xau_m15_shadow_snapshot_backfill(snapshot_path=path, apply=True, store=store)

        self.assertTrue(result["payload_valid"])
        self.assertFalse(result["applied"])
        self.assertTrue(result["existing_shadow_found"])
        self.assertTrue(result["duplicate_prevented"])
        self.assertEqual(result["reason"], "shadow_already_persisted")
        self.assertEqual(store.recorded, [])

    def test_different_open_shadow_blocks_backfill(self) -> None:
        existing = {"shadow_trade_id": "different-shadow", "symbol": "XAUUSD", "timeframe": "M15", "status": "open"}
        store = _FakeStore(existing=[existing])
        path = _write_temp_snapshot(_valid_snapshot())

        result = run_xau_m15_shadow_snapshot_backfill(snapshot_path=path, apply=True, store=store)

        self.assertEqual(result["status"], "xau_m15_shadow_snapshot_backfill_blocked")
        self.assertEqual(result["reason"], "blocked_multiple_open_shadows")
        self.assertFalse(result["applied"])
        self.assertTrue(result["duplicate_prevented"])
        self.assertEqual(store.recorded, [])

    def test_invalid_safety_flags_are_sanitized_but_real_trading_flag_blocks(self) -> None:
        payload = _valid_snapshot()
        payload["trades"][0]["broker_touched"] = True
        payload["trades"][0]["order_executed"] = True
        payload["trades"][0]["order_policy"] = "unsafe"
        payload["trades"][0]["applies_to_real_trading"] = True

        result = validate_xau_m15_shadow_snapshot(payload)

        self.assertFalse(result["payload_valid"])
        self.assertIn("broker_touched_must_be_false", result["validation_errors"])
        self.assertIn("order_executed_must_be_false", result["validation_errors"])
        self.assertIn("order_policy_must_be_journal_only_no_broker", result["validation_errors"])
        self.assertIn("applies_to_real_trading_must_be_false", result["validation_errors"])
        shadow = result["shadow_trade"]
        self.assertFalse(shadow["broker_touched"])
        self.assertFalse(shadow["order_executed"])
        self.assertEqual(shadow["order_policy"], "journal_only_no_broker")

    def test_schema_contains_restart_safe_shadow_columns(self) -> None:
        self.assertIn("broker_symbol", CREATE_SCHEMA_SQL)
        self.assertIn("strategy_profile", CREATE_SCHEMA_SQL)
        self.assertIn("source", CREATE_SCHEMA_SQL)
        self.assertIn("stop_loss", CREATE_SCHEMA_SQL)
        self.assertIn("take_profit", CREATE_SCHEMA_SQL)


class _FakeStore:
    def __init__(self, *, existing: list[dict[str, object]] | None = None) -> None:
        self.existing = [dict(row) for row in (existing or [])]
        self.recorded: list[dict[str, object]] = []

    def _safe_select(self, table: str, *, params: dict[str, str]) -> dict[str, object]:
        if table == "mt5_shadow_trades":
            return {"ok": True, "db_degraded": False, "rows": [dict(row) for row in self.existing]}
        return {"ok": True, "db_degraded": False, "rows": []}

    def record_shadow_trade(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        self.recorded.append(dict(payload))
        return {"ok": True, "table": "mt5_shadow_trades", "broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}


def _valid_snapshot() -> dict[str, object]:
    return {
        "ok": True,
        "status": "mt5_shadow_trades_open_ready",
        "symbol": "XAUUSD",
        "open_count": 1,
        "trades": [
            {
                "shadow_trade_id": "xau-m15-backfill-test",
                "symbol": "XAUUSD",
                "broker_symbol": "XAUUSD.b",
                "timeframe": "M15",
                "side": "buy",
                "entry_price": 4219.6,
                "stop_loss": 4202.7216,
                "take_profit": 4239.85408,
                "status": "open",
                "source": "paper_observation_shadow_once",
                "strategy_profile": CANDIDATE_PROFILE,
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
                "applies_to_real_trading": False,
            }
        ],
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _write_temp_snapshot(payload: dict[str, object]) -> Path:
    handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, suffix=".json")
    with handle:
        json.dump(payload, handle)
    return Path(handle.name)


if __name__ == "__main__":
    unittest.main()

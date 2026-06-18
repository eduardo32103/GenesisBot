from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from services.mt5.mt5_xau_m15_runtime_open_shadow_backfill import run_xau_m15_runtime_open_shadow_backfill


class MT5XauM15RuntimeOpenShadowBackfillTests(unittest.TestCase):
    def test_requires_confirmation(self) -> None:
        result = run_xau_m15_runtime_open_shadow_backfill(snapshot=_snapshot(), confirm_paper_only_backfill=False, store=_BackfillStore())

        self.assertFalse(result["payload_valid"])
        self.assertEqual(result["reason"], "confirm_paper_only_backfill_required")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_validates_snapshot_open_count_one(self) -> None:
        snapshot = {**_snapshot(), "open_count": 2, "merged_open_count": 2}

        result = run_xau_m15_runtime_open_shadow_backfill(snapshot=snapshot, confirm_paper_only_backfill=True, store=_BackfillStore())

        self.assertEqual(result["status"], "xau_m15_runtime_open_shadow_backfill_rejected")
        self.assertEqual(result["reason"], "snapshot_open_count_not_one")
        self.assertEqual(result["rows_written"], 0)

    def test_rejects_broker_or_order_flags(self) -> None:
        snapshot = _snapshot()
        snapshot["trades"][0]["order_executed"] = True

        result = run_xau_m15_runtime_open_shadow_backfill(snapshot=snapshot, confirm_paper_only_backfill=True, store=_BackfillStore())

        self.assertEqual(result["status"], "xau_m15_runtime_open_shadow_backfill_rejected")
        self.assertEqual(result["reason"], "trade_safety_flags_invalid")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_upserts_one_runtime_open_shadow(self) -> None:
        store = _BackfillStore()

        result = run_xau_m15_runtime_open_shadow_backfill(snapshot=_snapshot(), confirm_paper_only_backfill=True, store=store)

        self.assertEqual(result["status"], "xau_m15_runtime_open_shadow_backfill_applied")
        self.assertTrue(result["payload_valid"])
        self.assertTrue(result["applied"])
        self.assertTrue(result["persistent_open_ready"])
        self.assertEqual(result["rows_written"], 1)
        self.assertEqual(store.recorded[0]["shadow_trade_id"], "xau-m15-paper-live-open")
        self.assertEqual(store.recorded[0]["status"], "open")
        self.assertFalse(store.recorded[0]["broker_touched"])
        self.assertFalse(store.recorded[0]["order_executed"])
        self.assertEqual(store.recorded[0]["order_policy"], "journal_only_no_broker")

    def test_duplicate_prevention_same_id(self) -> None:
        store = _BackfillStore(existing=[{"shadow_trade_id": "xau-m15-paper-live-open", "symbol": "XAUUSD", "timeframe": "M15"}])

        result = run_xau_m15_runtime_open_shadow_backfill(snapshot=_snapshot(), confirm_paper_only_backfill=True, store=store)

        self.assertEqual(result["status"], "xau_m15_runtime_open_shadow_backfill_already_present")
        self.assertTrue(result["duplicate_prevented"])
        self.assertEqual(result["rows_written"], 0)
        self.assertEqual(store.recorded, [])

    def test_blocks_different_existing_open(self) -> None:
        store = _BackfillStore(existing=[{"shadow_trade_id": "other-open", "symbol": "XAUUSD", "timeframe": "M15"}])

        result = run_xau_m15_runtime_open_shadow_backfill(snapshot=_snapshot(), confirm_paper_only_backfill=True, store=store)

        self.assertEqual(result["status"], "blocked_duplicate_open_shadow")
        self.assertTrue(result["duplicate_prevented"])
        self.assertEqual(result["rows_written"], 0)
        self.assertEqual(store.recorded, [])

    def test_reads_snapshot_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "snapshot.json"
            path.write_text(__import__("json").dumps(_snapshot()), encoding="utf-8")

            result = run_xau_m15_runtime_open_shadow_backfill(snapshot_file=path, confirm_paper_only_backfill=True, store=_BackfillStore())

        self.assertEqual(result["status"], "xau_m15_runtime_open_shadow_backfill_applied")
        self.assertEqual(result["shadow_trade_id"], "xau-m15-paper-live-open")


class _BackfillStore:
    def __init__(self, *, existing: list[dict[str, object]] | None = None) -> None:
        self.existing = existing or []
        self.recorded: list[dict[str, object]] = []

    def open_shadow_trades(self, *, limit: int = 50) -> dict[str, object]:
        return {"ok": True, "open_trades": [dict(row) for row in self.existing], "broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}

    def record_shadow_trade(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        self.recorded.append(dict(payload))
        return {"ok": True, "critical": bool(critical), "broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}


def _snapshot() -> dict[str, object]:
    return {
        "ok": True,
        "open_count": 1,
        "open_source": "runtime_memory",
        "runtime_open_count": 1,
        "persistent_open_count": 0,
        "merged_open_count": 1,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "trades": [
            {
                "shadow_trade_id": "xau-m15-paper-live-open",
                "symbol": "XAUUSD",
                "broker_symbol": "XAUUSD.b",
                "timeframe": "M15",
                "side": "buy",
                "entry_price": 4270.63,
                "last_price": 4265.7,
                "opened_at": "2026-06-18T09:22:47.562872+00:00",
                "stop_loss": 4253.54748,
                "take_profit": 4291.129024,
                "source": "paper_observation_shadow_once",
                "strategy_profile": "volatility_compression_breakout|mode=nr7_trailing_defensive",
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        ],
    }


if __name__ == "__main__":
    unittest.main()

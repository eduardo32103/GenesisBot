from __future__ import annotations

import unittest

from services.mt5.mt5_shadow_trading import get_recent_mt5_shadow_trades_fast


class MT5ShadowTradingReadTests(unittest.TestCase):
    def test_recent_shadow_trades_keeps_latest_event_per_shadow_id(self) -> None:
        memory = _Memory(
            [
                _row("shadow-1", "open", "2026-07-07T01:00:00+00:00"),
                _row("shadow-1", "closed", "2026-07-07T01:05:00+00:00"),
            ]
        )

        rows = get_recent_mt5_shadow_trades_fast(memory, "BTCUSD", limit=200)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["shadow_trade_id"], "shadow-1")
        self.assertEqual(rows[0]["status"], "closed")

    def test_recent_shadow_trades_respects_limit_above_one_hundred(self) -> None:
        memory = _Memory([_row(f"shadow-{idx}", "open", f"2026-07-07T01:{idx % 60:02d}:00+00:00") for idx in range(180)])

        rows = get_recent_mt5_shadow_trades_fast(memory, "BTCUSD", limit=500)

        self.assertEqual(memory.last_limit, 500)
        self.assertEqual(len(rows), 180)

    def test_recent_shadow_trades_prefers_closed_event_when_rows_are_oldest_first(self) -> None:
        rows = [_row(f"filler-{idx}", "closed", f"2026-07-07T00:{idx % 60:02d}:00+00:00") for idx in range(120)]
        rows.extend(
            [
                _row("shadow-live", "open", "2026-07-07T01:00:00+00:00"),
                _row("shadow-live", "closed", "2026-07-07T01:10:00+00:00"),
            ]
        )
        memory = _Memory(rows)

        trades = get_recent_mt5_shadow_trades_fast(memory, "BTCUSD", limit=500)
        selected = [row for row in trades if row.get("shadow_trade_id") == "shadow-live"]

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["status"], "closed")


class _Memory:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.last_limit = 0

    def get_mt5_events(self, collection: str, symbol: str | None, *, limit: int) -> list[dict[str, object]]:
        self.last_limit = limit
        self.assert_contract(collection, symbol)
        return self.rows[:limit]

    @staticmethod
    def assert_contract(collection: str, symbol: str | None) -> None:
        if collection != "mt5_shadow_trades":
            raise AssertionError(collection)
        if symbol != "BTCUSD":
            raise AssertionError(symbol)


def _row(shadow_id: str, status: str, created_at: str) -> dict[str, object]:
    return {
        "created_at": created_at,
        "payload": {
            "shadow_trade_id": shadow_id,
            "symbol": "BTCUSD",
            "normalized_symbol": "BTCUSD",
            "instrument_type": "crypto_spot",
            "is_spot_crypto": True,
            "manual_test": False,
            "timeframe": "M15",
            "status": status,
            "created_at": created_at,
            "updated_at": created_at,
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        },
    }


if __name__ == "__main__":
    unittest.main()

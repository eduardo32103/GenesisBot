from __future__ import annotations

import unittest

from services.mt5.mt5_shadow_trade_cleanup import run_shadow_trade_cleanup


class MT5ShadowTradeCleanupTests(unittest.TestCase):
    def test_dry_run_cleanup_does_not_close_anything(self) -> None:
        closed: list[dict[str, object]] = []
        result = run_shadow_trade_cleanup(
            open_trades=[_open_trade("shadow-old", opened_at="2000-01-01T00:00:00+00:00")],
            apply_paper_cleanup=False,
            load_shadow_snapshot=False,
            load_persistent_db=False,
            closer=lambda trade, reason: closed.append({"trade": trade, "reason": reason}) or {"ok": True},
        )

        self.assertEqual(result["closed_paper_only"], 0)
        self.assertEqual(closed, [])
        self.assertFalse(result["history_deleted"])
        self.assertFalse(result["metrics_reset"])
        self.assertFalse(result["losses_reset"])
        _assert_safety(self, result)

    def test_apply_cleanup_only_closes_safe_paper_trades(self) -> None:
        closed: list[dict[str, object]] = []
        result = run_shadow_trade_cleanup(
            open_trades=[
                _open_trade("shadow-keep", opened_at="2026-06-11T00:00:00+00:00"),
                _open_trade("shadow-dup", opened_at="2026-06-11T00:01:00+00:00"),
                _open_trade("shadow-broker", opened_at="2000-01-01T00:00:00+00:00", broker_touched=True),
            ],
            apply_paper_cleanup=True,
            stale_hours=1000000,
            load_shadow_snapshot=False,
            load_persistent_db=False,
            expected_live_capital_count=3,
            confirm_source_fingerprint="source-fp-3",
            source_fingerprint=_source_fingerprint(3, "source-fp-3"),
            closer=lambda trade, reason: closed.append({"trade": trade, "reason": reason}) or {"ok": True, "closed_at": "2026-06-11T00:02:00+00:00"},
        )

        self.assertEqual(result["closed_paper_only"], 1)
        self.assertEqual(closed[0]["trade"]["shadow_trade_id"], "shadow-dup")
        self.assertEqual(closed[0]["reason"], "duplicate_paper_shadow_cleanup")
        self.assertEqual(len(result["skipped_unsafe"]), 1)
        self.assertFalse(result["capital_protection_relaxed"])
        self.assertFalse(result["risk_governor_relaxed"])
        _assert_safety(self, result)

    def test_apply_cleanup_never_closes_broker_or_executed_trades(self) -> None:
        closed: list[dict[str, object]] = []
        result = run_shadow_trade_cleanup(
            open_trades=[
                _open_trade("shadow-broker", opened_at="2000-01-01T00:00:00+00:00", broker_touched=True),
                _open_trade("shadow-executed", opened_at="2000-01-01T00:00:00+00:00", order_executed=True),
            ],
            apply_paper_cleanup=True,
            stale_hours=1,
            load_shadow_snapshot=False,
            load_persistent_db=False,
            expected_live_capital_count=2,
            confirm_source_fingerprint="source-fp-2",
            source_fingerprint=_source_fingerprint(2, "source-fp-2"),
            closer=lambda trade, reason: closed.append({"trade": trade, "reason": reason}) or {"ok": True},
        )

        self.assertEqual(result["closed_paper_only"], 0)
        self.assertEqual(closed, [])
        reasons = {reason for row in result["skipped_unsafe"] for reason in row.get("reasons", [])}
        self.assertIn("broker_touched", reasons)
        self.assertIn("order_executed", reasons)
        _assert_safety(self, result)

    def test_cleanup_blocks_when_dry_run_count_mismatches_live_capital_count(self) -> None:
        closed: list[dict[str, object]] = []
        result = run_shadow_trade_cleanup(
            open_trades=[_open_trade("shadow-local", opened_at="2000-01-01T00:00:00+00:00")],
            apply_paper_cleanup=True,
            stale_hours=1,
            load_shadow_snapshot=False,
            load_persistent_db=False,
            expected_live_capital_count=48,
            confirm_source_fingerprint="source-fp-1",
            source_fingerprint=_source_fingerprint(1, "source-fp-1"),
            closer=lambda trade, reason: closed.append({"trade": trade, "reason": reason}) or {"ok": True},
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "dry_run_count_mismatches_live_capital_count")
        self.assertEqual(result["closed_paper_only"], 0)
        self.assertEqual(closed, [])
        _assert_safety(self, result)

    def test_cleanup_still_blocks_without_confirmed_fingerprint(self) -> None:
        closed: list[dict[str, object]] = []
        result = run_shadow_trade_cleanup(
            open_trades=[_open_trade("shadow-local", opened_at="2000-01-01T00:00:00+00:00")],
            apply_paper_cleanup=True,
            stale_hours=1,
            load_shadow_snapshot=False,
            load_persistent_db=False,
            expected_live_capital_count=1,
            confirm_source_fingerprint="",
            source_fingerprint=_source_fingerprint(1, "source-fp-1"),
            closer=lambda trade, reason: closed.append({"trade": trade, "reason": reason}) or {"ok": True},
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "missing_confirm_source_fingerprint")
        self.assertEqual(result["closed_paper_only"], 0)
        self.assertEqual(closed, [])
        _assert_safety(self, result)

    def test_cleanup_still_blocks_count_mismatch(self) -> None:
        closed: list[dict[str, object]] = []
        result = run_shadow_trade_cleanup(
            open_trades=[_open_trade("shadow-local", opened_at="2000-01-01T00:00:00+00:00")],
            apply_paper_cleanup=True,
            stale_hours=1,
            load_shadow_snapshot=False,
            load_persistent_db=False,
            expected_live_capital_count=48,
            confirm_source_fingerprint="source-fp-1",
            source_fingerprint=_source_fingerprint(1, "source-fp-1"),
            closer=lambda trade, reason: closed.append({"trade": trade, "reason": reason}) or {"ok": True},
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "dry_run_count_mismatches_live_capital_count")
        self.assertEqual(result["closed_paper_only"], 0)
        self.assertEqual(closed, [])
        _assert_safety(self, result)

    def test_cleanup_hardening_still_blocks_count_mismatch(self) -> None:
        closed: list[dict[str, object]] = []
        result = run_shadow_trade_cleanup(
            open_trades=[_open_trade("shadow-local", opened_at="2000-01-01T00:00:00+00:00")],
            apply_paper_cleanup=True,
            stale_hours=1,
            load_shadow_snapshot=False,
            load_persistent_db=False,
            expected_live_capital_count=48,
            confirm_source_fingerprint="source-fp-1",
            source_fingerprint=_source_fingerprint(1, "source-fp-1"),
            closer=lambda trade, reason: closed.append({"trade": trade, "reason": reason}) or {"ok": True},
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "dry_run_count_mismatches_live_capital_count")
        self.assertEqual(result["closed_paper_only"], 0)
        self.assertEqual(closed, [])
        _assert_safety(self, result)

    def test_cleanup_hardening_still_requires_confirmed_fingerprint(self) -> None:
        closed: list[dict[str, object]] = []
        result = run_shadow_trade_cleanup(
            open_trades=[_open_trade("shadow-local", opened_at="2000-01-01T00:00:00+00:00")],
            apply_paper_cleanup=True,
            stale_hours=1,
            load_shadow_snapshot=False,
            load_persistent_db=False,
            expected_live_capital_count=1,
            confirm_source_fingerprint="",
            source_fingerprint=_source_fingerprint(1, "source-fp-1"),
            closer=lambda trade, reason: closed.append({"trade": trade, "reason": reason}) or {"ok": True},
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "missing_confirm_source_fingerprint")
        self.assertEqual(result["closed_paper_only"], 0)
        self.assertEqual(closed, [])
        _assert_safety(self, result)

    def test_cleanup_blocks_when_source_is_local_sqlite_but_live_required(self) -> None:
        closed: list[dict[str, object]] = []
        result = run_shadow_trade_cleanup(
            open_trades=[_open_trade("shadow-local", opened_at="2000-01-01T00:00:00+00:00")],
            apply_paper_cleanup=True,
            stale_hours=1,
            load_shadow_snapshot=False,
            load_persistent_db=False,
            require_live_db=True,
            expected_live_capital_count=1,
            confirm_source_fingerprint="sqlite-fp",
            source_fingerprint=_source_fingerprint(1, "sqlite-fp", backend="sqlite", live_db_detected=False, source_matches=False),
            closer=lambda trade, reason: closed.append({"trade": trade, "reason": reason}) or {"ok": True},
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "source_is_local_sqlite_but_live_required")
        self.assertEqual(result["closed_paper_only"], 0)
        self.assertEqual(closed, [])
        _assert_safety(self, result)

    def test_cleanup_still_blocks_local_sqlite_when_live_required(self) -> None:
        closed: list[dict[str, object]] = []
        result = run_shadow_trade_cleanup(
            open_trades=[_open_trade("shadow-local", opened_at="2000-01-01T00:00:00+00:00")],
            apply_paper_cleanup=True,
            stale_hours=1,
            load_shadow_snapshot=False,
            load_persistent_db=False,
            require_live_db=True,
            expected_live_capital_count=1,
            confirm_source_fingerprint="sqlite-fp",
            source_fingerprint=_source_fingerprint(1, "sqlite-fp", backend="sqlite", live_db_detected=False, source_matches=False),
            closer=lambda trade, reason: closed.append({"trade": trade, "reason": reason}) or {"ok": True},
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "source_is_local_sqlite_but_live_required")
        self.assertEqual(result["closed_paper_only"], 0)
        self.assertEqual(closed, [])
        _assert_safety(self, result)


def _open_trade(
    trade_id: str,
    *,
    opened_at: str,
    broker_touched: bool = False,
    order_executed: bool = False,
) -> dict[str, object]:
    return {
        "shadow_trade_id": trade_id,
        "symbol": "BTCUSD",
        "timeframe": "M30",
        "strategy_profile": "btc_m30_profile",
        "side": "buy",
        "status": "open",
        "opened_at": opened_at,
        "source": "paper_shadow_test",
        "broker_touched": broker_touched,
        "order_executed": order_executed,
        "order_policy": "journal_only_no_broker",
        "applies_to_real_trading": False,
    }


def _assert_safety(test: unittest.TestCase, result: dict[str, object]) -> None:
    test.assertFalse(result["broker_touched"])
    test.assertFalse(result["order_executed"])
    test.assertEqual(result["order_policy"], "journal_only_no_broker")


def _source_fingerprint(
    count: int,
    fingerprint: str,
    *,
    backend: str = "postgres",
    live_db_detected: bool = True,
    source_matches: bool = True,
) -> dict[str, object]:
    return {
        "ok": True,
        "source_fingerprint": fingerprint,
        "backend_type": backend,
        "live_db_detected": live_db_detected,
        "source_matches_capital_protection": source_matches,
        "open_shadow_trades_count": count,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


if __name__ == "__main__":
    unittest.main()

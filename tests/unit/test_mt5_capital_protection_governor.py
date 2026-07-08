from __future__ import annotations

import contextlib
import io
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from api.main import create_app
from scripts.run_capital_protection_governor import main as capital_main
from services.mt5.mt5_bridge import mt5_capital_protection_status
from services.mt5.mt5_capital_protection_governor import (
    capital_protection_enforcement,
    run_capital_protection_governor,
)


class MT5CapitalProtectionGovernorTests(unittest.TestCase):
    def test_daily_loss_breaker_activates_kill_switch(self) -> None:
        result = run_capital_protection_governor(
            closed_trades=[
                _trade("BTCUSD", "M30", "btc_m30_test", -1.6, "loss"),
                _trade("BTCUSD", "M30", "btc_m30_test", -1.5, "loss"),
            ],
            open_trades=[],
            limits={"max_daily_loss_pct": 3.0},
            load_shadow_snapshot=False,
            load_persistent=False,
            persist_events=False,
        )

        self.assertEqual(result["capital_state"], "kill_switch")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertFalse(result["safe_to_trade"])
        self.assertTrue(_breaker_active(result, "max_daily_loss_pct"))
        self.assertEqual(result["recommended_action"], "kill_switch")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_weekly_loss_breaker_activates_kill_switch(self) -> None:
        result = run_capital_protection_governor(
            closed_trades=[
                _trade("XAUUSD", "M15", "xau_m15_test", -3.5, "loss"),
                _trade("XAUUSD", "M15", "xau_m15_test", -3.6, "loss"),
            ],
            open_trades=[],
            limits={"max_daily_loss_pct": 99.0, "max_weekly_loss_pct": 7.0},
            load_shadow_snapshot=False,
            load_persistent=False,
            persist_events=False,
        )

        self.assertEqual(result["capital_state"], "kill_switch")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertTrue(_breaker_active(result, "max_weekly_loss_pct"))
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_drawdown_breaker_activates_kill_switch(self) -> None:
        result = run_capital_protection_governor(
            closed_trades=[
                _trade("EURUSD", "H1", "eur_h1_test", 2.0, "win"),
                _trade("EURUSD", "H1", "eur_h1_test", -6.0, "loss"),
                _trade("EURUSD", "H1", "eur_h1_test", -6.0, "loss"),
            ],
            open_trades=[],
            limits={"max_daily_loss_pct": 99.0, "max_weekly_loss_pct": 99.0, "max_drawdown_pct": 10.0},
            load_shadow_snapshot=False,
            load_persistent=False,
            persist_events=False,
        )

        self.assertEqual(result["capital_state"], "kill_switch")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertTrue(_breaker_active(result, "max_drawdown_pct"))
        self.assertGreaterEqual(result["max_drawdown_pct"], 10.0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_db_degraded_blocks_critical_trade(self) -> None:
        result = capital_protection_enforcement(
            symbol="BTCUSD",
            timeframe="M30",
            profile="btc_m30_test",
            governor_result=run_capital_protection_governor(
                closed_trades=[_trade("BTCUSD", "M30", "btc_m30_test", 1.0, "win")],
                open_trades=[],
                persistent_status={"db_available": True, "db_degraded": True, "tables_ready": True},
                load_shadow_snapshot=False,
                persist_events=False,
            ),
        )

        self.assertTrue(result["blocked"])
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "capital_protection:persistent_db_degraded")
        self.assertFalse(result["safe_to_open_new_shadow"])
        self.assertFalse(result["paper_exploration_created"])
        self.assertEqual(result["shadow_trade_id"], "")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_open_shadow_and_profile_exposure_breakers_are_safe(self) -> None:
        result = run_capital_protection_governor(
            closed_trades=[],
            open_trades=[
                _open_trade("BTCUSD", "M30", "btc_m30_test"),
                _open_trade("BTCUSD", "M30", "btc_m30_test"),
            ],
            limits={"max_profile_exposure": 1, "max_open_shadow_trades": 3},
            load_shadow_snapshot=False,
            load_persistent=False,
            persist_events=False,
        )

        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertTrue(_breaker_active(result, "max_profile_exposure"))
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_normal_state_is_review_only_and_never_real_trading(self) -> None:
        result = run_capital_protection_governor(
            closed_trades=[_trade("US500", "H1", "us500_h1_test", 0.5, "win")],
            open_trades=[],
            load_shadow_snapshot=False,
            load_persistent=False,
            persist_events=False,
        )

        self.assertEqual(result["capital_state"], "normal")
        self.assertEqual(result["decision"], "ALLOW_PAPER_REVIEW")
        self.assertTrue(result["safe_to_trade"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_script_runs_and_prints_safety(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = capital_main(["--no-shadow-snapshot", "--no-persistent", "--no-persist-events"])

        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("MT5 Capital Protection Governor", text)
        self.assertIn("capital_state=", text)
        self.assertIn("safe_to_trade=", text)
        self.assertIn("recommended_action=", text)
        self.assertIn("broker_touched=False", text)
        self.assertIn("order_executed=False", text)
        self.assertIn("order_policy=journal_only_no_broker", text)

    def test_endpoint_is_exposed(self) -> None:
        app = create_app()

        self.assertEqual(
            app["genesis_mt5_capital_protection_status_endpoint"],
            "/api/genesis/mt5/capital-protection/status",
        )

    def test_status_endpoint_runs_governor_read_only_without_risk_event_writes(self) -> None:
        calls: list[dict[str, object]] = []

        def fake_governor(**kwargs: object) -> dict[str, object]:
            calls.append(kwargs)
            return {
                "ok": True,
                "status": "capital_protection_governor_ready",
                "capital_state": "normal",
                "safe_to_trade": True,
                "candidate_activated": False,
                "paper_forward_onboarding_started": False,
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }

        with patch("services.mt5.mt5_bridge._schema_missing_fast_fail", return_value={}), patch(
            "services.mt5.mt5_bridge.run_capital_protection_governor", side_effect=fake_governor
        ):
            result = mt5_capital_protection_status()

        self.assertEqual(len(calls), 1)
        self.assertFalse(calls[0]["persist_events"])
        self.assertTrue(result["status_endpoints_write_free"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_latest_state_snapshot_does_not_count_reconciled_legacy_open_as_open(self) -> None:
        rows = [
            _event(_shadow_event("shadow-66", "breakeven", updated_at="2026-07-07T01:10:00+00:00", pnl=0.0)),
            _event(_shadow_event("shadow-66", "open", updated_at="2026-07-07T01:00:00+00:00")),
        ]

        with patch("services.mt5.mt5_shadow_snapshot_source.load_settings", return_value=SimpleNamespace(database_url=_POSTGRES_URL)), patch(
            "services.mt5.mt5_shadow_snapshot_source.MemoryStore", return_value=_FakeMemory(rows)
        ):
            result = run_capital_protection_governor(
                persistent_status={"db_available": True, "db_degraded": False, "tables_ready": True},
                load_persistent=False,
                persist_events=False,
            )

        self.assertEqual(result["open_shadow_trades"], 0)
        self.assertFalse(_breaker_active(result, "max_open_shadow_trades"))
        self.assertEqual(result["capital_state"], "normal")
        self.assertEqual(result["decision"], "ALLOW_PAPER_REVIEW")
        self.assertEqual(result["shadow_snapshot_source"]["backend_type"], "postgres")
        self.assertTrue(result["shadow_snapshot_source"]["live_db_required"])
        self.assertTrue(result["shadow_snapshot_source"]["live_db_detected"])
        self.assertEqual(result["shadow_snapshot_source"]["open_shadow_trades_count"], 0)
        self.assertEqual(result["shadow_snapshot_source"]["closed_shadow_trades_count"], 1)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_live_source_open_trades_still_activate_capital_protection(self) -> None:
        rows = [
            _event(_shadow_event(f"shadow-open-{index}", "open", updated_at=f"2026-07-07T01:0{index}:00+00:00"))
            for index in range(4)
        ]

        with patch("services.mt5.mt5_shadow_snapshot_source.load_settings", return_value=SimpleNamespace(database_url=_POSTGRES_URL)), patch(
            "services.mt5.mt5_shadow_snapshot_source.MemoryStore", return_value=_FakeMemory(rows)
        ):
            result = run_capital_protection_governor(
                persistent_status={"db_available": True, "db_degraded": False, "tables_ready": True},
                limits={"max_open_shadow_trades": 3},
                load_persistent=False,
                persist_events=False,
            )

        self.assertEqual(result["open_shadow_trades"], 4)
        self.assertEqual(result["capital_state"], "kill_switch")
        self.assertEqual(result["reason"], "capital_protection:max_open_shadow_trades")
        self.assertTrue(_breaker_active(result, "max_open_shadow_trades"))
        self.assertFalse(result["safe_to_trade"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_latest_state_snapshot_source_unavailable_fails_closed_without_reset(self) -> None:
        with patch("services.mt5.mt5_shadow_snapshot_source.load_settings", return_value=SimpleNamespace(database_url=_POSTGRES_URL)), patch(
            "services.mt5.mt5_shadow_snapshot_source.MemoryStore", side_effect=RuntimeError("source_unavailable_require_live_db")
        ):
            result = run_capital_protection_governor(
                persistent_status={"db_available": True, "db_degraded": False, "tables_ready": True},
                load_persistent=False,
                persist_events=False,
            )

        self.assertEqual(result["capital_state"], "kill_switch")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "capital_protection:shadow_snapshot_source_unavailable")
        self.assertTrue(result["shadow_snapshot_source_unavailable"])
        self.assertTrue(_breaker_active(result, "shadow_snapshot_source_unavailable"))
        self.assertFalse(result.get("kill_switch_reset", False))
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")


def _trade(symbol: str, timeframe: str, profile: str, pnl_pct: float, status: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy_profile": profile,
        "pnl": pnl_pct,
        "pnl_pct": pnl_pct,
        "status": status,
    }


def _open_trade(symbol: str, timeframe: str, profile: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy_profile": profile,
        "risk_pct": 0.5,
        "lifecycle_status": "open",
    }


def _breaker_active(result: dict[str, object], name: str) -> bool:
    return any(
        isinstance(row, dict) and row.get("name") == name and row.get("active")
        for row in result.get("circuit_breakers", [])
    )


_POSTGRES_URL = "postgresql://user:secret@db.railway.internal:5432/railway"


class _FakeMemory:
    backend = "postgres"
    resolver_used = "env:DATABASE_URL"
    postgres_resolver_used = "env:DATABASE_URL"
    db_fingerprint = "railway_postgres:test"
    database_url = _POSTGRES_URL

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def get_mt5_events(self, collection: str | None = None, symbol: str | None = None, limit: int = 30) -> list[dict[str, object]]:
        return self._rows[:limit]


def _event(payload: dict[str, object]) -> dict[str, object]:
    return {
        "event_type": "mt5_shadow_trade",
        "payload": payload,
        "source": "test",
        "confidence": "media",
        "created_at": payload.get("updated_at") or "2026-07-07T01:00:00+00:00",
    }


def _shadow_event(shadow_trade_id: str, status: str, *, updated_at: str, pnl: float = 0.0) -> dict[str, object]:
    return {
        "shadow_trade_id": shadow_trade_id,
        "symbol": "XAUUSD",
        "normalized_symbol": "XAUUSD",
        "timeframe": "M15",
        "strategy_profile": "xau_m15_latest_state_test",
        "risk_pct": 0.25,
        "status": status,
        "lifecycle_status": "closed" if status in {"closed", "win", "loss", "breakeven"} else "open",
        "pnl": pnl,
        "updated_at": updated_at,
        "closed_at": updated_at if status != "open" else "",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


if __name__ == "__main__":
    unittest.main()

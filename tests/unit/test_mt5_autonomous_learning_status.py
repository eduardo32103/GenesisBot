from __future__ import annotations

import unittest
from unittest.mock import patch

from api.main import create_app
from api.routes.genesis import get_genesis_mt5_autonomous_learning_status
from services.mt5.mt5_autonomous_learning_status import run_autonomous_learning_status
from services.mt5.mt5_bridge import mt5_learning_status


class MT5AutonomousLearningStatusTests(unittest.TestCase):
    def test_status_uses_read_only_orchestrator_options(self) -> None:
        calls: list[dict[str, object]] = []

        def fake_orchestrator(**kwargs: object) -> dict[str, object]:
            calls.append(kwargs)
            return _cycle(db_state=_db_ready())

        with patch("services.mt5.mt5_autonomous_learning_status.run_autonomous_learning_orchestrator", side_effect=fake_orchestrator):
            result = run_autonomous_learning_status(symbol="BTCUSD", timeframe="M30")

        self.assertEqual(result["status"], "autonomous_learning_status_ready")
        self.assertEqual(calls[0]["dry_run"], True)
        self.assertEqual(calls[0]["apply_paper_rotation"], False)
        self.assertEqual(calls[0]["load_rotation"], False)
        self.assertEqual(calls[0]["run_trade_learning"], False)
        self.assertEqual(calls[0]["persist_events"], False)
        self.assertFalse(result["loop_started"])
        self.assertFalse(result["paper_rotation_applied"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        _assert_safety(self, result)

    def test_status_reports_real_db_state_when_persistent_intelligence_green(self) -> None:
        result = run_autonomous_learning_status(orchestrator_result=_cycle(db_state=_db_ready(provider="railway_postgres")))

        self.assertEqual(result["provider"], "railway_postgres")
        self.assertTrue(result["db_available"])
        self.assertFalse(result["db_degraded"])
        self.assertTrue(result["tables_ready"])
        self.assertEqual(result["learning_state"], "continue_research")
        _assert_safety(self, result)

    def test_db_degraded_pauses_learning(self) -> None:
        result = run_autonomous_learning_status(orchestrator_result=_cycle(db_state=_db_degraded(recommendation="verify_database_connection")))

        self.assertEqual(result["learning_state"], "paused_by_db_degraded")
        self.assertEqual(result["recommended_next_action"], "NO_TRADE")
        self.assertFalse(result["safe_to_learn"])
        _assert_safety(self, result)

    def test_schema_missing_pauses_learning(self) -> None:
        result = run_autonomous_learning_status(orchestrator_result=_cycle(db_state=_db_degraded(recommendation="apply_schema_sql", missing_tables=["mt5_profile_state"])))

        self.assertEqual(result["learning_state"], "paused_by_db_schema_missing")
        self.assertEqual(result["recommended_next_action"], "NO_TRADE")
        self.assertFalse(result["safe_to_learn"])
        _assert_safety(self, result)

    def test_legacy_learning_status_is_marked_legacy(self) -> None:
        fake_router = type("FakeRouter", (), {"learning_status": lambda self, symbol="": {"ok": True, "status": "legacy", "symbol": symbol}})()

        with patch("services.mt5.mt5_bridge._schema_missing_fast_fail", return_value={}), patch(
            "services.mt5.mt5_bridge.build_router", return_value=fake_router
        ):
            result = mt5_learning_status(symbol="BTCUSD")

        self.assertTrue(result["legacy_learning_status"])
        self.assertIn("/api/genesis/mt5/autonomous-learning/status", result["autonomous_learning_status_endpoint"])

    def test_route_and_app_expose_new_status_endpoint(self) -> None:
        app = create_app()
        self.assertEqual(
            app["genesis_mt5_autonomous_learning_status_endpoint"],
            "/api/genesis/mt5/autonomous-learning/status?symbol={symbol}&timeframe={timeframe}",
        )
        with patch("api.routes.genesis.mt5_autonomous_learning_status", return_value=_cycle(db_state=_db_ready())):
            result = get_genesis_mt5_autonomous_learning_status(symbol="BTCUSD", timeframe="M30")
        self.assertTrue(result["ok"])


def _cycle(*, db_state: dict[str, object]) -> dict[str, object]:
    return {
        "ok": True,
        "status": "autonomous_learning_orchestrator_ready",
        "db_state": db_state,
        "learning_state": "continue_research",
        "capital_state": "normal",
        "capital_protection": {"capital_state": "normal", "safe_to_trade": True},
        "adaptive_state": "watch",
        "adaptive_governor": {"global_state": "watch"},
        "safe_to_learn": _db_ready_bool(db_state),
        "safe_to_open_new_shadow": True,
        "active_profiles": [],
        "paused_profiles": [],
        "degraded_profiles": [],
        "tournament_top_candidate": {"symbol": "BTCUSD", "timeframe": "M30", "profile": "unit_profile"},
        "paper_rotation_recommendation": "continue_research",
        "paper_rotation_applied": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "circuit_breakers": [],
        "recommended_next_action": "continue_research",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _db_ready(*, provider: str = "unit") -> dict[str, object]:
    return {"provider": provider, "db_available": True, "db_degraded": False, "tables_ready": True, "recommendation": "persistent_intelligence_ready"}


def _db_degraded(*, recommendation: str, missing_tables: list[str] | None = None) -> dict[str, object]:
    return {
        "provider": "railway_postgres",
        "db_available": recommendation == "apply_schema_sql",
        "db_degraded": True,
        "tables_ready": False,
        "missing_tables": missing_tables or [],
        "recommendation": recommendation,
    }


def _db_ready_bool(db_state: dict[str, object]) -> bool:
    return bool(db_state.get("db_available") and db_state.get("tables_ready") and not db_state.get("db_degraded"))


def _assert_safety(test: unittest.TestCase, result: dict[str, object]) -> None:
    test.assertFalse(result["broker_touched"])
    test.assertFalse(result["order_executed"])
    test.assertEqual(result["order_policy"], "journal_only_no_broker")

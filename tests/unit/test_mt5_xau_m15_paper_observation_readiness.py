from __future__ import annotations

import unittest
from datetime import datetime, timezone

from services.mt5.mt5_xau_m15_paper_observation_readiness import (
    CANDIDATE_PROFILE,
    run_xau_m15_paper_observation_cycle,
    run_xau_m15_paper_observation_readiness,
)


class MT5XauM15PaperObservationReadinessTests(unittest.TestCase):
    def test_missing_runtime_blocks(self) -> None:
        result = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile()],
            strategy_registry_rows=[_strategy()],
            runtime_snapshot={},
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state=_risk(),
        )

        self.assertFalse(result["runtime_context_available"])
        self.assertEqual(result["readiness_state"], "blocked")
        self.assertEqual(result["recommendation"], "configure_mt5_bridge_for_xauusd_m15")
        self.assertIn("runtime_context_available", result["failed_gates"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_missing_m15_bars_blocks(self) -> None:
        snapshot = {**_snapshot(), "bars_count": 20, "ohlc_recent": []}
        result = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile()],
            strategy_registry_rows=[_strategy()],
            runtime_snapshot=snapshot,
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state=_risk(),
        )

        self.assertFalse(result["bars_available"])
        self.assertEqual(result["m15_bars_status"], "missing_or_insufficient")
        self.assertIn("m15_bars_available", result["failed_gates"])
        self.assertIn("m15_bars_count", result["failed_gates"])
        self.assertEqual(result["recommendation"], "configure_mt5_bridge_for_xauusd_m15")

    def test_db_degraded_blocks(self) -> None:
        result = run_xau_m15_paper_observation_readiness(
            db_state={**_db(), "db_degraded": True, "queue_depth": 7},
            profile_state_rows=[_profile()],
            strategy_registry_rows=[_strategy()],
            runtime_snapshot=_snapshot(),
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state=_risk(),
        )

        self.assertEqual(result["readiness_state"], "blocked")
        self.assertIn("persistent_db_healthy", result["failed_gates"])
        self.assertIn("db_queue_pressure_clear", result["failed_gates"])
        self.assertEqual(result["recommendation"], "repair_persistent_intelligence_before_observation")

    def test_candidate_not_found_blocks(self) -> None:
        result = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[],
            strategy_registry_rows=[],
            runtime_snapshot=_snapshot(),
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state=_risk(),
        )

        self.assertFalse(result["candidate_found"])
        self.assertIn("candidate_found", result["failed_gates"])
        self.assertEqual(result["recommendation"], "register_xau_m15_candidate_before_observation")

    def test_candidate_activated_true_fails_safety(self) -> None:
        result = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile(active=True)],
            strategy_registry_rows=[_strategy()],
            runtime_snapshot=_snapshot(),
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state=_risk(),
        )

        self.assertIn("candidate_not_activated", result["failed_gates"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])

    def test_applies_to_real_trading_true_fails_safety(self) -> None:
        result = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile(applies_to_real_trading=True)],
            strategy_registry_rows=[_strategy()],
            runtime_snapshot=_snapshot(),
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state=_risk(),
        )

        self.assertIn("candidate_not_real_trading", result["failed_gates"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["broker_touched"])

    def test_all_ready_returns_ready_for_one_cycle_paper_observation(self) -> None:
        result = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile()],
            strategy_registry_rows=[_strategy()],
            runtime_snapshot=_snapshot(),
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state=_risk(),
        )

        self.assertTrue(result["candidate_found"])
        self.assertEqual(result["candidate_status"], "paper_observation_review")
        self.assertTrue(result["runtime_context_available"])
        self.assertTrue(result["runtime_context_recent"])
        self.assertTrue(result["bars_available"])
        self.assertGreaterEqual(result["bars_count"], 100)
        self.assertTrue(result["tick_available"])
        self.assertTrue(result["spread_available"])
        self.assertEqual(result["readiness_state"], "ready_for_one_cycle_paper_observation")
        self.assertEqual(result["recommendation"], "ready_for_one_cycle_paper_observation")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["order_executed"])

    def test_dry_run_observation_creates_no_shadow(self) -> None:
        readiness = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile()],
            strategy_registry_rows=[_strategy()],
            runtime_snapshot=_snapshot(),
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state=_risk(),
        )

        result = run_xau_m15_paper_observation_cycle(readiness_result=readiness)

        self.assertEqual(result["mode"], "dry_run")
        self.assertEqual(result["readiness_state"], "ready_for_one_cycle_paper_observation")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(result["shadow_trade_id"], "")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_explicit_shadow_once_is_blocked_pending_human_approval(self) -> None:
        readiness = {"readiness_state": "ready_for_one_cycle_paper_observation", "runtime_snapshot_context": "bar_context", "bars_count": 120}

        result = run_xau_m15_paper_observation_cycle(
            readiness_result=readiness,
            paper_shadow_once=True,
        )

        self.assertTrue(result["paper_shadow_once_requested"])
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(result["recommendation"], "do_not_start_paper_shadow_yet")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["order_executed"])


def _db() -> dict[str, object]:
    return {
        "provider": "railway_postgres",
        "db_available": True,
        "db_degraded": False,
        "tables_ready": True,
        "queue_depth": 0,
        "recommendation": "persistent_intelligence_ready",
    }


def _profile(*, active: bool = False, applies_to_real_trading: bool = False) -> dict[str, object]:
    return {
        "symbol": "XAUUSD",
        "timeframe": "M15",
        "profile": CANDIDATE_PROFILE,
        "status": "paper_observation_review",
        "active": active,
        "applies_to_paper_shadow": False,
        "applies_to_real_trading": applies_to_real_trading,
    }


def _strategy() -> dict[str, object]:
    return {
        "symbol": "XAUUSD",
        "timeframe": "M15",
        "profile": CANDIDATE_PROFILE,
        "family": "volatility_compression_breakout",
        "status": "paper_observation_review",
    }


def _snapshot() -> dict[str, object]:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "symbol": "XAUUSD",
        "normalized_symbol": "XAUUSD",
        "timeframe": "M15",
        "runtime_snapshot_available": True,
        "runtime_snapshot_recent": True,
        "runtime_snapshot_complete": True,
        "runtime_snapshot_context": "bar_context",
        "last_tick_at": now,
        "updated_at": now,
        "last_tick": {"last": 2350.5, "spread": 25, "regime": "trend", "timeframe": "M15"},
        "ohlc_recent": [{"close": 2350.5}],
        "bars_count": 120,
        "min_bars_required": 100,
        "bars_last_at": now,
        "last_bars_at": now,
        "tick_merged_into_bar_context": True,
        "market_regime": "trend",
        "latest_performance_summary": {"closed": 0, "profit_factor": 1.2, "expectancy": 0.01},
    }


def _capital() -> dict[str, object]:
    return {"capital_state": "normal", "safe_to_trade": True, "reason": ""}


def _adaptive() -> dict[str, object]:
    return {"global_state": "watch", "recommended_next_action": "rotate_candidate_review", "reason": ""}


def _risk() -> dict[str, object]:
    return {"allowed": True, "risk_state": "normal", "reason": "risk_governor_pass"}


if __name__ == "__main__":
    unittest.main()

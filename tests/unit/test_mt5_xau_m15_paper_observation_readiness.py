from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from api.main import create_app
from api.routes.genesis import (
    get_genesis_mt5_xau_m15_paper_observation_cycle,
    get_genesis_mt5_xau_m15_paper_observation_readiness,
    post_genesis_mt5_xau_m15_paper_observation_shadow_once,
)
from services.mt5.instrument_resolver import normalize_mt5_symbol
from services.mt5.mt5_bridge import mt5_bars
from services.mt5.mt5_runtime_snapshot import (
    get_snapshot,
    reset_runtime_snapshots_for_tests,
    runtime_snapshot_inventory,
    update_bars,
    update_open_shadow_trade,
    update_tick,
)
from services.mt5.mt5_xau_m15_paper_observation_readiness import (
    CANDIDATE_PROFILE,
    run_xau_m15_paper_observation_cycle,
    run_xau_m15_paper_observation_readiness,
    run_xau_m15_paper_observation_shadow_once,
)


class MT5XauM15PaperObservationReadinessTests(unittest.TestCase):
    def test_create_app_exposes_xau_m15_live_http_endpoints(self) -> None:
        app = create_app()

        self.assertEqual(
            app["genesis_mt5_xau_m15_paper_observation_readiness_endpoint"],
            "/api/genesis/mt5/xau-m15/paper-observation/readiness",
        )
        self.assertEqual(
            app["genesis_mt5_xau_m15_paper_observation_cycle_endpoint"],
            "/api/genesis/mt5/xau-m15/paper-observation/cycle",
        )
        self.assertEqual(
            app["genesis_mt5_xau_m15_paper_observation_shadow_once_endpoint"],
            "/api/genesis/mt5/xau-m15/paper-observation/shadow-once",
        )

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

    def test_recent_edge_negative_blocks_real_trading_but_reports_adaptive_paper_cooldown(self) -> None:
        result = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile()],
            strategy_registry_rows=[_strategy()],
            runtime_snapshot={**_snapshot(), "side": "buy"},
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state={"allowed": False, "risk_state": "blocked", "reason": "recent_edge_negative"},
        )

        self.assertEqual(result["readiness_state"], "blocked")
        self.assertFalse(result["risk_allows_observation"])
        self.assertEqual(result["risk_governor_reason"], "recent_edge_negative")
        self.assertTrue(result["recent_edge_negative"])
        self.assertEqual(result["entry_block_type"], "adaptive_paper_cooldown")
        self.assertIn("risk_allows_observation", result["failed_gate_names"])
        self.assertIn("risk_allows_observation", result["failed_gate_reasons"])
        self.assertEqual(result["recommendation"], "strict_paper_probe_allowed_real_trading_still_blocked")
        self.assertTrue(result["entry_allowed_for_paper_test"])
        self.assertTrue(result["strict_paper_probe"]["strict_paper_probe_passed"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_recent_edge_negative_without_explicit_direction_waits_for_strict_signal(self) -> None:
        result = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile()],
            strategy_registry_rows=[_strategy()],
            runtime_snapshot=_snapshot(),
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state={"allowed": False, "risk_state": "blocked", "reason": "recent_edge_negative"},
        )

        self.assertEqual(result["readiness_state"], "blocked")
        self.assertTrue(result["recent_edge_negative"])
        self.assertFalse(result["entry_allowed_for_paper_test"])
        self.assertEqual(result["entry_block_type"], "adaptive_paper_cooldown")
        self.assertEqual(result["recommendation"], "adaptive_paper_cooldown_wait_for_high_quality_paper_signal")
        self.assertIn("signal_direction", result["strict_paper_probe"]["failed_strict_gate_names"])
        self.assertFalse(result["paper_shadow_created"])

    def test_xauusd_b_m15_runtime_snapshot_is_read_by_logical_symbol(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)

        self.assertEqual(normalize_mt5_symbol("XAUUSD.b"), "XAUUSD")
        update_bars(
            "XAUUSD.b",
            "M15",
            _bars(120),
            tick={"bid": 2350.0, "ask": 2350.25, "last": 2350.12, "spread": 0.25, "timeframe": "M15"},
            min_bars=100,
        )
        update_tick(
            "XAUUSD.b",
            {"bid": 2350.1, "ask": 2350.35, "last": 2350.2, "spread": 0.25, "timeframe": "M15"},
        )

        result = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile()],
            strategy_registry_rows=[_strategy()],
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state=_risk(),
        )

        self.assertEqual(result["symbol"], "XAUUSD")
        self.assertEqual(result["broker_symbol"], "XAUUSD.b")
        self.assertEqual(result["timeframe"], "M15")
        self.assertEqual(result["symbol_alias_used"], "XAUUSD")
        self.assertTrue(result["runtime_context_available"])
        self.assertTrue(result["runtime_context_recent"])
        self.assertTrue(result["runtime_snapshot_complete"])
        self.assertEqual(result["runtime_snapshot_context"], "bar_context")
        self.assertTrue(result["bars_available"])
        self.assertGreaterEqual(result["bars_count"], 100)
        self.assertTrue(result["tick_available"])
        self.assertTrue(result["tick_merged_into_bar_context"])
        self.assertTrue(result["latest_tick_at"])
        self.assertTrue(result["latest_bars_at"])
        self.assertEqual(result["readiness_state"], "ready_for_one_cycle_paper_observation")
        self.assertFalse(result["paper_shadow_created"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_bars_endpoint_stores_xauusd_b_m15_as_xauusd_m15_with_diagnostics(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)

        response = mt5_bars(_bars_payload("XAUUSD.b", "M15", _bars(120)))
        snapshot = get_snapshot("XAUUSD", "M15") or {}
        inventory = runtime_snapshot_inventory(lookup_symbols=["XAUUSD", "XAUUSD.b"], lookup_timeframe="M15")

        self.assertTrue(response["ok"])
        self.assertEqual(response["status"], "mt5_bars_recorded_fast_path")
        self.assertEqual(response["raw_symbol_received"], "XAUUSD.b")
        self.assertEqual(response["normalized_symbol"], "XAUUSD")
        self.assertEqual(response["timeframe_received"], "M15")
        self.assertEqual(response["bars_received_count"], 120)
        self.assertEqual(response["bars_stored_count"], 120)
        self.assertEqual(response["storage_key"], "XAUUSD:M15")
        self.assertEqual(response["response_status"], "mt5_bars_recorded_fast_path")
        self.assertEqual(snapshot["normalized_symbol"], "XAUUSD")
        self.assertEqual(snapshot["timeframe"], "M15")
        self.assertEqual(snapshot["bars_count"], 120)
        self.assertTrue(snapshot["tick_merged_into_bar_context"])
        self.assertIn("XAUUSD:M15", inventory["snapshot_keys"])
        self.assertEqual(inventory["xauusd_lookup_result"]["bars_count"], 120)
        self.assertTrue(inventory["xauusd_b_lookup_result"]["timeframe_found"])
        self.assertFalse(response["broker_touched"])
        self.assertFalse(response["order_executed"])
        self.assertEqual(response["order_policy"], "journal_only_no_broker")

    def test_m15_and_m30_runtime_snapshots_do_not_overwrite_each_other(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)

        m15 = mt5_bars(_bars_payload("XAUUSD.b", "M15", _bars(120)))
        m30 = mt5_bars(_bars_payload("XAUUSD.b", "M30", _bars(150)))
        m15_snapshot = get_snapshot("XAUUSD", "M15") or {}
        m30_snapshot = get_snapshot("XAUUSD", "M30") or {}
        inventory = runtime_snapshot_inventory(lookup_symbols=["XAUUSD", "XAUUSD.b"], lookup_timeframe="M15")

        self.assertEqual(m15["storage_key"], "XAUUSD:M15")
        self.assertEqual(m30["storage_key"], "XAUUSD:M30")
        self.assertEqual(m15_snapshot["timeframe"], "M15")
        self.assertEqual(m15_snapshot["bars_count"], 120)
        self.assertEqual(m30_snapshot["timeframe"], "M30")
        self.assertEqual(m30_snapshot["bars_count"], 150)
        self.assertEqual(inventory["timeframes_seen_by_symbol"]["XAUUSD"], ["M15", "M30"])

    def test_readiness_blocks_when_only_m30_bars_exist(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)

        mt5_bars(_bars_payload("XAUUSD.b", "M30", _bars(120)))
        result = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile()],
            strategy_registry_rows=[_strategy()],
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state=_risk(),
        )

        self.assertEqual(result["readiness_state"], "blocked")
        self.assertIn("m15_bars_available", result["failed_gates"])
        self.assertIn("m15_bars_count", result["failed_gates"])
        self.assertEqual(result["bars_count"], 0)
        self.assertFalse(result["paper_shadow_created"])
        self.assertFalse(result["candidate_activated"])

    def test_readiness_blocks_when_only_tick_exists_without_m15_bars(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)

        update_tick("XAUUSD.b", {"bid": 2350.1, "ask": 2350.35, "last": 2350.2, "spread": 0.25, "timeframe": "M15"})
        result = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile()],
            strategy_registry_rows=[_strategy()],
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state=_risk(),
        )

        self.assertTrue(result["runtime_context_available"])
        self.assertFalse(result["runtime_snapshot_complete"])
        self.assertIn("m15_bars_available", result["failed_gates"])
        self.assertIn("m15_bars_count", result["failed_gates"])
        self.assertFalse(result["paper_shadow_created"])
        self.assertFalse(result["broker_touched"])

    def test_http_readiness_uses_live_runtime_snapshot_and_returns_ready(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)
        mt5_bars(_bars_payload("XAUUSD.b", "M15", _bars(120)))

        with _patched_live_readiness_dependencies():
            result = get_genesis_mt5_xau_m15_paper_observation_readiness()

        self.assertTrue(result["candidate_found"])
        self.assertEqual(result["candidate_status"], "paper_observation_review")
        self.assertTrue(result["runtime_context_available"])
        self.assertTrue(result["runtime_context_recent"])
        self.assertEqual(result["runtime_snapshot_context"], "bar_context")
        self.assertEqual(result["symbol_alias_used"], "XAUUSD")
        self.assertTrue(result["latest_tick_at"])
        self.assertTrue(result["latest_bars_at"])
        self.assertTrue(result["bars_available"])
        self.assertGreaterEqual(result["bars_count"], 100)
        self.assertTrue(result["tick_available"])
        self.assertTrue(result["tick_merged_into_bar_context"])
        self.assertEqual(result["readiness_state"], "ready_for_one_cycle_paper_observation")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_http_readiness_blocks_when_no_live_m15_bars(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)

        with _patched_live_readiness_dependencies():
            result = get_genesis_mt5_xau_m15_paper_observation_readiness()

        self.assertEqual(result["readiness_state"], "blocked")
        self.assertIn("m15_bars_available", result["failed_gates"])
        self.assertIn("m15_bars_count", result["failed_gates"])
        self.assertFalse(result["paper_shadow_created"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["broker_touched"])

    def test_http_cycle_endpoint_default_is_dry_run_and_creates_no_shadow(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)
        mt5_bars(_bars_payload("XAUUSD.b", "M15", _bars(120)))

        with _patched_live_readiness_dependencies():
            result = get_genesis_mt5_xau_m15_paper_observation_cycle()

        self.assertEqual(result["mode"], "dry_run")
        self.assertEqual(result["readiness_state"], "ready_for_one_cycle_paper_observation")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(result["shadow_trade_id"], "")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")
        self.assertFalse((get_snapshot("XAUUSD", "M15") or {}).get("open_shadow_trade"))

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

    def test_shadow_once_without_confirmation_fails(self) -> None:
        result = post_genesis_mt5_xau_m15_paper_observation_shadow_once({"symbol": "XAUUSD", "timeframe": "M15"})

        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "rejected_missing_confirmation")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(result["shadow_trade_id"], "")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_shadow_once_with_readiness_false_fails(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)

        with _patched_live_readiness_dependencies():
            result = post_genesis_mt5_xau_m15_paper_observation_shadow_once(
                {"confirm_paper_shadow_only": True, "symbol": "XAUUSD", "timeframe": "M15"}
            )

        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "blocked_by_readiness_gate:readiness_state")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(result["shadow_trade_id"], "")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_shadow_once_with_existing_shadow_fails_without_closing_it(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)
        mt5_bars(_bars_payload("XAUUSD.b", "M15", _bars(120)))
        update_open_shadow_trade(
            "XAUUSD",
            {
                "shadow_trade_id": "existing-xau-shadow",
                "symbol": "XAUUSD",
                "broker_symbol": "XAUUSD.b",
                "timeframe": "M15",
                "status": "open",
                "source": "unit_test_existing_shadow",
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            },
            timeframe="M15",
        )

        with _patched_live_readiness_dependencies():
            result = post_genesis_mt5_xau_m15_paper_observation_shadow_once(
                {"confirm_paper_shadow_only": True, "symbol": "XAUUSD", "timeframe": "M15"}
            )

        self.assertEqual(result["reason"], "blocked_existing_open_shadow")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(result["open_shadow_count_before"], 1)
        self.assertEqual((get_snapshot("XAUUSD", "M15") or {}).get("open_shadow_trade", {}).get("shadow_trade_id"), "existing-xau-shadow")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_shadow_once_with_readiness_true_creates_exactly_one_paper_shadow(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)
        mt5_bars(_bars_payload("XAUUSD.b", "M15", _bars(120)))

        with _patched_live_readiness_dependencies():
            result = post_genesis_mt5_xau_m15_paper_observation_shadow_once(
                {"confirm_paper_shadow_only": True, "symbol": "XAUUSD", "timeframe": "M15"}
            )

        snapshot = get_snapshot("XAUUSD", "M15") or {}
        trade = snapshot.get("open_shadow_trade") if isinstance(snapshot.get("open_shadow_trade"), dict) else {}
        self.assertTrue(result["paper_shadow_created"])
        self.assertTrue(result["persistent_shadow_write_ok"])
        self.assertEqual(result["open_shadow_count_before"], 0)
        self.assertEqual(result["open_shadow_count_after"], 1)
        self.assertEqual(result["shadow_trade_id"], trade.get("shadow_trade_id"))
        self.assertEqual(trade.get("symbol"), "XAUUSD")
        self.assertEqual(trade.get("broker_symbol"), "XAUUSD.b")
        self.assertEqual(trade.get("timeframe"), "M15")
        self.assertEqual(trade.get("candidate_profile"), CANDIDATE_PROFILE)
        self.assertEqual(trade.get("status"), "open")
        self.assertEqual(trade.get("source"), "paper_observation_shadow_once")
        self.assertFalse(trade.get("broker_touched"))
        self.assertFalse(trade.get("order_executed"))
        self.assertEqual(trade.get("order_policy"), "journal_only_no_broker")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

        with _patched_live_readiness_dependencies():
            duplicate = run_xau_m15_paper_observation_shadow_once(
                payload={"confirm_paper_shadow_only": True, "symbol": "XAUUSD", "timeframe": "M15"}
            )
        self.assertEqual(duplicate["reason"], "blocked_existing_open_shadow")
        self.assertFalse(duplicate["paper_shadow_created"])
        self.assertEqual((get_snapshot("XAUUSD", "M15") or {}).get("open_shadow_trade", {}).get("shadow_trade_id"), result["shadow_trade_id"])

    def test_shadow_once_explicit_buy_opens_buy_shadow(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)
        readiness = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile()],
            strategy_registry_rows=[_strategy()],
            runtime_snapshot=_snapshot(),
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state=_risk(),
        )
        store = _FakeStore()

        result = run_xau_m15_paper_observation_shadow_once(
            payload={"confirm_paper_shadow_only": True, "symbol": "XAUUSD", "timeframe": "M15", "side": "buy"},
            readiness_result=readiness,
            store=store,
        )

        self.assertTrue(result["paper_shadow_created"])
        self.assertEqual(result["side"], "buy")
        self.assertEqual(result["signal_direction"], "buy")
        self.assertEqual(result["entry_reason"], "explicit_paper_test_direction")
        self.assertEqual(store.recorded_shadow_trades[0]["side"], "buy")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_shadow_once_explicit_sell_opens_sell_shadow(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)
        readiness = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile()],
            strategy_registry_rows=[_strategy()],
            runtime_snapshot=_snapshot(),
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state=_risk(),
        )
        store = _FakeStore()

        result = run_xau_m15_paper_observation_shadow_once(
            payload={"confirm_paper_shadow_only": True, "symbol": "XAUUSD", "timeframe": "M15", "side": "sell"},
            readiness_result=readiness,
            store=store,
        )

        trade = store.recorded_shadow_trades[0]
        self.assertTrue(result["paper_shadow_created"])
        self.assertEqual(result["side"], "sell")
        self.assertEqual(result["signal_direction"], "sell")
        self.assertEqual(trade["side"], "sell")
        self.assertGreaterEqual(trade["stop_loss"], trade["entry_price"])
        self.assertLessEqual(trade["take_profit"], trade["entry_price"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_shadow_once_no_direction_returns_no_trade_signal(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)
        readiness = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile()],
            strategy_registry_rows=[_strategy()],
            runtime_snapshot=_snapshot(),
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state=_risk(),
        )
        store = _FakeStore()

        result = run_xau_m15_paper_observation_shadow_once(
            payload={"confirm_paper_shadow_only": True, "symbol": "XAUUSD", "timeframe": "M15"},
            readiness_result=readiness,
            store=store,
        )

        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "no_trade_signal")
        self.assertEqual(result["invalidation_reason"], "no_high_confidence_direction")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(store.recorded_shadow_trades, [])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_recent_edge_negative_shadow_once_requires_strict_paper_probe(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)
        readiness = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile()],
            strategy_registry_rows=[_strategy()],
            runtime_snapshot={**_snapshot(), "side": "buy"},
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state={"allowed": False, "risk_state": "blocked", "reason": "recent_edge_negative"},
        )
        store = _FakeStore()

        result = run_xau_m15_paper_observation_shadow_once(
            payload={"confirm_paper_shadow_only": True, "symbol": "XAUUSD", "timeframe": "M15", "side": "buy"},
            readiness_result=readiness,
            store=store,
        )

        self.assertEqual(result["reason"], "adaptive_paper_cooldown_requires_strict_paper_probe")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(store.recorded_shadow_trades, [])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_recent_edge_negative_strict_paper_probe_can_open_when_all_strict_gates_pass(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)
        readiness = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile()],
            strategy_registry_rows=[_strategy()],
            runtime_snapshot={**_snapshot(), "side": "buy"},
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state={"allowed": False, "risk_state": "blocked", "reason": "recent_edge_negative"},
        )
        store = _FakeStore()

        result = run_xau_m15_paper_observation_shadow_once(
            payload={"confirm_paper_shadow_only": True, "symbol": "XAUUSD", "timeframe": "M15", "side": "buy", "strict_paper_probe": True},
            readiness_result=readiness,
            store=store,
        )

        self.assertTrue(result["paper_shadow_created"])
        self.assertEqual(result["side"], "buy")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_shadow_once_open_persistence_failure_does_not_claim_created_or_update_runtime(self) -> None:
        reset_runtime_snapshots_for_tests()
        self.addCleanup(reset_runtime_snapshots_for_tests)
        readiness = run_xau_m15_paper_observation_readiness(
            db_state=_db(),
            profile_state_rows=[_profile()],
            strategy_registry_rows=[_strategy()],
            runtime_snapshot=_snapshot(),
            capital_state=_capital(),
            adaptive_state=_adaptive(),
            risk_state=_risk(),
        )
        store = _FailingShadowStore()

        result = run_xau_m15_paper_observation_shadow_once(
            payload={"confirm_paper_shadow_only": True, "symbol": "XAUUSD", "timeframe": "M15", "side": "buy"},
            readiness_result=readiness,
            store=store,
        )

        self.assertEqual(result["status"], "xau_m15_paper_observation_shadow_once_open_persistence_failed")
        self.assertEqual(result["reason"], "open_persistence_failed")
        self.assertFalse(result["paper_shadow_created"])
        self.assertTrue(result["open_persistence_failed"])
        self.assertTrue(result["open_write_retained_critical"])
        self.assertEqual(result["next_action"], "drain_queue_or_backfill_runtime_open_shadow")
        self.assertTrue(store.critical_seen)
        self.assertFalse((get_snapshot("XAUUSD", "M15") or {}).get("open_shadow_trade"))
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_shadow_once_service_adds_no_order_send_reference(self) -> None:
        service_text = Path("services/mt5/mt5_xau_m15_paper_observation_readiness.py").read_text(encoding="utf-8")
        self.assertNotIn("order_send", service_text)


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


class _FakeStore:
    def __init__(self) -> None:
        self.recorded_shadow_trades: list[dict[str, object]] = []

    def healthcheck(self, write_test_event: bool = False) -> dict[str, object]:
        return _db()

    def _safe_select(self, table: str, params: dict[str, object] | None = None) -> dict[str, object]:
        if table == "mt5_profile_state":
            return {"rows": [_profile()]}
        if table == "mt5_strategy_registry":
            return {"rows": [_strategy()]}
        return {"rows": []}

    def record_shadow_trade(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        self.recorded_shadow_trades.append(dict(payload))
        return {"ok": True, "table": "mt5_shadow_trades", "broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}


class _FailingShadowStore:
    def __init__(self) -> None:
        self.critical_seen = False

    def record_shadow_trade(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        self.critical_seen = bool(critical)
        return {
            "ok": False,
            "queued": True,
            "critical_persistence_failed": True,
            "reason": "simulated_write_backpressure",
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }


def _patched_live_readiness_dependencies():
    return patch.multiple(
        "services.mt5.mt5_xau_m15_paper_observation_readiness",
        MT5PersistentIntelligenceStore=lambda: _FakeStore(),
        run_capital_protection_governor=lambda **kwargs: _capital(),
        run_adaptive_strategy_governor=lambda **kwargs: _adaptive(),
        assess_runtime_risk=lambda *args, **kwargs: _risk(),
    )


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


def _bars(count: int) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    base = 2350.0
    start = datetime(2026, 6, 12, tzinfo=timezone.utc)
    for idx in range(count):
        price = base + (idx * 0.05)
        rows.append(
            {
                "time": (start + timedelta(minutes=15 * idx)).isoformat(),
                "open": price,
                "high": price + 0.8,
                "low": price - 0.7,
                "close": price + 0.2,
                "volume": 100 + idx,
                "tick_volume": 100 + idx,
            }
        )
    return rows


def _bars_payload(symbol: str, timeframe: str, bars: list[dict[str, object]], *, spread: float = 0.25) -> dict[str, object]:
    last = float(bars[-1]["close"])
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "bars": bars,
        "bid": last - spread / 2.0,
        "ask": last + spread / 2.0,
        "last": last,
        "spread": spread,
        "source": "unit_test_xau_m15",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _capital() -> dict[str, object]:
    return {"capital_state": "normal", "safe_to_trade": True, "reason": ""}


def _adaptive() -> dict[str, object]:
    return {"global_state": "watch", "recommended_next_action": "rotate_candidate_review", "reason": ""}


def _risk() -> dict[str, object]:
    return {"allowed": True, "risk_state": "normal", "reason": "risk_governor_pass"}


if __name__ == "__main__":
    unittest.main()

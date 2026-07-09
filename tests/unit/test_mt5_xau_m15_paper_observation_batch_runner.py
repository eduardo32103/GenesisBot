from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from services.mt5.mt5_market_active_guard import MarketActiveGuard
from services.mt5.mt5_xau_m15_paper_observation_batch_runner import (
    HttpPaperObservationClient,
    LocalPaperObservationClient,
    compute_xau_m15_paper_batch_stats,
    run_multi_asset_paper_observation_readiness,
    run_xau_m15_paper_observation_batch_runner,
    run_xau_m15_paper_observation_batch_step,
)
from services.mt5.mt5_runtime_snapshot import reset_runtime_snapshots_for_tests, update_bars


class MT5XauM15PaperObservationBatchRunnerTests(unittest.TestCase):
    def tearDown(self) -> None:
        reset_runtime_snapshots_for_tests()

    def test_dry_run_does_not_open_shadow(self) -> None:
        client = _FakeClient()

        result = run_xau_m15_paper_observation_batch_runner(client=client, dry_run=True, target_trades=3, max_cycles=5, state_file=None, results_file=None)

        self.assertEqual(result["runner_state"], "idle_no_shadow")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_no_open_if_db_degraded(self) -> None:
        client = _FakeClient(db={**_db(), "db_degraded": True})

        result = _step(client)

        self.assertEqual(result["runner_state"], "stopped_by_db")
        self.assertEqual(result["stop_reason"], "db_degraded")
        self.assertEqual(client.open_calls, 0)

    def test_no_open_if_queue_depth_positive(self) -> None:
        client = _FakeClient(db={**_db(), "queue_depth": 1})

        result = _step(client)

        self.assertEqual(result["runner_state"], "stopped_by_db")
        self.assertEqual(result["stop_reason"], "queue_depth_high")
        self.assertEqual(client.open_calls, 0)

    def test_no_open_if_failed_writes_remain_after_queue_empty(self) -> None:
        client = _FakeClient(db={**_db(), "queue_depth": 0, "queued_writes": 0, "failed_writes": 3, "failed_writes_total": 3, "failed_writes_unresolved": 3})

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "stopped_by_db")
        self.assertEqual(result["stop_reason"], "failed_writes_unresolved")
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["paper_shadow_created"])

    def test_db_readiness_blocks_active_failed_writes(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        store = _MemoryShadowStore()
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")], store=store, db_state={**_db(), "failed_writes": 1, "failed_writes_total": 1, "failed_writes_active": 1})

        readiness = client.readiness()
        opened = client.open_shadow_once()

        self.assertEqual(readiness["readiness_state"], "blocked_db_not_clean")
        self.assertIn("failed_writes_active", readiness["failed_gate_names"])
        self.assertFalse(readiness["entry_allowed_for_paper_test"])
        self.assertFalse(opened["paper_shadow_created"])
        self.assertEqual(opened["open_count"], 0)
        self.assertEqual(len(store.rows), 0)

    def test_db_readiness_blocks_unresolved_critical_failed_writes(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        store = _MemoryShadowStore()
        db = {
            **_db(),
            "failed_writes": 1,
            "failed_writes_total": 1,
            "failed_writes_active": 1,
            "failed_writes_unresolved": 1,
            "failed_writes_critical": 1,
        }
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")], store=store, db_state=db)

        readiness = client.readiness()
        opened = client.open_shadow_once()

        self.assertEqual(readiness["readiness_state"], "blocked_db_not_clean")
        self.assertIn("failed_writes_critical", readiness["failed_gate_names"])
        self.assertFalse(readiness["entry_allowed_for_paper_test"])
        self.assertFalse(opened["paper_shadow_created"])
        self.assertEqual(len(store.rows), 0)

    def test_preflight_blocks_failed_write_semantics_unknown_even_with_zero_active(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        store = _MemoryShadowStore()
        db = {
            **_db(),
            "failed_writes": 4,
            "failed_writes_total": 4,
            "failed_writes_active": 0,
            "failed_writes_unresolved": 0,
            "failed_writes_critical": 0,
            "failed_write_semantics_known": False,
            "dropped_noncritical_writes_total": 4,
            "queue_drain_succeeded": True,
        }
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")], store=store, db_state=db)

        readiness = client.readiness()
        opened = client.open_shadow_once()

        self.assertEqual(readiness["readiness_state"], "blocked_db_not_clean")
        self.assertIn("failed_write_semantics_unknown", readiness["failed_gate_names"])
        self.assertFalse(readiness["entry_allowed_for_paper_test"])
        self.assertFalse(opened["paper_shadow_created"])
        self.assertEqual(len(store.rows), 0)

    def test_preflight_blocks_db_readiness_blocking_reason_failed_write_semantics_unknown(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        store = _MemoryShadowStore()
        db = {
            **_db(),
            "failed_writes": 4,
            "failed_writes_total": 4,
            "failed_writes_active": 0,
            "failed_writes_unresolved": 0,
            "failed_writes_critical": 0,
            "failed_write_semantics_known": True,
            "dropped_noncritical_writes_total": 4,
            "queue_drain_succeeded": True,
            "db_readiness_blocking_reason": "failed_write_semantics_unknown",
        }
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")], store=store, db_state=db)

        readiness = client.readiness()
        opened = client.open_shadow_once()

        self.assertEqual(readiness["readiness_state"], "blocked_db_not_clean")
        self.assertIn("failed_write_semantics_unknown", readiness["failed_gate_names"])
        self.assertFalse(readiness["entry_allowed_for_paper_test"])
        self.assertFalse(opened["paper_shadow_created"])
        self.assertEqual(len(store.rows), 0)

    def test_preflight_does_not_allow_dropped_noncritical_when_semantics_unknown(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        store = _MemoryShadowStore()
        db = {
            **_db(),
            "failed_writes": 4,
            "failed_writes_total": 4,
            "failed_writes_active": 0,
            "failed_writes_unresolved": 0,
            "failed_writes_critical": 0,
            "failed_write_semantics_known": False,
            "dropped_noncritical_writes": 4,
            "dropped_noncritical_writes_total": 4,
            "last_db_error_category": "",
            "last_db_error_at": "",
            "queue_drain_succeeded": True,
        }
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")], store=store, db_state=db)

        readiness = client.readiness()
        opened = client.open_shadow_once()

        self.assertEqual(readiness["readiness_state"], "blocked_db_not_clean")
        self.assertIn("failed_write_semantics_unknown", readiness["failed_gate_names"])
        self.assertFalse(readiness["entry_allowed_for_paper_test"])
        self.assertFalse(opened["paper_shadow_created"])
        self.assertEqual(len(store.rows), 0)

    def test_preflight_allows_historical_dropped_noncritical_only_when_semantics_known(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        store = _MemoryShadowStore()
        db = {
            **_db(),
            "failed_writes": 4,
            "failed_writes_total": 4,
            "failed_writes_active": 0,
            "failed_writes_unresolved": 0,
            "failed_writes_critical": 0,
            "dropped_noncritical_writes": 4,
            "dropped_noncritical_writes_total": 4,
            "last_db_error_category": "",
            "last_db_error_at": "",
            "queue_drain_succeeded": True,
        }
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")], store=store, db_state=db)

        readiness = client.readiness()
        opened = client.open_shadow_once()

        self.assertEqual(readiness["readiness_state"], "ready_for_one_cycle_paper_observation")
        self.assertEqual(readiness["failed_gate_names"], [])
        self.assertTrue(readiness["entry_allowed_for_paper_test"])
        self.assertTrue(opened["paper_shadow_created"])
        self.assertEqual(opened["open_shadow_count_after"], 1)
        self.assertEqual(len(store.rows), 1)
        self.assertFalse(opened["broker_touched"])
        self.assertFalse(opened["order_executed"])

    def test_preflight_fails_closed_when_failed_write_semantic_fields_missing(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        store = _MemoryShadowStore()
        db = {**_db(), "failed_writes": 4}
        for key in ("failed_writes_total", "failed_writes_active", "failed_writes_unresolved", "failed_writes_critical", "failed_write_semantics_known"):
            db.pop(key, None)
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")], store=store, db_state=db)

        readiness = client.readiness()
        opened = client.open_shadow_once()

        self.assertEqual(readiness["readiness_state"], "blocked_db_not_clean")
        self.assertIn("failed_write_semantics_unknown", readiness["failed_gate_names"])
        self.assertFalse(readiness["entry_allowed_for_paper_test"])
        self.assertFalse(opened["paper_shadow_created"])
        self.assertEqual(len(store.rows), 0)

    def test_preflight_uses_failed_writes_active_not_total(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        store = _MemoryShadowStore()
        db = {
            **_db(),
            "failed_writes": 2,
            "failed_writes_total": 2,
            "failed_writes_active": 0,
            "failed_writes_unresolved": 0,
            "failed_writes_critical": 0,
            "dropped_noncritical_writes_total": 2,
            "queue_drain_succeeded": True,
        }
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")], store=store, db_state=db)

        readiness = client.readiness()

        self.assertEqual(readiness["failed_gate_names"], [])
        self.assertTrue(readiness["entry_allowed_for_paper_test"])

    def test_no_broker_no_order_send(self) -> None:
        client = _FakeClient()

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_db_ok_blocks_queued_writes(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        store = _MemoryShadowStore()
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")], store=store, db_state={**_db(), "queued_writes": 2})

        readiness = client.readiness()
        opened = client.open_shadow_once()

        self.assertEqual(readiness["readiness_state"], "blocked_db_not_clean")
        self.assertIn("queued_writes_pending", readiness["failed_gate_names"])
        self.assertFalse(readiness["entry_allowed_for_paper_test"])
        self.assertFalse(opened["paper_shadow_created"])
        self.assertEqual(opened["open_count"], 0)
        self.assertEqual(len(store.rows), 0)

    def test_db_ok_fails_closed_when_write_counters_missing(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        db = _db()
        db.pop("failed_writes")
        db.pop("queued_writes")
        store = _MemoryShadowStore()
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")], store=store, db_state=db)

        readiness = client.readiness()
        opened = client.open_shadow_once()

        self.assertEqual(readiness["readiness_state"], "blocked_db_not_clean")
        self.assertIn("queued_writes_missing", readiness["failed_gate_names"])
        self.assertFalse(readiness["entry_allowed_for_paper_test"])
        self.assertFalse(opened["paper_shadow_created"])
        self.assertEqual(opened["open_count"], 0)
        self.assertEqual(len(store.rows), 0)

    def test_no_open_if_shadow_already_open(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_open())

        result = _step(client)

        self.assertEqual(result["runner_state"], "shadow_open_monitoring")
        self.assertEqual(result["open_shadow_count"], 1)
        self.assertEqual(client.open_calls, 0)

    def test_opens_one_shadow_when_gates_green_and_confirmed(self) -> None:
        client = _FakeClient()

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "opening_shadow")
        self.assertTrue(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 1)
        self.assertEqual(result["shadow_trade_id"], "xau-batch-opened")
        self.assertFalse(result["candidate_activated"])

    def test_btc_local_multi_asset_market_active_opens_paper_only_shadow(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        store = _MemoryShadowStore()
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")], store=store, db_state=_db())

        result = _step(client, dry_run=False, paper_only_confirmed=True, symbol="BTCUSD", broker_symbol="BTCUSD")

        self.assertEqual(result["runner_state"], "opening_shadow")
        self.assertTrue(result["paper_shadow_created"])
        self.assertEqual(result["symbol"], "BTCUSD")
        self.assertEqual(result["open_result"]["symbol"], "BTCUSD")
        self.assertEqual(result["open_result"]["open_shadow_count_after"], 1)
        self.assertEqual(len(store.rows), 1)
        self.assertEqual(store.rows[0]["status"], "open")
        self.assertEqual(store.rows[0]["journal_metadata"]["strategy_profile"], "unit_test_multi_asset|symbol=BTCUSD|timeframe=M15")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_eth_local_multi_asset_market_active_opens_paper_only_shadow(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("ETHUSD", closes=[200.0 - i * 0.5 for i in range(60)])
        store = _MemoryShadowStore()
        client = LocalPaperObservationClient(symbol="ETHUSD", broker_symbol="ETHUSD", timeframe="M15", asset_configs=[_asset_config("ETHUSD")], store=store, db_state=_db())

        result = _step(client, dry_run=False, paper_only_confirmed=True, symbol="ETHUSD", broker_symbol="ETHUSD")

        self.assertEqual(result["runner_state"], "opening_shadow")
        self.assertTrue(result["paper_shadow_created"])
        self.assertEqual(result["symbol"], "ETHUSD")
        self.assertEqual(result["open_result"]["side"], "sell")
        self.assertEqual(len(store.rows), 1)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_global_max_open_positions_total_one(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")], db_state=_db())

        result = client.readiness()

        self.assertEqual(result["max_open_positions"], 1)
        self.assertEqual(result["max_open_positions_total"], 1)
        self.assertEqual(result["open_count"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_crypto_readiness_requires_capital_protection(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")], db_state=_db())
        capital = {"ok": True, "capital_state": "normal", "safe_to_trade": True, "decision": "ALLOW_PAPER_REVIEW", **_safety()}

        with patch("services.mt5.mt5_xau_m15_paper_observation_batch_runner.run_capital_protection_governor", return_value=capital) as governor:
            with patch("services.mt5.mt5_xau_m15_paper_observation_batch_runner.assess_runtime_risk", return_value={"allowed": True, "reason": "", "risk_state": "normal", **_safety()}):
                result = client.readiness()

        governor.assert_called_once()
        self.assertFalse(governor.call_args.kwargs["persist_events"])
        self.assertFalse(governor.call_args.kwargs["load_persistent"])
        self.assertFalse(governor.call_args.kwargs["load_shadow_snapshot"])
        self.assertEqual(result["capital_state"], "normal")
        self.assertTrue(result["capital_allows_observation"])
        self.assertEqual(result["readiness_state"], "ready_for_one_cycle_paper_observation")
        self.assertTrue(result["entry_allowed_for_paper_test"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_crypto_readiness_blocks_when_capital_not_normal(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("ETHUSD", closes=[200.0 + i for i in range(60)])
        client = LocalPaperObservationClient(symbol="ETHUSD", broker_symbol="ETHUSD", timeframe="M15", asset_configs=[_asset_config("ETHUSD")], db_state=_db())
        capital = {
            "ok": True,
            "capital_state": "kill_switch",
            "safe_to_trade": False,
            "decision": "NO_TRADE",
            "reason": "capital_protection:recent_edge_negative",
            **_safety(),
        }

        with patch("services.mt5.mt5_xau_m15_paper_observation_batch_runner.run_capital_protection_governor", return_value=capital):
            with patch("services.mt5.mt5_xau_m15_paper_observation_batch_runner.assess_runtime_risk", return_value={"allowed": True, "reason": "", "risk_state": "normal", **_safety()}):
                result = _step(client, dry_run=False, paper_only_confirmed=True, symbol="ETHUSD", broker_symbol="ETHUSD")

        self.assertEqual(result["runner_state"], "readiness_blocked")
        self.assertEqual(result["readiness_state"], "blocked_capital_protection")
        self.assertIn("capital_allows_observation", result["failed_gate_names"])
        self.assertFalse(result["readiness"]["capital_allows_observation"])
        self.assertEqual(result["readiness"]["capital_state"], "kill_switch")
        self.assertFalse(result["entry_allowed_for_paper_test"])
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(result["open_count"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_crypto_http_client_never_calls_xau_monitor(self) -> None:
        client = HttpPaperObservationClient("https://example.invalid", symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")])
        client._get = Mock(side_effect=AssertionError("BTCUSD must not call XAU monitor"))
        client._post = Mock(side_effect=AssertionError("BTCUSD must not POST XAU monitor"))

        result = client.monitor()

        client._get.assert_not_called()
        client._post.assert_not_called()
        self.assertEqual(result["readiness_state"], "blocked_monitor_asset_mismatch")
        self.assertEqual(result["monitor_state"], "blocked_monitor_asset_mismatch")
        self.assertFalse(result["paper_close_applied"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_crypto_http_readiness_uses_generic_live_endpoint(self) -> None:
        client = HttpPaperObservationClient("https://example.invalid", symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD", min_bars=100)])
        client._get = Mock(
            return_value={
                "ok": True,
                "status": "multi_asset_paper_observation_readiness_ready",
                "symbol": "BTCUSD",
                "broker_symbol": "BTCUSD",
                "timeframe": "M15",
                "candidate_found": True,
                "candidate_status": "paper_observation_review",
                "readiness_state": "ready_for_one_cycle_paper_observation",
                "runtime_context_available": True,
                "runtime_context_recent": True,
                "bars_count": 120,
                "tick_available": True,
                "market_active": True,
                "price_moved_recently": True,
                "capital_allows_observation": True,
                "risk_allows_observation": True,
                "failed_gate_names": [],
                "failed_gates": [],
                "entry_allowed_for_paper_test": True,
                "candidate_activated": False,
                "paper_forward_onboarding_started": False,
                **_safety(),
            }
        )

        result = client.readiness()

        called_path = client._get.call_args.args[0]
        self.assertTrue(called_path.startswith("/api/genesis/mt5/paper-observation/readiness?"))
        self.assertIn("symbol=BTCUSD", called_path)
        self.assertIn("broker_symbol=BTCUSD", called_path)
        self.assertEqual(result["readiness_state"], "ready_for_one_cycle_paper_observation")
        self.assertTrue(result["entry_allowed_for_paper_test"])
        self.assertNotIn("generic_http_readiness_endpoint_missing", result["failed_gate_names"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_crypto_http_readiness_without_allowlist_fails_closed_before_http(self) -> None:
        client = HttpPaperObservationClient("https://example.invalid", symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15")
        client._get = Mock(side_effect=AssertionError("missing allowlist must not query live readiness"))

        result = client.readiness()

        client._get.assert_not_called()
        self.assertEqual(result["recommendation"], "missing_explicit_asset_config_allowlist")
        self.assertEqual(result["entry_block_type"], "asset_config_block")
        self.assertFalse(result["entry_allowed_for_paper_test"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_crypto_http_readiness_endpoint_failure_fails_closed(self) -> None:
        client = HttpPaperObservationClient("https://example.invalid", symbol="ETHUSD", broker_symbol="ETHUSD", timeframe="M15", asset_configs=[_asset_config("ETHUSD")])
        client._get = Mock(side_effect=RuntimeError("generic readiness route unavailable"))

        result = client.readiness()

        self.assertIn("generic_http_readiness_endpoint_missing", result["failed_gate_names"])
        self.assertFalse(result["entry_allowed_for_paper_test"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_multi_asset_readiness_wrapper_uses_runtime_snapshot(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(120)])

        with patch("services.mt5.mt5_xau_m15_paper_observation_batch_runner.run_capital_protection_governor", return_value={"capital_state": "normal", "safe_to_trade": True, **_safety()}):
            with patch("services.mt5.mt5_xau_m15_paper_observation_batch_runner.assess_runtime_risk", return_value={"allowed": True, "reason": "risk_governor_pass", "risk_state": "normal", **_safety()}):
                result = run_multi_asset_paper_observation_readiness(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", db_state=_db())

        self.assertEqual(result["readiness_state"], "ready_for_one_cycle_paper_observation")
        self.assertEqual(result["runtime_snapshot_context"], "bar_context")
        self.assertGreaterEqual(result["bars_count"], 100)
        self.assertTrue(result["market_active"])
        self.assertTrue(result["entry_allowed_for_paper_test"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_crypto_monitor_asset_mismatch_fails_closed(self) -> None:
        client = HttpPaperObservationClient("https://example.invalid", symbol="ETHUSD", broker_symbol="ETHUSD", timeframe="M15", asset_configs=[_asset_config("ETHUSD")])
        client._get = Mock(side_effect=AssertionError("ETHUSD must not call XAU monitor"))
        client._post = Mock(side_effect=AssertionError("ETHUSD must not POST XAU monitor"))

        result = client.monitor(apply_paper_close=True, exit_policy="fast_observation")

        client._get.assert_not_called()
        client._post.assert_not_called()
        self.assertEqual(result["readiness_state"], "blocked_monitor_asset_mismatch")
        self.assertEqual(result["exit_reason"], "blocked_monitor_asset_mismatch")
        self.assertFalse(result["should_close_paper"])
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(result["open_count"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_xau_backward_compatibility_still_uses_xau_monitor_only_for_xau(self) -> None:
        client = HttpPaperObservationClient("https://example.invalid", symbol="XAUUSD", broker_symbol="XAUUSD.b", timeframe="M15", asset_configs=[_asset_config("XAUUSD")])
        payload = {"ok": True, "monitor_state": "no_action", **_safety()}
        client._get = Mock(return_value=payload)
        client._post = Mock(side_effect=AssertionError("GET monitor should not POST"))

        result = client.monitor()

        self.assertEqual(result, payload)
        client._get.assert_called_once_with("/api/genesis/mt5/xau-m15/paper-shadow/monitor")
        client._post.assert_not_called()

    def test_multi_asset_without_explicit_allowlist_fails_closed(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", db_state=_db())

        result = _step(client, dry_run=False, paper_only_confirmed=True, symbol="BTCUSD", broker_symbol="BTCUSD")

        self.assertEqual(result["runner_state"], "readiness_blocked")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(result["stop_reason"], "missing_explicit_asset_config_allowlist")
        self.assertEqual(result["entry_block_type"], "asset_config_block")
        self.assertFalse(result["safety_violation"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_multi_asset_symbol_not_in_allowlist_fails_closed(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("LTCUSD", closes=[50.0 + i * 0.1 for i in range(60)])
        client = LocalPaperObservationClient(symbol="LTCUSD", broker_symbol="LTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")], db_state=_db())

        result = _step(client, dry_run=False, paper_only_confirmed=True, symbol="LTCUSD", broker_symbol="LTCUSD")

        self.assertEqual(result["runner_state"], "readiness_blocked")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(result["stop_reason"], "asset_not_in_explicit_asset_config_allowlist")
        self.assertEqual(result["entry_block_type"], "asset_config_block")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_only_allowlisted_assets_can_run(self) -> None:
        self.test_multi_asset_symbol_not_in_allowlist_fails_closed()

    def test_multi_asset_disabled_config_fails_closed(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        config = {**_asset_config("BTCUSD"), "enabled": False}
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[config], db_state=_db())

        result = _step(client, dry_run=False, paper_only_confirmed=True, symbol="BTCUSD", broker_symbol="BTCUSD")

        self.assertEqual(result["runner_state"], "readiness_blocked")
        self.assertEqual(result["stop_reason"], "asset_config_not_enabled")
        self.assertFalse(result["paper_shadow_created"])
        self.assertFalse(result["safety_violation"])

    def test_multi_asset_missing_safety_config_fails_closed(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        config = _asset_config("BTCUSD")
        config.pop("allow_broker_orders")
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[config], db_state=_db())

        result = _step(client, dry_run=False, paper_only_confirmed=True, symbol="BTCUSD", broker_symbol="BTCUSD")

        self.assertEqual(result["runner_state"], "readiness_blocked")
        self.assertEqual(result["stop_reason"], "asset_config_missing_allow_broker_orders")
        self.assertEqual(result["readiness_state"], "blocked_unsafe_order_policy")
        self.assertEqual(result["entry_block_type"], "unsafe_order_policy")
        self.assertEqual(result["open_count"], 0)
        self.assertFalse(result["paper_shadow_created"])
        self.assertFalse(result["safety_violation"])

    def test_multi_asset_missing_candidate_activation_config_fails_closed(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        config = _asset_config("BTCUSD")
        config.pop("allow_candidate_activation")
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[config], db_state=_db())

        result = _step(client, dry_run=False, paper_only_confirmed=True, symbol="BTCUSD", broker_symbol="BTCUSD")

        self.assertEqual(result["runner_state"], "readiness_blocked")
        self.assertEqual(result["stop_reason"], "asset_config_missing_allow_candidate_activation")
        self.assertEqual(result["readiness_state"], "blocked_unsafe_order_policy")
        self.assertEqual(result["entry_block_type"], "unsafe_order_policy")
        self.assertFalse(result["paper_shadow_created"])
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])

    def test_multi_asset_candidate_activation_true_is_safety_violation(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        config = {**_asset_config("BTCUSD"), "allow_candidate_activation": True}
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[config], db_state=_db())

        result = _step(client, dry_run=False, paper_only_confirmed=True, symbol="BTCUSD", broker_symbol="BTCUSD")

        self.assertEqual(result["runner_state"], "stopped_by_safety")
        self.assertEqual(result["readiness_state"], "blocked_unsafe_order_policy")
        self.assertTrue(result["safety_violation"])
        self.assertFalse(result["paper_shadow_created"])

    def test_multi_asset_paper_forward_true_is_safety_violation(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        config = {**_asset_config("BTCUSD"), "allow_paper_forward": True}
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[config], db_state=_db())

        result = _step(client, dry_run=False, paper_only_confirmed=True, symbol="BTCUSD", broker_symbol="BTCUSD")

        self.assertEqual(result["runner_state"], "stopped_by_safety")
        self.assertEqual(result["readiness_state"], "blocked_unsafe_order_policy")
        self.assertTrue(result["safety_violation"])
        self.assertFalse(result["paper_shadow_created"])

    def test_multi_asset_invalid_order_policy_is_safety_violation(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        config = {**_asset_config("BTCUSD"), "order_policy": "broker_orders_allowed"}
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[config], db_state=_db())

        result = _step(client, dry_run=False, paper_only_confirmed=True, symbol="BTCUSD", broker_symbol="BTCUSD")

        self.assertEqual(result["runner_state"], "stopped_by_safety")
        self.assertEqual(result["stop_reason"], "broker_or_order_flag_detected")
        self.assertEqual(result["readiness_state"], "blocked_unsafe_order_policy")
        self.assertEqual(result["entry_block_type"], "unsafe_order_policy")
        self.assertTrue(result["safety_violation"])
        self.assertFalse(result["paper_shadow_created"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_paper_supervisor_aborts_if_order_policy_not_journal_only(self) -> None:
        self.test_multi_asset_invalid_order_policy_is_safety_violation()

    def test_multi_asset_allow_broker_orders_is_safety_violation(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        config = {**_asset_config("BTCUSD"), "allow_broker_orders": True}
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[config], db_state=_db())

        result = _step(client, dry_run=False, paper_only_confirmed=True, symbol="BTCUSD", broker_symbol="BTCUSD")

        self.assertEqual(result["runner_state"], "stopped_by_safety")
        self.assertTrue(result["safety_violation"])
        self.assertFalse(result["paper_shadow_created"])

    def test_multi_asset_broker_touched_config_is_safety_violation(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        config = {**_asset_config("BTCUSD"), "broker_touched": True}
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[config], db_state=_db())

        result = _step(client, dry_run=False, paper_only_confirmed=True, symbol="BTCUSD", broker_symbol="BTCUSD")

        self.assertEqual(result["runner_state"], "stopped_by_safety")
        self.assertTrue(result["safety_violation"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_existing_shadow_without_asset_config_fails_closed_without_close(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        store = _MemoryShadowStore()
        store.rows.append({"shadow_trade_id": "btc-open", "symbol": "BTCUSD", "timeframe": "M15", "status": "open", **_safety()})
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", store=store, db_state=_db())

        result = _step(client, dry_run=False, paper_only_confirmed=True, symbol="BTCUSD", broker_symbol="BTCUSD")

        self.assertEqual(result["runner_state"], "readiness_blocked")
        self.assertEqual(result["stop_reason"], "missing_explicit_asset_config_allowlist")
        self.assertFalse(result["paper_close_applied"])
        self.assertEqual(len(store.rows), 1)

    def test_multi_asset_min_bars_config_is_enforced(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        config = _asset_config("BTCUSD", min_bars=100)
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[config], db_state=_db())

        result = _step(client, dry_run=False, paper_only_confirmed=True, symbol="BTCUSD", broker_symbol="BTCUSD")

        self.assertEqual(result["runner_state"], "readiness_blocked")
        self.assertIn("bars_count_below_100", result["failed_gate_names"])
        self.assertFalse(result["paper_shadow_created"])

    def test_multi_asset_frozen_market_blocks_open(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 for _ in range(60)])
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")], store=_MemoryShadowStore(), db_state=_db())

        result = _step(client, dry_run=False, paper_only_confirmed=True, symbol="BTCUSD", broker_symbol="BTCUSD")

        self.assertEqual(result["runner_state"], "readiness_blocked")
        self.assertEqual(result["readiness_state"], "blocked_market_inactive")
        self.assertIn("frozen_ohlc", result["failed_gate_names"])
        self.assertEqual(result["readiness"]["market_active_reason"], "frozen_ohlc")
        self.assertFalse(result["paper_shadow_created"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_market_active_guard_blocks_frozen_ohlc(self) -> None:
        self.test_multi_asset_frozen_market_blocks_open()

    def test_market_active_guard_reports_specific_block_reasons(self) -> None:
        now = datetime.now(timezone.utc)
        current_bars = _guard_bars(now=now, closes=[100.0 + idx * 0.2 for idx in range(10)])
        cases = [
            ("stale_bar", _guard_snapshot(bars=_guard_bars(now=now - timedelta(hours=4), closes=[100.0 + idx * 0.2 for idx in range(10)]))),
            ("stale_tick", _guard_snapshot(bars=current_bars, tick_time=now - timedelta(hours=4))),
            ("zero_spread", _guard_snapshot(bars=current_bars, spread=0.0)),
            ("excessive_spread", _guard_snapshot(bars=current_bars, spread=2.0), {"max_spread": 0.5}),
            ("invalid_quote", _guard_snapshot(bars=current_bars, bid=-1.0, ask=100.2, last=100.1, spread=0.1)),
            ("frozen_ohlc", _guard_snapshot(bars=_guard_bars(now=now, closes=[100.0 for _ in range(10)], frozen=True), bid=99.9, ask=100.1, last=100.0, spread=0.2)),
            ("insufficient_recent_movement", _guard_snapshot(bars=_guard_bars(now=now, closes=[100.0 + idx * 0.00001 for idx in range(10)], tiny_range=True)), {"min_absolute_move": 1.0}),
        ]

        for expected, snapshot, *override in cases:
            config = {**_asset_config("BTCUSD")["market_guard"], "min_bars": 10, **(override[0] if override else {})}
            result = MarketActiveGuard(config).evaluate(snapshot)
            self.assertFalse(result["market_active"], expected)
            self.assertFalse(result["entry_allowed_for_paper_test"], expected)
            self.assertEqual(result["readiness_state"], "blocked_market_inactive")
            self.assertEqual(result["reason"], expected)

    def test_btc_local_multi_asset_open_then_close_counts_valid_sample(self) -> None:
        reset_runtime_snapshots_for_tests()
        _seed_runtime("BTCUSD", closes=[100.0 + i for i in range(60)])
        store = _MemoryShadowStore()
        client = LocalPaperObservationClient(symbol="BTCUSD", broker_symbol="BTCUSD", timeframe="M15", asset_configs=[_asset_config("BTCUSD")], store=store, db_state=_db())

        opened = _step(client, dry_run=False, paper_only_confirmed=True, symbol="BTCUSD", broker_symbol="BTCUSD")
        _seed_runtime("BTCUSD", closes=[160.0 + i for i in range(60)])
        closed = _step(
            client,
            dry_run=False,
            paper_only_confirmed=True,
            exit_policy="fast_observation",
            time_stop_bars=1,
            symbol="BTCUSD",
            broker_symbol="BTCUSD",
        )

        self.assertEqual(opened["runner_state"], "opening_shadow")
        self.assertEqual(closed["runner_state"], "close_applied")
        self.assertTrue(closed["paper_close_applied"])
        self.assertEqual(closed["closed_trade"]["symbol"], "BTCUSD")
        self.assertEqual(closed["closed_trade"]["valid_winrate_sample"], True)
        self.assertEqual(closed["batch_stats"]["valid_trades_closed"], 1)
        self.assertEqual(closed["batch_stats"]["invalid_samples"], 0)
        self.assertEqual(len(store.rows), 2)
        self.assertEqual(store.rows[-1]["status"], "closed")
        self.assertFalse(closed["broker_touched"])
        self.assertFalse(closed["order_executed"])
        self.assertEqual(closed["order_policy"], "journal_only_no_broker")

    def test_recent_edge_negative_does_not_blindly_open_paper(self) -> None:
        client = _FakeClient(readiness=_recent_edge_ready(strict_passed=False))

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "waiting_for_high_quality_paper_signal")
        self.assertEqual(result["current_phase"], "adaptive_paper_cooldown")
        self.assertTrue(result["recent_edge_negative"])
        self.assertEqual(result["entry_block_type"], "adaptive_paper_cooldown")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_recent_edge_negative_can_wait_for_high_quality_paper_signal(self) -> None:
        client = _FakeClient(readiness=_recent_edge_ready(strict_passed=False, failed_strict=["trend_alignment_ok", "signal_direction"]))

        result = _step(client, dry_run=False, paper_only_confirmed=True, strict_paper_probe=True)

        self.assertEqual(result["runner_state"], "waiting_for_high_quality_paper_signal")
        self.assertIn("trend_alignment_ok", result["stop_reason"])
        self.assertIn("signal_direction", result["stop_reason"])
        self.assertEqual(result["next_action"], "wait_for_high_quality_paper_signal")
        self.assertFalse(result["entry_allowed_for_paper_test"])
        self.assertEqual(client.open_calls, 0)

    def test_legacy_readiness_recent_edge_negative_is_normalized_to_adaptive_wait(self) -> None:
        client = _FakeClient(readiness=_legacy_recent_edge_ready())

        result = _step(client, dry_run=False, paper_only_confirmed=True, strict_paper_probe=True)

        self.assertEqual(result["runner_state"], "waiting_for_high_quality_paper_signal")
        self.assertEqual(result["current_phase"], "adaptive_paper_cooldown")
        self.assertEqual(result["risk_governor_reason"], "recent_edge_negative")
        self.assertTrue(result["recent_edge_negative"])
        self.assertEqual(result["entry_block_type"], "adaptive_paper_cooldown")
        self.assertEqual(result["next_action"], "wait_for_high_quality_paper_signal")
        self.assertIn("signal_direction", result["stop_reason"])
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["paper_shadow_created"])

    def test_ready_readiness_with_stale_runtime_context_is_normalized_blocked(self) -> None:
        client = _FakeClient(readiness={**_ready(), "runtime_context_recent": False})

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "readiness_blocked")
        self.assertEqual(result["readiness_state"], "blocked")
        self.assertEqual(result["stop_reason"], "runtime_context_recent")
        self.assertIn("runtime_context_recent", result["failed_gate_names"])
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["paper_shadow_created"])

    def test_strict_paper_probe_opens_only_when_stricter_gates_pass(self) -> None:
        client = _FakeClient(readiness=_recent_edge_ready(strict_passed=True))

        result = _step(client, dry_run=False, paper_only_confirmed=True, strict_paper_probe=True)

        self.assertEqual(result["runner_state"], "opening_shadow")
        self.assertEqual(result["current_phase"], "strict_paper_probe")
        self.assertTrue(result["entry_allowed_for_paper_test"])
        self.assertTrue(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 1)
        self.assertTrue(client.last_strict_paper_probe)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_no_trade_signal_under_recent_edge_does_not_open(self) -> None:
        client = _FakeClient(readiness=_recent_edge_ready(strict_passed=False, failed_strict=["signal_direction"]))

        result = _step(client, dry_run=False, paper_only_confirmed=True, strict_paper_probe=True)

        self.assertEqual(result["runner_state"], "waiting_for_high_quality_paper_signal")
        self.assertIn("signal_direction", result["stop_reason"])
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 0)

    def test_open_persistence_failure_does_not_claim_shadow_created(self) -> None:
        client = _FakeClient(
            open_result={
                "ok": False,
                "paper_shadow_created": False,
                "shadow_trade_id": "xau-open-failed",
                "open_persistence_failed": True,
                "open_write_retained_critical": True,
                "reason": "open_persistence_failed",
                **_safety(),
            }
        )

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "stopped_by_open_persistence_failed")
        self.assertFalse(result["paper_shadow_created"])
        self.assertTrue(result["open_persistence_failed"])
        self.assertTrue(result["open_write_retained_critical"])
        self.assertEqual(result["stop_reason"], "open_persistence_failed")
        self.assertEqual(result["next_action"], "drain_queue_or_backfill_runtime_open_shadow")
        self.assertEqual(client.open_calls, 1)

    def test_created_open_response_without_shadow_id_is_invalid_contract(self) -> None:
        client = _FakeClient(
            open_result={
                "ok": True,
                "paper_shadow_created": True,
                "shadow_trade_id": "",
                "open_shadow_count_after": 0,
                **_safety(),
            }
        )

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "stopped_by_invalid_open_response")
        self.assertEqual(result["stop_reason"], "open_response_missing_shadow_trade_id")
        self.assertEqual(result["open_count"], 0)
        self.assertEqual(result["current_shadow_id"], "")
        self.assertEqual(result["current_shadow_source"], "")
        self.assertEqual(result["batch_stats"]["session_trades_opened"], 0)
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 1)

    def test_invalid_created_open_response_does_not_repeat_next_cycle(self) -> None:
        client = _FakeClient(
            open_result={
                "ok": True,
                "paper_shadow_created": True,
                "shadow_trade_id": "",
                "open_shadow_count_after": 0,
                **_safety(),
            }
        )

        result = run_xau_m15_paper_observation_batch_runner(
            client=client,
            dry_run=False,
            paper_only_confirmed=True,
            target_trades=3,
            max_cycles=2,
            state_file=None,
            results_file=None,
            sleep_fn=lambda _seconds: None,
        )

        self.assertEqual(result["runner_state"], "stopped_by_invalid_open_response")
        self.assertEqual(result["cycles_completed"], 1)
        self.assertEqual(result["session_trades_opened"], 0)
        self.assertEqual(result["current_shadow_id"], "")
        self.assertFalse(result["paper_shadow_created"])
        self.assertEqual(client.open_calls, 1)

    def test_does_not_close_entry_block_only(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_open(category="entry_block_only", should_watch=True, risk_block_type="entry_block"))

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "watch_only")
        self.assertTrue(result["should_watch_only"])
        self.assertFalse(result["paper_close_applied"])
        self.assertEqual(client.close_calls, 0)

    def test_does_not_close_caution_watch(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_open(category="caution_watch", should_watch=True))

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "watch_only")
        self.assertFalse(result["paper_close_applied"])
        self.assertEqual(client.close_calls, 0)

    def test_closes_take_profit_when_should_close_true(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_close("take_profit_hit", pnl=12.0, r=1.2))

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "close_applied")
        self.assertTrue(result["paper_close_applied"])
        self.assertEqual(client.close_calls, 1)
        self.assertEqual(result["closed_trade"]["exit_reason"], "take_profit_hit")

    def test_closes_stop_loss_when_should_close_true(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_close("stop_loss_hit", pnl=-10.0, r=-1.0))

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "close_applied")
        self.assertEqual(result["closed_trade"]["exit_reason"], "stop_loss_hit")
        self.assertEqual(client.close_calls, 1)

    def test_closes_trailing_exit_when_should_close_true(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_close("trailing_defensive_exit", pnl=4.0, r=0.4))

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "close_applied")
        self.assertEqual(result["closed_trade"]["exit_reason"], "trailing_defensive_exit")
        self.assertEqual(client.close_calls, 1)

    def test_closes_critical_safety_exit_when_should_close_true(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_close("critical_safety_exit", pnl=-2.0, r=-0.2, category="critical_safety_exit"))

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "close_applied")
        self.assertEqual(result["closed_trade"]["safety_exit_category"], "critical_safety_exit")
        self.assertEqual(client.close_calls, 1)

    def test_stops_on_multiple_open_shadows(self) -> None:
        client = _FakeClient(open_count=2, monitor={**_monitor_open(), "open_shadow_count": 2})

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "stopped_by_duplicate_shadow")
        self.assertEqual(result["stop_reason"], "multiple_open_shadows")
        self.assertEqual(client.open_calls, 0)

    def test_restart_safe_resume_open_shadow(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_open())
        state = {"current_open_shadow_id": "existing-shadow"}

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=[],
            cycle_number=1,
            target_trades=20,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["runner_state"], "shadow_open_monitoring")
        self.assertEqual(client.open_calls, 0)

    def test_orphan_does_not_invent_pnl(self) -> None:
        client = _FakeClient(open_count=0, monitor=_monitor_none())
        state = {"current_open_shadow_id": "lost-shadow"}

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=[],
            cycle_number=1,
            target_trades=20,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["anomaly"], "opened_shadow_missing_close_record")
        self.assertEqual(result["anomaly_type"], "opened_shadow_missing_close_record")
        self.assertEqual(result["closed_trade"], {})
        self.assertEqual(client.open_calls, 0)

    def test_broker_touched_true_blocks(self) -> None:
        client = _FakeClient(readiness={**_ready(), "broker_touched": True})

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "stopped_by_safety")
        self.assertTrue(result["safety_violation"])
        self.assertEqual(client.open_calls, 0)

    def test_order_executed_true_blocks(self) -> None:
        client = _FakeClient(monitor={**_monitor_none(), "order_executed": True})

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "stopped_by_safety")
        self.assertTrue(result["safety_violation"])
        self.assertEqual(client.open_calls, 0)

    def test_no_forbidden_execution_reference_added(self) -> None:
        forbidden = "order" + "_send"
        paths = [
            Path("services/mt5/mt5_market_active_guard.py"),
            Path("services/mt5/mt5_frozen_sample_guard.py"),
            Path("services/mt5/mt5_xau_m15_paper_observation_batch_runner.py"),
            Path("services/mt5/mt5_xau_m15_paper_test_supervisor.py"),
            Path("scripts/run_xau_m15_paper_observation_batch_runner.py"),
            Path("scripts/run_xau_m15_paper_test_supervisor.py"),
            Path("scripts/run_crypto_m15_paper_test_supervisor.py"),
        ]

        for path in paths:
            self.assertNotIn(forbidden, path.read_text(encoding="utf-8"), str(path))

    def test_paper_supervisor_never_calls_broker_order_send(self) -> None:
        self.test_no_forbidden_execution_reference_added()

    def test_stats_win_rate_profit_factor_expectancy(self) -> None:
        stats = compute_xau_m15_paper_batch_stats(
            [
                {"shadow_trade_id": "a", "pnl": 12.0, "r_multiple": 1.2, "exit_reason": "take_profit_hit", "age_minutes": 15},
                {"shadow_trade_id": "b", "pnl": -4.0, "r_multiple": -0.4, "exit_reason": "stop_loss_hit", "age_minutes": 30},
            ]
        )

        self.assertEqual(stats["trades_closed"], 2)
        self.assertEqual(stats["wins"], 1)
        self.assertEqual(stats["losses"], 1)
        self.assertEqual(stats["win_rate"], 50.0)
        self.assertEqual(stats["profit_factor"], 3.0)
        self.assertEqual(stats["expectancy"], 4.0)
        self.assertEqual(stats["avg_r"], 0.4)

    def test_max_cycles_limit_is_respected(self) -> None:
        client = _FakeClient()

        result = run_xau_m15_paper_observation_batch_runner(
            client=client,
            dry_run=False,
            paper_only_confirmed=True,
            max_cycles=2,
            target_trades=20,
            interval_seconds=0,
            state_file=None,
            results_file=None,
            sleep_fn=lambda _: None,
        )

        self.assertEqual(result["cycles_completed"], 2)
        self.assertLessEqual(result["cycles_completed"], 2)

    def test_wait_for_signal_dry_run_can_poll_multiple_cycles_without_opening(self) -> None:
        client = _FakeClient(readiness=_legacy_recent_edge_ready())

        result = run_xau_m15_paper_observation_batch_runner(
            client=client,
            dry_run=True,
            wait_for_signal=True,
            strict_paper_probe=True,
            max_cycles=2,
            target_trades=20,
            interval_seconds=0,
            state_file=None,
            results_file=None,
            sleep_fn=lambda _: None,
        )

        self.assertEqual(result["runner_state"], "waiting_for_high_quality_paper_signal")
        self.assertEqual(result["cycles_completed"], 2)
        self.assertEqual(client.open_calls, 0)
        self.assertFalse(result["paper_shadow_created"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])

    def test_target_trades_limit_stops(self) -> None:
        client = _FakeClient()

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state={},
            trades=[{"shadow_trade_id": "done", "pnl": 1.0, "r_multiple": 0.1, "exit_reason": "take_profit_hit"}],
            cycle_number=1,
            target_trades=1,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["runner_state"], "stopped_by_target_trades")
        self.assertEqual(result["last_closed_trade"]["shadow_trade_id"], "done")
        self.assertEqual(result["last_closed_trade"]["pnl"], 1.0)
        self.assertEqual(client.open_calls, 0)

    def test_target_trades_limit_ignores_invalid_samples(self) -> None:
        client = _FakeClient()

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state={},
            trades=[
                {
                    "shadow_trade_id": "invalid-flat",
                    "symbol": "XAUUSD",
                    "timeframe": "M15",
                    "entry_price": 100.0,
                    "exit_price": 100.0,
                    "pnl": 0.0,
                    "r_multiple": 0.0,
                    "exit_reason": "time_stop",
                    "market_inactive_or_frozen": True,
                    "status": "closed",
                    **_safety(),
                }
            ],
            cycle_number=1,
            target_trades=1,
            dry_run=True,
            paper_only_confirmed=False,
        )

        self.assertNotEqual(result["runner_state"], "stopped_by_target_trades")
        self.assertEqual(result["batch_stats"]["valid_trades_closed"], 0)
        self.assertEqual(result["batch_stats"]["invalid_samples"], 1)
        self.assertEqual(result["last_closed_trade"], {})
        self.assertEqual(client.open_calls, 0)

    def test_session_target_not_satisfied_by_old_history(self) -> None:
        client = _FakeClient()
        state = {"session_id": "session-new", "session_started_at": "2026-07-02T00:00:00+00:00"}
        old_history = [{"shadow_trade_id": "old", "pnl": 10.0, "r_multiple": 1.0, "exit_reason": "take_profit_hit", "session_id": "old-session"}]

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=old_history,
            cycle_number=1,
            target_trades=1,
            dry_run=True,
            paper_only_confirmed=False,
        )

        self.assertEqual(result["runner_state"], "idle_no_shadow")
        self.assertEqual(result["batch_stats"]["session_trades_closed"], 0)
        self.assertEqual(result["batch_stats"]["historical_closed_count"], 1)
        self.assertEqual(client.open_calls, 0)

    def test_target_not_reached_while_current_shadow_open(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_open())
        state = {
            "session_id": "session-live",
            "session_started_at": "2026-07-02T00:00:00+00:00",
            "current_open_shadow_id": "existing-shadow",
            "session_shadow_trade_ids": ["existing-shadow", "closed-one"],
        }
        trades = [{"shadow_trade_id": "closed-one", "pnl": 1.0, "r_multiple": 0.1, "exit_reason": "take_profit_hit", "session_id": "session-live"}]

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=trades,
            cycle_number=1,
            target_trades=1,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["runner_state"], "shadow_open_monitoring")
        self.assertEqual(result["open_shadow_count"], 1)
        self.assertEqual(client.open_calls, 0)

    def test_runtime_and_persistent_same_shadow_merges_to_one(self) -> None:
        client = _FakeClient(
            open_payload={
                "ok": True,
                "open_count": 2,
                "runtime_open_count": 1,
                "persistent_open_count": 1,
                "merged_open_count": 1,
                "duplicate_detected": True,
                "open_source": "merged",
                "trades": [{"shadow_trade_id": "same-shadow", "symbol": "XAUUSD", "timeframe": "M15"}],
                **_safety(),
            },
            monitor={**_monitor_open(), "shadow_trade_id": "same-shadow", "open_shadow_count": 1, "shadow_source": "merged"},
        )

        result = _step(client, dry_run=False, paper_only_confirmed=True)

        self.assertEqual(result["runner_state"], "shadow_open_monitoring")
        self.assertEqual(result["open_shadow_count"], 1)
        self.assertEqual(result["current_shadow_source"], "merged")
        self.assertEqual(client.open_calls, 0)

    def test_stats_are_session_scoped_and_side_stats_are_correct(self) -> None:
        stats = compute_xau_m15_paper_batch_stats(
            [
                {"shadow_trade_id": "buy-win", "side": "buy", "pnl": 4.0, "r_multiple": 0.4, "exit_reason": "take_profit_hit", "session_id": "session-a"},
                {"shadow_trade_id": "sell-loss", "side": "sell", "pnl": -2.0, "r_multiple": -0.2, "exit_reason": "stop_loss_hit", "session_id": "session-a"},
                {"shadow_trade_id": "old-win", "side": "buy", "pnl": 100.0, "r_multiple": 10.0, "exit_reason": "take_profit_hit", "session_id": "old-session"},
            ],
            state={"session_id": "session-a", "session_started_at": "2026-07-02T00:00:00+00:00"},
        )

        self.assertEqual(stats["session_trades_closed"], 2)
        self.assertEqual(stats["historical_closed_count"], 1)
        self.assertEqual(stats["wins"], 1)
        self.assertEqual(stats["losses"], 1)
        self.assertEqual(stats["win_rate"], 50.0)
        self.assertEqual(stats["side_stats"]["buy_count"], 1)
        self.assertEqual(stats["side_stats"]["sell_count"], 1)
        self.assertEqual(stats["side_stats"]["buy_win_rate"], 100.0)
        self.assertEqual(stats["side_stats"]["sell_win_rate"], 0.0)

    def test_xau_frozen_time_stop_does_not_count_for_winrate(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_close("paper_timebox_exit", pnl=0.0, r=0.0, market_inactive=True, no_price_movement=True))

        result = _step(client, dry_run=False, paper_only_confirmed=True, exit_policy="fast_observation", symbol="XAUUSD", broker_symbol="XAUUSD.b")

        self.assertEqual(result["runner_state"], "close_applied")
        self.assertTrue(result["paper_close_applied"])
        self.assertEqual(result["batch_stats"]["valid_trades_closed"], 0)
        self.assertEqual(result["batch_stats"]["invalid_samples"], 1)
        self.assertEqual(result["batch_stats"]["session_trades_closed"], 0)
        self.assertEqual(result["batch_stats"]["win_rate"], 0.0)
        self.assertEqual(result["closed_trade"]["sample_valid"], False)
        self.assertEqual(result["closed_trade"]["invalid_sample_reason"], "market_inactive_or_frozen")
        self.assertEqual(result["closed_trade"]["invalid_reason"], "market_inactive_or_frozen")
        self.assertEqual(result["closed_trade"]["metric_exclusion_reason"], "excluded_from_winrate_frozen_market")
        self.assertFalse(result["closed_trade"]["use_for_optimization"])
        self.assertFalse(result["closed_trade"]["use_for_calibration"])
        self.assertFalse(result["closed_trade"]["strategy_promotion_eligible"])
        self.assertFalse(result["closed_trade"]["candidate_promotion_eligible"])

    def test_literal_time_stop_frozen_sample_is_excluded_from_winrate_and_optimization(self) -> None:
        stats = compute_xau_m15_paper_batch_stats(
            [
                {
                    "shadow_trade_id": "flat-time-stop",
                    "symbol": "XAUUSD",
                    "timeframe": "M15",
                    "entry_price": 100.0,
                    "exit_price": 100.0,
                    "pnl": 0.0,
                    "r_multiple": 0.0,
                    "exit_reason": "time_stop",
                    "market_inactive_or_frozen": True,
                    "no_price_movement": True,
                    "status": "closed",
                    **_safety(),
                }
            ]
        )

        self.assertEqual(stats["valid_trades_closed"], 0)
        self.assertEqual(stats["invalid_samples"], 1)
        self.assertEqual(stats["win_rate"], 0.0)
        self.assertEqual(stats["profit_factor"], 0.0)

    def test_frozen_time_stop_sample_excluded_from_metrics(self) -> None:
        self.test_literal_time_stop_frozen_sample_is_excluded_from_winrate_and_optimization()

    def test_btc_active_close_counts_valid_sample(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_close("paper_timebox_exit", pnl=25.0, r=0.25, side="buy", market_active=True, price_source="tick_bid"))

        result = _step(client, dry_run=False, paper_only_confirmed=True, exit_policy="fast_observation", symbol="BTCUSD", broker_symbol="BTCUSD")

        self.assertEqual(result["closed_trade"]["symbol"], "BTCUSD")
        self.assertEqual(result["batch_stats"]["symbol"], "BTCUSD")
        self.assertEqual(result["batch_stats"]["valid_trades_closed"], 1)
        self.assertEqual(result["batch_stats"]["invalid_samples"], 0)
        self.assertEqual(result["batch_stats"]["wins"], 1)
        self.assertEqual(result["batch_stats"]["win_rate"], 100.0)

    def test_eth_active_close_counts_valid_sample(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_close("paper_timebox_exit", pnl=-4.0, r=-0.2, side="sell", market_active=True, price_source="tick_ask"))

        result = _step(client, dry_run=False, paper_only_confirmed=True, exit_policy="fast_observation", symbol="ETHUSD", broker_symbol="ETHUSD")

        self.assertEqual(result["closed_trade"]["symbol"], "ETHUSD")
        self.assertEqual(result["batch_stats"]["symbol"], "ETHUSD")
        self.assertEqual(result["batch_stats"]["valid_trades_closed"], 1)
        self.assertEqual(result["batch_stats"]["losses"], 1)
        self.assertEqual(result["batch_stats"]["win_rate"], 0.0)

    def test_entry_exit_equal_market_inactive_is_invalid_sample(self) -> None:
        stats = compute_xau_m15_paper_batch_stats(
            [
                {
                    "shadow_trade_id": "flat-frozen",
                    "symbol": "BTCUSD",
                    "timeframe": "M15",
                    "entry_price": 100.0,
                    "exit_price": 100.0,
                    "pnl": 0.0,
                    "r_multiple": 0.0,
                    "exit_reason": "paper_timebox_exit",
                    "market_inactive_or_frozen": True,
                    **_safety(),
                }
            ],
            symbol="BTCUSD",
            timeframe="M15",
        )

        self.assertEqual(stats["valid_trades_closed"], 0)
        self.assertEqual(stats["invalid_samples"], 1)
        self.assertEqual(stats["session_trades_closed"], 0)

    def test_legacy_xau_flat_time_stop_without_price_source_is_invalid_sample(self) -> None:
        stats = compute_xau_m15_paper_batch_stats(
            [
                {
                    "shadow_trade_id": "legacy-xau-flat",
                    "symbol": "XAUUSD",
                    "timeframe": "M15",
                    "entry_price": 4219.6,
                    "exit_price": 4219.6,
                    "pnl": 0.0,
                    "r_multiple": 0.0,
                    "exit_reason": "time_stop",
                    **_safety(),
                }
            ],
            symbol="XAUUSD",
            timeframe="M15",
        )

        self.assertEqual(stats["valid_trades_closed"], 0)
        self.assertEqual(stats["invalid_samples"], 1)
        self.assertEqual(stats["session_trades_closed"], 0)
        self.assertEqual(stats["win_rate"], 0.0)

    def test_entry_exit_equal_active_price_source_counts_breakeven(self) -> None:
        stats = compute_xau_m15_paper_batch_stats(
            [
                {
                    "shadow_trade_id": "flat-real",
                    "symbol": "ETHUSD",
                    "timeframe": "M15",
                    "entry_price": 100.0,
                    "exit_price": 100.0,
                    "pnl": 0.0,
                    "r_multiple": 0.0,
                    "exit_reason": "paper_timebox_exit",
                    "market_active": True,
                    "price_source": "tick_bid",
                    **_safety(),
                }
            ],
            symbol="ETHUSD",
            timeframe="M15",
        )

        self.assertEqual(stats["valid_trades_closed"], 1)
        self.assertEqual(stats["invalid_samples"], 0)
        self.assertEqual(stats["breakeven"], 1)

    def test_multi_symbol_stats_do_not_mix_history_or_session_stats(self) -> None:
        trades = [
            {"shadow_trade_id": "btc", "symbol": "BTCUSD", "timeframe": "M15", "pnl": 5.0, "r_multiple": 0.5, "exit_reason": "paper_timebox_exit", **_safety()},
            {"shadow_trade_id": "eth", "symbol": "ETHUSD", "timeframe": "M15", "pnl": -2.0, "r_multiple": -0.2, "exit_reason": "paper_timebox_exit", **_safety()},
        ]

        btc = compute_xau_m15_paper_batch_stats(trades, symbol="BTCUSD", timeframe="M15")
        eth = compute_xau_m15_paper_batch_stats(trades, symbol="ETHUSD", timeframe="M15")

        self.assertEqual(btc["valid_trades_closed"], 1)
        self.assertEqual(btc["wins"], 1)
        self.assertEqual(eth["valid_trades_closed"], 1)
        self.assertEqual(eth["losses"], 1)

    def test_pending_state_live_open_same_id_monitors_existing(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_open())
        state = {"current_open_shadow_id": "existing-shadow", "trades_opened": 1}

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=[],
            cycle_number=1,
            target_trades=20,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["runner_state"], "shadow_open_monitoring")
        self.assertEqual(client.open_calls, 0)

    def test_pending_state_open_count_zero_history_closed_imports_result(self) -> None:
        closed = _history_closed("pending-shadow", pnl=8.0, r=0.8)
        client = _FakeClient(history=[closed])
        state = {"current_open_shadow_id": "pending-shadow", "last_shadow_trade_id": "pending-shadow", "trades_opened": 1}

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=[],
            cycle_number=1,
            target_trades=20,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["runner_state"], "reconciled_closed_shadow")
        self.assertEqual(result["reconciled_shadow_trade_id"], "pending-shadow")
        self.assertEqual(result["closed_trade"]["pnl"], 8.0)
        self.assertEqual(result["batch_stats"]["trades_closed"], 1)
        self.assertEqual(result["batch_stats"]["wins"], 1)
        self.assertEqual(client.open_calls, 0)

    def test_pending_state_open_count_zero_history_missing_stops_orphan(self) -> None:
        client = _FakeClient(history=[])
        state = {"current_open_shadow_id": "missing-shadow", "last_shadow_trade_id": "missing-shadow", "trades_opened": 1}

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=[],
            cycle_number=1,
            target_trades=20,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["runner_state"], "stopped_by_orphaned_shadow_missing_close_record")
        self.assertEqual(result["anomaly_type"], "opened_shadow_missing_close_record")
        self.assertEqual(result["orphan_shadow_trade_id"], "missing-shadow")
        self.assertEqual(result["closed_trade"], {})
        self.assertEqual(client.open_calls, 0)

    def test_pending_state_open_count_zero_history_unavailable_stops_without_inventing_pnl(self) -> None:
        client = _FakeClient(
            history_payload={
                "ok": False,
                "history_available": False,
                "reason": "history_schema_optional_column_missing",
                "closed_trades": [],
                "trades": [],
                **_safety(),
            }
        )
        state = {"current_open_shadow_id": "pending-shadow", "last_shadow_trade_id": "pending-shadow", "trades_opened": 1}

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=[],
            cycle_number=1,
            target_trades=20,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["runner_state"], "stopped_by_history_unavailable")
        self.assertEqual(result["stop_reason"], "history_schema_optional_column_missing")
        self.assertEqual(result["next_action"], "fix_history_before_next_open")
        self.assertEqual(result["closed_trade"], {})
        self.assertEqual(client.open_calls, 0)

    def test_no_new_shadow_opened_while_pending_reconciliation_exists(self) -> None:
        client = _FakeClient(history=[])
        state = {"pending_reconciliation_shadow_id": "pending-shadow", "trades_opened": 1}

        result = run_xau_m15_paper_observation_batch_step(
            client=client,
            state=state,
            trades=[],
            cycle_number=1,
            target_trades=20,
            dry_run=False,
            paper_only_confirmed=True,
        )

        self.assertEqual(result["runner_state"], "stopped_by_orphaned_shadow_missing_close_record")
        self.assertEqual(client.open_calls, 0)

    def test_fast_observation_closes_timebox_exit(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_close("paper_timebox_exit", pnl=0.2, r=0.02, bars_since_entry=2))

        result = _step(client, dry_run=False, paper_only_confirmed=True, exit_policy="fast_observation", time_stop_bars=2)

        self.assertEqual(result["runner_state"], "close_applied")
        self.assertEqual(result["closed_trade"]["exit_reason"], "paper_timebox_exit")
        self.assertEqual(client.close_calls, 1)

    def test_fast_observation_closes_fast_trailing_exit(self) -> None:
        client = _FakeClient(open_count=1, monitor=_monitor_close("paper_fast_trailing_exit", pnl=2.0, r=0.2))

        result = _step(client, dry_run=False, paper_only_confirmed=True, exit_policy="fast_observation")

        self.assertEqual(result["runner_state"], "close_applied")
        self.assertEqual(result["closed_trade"]["exit_reason"], "paper_fast_trailing_exit")
        self.assertEqual(client.close_calls, 1)

    def test_history_endpoint_payload_is_read_only(self) -> None:
        client = _FakeClient(history=[_history_closed("closed-shadow", pnl=2.0, r=0.2)])

        history = client.shadow_trade_history()

        self.assertTrue(history["ok"])
        self.assertTrue(history["status_endpoints_write_free"])
        self.assertEqual(history["closed_count"], 1)
        self.assertFalse(history["broker_touched"])
        self.assertFalse(history["order_executed"])
        self.assertEqual(history["order_policy"], "journal_only_no_broker")


def _step(
    client: "_FakeClient",
    *,
    dry_run: bool = False,
    paper_only_confirmed: bool = False,
    exit_policy: str = "default",
    time_stop_bars: int = 2,
    strict_paper_probe: bool = False,
    symbol: str = "XAUUSD",
    broker_symbol: str = "XAUUSD.b",
) -> dict[str, object]:
    return run_xau_m15_paper_observation_batch_step(
        client=client,
        state={},
        trades=[],
        cycle_number=1,
        target_trades=20,
        dry_run=dry_run,
        paper_only_confirmed=paper_only_confirmed,
        exit_policy=exit_policy,
        time_stop_bars=time_stop_bars,
        strict_paper_probe=strict_paper_probe,
        symbol=symbol,
        broker_symbol=broker_symbol,
        timeframe="M15",
    )


class _FakeClient:
    source = "fake"

    def __init__(
        self,
        *,
        db: dict[str, object] | None = None,
        readiness: dict[str, object] | None = None,
        open_count: int = 0,
        monitor: dict[str, object] | None = None,
        history: list[dict[str, object]] | None = None,
        history_payload: dict[str, object] | None = None,
        open_result: dict[str, object] | None = None,
        open_payload: dict[str, object] | None = None,
    ) -> None:
        self.db = db or _db()
        self.ready = readiness or _ready()
        self.open_count = open_count
        self.monitor_payload = monitor or _monitor_none()
        self.history_rows = [dict(row) for row in (history or [])]
        self.history_payload = dict(history_payload) if history_payload is not None else None
        self.open_result = dict(open_result) if open_result is not None else None
        self.open_payload = dict(open_payload) if open_payload is not None else None
        self.open_calls = 0
        self.close_calls = 0
        self.last_strict_paper_probe = False

    def persistent_status(self) -> dict[str, object]:
        return dict(self.db)

    def open_shadow_trades(self) -> dict[str, object]:
        if self.open_payload is not None:
            return dict(self.open_payload)
        trades = [{"shadow_trade_id": "existing-shadow", "symbol": "XAUUSD", "timeframe": "M15"}] if self.open_count else []
        if self.open_count > 1:
            trades.append({"shadow_trade_id": "existing-shadow-2", "symbol": "XAUUSD", "timeframe": "M15"})
        return {"ok": True, "open_count": self.open_count, "trades": trades, **_safety()}

    def shadow_trade_history(self) -> dict[str, object]:
        if self.history_payload is not None:
            return dict(self.history_payload)
        open_rows = [row for row in self.history_rows if row.get("status") == "open"]
        closed_rows = [row for row in self.history_rows if row.get("status") == "closed" or row.get("closed_at")]
        return {
            "ok": True,
            "status": "persistent_intelligence_shadow_trade_history_ready",
            "status_endpoints_write_free": True,
            "trades": [dict(row) for row in self.history_rows],
            "open_trades": [dict(row) for row in open_rows],
            "closed_trades": [dict(row) for row in closed_rows],
            "open_count": len(open_rows),
            "closed_count": len(closed_rows),
            **_safety(),
        }

    def readiness(self) -> dict[str, object]:
        return dict(self.ready)

    def monitor(
        self,
        *,
        apply_paper_close: bool = False,
        exit_policy: str = "default",
        time_stop_bars: int = 2,
        max_hold_minutes: float | None = None,
        min_r_to_arm_trailing: float = 0.25,
        giveback_r: float = 0.15,
        fast_loss_cut_r: float = -0.25,
    ) -> dict[str, object]:
        del exit_policy, time_stop_bars, max_hold_minutes, min_r_to_arm_trailing, giveback_r, fast_loss_cut_r
        if apply_paper_close:
            self.close_calls += 1
            self.open_count = 0
            return {**self.monitor_payload, "paper_close_applied": True, "shadow_status_after": "closed", **_safety()}
        return dict(self.monitor_payload)

    def open_shadow_once(self, *, strict_paper_probe: bool = False) -> dict[str, object]:
        self.last_strict_paper_probe = bool(strict_paper_probe)
        self.open_calls += 1
        if self.open_result is not None:
            return dict(self.open_result)
        self.open_count = 1
        return {
            "ok": True,
            "paper_shadow_created": True,
            "shadow_trade_id": "xau-batch-opened",
            "open_shadow_count_after": 1,
            "candidate_activated": False,
            "paper_forward_onboarding_started": False,
            **_safety(),
        }


class _MemoryShadowStore:
    def __init__(self) -> None:
        self.rows: list[dict[str, object]] = []

    def record_shadow_trade(self, payload: dict[str, object], *, critical: bool | None = None) -> dict[str, object]:
        self.rows.append(dict(payload))
        return {"ok": True, "critical": bool(critical), **_safety()}


def _guard_bars(*, now: datetime, closes: list[float], frozen: bool = False, tiny_range: bool = False) -> list[dict[str, object]]:
    base = now - timedelta(minutes=max(0, len(closes) - 1))
    bars: list[dict[str, object]] = []
    for idx, close in enumerate(closes):
        if frozen:
            open_price = high = low = close
        elif tiny_range:
            open_price = close
            high = close + 0.000001
            low = close - 0.000001
        else:
            open_price = close - 0.05
            high = close + 0.1
            low = close - 0.1
        bars.append(
            {
                "time": (base + timedelta(minutes=idx)).isoformat(),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "tick_volume": 100 + idx,
            }
        )
    return bars


def _guard_snapshot(
    *,
    bars: list[dict[str, object]],
    bid: float = 101.0,
    ask: float = 101.2,
    last: float = 101.1,
    spread: float = 0.2,
    tick_time: datetime | None = None,
) -> dict[str, object]:
    now = datetime.now(timezone.utc)
    tick_at = tick_time or now
    return {
        "runtime_snapshot_recent": True,
        "bars_count": len(bars),
        "ohlc_recent": bars,
        "last_tick_at": tick_at.isoformat(),
        "last_tick": {
            "bid": bid,
            "ask": ask,
            "last": last,
            "spread": spread,
        },
        "spread": spread,
        "last_price": last,
        "bars_last_at": str(bars[-1].get("time") if bars else now.isoformat()),
    }


def _seed_runtime(symbol: str, *, closes: list[float]) -> None:
    base = datetime.now(timezone.utc) - timedelta(minutes=max(0, len(closes) - 1))
    bars = [
        {
            "time": (base + timedelta(minutes=idx)).isoformat(),
            "open": close,
            "high": close + 0.5,
            "low": close - 0.5,
            "close": close,
            "tick_volume": 100 + idx,
        }
        for idx, close in enumerate(closes)
    ]
    last = float(closes[-1])
    update_bars(
        symbol,
        "M15",
        bars,
        tick={
            "symbol": symbol,
            "timeframe": "M15",
            "bid": last - 0.01,
            "ask": last + 0.01,
            "last": last,
            "spread": 0.02,
        },
        min_bars=50,
    )


def _db() -> dict[str, object]:
    return {
        "provider": "railway_postgres",
        "db_available": True,
        "db_degraded": False,
        "tables_ready": True,
        "queue_depth": 0,
        "queued_writes": 0,
        "failed_writes": 0,
        "failed_writes_total": 0,
        "failed_writes_active": 0,
        "failed_writes_unresolved": 0,
        "failed_writes_critical": 0,
        "failed_write_semantics_known": True,
        "dropped_noncritical_writes": 0,
        "dropped_noncritical_writes_total": 0,
        "last_db_error_category": "",
        "last_db_error_at": "",
        "queue_drain_succeeded": True,
        "db_readiness_blocking_reason": "",
        "recommendation": "persistent_intelligence_ready",
        **_safety(),
    }


def _ready() -> dict[str, object]:
    return {
        "ok": True,
        "candidate_found": True,
        "candidate_status": "paper_observation_review",
        "readiness_state": "ready_for_one_cycle_paper_observation",
        "runtime_context_available": True,
        "runtime_context_recent": True,
        "bars_count": 120,
        "tick_available": True,
        "capital_allows_observation": True,
        "risk_allows_observation": True,
        "adaptive_allows_observation": True,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        **_safety(),
    }


def _recent_edge_ready(*, strict_passed: bool, failed_strict: list[str] | None = None) -> dict[str, object]:
    failed = failed_strict if failed_strict is not None else ([] if strict_passed else ["signal_direction"])
    return {
        **_ready(),
        "readiness_state": "blocked",
        "risk_allows_observation": False,
        "risk_governor_reason": "recent_edge_negative",
        "recent_edge_negative": True,
        "recommendation": "strict_paper_probe_allowed_real_trading_still_blocked" if strict_passed else "adaptive_paper_cooldown_wait_for_high_quality_paper_signal",
        "failed_gate_names": ["risk_allows_observation"],
        "failed_gate_reasons": {"risk_allows_observation": {"actual": "recent_edge_negative", "required": "risk_governor_pass"}},
        "entry_block_type": "adaptive_paper_cooldown",
        "entry_allowed_for_paper_test": bool(strict_passed),
        "gate_summary": {
            "failed_gate_names": ["risk_allows_observation"],
            "risk_governor_reason": "recent_edge_negative",
            "recent_edge_negative": True,
        },
        "strict_paper_probe": {
            "mode": "strict_paper_probe",
            "strict_paper_probe_passed": bool(strict_passed),
            "failed_strict_gate_names": failed,
            "trend_alignment_ok": "trend_alignment_ok" not in failed,
            "spread_ok": "spread_ok" not in failed,
            "volatility_ok": "volatility_ok" not in failed,
            "no_duplicate_shadow": "no_duplicate_shadow" not in failed,
            "signal_direction": "buy" if "signal_direction" not in failed else "",
            "db_healthy_and_queue_empty": True,
            "runtime_context_recent": True,
            **_safety(),
        },
    }


def _legacy_recent_edge_ready() -> dict[str, object]:
    return {
        **_ready(),
        "readiness_state": "blocked",
        "risk_allows_observation": False,
        "risk_state": "defensive",
        "recommendation": "resolve_observation_safety_gates",
        "failed_gates": ["risk_allows_observation"],
        "gates": {
            "risk_allows_observation": {
                "actual": "recent_edge_negative",
                "passed": False,
                "required": "risk_governor_pass",
            }
        },
    }


def _monitor_none() -> dict[str, object]:
    return {
        "ok": True,
        "monitor_state": "no_action",
        "open_shadow_count": 0,
        "shadow_trade_id": "",
        "exit_signal": False,
        "exit_reason": "no_open_shadow",
        "should_close_paper": False,
        "should_watch_only": False,
        "paper_close_applied": False,
        **_safety(),
    }


def _monitor_open(*, category: str = "none", should_watch: bool = False, risk_block_type: str = "none") -> dict[str, object]:
    return {
        "ok": True,
        "monitor_state": "open_monitoring",
        "open_shadow_count": 1,
        "shadow_trade_id": "existing-shadow",
        "exit_signal": False,
        "exit_reason": "caution_watch" if should_watch else "",
        "should_close_paper": False,
        "should_watch_only": should_watch,
        "safety_exit_category": category,
        "risk_block_type": risk_block_type,
        "paper_close_applied": False,
        **_safety(),
    }


def _monitor_close(
    exit_reason: str,
    *,
    pnl: float,
    r: float,
    category: str = "none",
    bars_since_entry: int = 2,
    side: str = "buy",
    market_active: bool | None = None,
    market_inactive: bool = False,
    no_price_movement: bool = False,
    price_source: str = "",
) -> dict[str, object]:
    return {
        "ok": True,
        "monitor_state": "exit_pending",
        "open_shadow_count": 1,
        "shadow_trade_id": "existing-shadow",
        "side": side,
        "entry_price": 100.0,
        "current_price": 100.0 + pnl,
        "stop_loss": 95.0,
        "take_profit": 110.0,
        "unrealized_pnl": pnl,
        "unrealized_pnl_pct": pnl,
        "r_multiple": r,
        "age_minutes": 20,
        "bars_since_entry": bars_since_entry,
        "exit_signal": True,
        "exit_reason": exit_reason,
        "should_close_paper": True,
        "should_watch_only": False,
        "safety_exit_category": category,
        "safety_exit_reason_detail": "unit_test",
        "close_decision_reason": f"close_paper:{exit_reason}",
        "market_active": market_active,
        "market_inactive_or_frozen": market_inactive,
        "no_price_movement": no_price_movement,
        "price_source": price_source,
        "paper_close_applied": False,
        **_safety(),
    }


def _history_closed(shadow_trade_id: str, *, pnl: float, r: float) -> dict[str, object]:
    return {
        "shadow_trade_id": shadow_trade_id,
        "symbol": "XAUUSD",
        "broker_symbol": "XAUUSD.b",
        "timeframe": "M15",
        "strategy_profile": "volatility_compression_breakout|mode=nr7_trailing_defensive",
        "side": "buy",
        "entry_price": 100.0,
        "exit_price": 100.0 + pnl,
        "pnl": pnl,
        "pnl_pct": pnl,
        "r_multiple": r,
        "status": "closed",
        "opened_at": "2026-06-16T09:00:00+00:00",
        "closed_at": "2026-06-16T09:30:00+00:00",
        "exit_reason": "take_profit_hit" if pnl > 0 else "stop_loss_hit",
        **_safety(),
    }


def _safety() -> dict[str, object]:
    return {"broker_touched": False, "order_executed": False, "order_policy": "journal_only_no_broker"}


def _asset_config(symbol: str, *, timeframe: str = "M15", min_bars: int = 50) -> dict[str, object]:
    clean_symbol = str(symbol).upper()
    return {
        "symbol": clean_symbol,
        "broker_symbol": clean_symbol,
        "timeframe": timeframe,
        "enabled": True,
        "order_policy": "journal_only_no_broker",
        "allow_broker_orders": False,
        "allow_candidate_activation": False,
        "allow_paper_forward": False,
        "max_open_positions": 1,
        "max_open_positions_total": 1,
        "min_bars": min_bars,
        "market_guard": {
            "min_bars": min_bars,
            "min_price_move_pct": 0.000001,
            "min_spread_move_multiple": 0.1,
            "min_absolute_move": 1e-12,
            "max_spread": None,
        },
        "journal_metadata": {
            "source": "unit_test_asset_config",
            "strategy_profile": f"unit_test_multi_asset|symbol={clean_symbol}|timeframe={timeframe}",
            "paper_only": True,
        },
    }


if __name__ == "__main__":
    unittest.main()

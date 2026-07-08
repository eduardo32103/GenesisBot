from __future__ import annotations

import contextlib
import io
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts.run_adaptive_strategy_governor import main as governor_main
from services.mt5.mt5_adaptive_strategy_governor import run_adaptive_strategy_governor


class MT5AdaptiveStrategyGovernorTests(unittest.TestCase):
    def test_degrades_strategy_with_low_pf_and_negative_expectancy(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[
                _trade("ETHUSD", "H1", "eth_h1_test_profile", 1.0, "win"),
                _trade("ETHUSD", "H1", "eth_h1_test_profile", -2.0, "loss"),
                _trade("ETHUSD", "H1", "eth_h1_test_profile", -2.0, "loss"),
                _trade("ETHUSD", "H1", "eth_h1_test_profile", -2.0, "loss"),
                _trade("ETHUSD", "H1", "eth_h1_test_profile", -2.0, "loss"),
            ],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            limits={"max_profile_drawdown": 99.0, "max_consecutive_losses_global": 99},
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["global_state"], "degrade_to_observation_only")
        self.assertEqual(len(result["degraded_profiles"]), 1)
        profile = result["degraded_profiles"][0]
        self.assertEqual(profile["recommended_action"], "degrade_to_observation_only")
        self.assertEqual(profile["active_state"], "observation_only")
        self.assertLess(profile["profit_factor"], 0.9)
        self.assertLessEqual(profile["expectancy"], 0)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_pauses_strategy_after_three_consecutive_losses(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[
                _trade("US500", "H1", "us500_h1_pause_profile", 5.0, "win"),
                _trade("US500", "H1", "us500_h1_pause_profile", -1.0, "loss"),
                _trade("US500", "H1", "us500_h1_pause_profile", -1.0, "loss"),
                _trade("US500", "H1", "us500_h1_pause_profile", -1.0, "loss"),
            ],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        self.assertEqual(result["global_state"], "pause_new_entries")
        self.assertEqual(len(result["paused_profiles"]), 1)
        profile = result["paused_profiles"][0]
        self.assertEqual(profile["consecutive_losses"], 3)
        self.assertEqual(profile["recommended_action"], "pause_new_entries")
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_kill_switch_when_profile_drawdown_exceeds_limit(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[
                _trade("BTCUSD", "M30", "btc_m30_drawdown_profile", 2.0, "win"),
                _trade("BTCUSD", "M30", "btc_m30_drawdown_profile", -3.0, "loss"),
                _trade("BTCUSD", "M30", "btc_m30_drawdown_profile", -3.0, "loss"),
            ],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            limits={"max_profile_drawdown": 2.0},
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        self.assertEqual(result["global_state"], "kill_switch")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertTrue(any(row["name"] == "max_profile_drawdown" and row["active"] for row in result["circuit_breakers"]))
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_adaptive_drawdown_uses_r_multiple_before_raw_pnl(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[
                _trade("BTCUSD", "M30", "btc_m30_unit_profile", 303.49, "win", r_multiple=0.5),
                _trade("BTCUSD", "M30", "btc_m30_unit_profile", -148.63, "loss", r_multiple=-0.1),
                _trade("BTCUSD", "M30", "btc_m30_unit_profile", -80.0, "loss", r_multiple=-0.05),
            ],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            limits={"max_profile_drawdown": 3.0},
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        profile = result["profile_states"][0]
        self.assertEqual(profile["drawdown_metric_unit"], "r_multiple")
        self.assertEqual(profile["max_drawdown"], 0.15)
        self.assertFalse(profile["drawdown_metric_unavailable"])
        self.assertFalse(_breaker_active(result, "max_profile_drawdown"))
        self.assertFalse(_breaker_active(result, "drawdown_metric_unavailable"))
        self.assertNotEqual(result["global_state"], "kill_switch")

    def test_adaptive_drawdown_does_not_compare_raw_pnl_to_r_limit(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[
                _trade("BTCUSD", "M30", "btc_m30_raw_only_profile", 303.49, "win", include_r_multiple=False),
                _trade("BTCUSD", "M30", "btc_m30_raw_only_profile", -148.63, "loss", include_r_multiple=False),
            ],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            limits={"max_profile_drawdown": 3.0},
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        profile = result["profile_states"][0]
        self.assertIsNone(profile["max_drawdown"])
        self.assertTrue(profile["drawdown_metric_unavailable"])
        self.assertEqual(profile["drawdown_metric_unavailable_count"], 2)
        self.assertEqual(result["global_state"], "kill_switch")
        self.assertEqual(result["reason"], "adaptive_governor:drawdown_metric_unavailable")
        self.assertTrue(_breaker_active(result, "drawdown_metric_unavailable"))
        self.assertFalse(_breaker_active(result, "max_profile_drawdown"))

    def test_adaptive_drawdown_excludes_sample_valid_false(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[
                _trade("XAUUSD", "M15", "xau_m15_invalid_profile", -500.0, "loss", r_multiple=-10.0, sample_valid=False),
                _trade("XAUUSD", "M15", "xau_m15_invalid_profile", 20.0, "win", r_multiple=0.2),
            ],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            limits={"max_profile_drawdown": 3.0},
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        profile = result["profile_states"][0]
        self.assertEqual(profile["raw_trades_forward"], 2)
        self.assertEqual(profile["trades_forward"], 1)
        self.assertEqual(profile["invalid_samples"], 1)
        self.assertEqual(profile["max_drawdown"], 0.0)
        self.assertFalse(_breaker_active(result, "max_profile_drawdown"))
        self.assertFalse(_breaker_active(result, "drawdown_metric_unavailable"))

    def test_adaptive_drawdown_all_sample_valid_false_fails_closed(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[
                _trade("XAUUSD", "M15", "xau_m15_all_invalid_profile", -500.0, "loss", r_multiple=-10.0, sample_valid=False),
                _trade("XAUUSD", "M15", "xau_m15_all_invalid_profile", 0.0, "breakeven", r_multiple=0.0, sample_valid=False),
            ],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            limits={"max_profile_drawdown": 3.0},
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        profile = result["profile_states"][0]
        self.assertEqual(profile["raw_trades_forward"], 2)
        self.assertEqual(profile["trades_forward"], 0)
        self.assertEqual(profile["invalid_samples"], 2)
        self.assertTrue(profile["no_valid_metric_samples"])
        self.assertIsNone(profile["max_drawdown"])
        self.assertEqual(result["global_state"], "kill_switch")
        self.assertEqual(result["reason"], "adaptive_governor:no_valid_metric_samples")
        self.assertTrue(_breaker_active(result, "no_valid_metric_samples"))
        self.assertFalse(_breaker_active(result, "max_profile_drawdown"))

    def test_adaptive_drawdown_excludes_frozen_invalid_samples(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[
                _trade(
                    "XAUUSD",
                    "M15",
                    "xau_m15_frozen_profile",
                    0.0,
                    "breakeven",
                    r_multiple=-10.0,
                    entry_price=2340.1,
                    exit_price=2340.1,
                    exit_reason="time_stop",
                    market_inactive_or_frozen=True,
                ),
                _trade("XAUUSD", "M15", "xau_m15_frozen_profile", 5.0, "win", r_multiple=0.1),
            ],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            limits={"max_profile_drawdown": 3.0},
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        profile = result["profile_states"][0]
        self.assertEqual(profile["trades_forward"], 1)
        self.assertEqual(profile["invalid_samples"], 1)
        self.assertEqual(profile["max_drawdown"], 0.0)
        self.assertFalse(_breaker_active(result, "max_profile_drawdown"))

    def test_adaptive_drawdown_all_frozen_invalid_samples_fail_closed(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[
                _trade(
                    "XAUUSD",
                    "M15",
                    "xau_m15_all_frozen_profile",
                    0.0,
                    "breakeven",
                    r_multiple=-10.0,
                    entry_price=2340.1,
                    exit_price=2340.1,
                    exit_reason="time_stop",
                    market_inactive_or_frozen=True,
                ),
                _trade(
                    "XAUUSD",
                    "M15",
                    "xau_m15_all_frozen_profile",
                    0.0,
                    "breakeven",
                    r_multiple=0.0,
                    invalid_reason="market_inactive_or_frozen",
                ),
            ],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            limits={"max_profile_drawdown": 3.0},
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        profile = result["profile_states"][0]
        self.assertEqual(profile["raw_trades_forward"], 2)
        self.assertEqual(profile["trades_forward"], 0)
        self.assertEqual(profile["invalid_samples"], 2)
        self.assertTrue(profile["no_valid_metric_samples"])
        self.assertIsNone(profile["max_drawdown"])
        self.assertEqual(result["global_state"], "kill_switch")
        self.assertEqual(result["reason"], "adaptive_governor:no_valid_metric_samples")
        self.assertTrue(_breaker_active(result, "no_valid_metric_samples"))
        self.assertFalse(_breaker_active(result, "max_profile_drawdown"))

    def test_adaptive_drawdown_fails_closed_when_normalized_metric_missing(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[
                _trade("ETHUSD", "H1", "eth_h1_pct_only_profile", 40.0, "win", include_r_multiple=False, pnl_pct=0.2),
                _trade("ETHUSD", "H1", "eth_h1_pct_only_profile", -20.0, "loss", include_r_multiple=False, pnl_pct=-0.1),
            ],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            limits={"max_profile_drawdown": 3.0},
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        self.assertEqual(result["global_state"], "kill_switch")
        self.assertEqual(result["reason"], "adaptive_governor:drawdown_metric_unavailable")
        self.assertTrue(_breaker_active(result, "drawdown_metric_unavailable"))

    def test_adaptive_drawdown_legacy_valid_record_not_auto_invalid(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[
                _trade("EURUSD", "M15", "eurusd_m15_legacy_profile", 30.0, "win", r_multiple=0.4),
                _trade("EURUSD", "M15", "eurusd_m15_legacy_profile", -10.0, "loss", r_multiple=-0.2),
            ],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            limits={"max_profile_drawdown": 3.0},
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        profile = result["profile_states"][0]
        self.assertEqual(profile["raw_trades_forward"], 2)
        self.assertEqual(profile["trades_forward"], 2)
        self.assertEqual(profile["invalid_samples"], 0)
        self.assertEqual(profile["max_drawdown"], 0.2)
        self.assertFalse(profile["drawdown_metric_unavailable"])
        self.assertFalse(_breaker_active(result, "drawdown_metric_unavailable"))

    def test_no_direct_kill_switch_reset(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[_trade("BTCUSD", "M30", "btc_m30_raw_only_profile", -148.63, "loss", include_r_multiple=False)],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        self.assertEqual(result["global_state"], "kill_switch")
        self.assertFalse(result.get("kill_switch_reset", False))

    def test_no_order_send(self) -> None:
        source = Path("services/mt5/mt5_adaptive_strategy_governor.py").read_text(encoding="utf-8")

        self.assertNotIn("order_send", source)

    def test_does_not_rotate_to_rejected_degraded_or_sibling_candidates(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[_trade("US500", "H1", "us500_h1_healthy_profile", 1.0, "win")],
            open_trades=[],
            rotation_result={
                "recommendation": "paper_forward_candidate_review",
                "recommended_candidate": None,
                "ranking": [
                    _rotation_row("ETHUSD", "M30", "eth_m30_vol_breakout_chop_guard_v1", "excluded_by_degradation_registry", degraded=True),
                    _rotation_row("EURUSD", "H1", "eurusd_h1_session_vwap_reclaim", "excluded_by_research_rejection_registry", rejected=True),
                    _rotation_row("ETHUSD", "M30", "eth_m30_vol_breakout_regime_filtered_v1", "blocked_by_sibling_risk", sibling=True),
                ],
                "candidate_activated": False,
                "paper_forward_onboarding_started": False,
            },
            intelligence_result=_empty_intelligence(),
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        self.assertEqual(result["rotation_candidates"], [])
        self.assertEqual(len(result["rejected_candidates"]), 3)
        self.assertEqual(result["recommended_next_action"], "continue_research")
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_clean_rotation_candidate_is_review_only(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[_trade("US500", "H1", "us500_h1_healthy_profile", 1.0, "win")],
            open_trades=[],
            rotation_result={
                "recommendation": "paper_forward_candidate_review",
                "recommended_candidate": _rotation_row("US500", "H1", "us500_h1_clean_profile", "paper_forward_review_ready"),
                "ranking": [],
                "candidate_activated": False,
                "paper_forward_onboarding_started": False,
            },
            intelligence_result=_empty_intelligence(),
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        self.assertEqual(result["recommended_next_action"], "rotate_candidate_review")
        self.assertEqual(len(result["rotation_candidates"]), 1)
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["rotation_candidates"][0]["candidate_activated"])
        self.assertFalse(result["rotation_candidates"][0]["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_safety_flags_are_false_and_journal_only_policy_is_preserved(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[_trade("GBPUSD", "H1", "gbpusd_h1_safe_profile", 1.0, "win")],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        self.assertFalse(result["safety_state"]["broker_touched"])
        self.assertFalse(result["safety_state"]["order_executed"])
        self.assertEqual(result["safety_state"]["order_policy"], "journal_only_no_broker")
        self.assertFalse(result["safety_state"]["applies_to_real_trading"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_missing_data_returns_no_trade(self) -> None:
        result = run_adaptive_strategy_governor(
            closed_trades=[],
            open_trades=[],
            rotation_result=_empty_rotation(),
            intelligence_result=_empty_intelligence(),
            load_shadow_snapshot=False,
            load_rotation=False,
            load_intelligence=False,
        )

        self.assertEqual(result["global_state"], "no_trade")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "adaptive_governor:missing_data")
        self.assertTrue(any(row["name"] == "missing_shadow_trade_data" and row["active"] for row in result["circuit_breakers"]))
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
            result = run_adaptive_strategy_governor(
                rotation_result=_empty_rotation(),
                intelligence_result=_empty_intelligence(),
                load_rotation=False,
                load_intelligence=False,
                persist_events=False,
            )

        self.assertEqual(result["global_state"], "watch")
        self.assertFalse(any(row["name"] == "max_open_shadow_trades" and row["active"] for row in result["circuit_breakers"]))
        self.assertEqual(result["shadow_snapshot_source"]["backend_type"], "postgres")
        self.assertTrue(result["shadow_snapshot_source"]["live_db_required"])
        self.assertTrue(result["shadow_snapshot_source"]["live_db_detected"])
        self.assertEqual(result["shadow_snapshot_source"]["open_shadow_trades_count"], 0)
        self.assertEqual(result["shadow_snapshot_source"]["closed_shadow_trades_count"], 1)
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_latest_state_snapshot_source_unavailable_fails_closed_without_reset(self) -> None:
        with patch("services.mt5.mt5_shadow_snapshot_source.load_settings", return_value=SimpleNamespace(database_url=_POSTGRES_URL)), patch(
            "services.mt5.mt5_shadow_snapshot_source.MemoryStore", side_effect=RuntimeError("source_unavailable_require_live_db")
        ):
            result = run_adaptive_strategy_governor(
                rotation_result=_empty_rotation(),
                intelligence_result=_empty_intelligence(),
                load_rotation=False,
                load_intelligence=False,
                persist_events=False,
            )

        self.assertEqual(result["global_state"], "kill_switch")
        self.assertEqual(result["decision"], "NO_TRADE")
        self.assertEqual(result["reason"], "adaptive_governor:shadow_snapshot_source_unavailable")
        self.assertTrue(result["shadow_snapshot_source_unavailable"])
        self.assertTrue(any(row["name"] == "shadow_snapshot_source_unavailable" and row["active"] for row in result["circuit_breakers"]))
        self.assertFalse(result.get("kill_switch_reset", False))
        self.assertFalse(result["candidate_activated"])
        self.assertFalse(result["paper_forward_onboarding_started"])
        self.assertFalse(result["broker_touched"])
        self.assertFalse(result["order_executed"])
        self.assertEqual(result["order_policy"], "journal_only_no_broker")

    def test_script_runs_without_activation(self) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            code = governor_main(["--no-shadow-snapshot", "--no-rotation", "--no-intelligence"])

        self.assertEqual(code, 0)
        text = output.getvalue()
        self.assertIn("MT5 Adaptive Strategy Governor", text)
        self.assertIn("global_state=no_trade", text)
        self.assertIn("decision=NO_TRADE", text)
        self.assertIn("candidate_activated=False", text)
        self.assertIn("paper_forward_onboarding_started=False", text)
        self.assertIn("broker_touched=False", text)
        self.assertIn("order_executed=False", text)
        self.assertIn("order_policy=journal_only_no_broker", text)


def _trade(
    symbol: str,
    timeframe: str,
    profile: str,
    pnl: float,
    status: str,
    *,
    r_multiple: float | None = None,
    include_r_multiple: bool = True,
    pnl_pct: float | None = None,
    **overrides: object,
) -> dict[str, object]:
    row: dict[str, object] = {
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy_profile": profile,
        "pnl": pnl,
        "status": status,
    }
    if include_r_multiple:
        row["r_multiple"] = pnl if r_multiple is None else r_multiple
    if pnl_pct is not None:
        row["pnl_pct"] = pnl_pct
    row.update(overrides)
    return row


def _rotation_row(
    symbol: str,
    timeframe: str,
    profile: str,
    status: str,
    *,
    degraded: bool = False,
    rejected: bool = False,
    sibling: bool = False,
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "profile": profile,
        "family": profile,
        "candidate_status": status,
        "recommended_next_action": "paper_forward_candidate_review",
        "degraded_by_registry": degraded,
        "rejected_by_research_registry": rejected,
        "sibling_risk": sibling,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _empty_rotation() -> dict[str, object]:
    return {
        "recommendation": "continue_research",
        "recommended_candidate": None,
        "ranking": [],
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _empty_intelligence() -> dict[str, object]:
    return {
        "recommendation": "research_plan_ready",
        "recommended_next_research_phase": "continue_research",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
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
        "status": status,
        "lifecycle_status": "closed" if status in {"closed", "win", "loss", "breakeven"} else "open",
        "pnl": pnl,
        "r_multiple": pnl,
        "updated_at": updated_at,
        "closed_at": updated_at if status != "open" else "",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


if __name__ == "__main__":
    unittest.main()

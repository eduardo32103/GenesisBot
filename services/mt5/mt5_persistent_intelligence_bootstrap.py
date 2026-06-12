from __future__ import annotations

from typing import Any

from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation_registry_status
from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore
from services.mt5.mt5_research_rejection_registry import research_rejection_registry_status


BOOTSTRAP_VERSION = "2026-06-11.mt5_persistent_intelligence_bootstrap.v1"

_KNOWN_STRATEGIES: tuple[dict[str, Any], ...] = (
    {
        "symbol": "ETHUSD",
        "timeframe": "M30",
        "profile": "eth_m30_vol_breakout_chop_guard_v1",
        "family": "volatility_breakout",
        "status": "observation_only",
    },
    {
        "symbol": "ETHUSD",
        "timeframe": "M30",
        "profile": "eth_m30_vol_breakout_regime_filtered_v1",
        "family": "volatility_breakout",
        "status": "blocked_by_sibling_risk",
    },
    {
        "symbol": "XAUUSD",
        "timeframe": "M15",
        "profile": "xauusd_m15_recent_session_open_continuation",
        "family": "session_open_continuation",
        "status": "rejected_after_hardening",
    },
    {
        "symbol": "BTCUSD",
        "timeframe": "H1",
        "profile": "btcusd_h1_recent_ema_reclaim",
        "family": "ema_reclaim",
        "status": "rejected_after_hardening",
    },
    {
        "symbol": "BTCUSD",
        "timeframe": "H1",
        "profile": "btcusd_h1_tournament_edge_candidate_paper_review_v1",
        "family": "tournament_edge",
        "status": "rejected_after_deep_validation",
    },
    {
        "symbol": "BTCUSD",
        "timeframe": "H1",
        "profile": "btcusd_h1_recent_liquidity_sweep",
        "family": "recent_liquidity_sweep",
        "status": "rejected_after_deep_validation",
    },
    {
        "symbol": "BTCUSD",
        "timeframe": "M30",
        "profile": "btcusd_m30_recent_london_us_breakout",
        "family": "london_us_breakout",
        "status": "rejected_after_deep_validation",
    },
    {
        "symbol": "BTCUSD",
        "timeframe": "M30",
        "profile": "btcusd_m30_opening_range_fakeout",
        "family": "opening_range_fakeout",
        "status": "rejected_as_correlated_family",
    },
    {
        "symbol": "EURUSD",
        "timeframe": "H1",
        "profile": "eurusd_h1_session_vwap_reclaim_distance_filter",
        "family": "session_vwap_reclaim",
        "status": "rejected_after_real_hardening",
    },
    {
        "symbol": "USTEC",
        "timeframe": "M30",
        "profile": "ustec_m30_h1_trend_pullback_rsi_filter",
        "family": "multi_timeframe_trend_pullback",
        "status": "rejected_after_real_hardening",
    },
)

_RESEARCH_LESSONS: tuple[dict[str, Any], ...] = (
    {
        "family": "volatility_breakout",
        "symbol": "ETHUSD",
        "timeframe": "M30",
        "lesson_type": "forward_degradation",
        "failure_pattern": "early_forward_edge_failed",
        "summary": "ETHUSD M30 volatility breakout failed early paper-forward evidence and is observation_only.",
        "avoid_next": ["eth_m30_vol_breakout_chop_guard_v1", "eth_m30_vol_breakout_sibling_cluster"],
        "recommended_next_research_phase": "research_non_sibling_families",
    },
    {
        "family": "session_open_continuation",
        "symbol": "XAUUSD",
        "timeframe": "M15",
        "lesson_type": "hardening_rejection",
        "failure_pattern": "mc_and_remove_best_5_failure",
        "summary": "XAUUSD M15 session continuation failed Monte Carlo, MC expectancy, and remove_best_5 gates.",
        "avoid_next": ["xauusd_m15_recent_session_open_continuation"],
        "recommended_next_research_phase": "continue_research",
    },
    {
        "family": "ema_reclaim",
        "symbol": "BTCUSD",
        "timeframe": "H1",
        "lesson_type": "hardening_rejection",
        "failure_pattern": "pf_mc_dependency_failure",
        "summary": "BTCUSD H1 EMA reclaim failed PF, Monte Carlo, remove_best_5, fragile regime, and single-trade dependency gates.",
        "avoid_next": ["btcusd_h1_recent_ema_reclaim"],
        "recommended_next_research_phase": "continue_research",
    },
    {
        "family": "BTCUSD H1 tournament_edge / recent_liquidity_sweep",
        "symbol": "BTCUSD",
        "timeframe": "H1",
        "lesson_type": "deep_validation_failure",
        "failure_pattern": "small_sample_false_positive_monte_carlo_fragility",
        "summary": "BTCUSD H1 tournament candidate failed deep validation despite high tournament PF/winrate.",
        "avoid_next": [
            "unknown_profile_tournament_candidates",
            "trades_forward_below_20",
            "recent_pf_below_1_15",
            "monte_carlo_fragility",
            "remove_best_dependency",
            "single_trade_dependency",
            "fragile_regime_dependency",
        ],
        "recommended_next_research_phase": "search_high_sample_low_dependency_candidates",
    },
    {
        "family": "london_us_breakout",
        "symbol": "BTCUSD",
        "timeframe": "M30",
        "lesson_type": "deep_validation_rejection",
        "failure_pattern": "sample_or_robustness_failure",
        "summary": "BTCUSD M30 London-US breakout improved in hardening but failed deep validation sample and robustness gates.",
        "avoid_next": ["btcusd_m30_recent_london_us_breakout", "btcusd_m30_opening_range_fakeout"],
        "recommended_next_research_phase": "new_family_edge_discovery",
    },
    {
        "family": "session_vwap_reclaim",
        "symbol": "EURUSD",
        "timeframe": "H1",
        "lesson_type": "proxy_false_positive",
        "failure_pattern": "proxy_false_positive_after_costs_and_mc_failure",
        "summary": "EURUSD H1 VWAP reclaim proxy did not survive real hardening after costs and Monte Carlo.",
        "avoid_next": ["eurusd_h1_session_vwap_reclaim"],
        "recommended_next_research_phase": "design_next_processed_feature_scan",
    },
    {
        "family": "multi_timeframe_trend_pullback",
        "symbol": "USTEC",
        "timeframe": "M30",
        "lesson_type": "proxy_false_positive",
        "failure_pattern": "proxy_false_positive_after_monte_carlo_failure",
        "summary": "USTEC M30/H1 trend pullback proxy failed real hardening on Monte Carlo fragility despite large sample.",
        "avoid_next": ["ustec_m30_h1_trend_pullback"],
        "recommended_next_research_phase": "design_volatility_compression_breakout_processed_feature_scan",
    },
)


def persistent_intelligence_bootstrap_status(*, store: MT5PersistentIntelligenceStore | None = None) -> dict[str, Any]:
    active_store = store or MT5PersistentIntelligenceStore()
    healthcheck = active_store.healthcheck(write_test_event=False)
    degradation_rows = _degradation_rows()
    rejection_rows = _research_rejection_rows()
    return {
        "ok": True,
        "status": "persistent_intelligence_bootstrap_status_ready",
        "bootstrap_version": BOOTSTRAP_VERSION,
        "bootstrap_writes_enabled": False,
        "db_available": bool(healthcheck.get("db_available")),
        "tables_ready": bool(healthcheck.get("tables_ready")),
        "db_degraded": bool(healthcheck.get("db_degraded")),
        "planned_degradation_rows": len(degradation_rows),
        "planned_rejection_rows": len(rejection_rows),
        "planned_strategy_rows": len(_KNOWN_STRATEGIES),
        "planned_profile_state_rows": len(degradation_rows),
        "planned_research_lesson_rows": len(_RESEARCH_LESSONS),
        "planned_candidate_rotation_rows": 1,
        "recommendation": "run_persistent_intelligence_bootstrap_script" if healthcheck.get("tables_ready") else "repair_persistent_db_before_bootstrap",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "secrets_printed": False,
        **_safety(),
    }


def run_persistent_intelligence_bootstrap(
    *,
    store: MT5PersistentIntelligenceStore | None = None,
) -> dict[str, Any]:
    active_store = store or MT5PersistentIntelligenceStore()
    healthcheck = active_store.healthcheck(write_test_event=False)
    result = _base_result(healthcheck)
    if not (healthcheck.get("db_available") and healthcheck.get("tables_ready") and not healthcheck.get("db_degraded")):
        result.update(
            {
                "status": "persistent_intelligence_bootstrap_aborted_db_degraded",
                "recommendation": "repair_persistent_db_before_bootstrap",
                "decision": "NO_TRADE",
                "reason": "persistent_intelligence_db_degraded",
            }
        )
        return result

    _seed_degradation_registry(active_store, result)
    _seed_research_rejection_registry(active_store, result)
    _seed_strategy_registry(active_store, result)
    _seed_profile_state(active_store, result)
    _seed_research_lessons(active_store, result)
    _seed_adaptive_governor_state(active_store, result)
    _seed_candidate_rotation_state(active_store, result)

    result["status"] = "persistent_intelligence_bootstrap_complete" if not result["errors"] else "persistent_intelligence_bootstrap_completed_with_errors"
    result["recommendation"] = "run_one_autonomous_paper_learning_cycle" if not result["errors"] else "review_bootstrap_errors"
    result["decision"] = "NO_TRADE"
    result["reason"] = "persistent_intelligence_bootstrap_paper_only"
    return result


def _base_result(healthcheck: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": True,
        "status": "persistent_intelligence_bootstrap_ready",
        "bootstrap_version": BOOTSTRAP_VERSION,
        "db_available": bool(healthcheck.get("db_available")),
        "tables_ready": bool(healthcheck.get("tables_ready")),
        "db_degraded": bool(healthcheck.get("db_degraded")),
        "seeded_degradation_rows": 0,
        "seeded_rejection_rows": 0,
        "seeded_strategy_rows": 0,
        "seeded_profile_state_rows": 0,
        "seeded_research_lesson_rows": 0,
        "seeded_adaptive_governor_state_rows": 0,
        "seeded_candidate_rotation_rows": 0,
        "skipped_existing_rows": 0,
        "errors": [],
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        "secrets_printed": False,
        **_safety(),
    }


def _seed_degradation_registry(store: MT5PersistentIntelligenceStore, result: dict[str, Any]) -> None:
    for row in _degradation_rows():
        _seed_upsert(
            store,
            result,
            table="mt5_degradation_registry",
            exists_params=_exists_params("symbol,timeframe,profile", symbol=row["symbol"], timeframe=row["timeframe"], profile=row["profile"]),
            write=lambda row=row: store.upsert_degradation_registry(row, critical=False),
            counter="seeded_degradation_rows",
        )


def _seed_research_rejection_registry(store: MT5PersistentIntelligenceStore, result: dict[str, Any]) -> None:
    for row in _research_rejection_rows():
        _seed_upsert(
            store,
            result,
            table="mt5_research_rejection_registry",
            exists_params=_exists_params("symbol,timeframe,family_pattern", symbol=row["symbol"], timeframe=row["timeframe"], family_pattern=row["family_pattern"]),
            write=lambda row=row: store.upsert_research_rejection_registry(row, critical=False),
            counter="seeded_rejection_rows",
        )


def _seed_strategy_registry(store: MT5PersistentIntelligenceStore, result: dict[str, Any]) -> None:
    for row in _strategy_rows():
        _seed_upsert(
            store,
            result,
            table="mt5_strategy_registry",
            exists_params=_exists_params("symbol,timeframe,profile", symbol=row["symbol"], timeframe=row["timeframe"], profile=row["profile"]),
            write=lambda row=row: store.upsert_strategy_registry(row, critical=False),
            counter="seeded_strategy_rows",
        )


def _seed_profile_state(store: MT5PersistentIntelligenceStore, result: dict[str, Any]) -> None:
    for row in _degradation_rows():
        state = {
            "symbol": row["symbol"],
            "timeframe": row["timeframe"],
            "profile": row["profile"],
            "status": "observation_only",
            "active": False,
            "applies_to_paper_shadow": False,
            "applies_to_real_trading": False,
            "degradation_reason": row["degradation_reason"],
            "registry_source": "persistent_intelligence_bootstrap",
        }
        _seed_upsert(
            store,
            result,
            table="mt5_profile_state",
            exists_params=_exists_params("symbol,timeframe,profile", symbol=state["symbol"], timeframe=state["timeframe"], profile=state["profile"]),
            write=lambda state=state: store.upsert_profile_state(state, critical=False),
            counter="seeded_profile_state_rows",
        )


def _seed_research_lessons(store: MT5PersistentIntelligenceStore, result: dict[str, Any]) -> None:
    for row in _RESEARCH_LESSONS:
        _seed_upsert(
            store,
            result,
            table="mt5_research_lessons",
            exists_params=_exists_params(
                "timestamp",
                symbol=row["symbol"],
                timeframe=row["timeframe"],
                lesson_type=row["lesson_type"],
                failure_pattern=row["failure_pattern"],
            ),
            write=lambda row=row: store.record_research_lesson(row, critical=False),
            counter="seeded_research_lesson_rows",
        )


def _seed_adaptive_governor_state(store: MT5PersistentIntelligenceStore, result: dict[str, Any]) -> None:
    payload = {
        "global_state": "watch",
        "recommended_next_action": "run_one_autonomous_paper_learning_cycle",
        "active_profiles": [],
        "paused_profiles": [],
        "degraded_profiles": _degradation_rows(),
        "circuit_breakers": [],
        "open_shadow_trades": 0,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
    _seed_upsert(
        store,
        result,
        table="mt5_adaptive_governor_state",
        exists_params={"select": "timestamp", "recommended_next_action": "eq.run_one_autonomous_paper_learning_cycle", "limit": "1"},
        write=lambda: store.record_adaptive_governor_state(payload, critical=False),
        counter="seeded_adaptive_governor_state_rows",
    )


def _seed_candidate_rotation_state(store: MT5PersistentIntelligenceStore, result: dict[str, Any]) -> None:
    payload = {
        "run_id": "bootstrap-current-candidate-rotation-state",
        "recommendation": "continue_research",
        "recommended_candidate": {},
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
    _seed_upsert(
        store,
        result,
        table="mt5_candidate_rotation_runs",
        exists_params={"select": "run_id", "run_id": "eq.bootstrap-current-candidate-rotation-state", "limit": "1"},
        write=lambda: store.record_candidate_rotation_run(payload, critical=False),
        counter="seeded_candidate_rotation_rows",
    )


def _seed_upsert(
    store: MT5PersistentIntelligenceStore,
    result: dict[str, Any],
    *,
    table: str,
    exists_params: dict[str, str],
    write: Any,
    counter: str,
) -> None:
    existing = store._safe_select(table, params=exists_params)
    if existing.get("rows"):
        result["skipped_existing_rows"] += 1
        return
    _record_write(result, write(), counter)


def _record_write(result: dict[str, Any], write_result: dict[str, Any], counter: str) -> None:
    if write_result.get("ok"):
        result[counter] += 1
        return
    result["errors"].append(
        {
            "table": write_result.get("table") or "",
            "reason": write_result.get("reason") or write_result.get("last_db_error_category") or "write_failed",
            "db_degraded": bool(write_result.get("db_degraded")),
        }
    )


def _degradation_rows() -> list[dict[str, Any]]:
    registry = forward_profile_degradation_registry_status()
    rows = []
    for item in registry.get("degraded_profiles") or []:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "symbol": _symbol(item.get("symbol")),
                "timeframe": _timeframe(item.get("timeframe")),
                "profile": str(item.get("profile") or "").strip(),
                "degradation_reason": item.get("degradation_reason") or "",
                "applies_to_paper_shadow": False,
                "applies_to_real_trading": False,
                "registry_version": item.get("registry_version") or registry.get("registry_version") or "",
            }
        )
    return [row for row in rows if row["symbol"] and row["timeframe"] and row["profile"]]


def _research_rejection_rows() -> list[dict[str, Any]]:
    registry = research_rejection_registry_status()
    rows: list[dict[str, Any]] = []
    for item in registry.get("research_rejections") or []:
        if not isinstance(item, dict):
            continue
        for symbol in _symbols_for_rejection(item):
            for pattern in item.get("family_profile_patterns") or ():
                rows.append(
                    {
                        "symbol": symbol,
                        "timeframe": _timeframe(item.get("timeframe")),
                        "family_pattern": str(pattern or "").strip(),
                        "rejection_reason": item.get("rejection_reason") or "",
                        "rejection_status": item.get("rejection_status") or "",
                        "reviewed_at_version": item.get("reviewed_at_version") or registry.get("registry_version") or "",
                        "allow_future_research": bool(item.get("allow_future_research")),
                        "allow_manual_override": bool(item.get("allow_manual_override") if "allow_manual_override" in item else True),
                    }
                )
    return [row for row in rows if row["symbol"] and row["timeframe"] and row["family_pattern"]]


def _strategy_rows() -> list[dict[str, Any]]:
    return [
        {
            **row,
            "source": "persistent_intelligence_bootstrap",
        }
        for row in _KNOWN_STRATEGIES
    ]


def _symbols_for_rejection(item: dict[str, Any]) -> list[str]:
    symbols = [_symbol(item.get("symbol"))]
    for alias in item.get("aliases") or ():
        clean = _symbol(alias)
        if clean and clean not in symbols:
            symbols.append(clean)
    return [symbol for symbol in symbols if symbol]


def _exists_params(select: str, **eq_values: str) -> dict[str, str]:
    params = {"select": select, "limit": "1"}
    for key, value in eq_values.items():
        params[key] = f"eq.{value}"
    return params


def _symbol(value: object) -> str:
    return str(value or "").upper().strip().replace(".B", "")


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

from __future__ import annotations

import time
from typing import Any

from services.mt5.mt5_adaptive_strategy_governor import run_adaptive_strategy_governor
from services.mt5.mt5_capital_protection_governor import run_capital_protection_governor
from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore
from services.mt5.mt5_risk_governor import assess_runtime_risk
from services.mt5.mt5_runtime_context_diagnostics import run_runtime_context_diagnostics
from services.mt5.mt5_runtime_snapshot import get_snapshot
from services.mt5.mt5_symbol_cost_model import build_symbol_cost_model


READINESS_VERSION = "2026-06-12.mt5_xau_m15_paper_observation_readiness.v1"

SYMBOL = "XAUUSD"
BROKER_SYMBOL = "XAUUSD.b"
TIMEFRAME = "M15"
FAMILY = "volatility_compression_breakout"
MODE = "nr7_trailing_defensive"
CANDIDATE_PROFILE = f"{FAMILY}|mode={MODE}"
MIN_BARS_COUNT = 100
MAX_OPEN_SHADOW_TRADES = 3
MAX_QUEUE_DEPTH = 3


def run_xau_m15_paper_observation_readiness(
    *,
    store: MT5PersistentIntelligenceStore | Any | None = None,
    db_state: dict[str, Any] | None = None,
    profile_state_rows: list[dict[str, Any]] | None = None,
    strategy_registry_rows: list[dict[str, Any]] | None = None,
    runtime_snapshot: dict[str, Any] | None = None,
    generic_runtime_snapshot: dict[str, Any] | None = None,
    capital_state: dict[str, Any] | None = None,
    adaptive_state: dict[str, Any] | None = None,
    risk_state: dict[str, Any] | None = None,
    open_shadow_trades: list[dict[str, Any]] | None = None,
    max_open_shadow_trades: int = MAX_OPEN_SHADOW_TRADES,
) -> dict[str, Any]:
    started = time.monotonic()
    active_store = store or MT5PersistentIntelligenceStore()
    db = _db_state(active_store, db_state)
    profile_rows = _candidate_rows(active_store, "mt5_profile_state", profile_state_rows)
    strategy_rows = _candidate_rows(active_store, "mt5_strategy_registry", strategy_registry_rows)
    candidate = _candidate_state(profile_rows, strategy_rows)
    snapshot, generic_snapshot, symbol_alias_used = _runtime_snapshots(
        runtime_snapshot=runtime_snapshot,
        generic_runtime_snapshot=generic_runtime_snapshot,
    )
    runtime = run_runtime_context_diagnostics(
        symbol=SYMBOL,
        timeframe=TIMEFRAME,
        snapshot=snapshot,
        generic_snapshot=_generic_snapshot_for_timeframe(generic_snapshot, TIMEFRAME),
    )
    open_trades = [trade for trade in (open_shadow_trades or _open_shadow_from_snapshot(snapshot, generic_snapshot)) if isinstance(trade, dict)]
    spread = _spread_state(snapshot, generic_snapshot)
    capital = capital_state or _capital_state(db, snapshot, open_trades)
    adaptive = adaptive_state or _adaptive_state(snapshot, open_trades)
    risk = risk_state or _risk_state(snapshot)
    gates = _gates(
        db=db,
        candidate=candidate,
        runtime=runtime,
        spread=spread,
        capital=capital,
        adaptive=adaptive,
        risk=risk,
        open_shadow_count=len(open_trades),
        max_open_shadow_trades=max_open_shadow_trades,
    )
    failed = [name for name, gate in gates.items() if not gate["passed"]]
    ready = not failed
    recommendation = _recommendation(failed)
    return {
        "ok": True,
        "status": "xau_m15_paper_observation_readiness_ready",
        "readiness_version": READINESS_VERSION,
        "candidate_found": bool(candidate["candidate_found"]),
        "profile_state_found": bool(candidate["profile_state_found"]),
        "strategy_registry_found": bool(candidate["strategy_registry_found"]),
        "candidate_status": candidate["candidate_status"],
        "candidate_profile": CANDIDATE_PROFILE,
        "db_state": _public_db_state(db),
        "symbol": SYMBOL,
        "broker_symbol": BROKER_SYMBOL,
        "timeframe": TIMEFRAME,
        "runtime_context_available": bool(runtime.get("runtime_snapshot_available")),
        "runtime_context_recent": bool(runtime.get("runtime_snapshot_recent")),
        "runtime_snapshot_complete": bool(runtime.get("runtime_snapshot_complete")),
        "runtime_snapshot_context": runtime.get("runtime_snapshot_context") or "",
        "runtime_context_missing_fields": runtime.get("runtime_context_missing_fields") or [],
        "runtime_snapshot_source": runtime.get("snapshot_source") or "",
        "symbol_alias_used": symbol_alias_used,
        "latest_tick_at": runtime.get("last_tick_at") or "",
        "latest_bars_at": runtime.get("bars_last_at") or "",
        "bars_available": bool(runtime.get("latest_bars_available")),
        "bars_count": int(_number(runtime.get("bars_count")) or 0),
        "m15_bars_status": "ready" if gates["m15_bars_count"]["passed"] else "missing_or_insufficient",
        "tick_available": bool(runtime.get("latest_tick")),
        "tick_merged_into_bar_context": bool(runtime.get("tick_merged_into_bar_context")),
        "spread_available": bool(spread.get("spread_available")),
        "spread_state": spread,
        "capital_state": capital.get("capital_state") or capital.get("status") or "",
        "capital_allows_observation": _capital_allows(capital),
        "adaptive_state": adaptive.get("global_state") or adaptive.get("adaptive_state") or adaptive.get("status") or "",
        "adaptive_allows_observation": _adaptive_allows(adaptive),
        "risk_state": risk.get("risk_state") or "",
        "risk_allows_observation": _risk_allows(risk),
        "open_shadow_count": len(open_trades),
        "readiness_state": "ready_for_one_cycle_paper_observation" if ready else "blocked",
        "recommendation": recommendation,
        "gates": gates,
        "failed_gates": failed,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "paper_shadow_created": False,
        "shadow_trade_id": "",
        "applies_to_real_trading": False,
        "duration_ms": int((time.monotonic() - started) * 1000),
        **_safety(),
    }


def run_xau_m15_paper_observation_cycle(
    *,
    paper_shadow_once: bool = False,
    readiness_result: dict[str, Any] | None = None,
    **readiness_kwargs: Any,
) -> dict[str, Any]:
    readiness = readiness_result or run_xau_m15_paper_observation_readiness(**readiness_kwargs)
    ready = str(readiness.get("readiness_state") or "") == "ready_for_one_cycle_paper_observation"
    requested = bool(paper_shadow_once)
    if requested:
        reason = "paper_shadow_once_requires_human_approval_in_next_phase"
        recommendation = "do_not_start_paper_shadow_yet"
    elif ready:
        reason = "dry_run_ready_no_shadow_created"
        recommendation = "ready_for_one_cycle_paper_observation"
    else:
        reason = "dry_run_blocked_by_readiness_gate"
        recommendation = readiness.get("recommendation") or "resolve_readiness_gates"
    snapshot_signal = _hypothetical_signal(readiness) if ready else {}
    return {
        "ok": True,
        "status": "xau_m15_paper_observation_cycle_dry_run_ready",
        "mode": "dry_run" if not requested else "explicit_request_blocked_pending_human_approval",
        "symbol": SYMBOL,
        "broker_symbol": BROKER_SYMBOL,
        "timeframe": TIMEFRAME,
        "candidate_profile": CANDIDATE_PROFILE,
        "paper_shadow_once_requested": requested,
        "readiness_state": readiness.get("readiness_state") or "blocked",
        "recommendation": recommendation,
        "reason": reason,
        "hypothetical_signal": snapshot_signal,
        "readiness": readiness,
        "paper_shadow_created": False,
        "shadow_trade_id": "",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _db_state(store: Any, injected: dict[str, Any] | None) -> dict[str, Any]:
    if injected is not None:
        return dict(injected)
    try:
        return dict(store.healthcheck(write_test_event=False))
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return {"db_available": False, "db_degraded": True, "tables_ready": False, "reason": type(exc).__name__, **_safety()}


def _candidate_rows(store: Any, table: str, injected: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    if injected is not None:
        return [dict(row) for row in injected if isinstance(row, dict)]
    if not hasattr(store, "_safe_select"):
        return []
    try:
        result = store._safe_select(
            table,
            params={
                "select": "*",
                "symbol": f"eq.{SYMBOL}",
                "timeframe": f"eq.{TIMEFRAME}",
                "profile": f"eq.{CANDIDATE_PROFILE}",
                "limit": "5",
            },
        )
    except Exception:
        return []
    return [dict(row) for row in (result.get("rows") or []) if isinstance(row, dict)]


def _runtime_snapshots(
    *,
    runtime_snapshot: dict[str, Any] | None,
    generic_runtime_snapshot: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    if runtime_snapshot is not None:
        return dict(runtime_snapshot), dict(generic_runtime_snapshot or {}), "injected"

    aliases = (SYMBOL, BROKER_SYMBOL, BROKER_SYMBOL.upper(), "XAUUSD.B", "GOLD")
    selected_alias = ""
    selected_snapshot: dict[str, Any] = {}
    for alias in aliases:
        candidate = get_snapshot(alias, TIMEFRAME) or {}
        if candidate:
            selected_alias = alias
            selected_snapshot = dict(candidate)
            break

    if generic_runtime_snapshot is not None:
        selected_generic = dict(generic_runtime_snapshot)
    else:
        selected_generic = {}
        generic_aliases = (selected_alias,) + aliases if selected_alias else aliases
        for alias in generic_aliases:
            if not alias:
                continue
            candidate = get_snapshot(alias) or {}
            if candidate:
                selected_generic = dict(candidate)
                if not selected_alias:
                    selected_alias = alias
                break

    return selected_snapshot, selected_generic, selected_alias or "none"


def _generic_snapshot_for_timeframe(generic_snapshot: dict[str, Any], timeframe: str) -> dict[str, Any]:
    if not generic_snapshot:
        return {}
    snapshot_timeframe = _timeframe(generic_snapshot.get("timeframe"))
    requested = _timeframe(timeframe)
    if snapshot_timeframe and requested and snapshot_timeframe != requested:
        return {}
    return dict(generic_snapshot)


def _candidate_state(profile_rows: list[dict[str, Any]], strategy_rows: list[dict[str, Any]]) -> dict[str, Any]:
    profile = _matching_candidate(profile_rows)
    strategy = _matching_candidate(strategy_rows)
    status = str(profile.get("status") or strategy.get("status") or "")
    active = bool(profile.get("active") or strategy.get("active") or False)
    real = bool(profile.get("applies_to_real_trading") or strategy.get("applies_to_real_trading") or False)
    return {
        "candidate_found": bool(profile and strategy),
        "profile_state_found": bool(profile),
        "strategy_registry_found": bool(strategy),
        "candidate_status": status,
        "candidate_activated": active,
        "applies_to_real_trading": real,
        "profile_state": profile,
        "strategy_registry": strategy,
    }


def _matching_candidate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    for row in rows:
        if _symbol(row.get("symbol")) == SYMBOL and _timeframe(row.get("timeframe")) == TIMEFRAME and str(row.get("profile") or "") == CANDIDATE_PROFILE:
            return dict(row)
    return {}


def _spread_state(snapshot: dict[str, Any], generic_snapshot: dict[str, Any]) -> dict[str, Any]:
    active = snapshot if snapshot else generic_snapshot
    tick = active.get("last_tick") if isinstance(active.get("last_tick"), dict) else {}
    spread = _number(tick.get("spread") or active.get("spread"))
    price = _number(tick.get("last") or tick.get("price") or active.get("last") or active.get("last_price"))
    try:
        model = build_symbol_cost_model(SYMBOL, resolved_symbol=BROKER_SYMBOL, first_price=price)
        return {
            "spread_available": spread is not None or bool(model.estimated_spread_price),
            "runtime_spread": spread,
            "estimated_spread_price": model.estimated_spread_price,
            "cost_model_confidence": model.cost_model_confidence,
            **_safety(),
        }
    except Exception as exc:  # pragma: no cover - defensive guard
        return {"spread_available": False, "reason": type(exc).__name__, **_safety()}


def _capital_state(db: dict[str, Any], snapshot: dict[str, Any], open_trades: list[dict[str, Any]]) -> dict[str, Any]:
    return run_capital_protection_governor(
        open_trades=open_trades,
        closed_trades=[],
        persistent_status=db,
        runtime_snapshot=snapshot,
        load_shadow_snapshot=False,
        load_persistent=False,
        persist_events=False,
    )


def _adaptive_state(snapshot: dict[str, Any], open_trades: list[dict[str, Any]]) -> dict[str, Any]:
    return run_adaptive_strategy_governor(
        closed_trades=[{"symbol": SYMBOL, "timeframe": TIMEFRAME, "profile": CANDIDATE_PROFILE, "pnl": 0.0, "status": "closed"}],
        open_trades=open_trades,
        runtime_snapshot=snapshot,
        load_shadow_snapshot=False,
        load_rotation=False,
        load_intelligence=False,
        persist_events=False,
    )


def _risk_state(snapshot: dict[str, Any]) -> dict[str, Any]:
    tick = snapshot.get("last_tick") if isinstance(snapshot.get("last_tick"), dict) else {}
    return assess_runtime_risk(SYMBOL, timeframe=TIMEFRAME, tick=tick)


def _open_shadow_from_snapshot(snapshot: dict[str, Any], generic: dict[str, Any]) -> list[dict[str, Any]]:
    for source in (snapshot, generic):
        trade = source.get("open_shadow_trade") if isinstance(source.get("open_shadow_trade"), dict) else {}
        if trade:
            return [trade]
    return []


def _gates(
    *,
    db: dict[str, Any],
    candidate: dict[str, Any],
    runtime: dict[str, Any],
    spread: dict[str, Any],
    capital: dict[str, Any],
    adaptive: dict[str, Any],
    risk: dict[str, Any],
    open_shadow_count: int,
    max_open_shadow_trades: int,
) -> dict[str, dict[str, Any]]:
    bars_count = int(_number(runtime.get("bars_count")) or 0)
    return {
        "persistent_db_healthy": _gate(_db_healthy(db), _public_db_state(db), "db_available=true,tables_ready=true,db_degraded=false"),
        "db_queue_pressure_clear": _gate(int(_number(db.get("queue_depth")) or 0) <= MAX_QUEUE_DEPTH, db.get("queue_depth", 0), f"<={MAX_QUEUE_DEPTH}"),
        "candidate_found": _gate(bool(candidate.get("candidate_found")), candidate.get("candidate_found"), True),
        "candidate_status_review": _gate(str(candidate.get("candidate_status") or "") == "paper_observation_review", candidate.get("candidate_status"), "paper_observation_review"),
        "candidate_not_activated": _gate(not bool(candidate.get("candidate_activated")), candidate.get("candidate_activated"), False),
        "candidate_not_real_trading": _gate(not bool(candidate.get("applies_to_real_trading")), candidate.get("applies_to_real_trading"), False),
        "runtime_context_available": _gate(bool(runtime.get("runtime_snapshot_available")), runtime.get("runtime_snapshot_available"), True),
        "runtime_context_recent": _gate(bool(runtime.get("runtime_snapshot_recent")), runtime.get("runtime_snapshot_recent"), True),
        "runtime_bar_context_complete": _gate(
            bool(runtime.get("runtime_snapshot_complete")) and str(runtime.get("runtime_snapshot_context") or "") == "bar_context",
            {"complete": runtime.get("runtime_snapshot_complete"), "context": runtime.get("runtime_snapshot_context")},
            "complete_bar_context",
        ),
        "m15_bars_available": _gate(bool(runtime.get("latest_bars_available")), runtime.get("latest_bars_available"), True),
        "m15_bars_count": _gate(bars_count >= MIN_BARS_COUNT, bars_count, f">={MIN_BARS_COUNT}"),
        "latest_tick_available": _gate(bool(runtime.get("latest_tick")), bool(runtime.get("latest_tick")), True),
        "tick_merged_into_bar_context": _gate(bool(runtime.get("tick_merged_into_bar_context")), runtime.get("tick_merged_into_bar_context"), True),
        "spread_available": _gate(bool(spread.get("spread_available")), spread.get("spread_available"), True),
        "capital_allows_observation": _gate(_capital_allows(capital), capital.get("capital_state") or capital.get("reason") or "", "allows"),
        "adaptive_allows_observation": _gate(_adaptive_allows(adaptive), adaptive.get("global_state") or adaptive.get("reason") or "", "allows"),
        "risk_allows_observation": _gate(_risk_allows(risk), risk.get("reason") or risk.get("risk_governor_reason") or "", "risk_governor_pass"),
        "open_shadow_capacity": _gate(open_shadow_count < max_open_shadow_trades, open_shadow_count, f"<{max_open_shadow_trades}"),
    }


def _recommendation(failed: list[str]) -> str:
    if not failed:
        return "ready_for_one_cycle_paper_observation"
    if any(name.startswith("runtime_") or name.startswith("m15_") or name in {"latest_tick_available", "tick_merged_into_bar_context"} for name in failed):
        return "configure_mt5_bridge_for_xauusd_m15"
    if "persistent_db_healthy" in failed or "db_queue_pressure_clear" in failed:
        return "repair_persistent_intelligence_before_observation"
    if any(name.startswith("candidate_") for name in failed):
        return "register_xau_m15_candidate_before_observation"
    return "resolve_observation_safety_gates"


def _db_healthy(db: dict[str, Any]) -> bool:
    return bool(db.get("db_available") and db.get("tables_ready") and not db.get("db_degraded"))


def _public_db_state(db: dict[str, Any]) -> dict[str, Any]:
    return {
        "provider": db.get("provider") or "",
        "db_available": bool(db.get("db_available")),
        "db_degraded": bool(db.get("db_degraded")),
        "tables_ready": bool(db.get("tables_ready")),
        "queue_depth": int(_number(db.get("queue_depth")) or 0),
        "recommendation": db.get("recommendation") or "",
        **_safety(),
    }


def _capital_allows(capital: dict[str, Any]) -> bool:
    if "allowed" in capital:
        return bool(capital.get("allowed"))
    if "safe_to_trade" in capital:
        return bool(capital.get("safe_to_trade"))
    state = str(capital.get("capital_state") or "").casefold()
    return state in {"normal", "watch", "ready", "allow_paper_review"}


def _adaptive_allows(adaptive: dict[str, Any]) -> bool:
    if "allowed" in adaptive:
        return bool(adaptive.get("allowed"))
    state = str(adaptive.get("global_state") or adaptive.get("adaptive_state") or "").casefold()
    action = str(adaptive.get("recommended_next_action") or "").casefold()
    blocked = {"kill_switch", "pause_new_entries", "degrade_to_observation_only", "observation_only", "no_trade"}
    if state in blocked or action in blocked:
        return False
    return state in {"watch", "ready", "normal", "allow_paper_review"}


def _risk_allows(risk: dict[str, Any]) -> bool:
    if "allowed" in risk:
        return bool(risk.get("allowed"))
    if "risk_governor_allowed" in risk:
        return bool(risk.get("risk_governor_allowed"))
    return str(risk.get("reason") or "") == "risk_governor_pass"


def _hypothetical_signal(readiness: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": readiness.get("symbol") or SYMBOL,
        "broker_symbol": readiness.get("broker_symbol") or BROKER_SYMBOL,
        "timeframe": readiness.get("timeframe") or TIMEFRAME,
        "candidate_profile": readiness.get("candidate_profile") or CANDIDATE_PROFILE,
        "context": readiness.get("runtime_snapshot_context") or "",
        "bars_count": readiness.get("bars_count") or 0,
        "paper_shadow_created": False,
        **_safety(),
    }


def _gate(passed: bool, actual: Any, required: Any) -> dict[str, Any]:
    return {"passed": bool(passed), "actual": actual, "required": required}


def _symbol(value: object) -> str:
    symbol = str(value or "").upper().strip().replace(".B", "")
    if symbol == "XAUUSDB":
        return "XAUUSD"
    return symbol


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

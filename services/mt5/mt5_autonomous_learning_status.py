from __future__ import annotations

from typing import Any

from services.mt5.mt5_autonomous_learning_orchestrator import run_autonomous_learning_orchestrator


AUTONOMOUS_LEARNING_STATUS_VERSION = "2026-06-11.mt5_autonomous_learning_status.v1"


def run_autonomous_learning_status(
    *,
    symbol: str = "BTCUSD",
    timeframe: str = "",
    orchestrator_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a read-only status view for autonomous paper learning."""

    cycle = orchestrator_result
    if cycle is None:
        cycle = run_autonomous_learning_orchestrator(
            symbol=symbol,
            timeframe=timeframe,
            dry_run=True,
            apply_paper_rotation=False,
            load_persistent=True,
            load_shadow_snapshot=True,
            load_rotation=False,
            run_trade_learning=False,
            persist_events=False,
        )
    return _status_from_cycle(cycle)


def _status_from_cycle(cycle: dict[str, Any]) -> dict[str, Any]:
    db_state = cycle.get("db_state") if isinstance(cycle.get("db_state"), dict) else {}
    learning_state = _learning_state(cycle, db_state)
    capital_state = str(cycle.get("capital_state") or "")
    adaptive_state = str(cycle.get("adaptive_state") or "")
    ready = _db_ready(db_state) and bool(cycle.get("safe_to_learn")) and learning_state not in {
        "paused_by_db_degraded",
        "paused_by_db_schema_missing",
    }
    return {
        "ok": True,
        "status": "autonomous_learning_status_ready" if ready else f"autonomous_learning_status_{learning_state}",
        "status_version": AUTONOMOUS_LEARNING_STATUS_VERSION,
        "legacy_learning_status": False,
        "mode": "read_only_status",
        "provider": db_state.get("provider") or "",
        "db_available": bool(db_state.get("db_available")),
        "db_degraded": bool(db_state.get("db_degraded")),
        "tables_ready": bool(db_state.get("tables_ready")),
        "db_state": db_state,
        "learning_state": learning_state,
        "capital_state": capital_state,
        "capital_protection": cycle.get("capital_protection") if isinstance(cycle.get("capital_protection"), dict) else {},
        "adaptive_state": adaptive_state,
        "adaptive_governor": cycle.get("adaptive_governor") if isinstance(cycle.get("adaptive_governor"), dict) else {},
        "safe_to_learn": bool(cycle.get("safe_to_learn")),
        "safe_to_open_new_shadow": bool(cycle.get("safe_to_open_new_shadow")),
        "active_profiles": _rows(cycle.get("active_profiles")),
        "paused_profiles": _rows(cycle.get("paused_profiles")),
        "degraded_profiles": _rows(cycle.get("degraded_profiles")),
        "tournament_top_candidate": cycle.get("tournament_top_candidate") if isinstance(cycle.get("tournament_top_candidate"), dict) else None,
        "paper_rotation_recommendation": cycle.get("paper_rotation_recommendation") or "",
        "paper_rotation_applied": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "circuit_breakers": _rows(cycle.get("circuit_breakers")),
        "recommended_next_action": _recommended_next_action(cycle, learning_state),
        "orchestrator_status": cycle.get("status") or "",
        "orchestrator_read_only": True,
        "loop_started": False,
        "paper_rotation_apply_requested": False,
        "mutations_allowed": False,
        "applies_to_real_trading": False,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        **_safety(),
    }


def _learning_state(cycle: dict[str, Any], db_state: dict[str, Any]) -> str:
    state = str(cycle.get("learning_state") or "").strip()
    if not _db_ready(db_state):
        recommendation = str(db_state.get("recommendation") or "").casefold()
        missing_tables = db_state.get("missing_tables") if isinstance(db_state.get("missing_tables"), list) else []
        if not bool(db_state.get("tables_ready")) and (missing_tables or recommendation == "apply_schema_sql"):
            return "paused_by_db_schema_missing"
        return "paused_by_db_degraded"
    return state or "continue_research"


def _recommended_next_action(cycle: dict[str, Any], learning_state: str) -> str:
    if learning_state in {"paused_by_db_degraded", "paused_by_db_schema_missing"}:
        return "NO_TRADE"
    return str(cycle.get("recommended_next_action") or "continue_research")


def _db_ready(db_state: dict[str, Any]) -> bool:
    return bool(db_state.get("db_available") and db_state.get("tables_ready") and not db_state.get("db_degraded"))


def _rows(value: object) -> list[dict[str, Any]]:
    return [row for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

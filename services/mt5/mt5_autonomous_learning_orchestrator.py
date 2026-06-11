from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.mt5.mt5_adaptive_strategy_governor import run_adaptive_strategy_governor
from services.mt5.mt5_capital_protection_governor import run_capital_protection_governor
from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation
from services.mt5.mt5_persistent_intelligence_store import (
    MT5PersistentIntelligenceStore,
    persist_candidate_rotation_run,
    persist_research_lesson,
    persist_risk_event,
)
from services.mt5.mt5_research_rejection_registry import research_rejection
from services.mt5.mt5_risk_governor import assess_runtime_risk
from services.mt5.mt5_shadow_trade_hygiene import run_shadow_trade_hygiene
from services.mt5.mt5_strategy_tournament import run_strategy_tournament


ORCHESTRATOR_VERSION = "2026-06-11.mt5_autonomous_learning_orchestrator.v1"

DEFAULT_SCORE_MARGIN = 5.0
DEFAULT_MIN_AUTO_ROTATION_TRADES = 15
DEFAULT_MAX_DRAWDOWN_FOR_ROTATION = 5.0
DEFAULT_LOCK_PATH = Path(os.environ.get("TEMP") or ".") / "genesis_mt5_autonomous_learning.lock"


def run_autonomous_learning_orchestrator(
    *,
    symbol: str = "BTCUSD",
    timeframe: str = "",
    dry_run: bool = False,
    apply_paper_rotation: bool = False,
    persistent_status: dict[str, Any] | None = None,
    recent_events: dict[str, Any] | None = None,
    capital_result: dict[str, Any] | None = None,
    adaptive_result: dict[str, Any] | None = None,
    hygiene_result: dict[str, Any] | None = None,
    tournament_result: dict[str, Any] | None = None,
    risk_governor_result: dict[str, Any] | None = None,
    learning_result: dict[str, Any] | None = None,
    active_profile: dict[str, Any] | None = None,
    closed_trades: list[dict[str, Any]] | None = None,
    open_trades: list[dict[str, Any]] | None = None,
    profile_performance: list[dict[str, Any]] | None = None,
    score_margin: float = DEFAULT_SCORE_MARGIN,
    min_auto_rotation_trades: int = DEFAULT_MIN_AUTO_ROTATION_TRADES,
    max_drawdown_for_rotation: float = DEFAULT_MAX_DRAWDOWN_FOR_ROTATION,
    load_persistent: bool = True,
    load_shadow_snapshot: bool = True,
    load_rotation: bool = True,
    run_trade_learning: bool = True,
    persist_events: bool = True,
    store: MT5PersistentIntelligenceStore | None = None,
) -> dict[str, Any]:
    clean_symbol = _symbol(symbol)
    clean_timeframe = _timeframe(timeframe)
    persistent = _persistent_context(
        persistent_status=persistent_status,
        recent_events=recent_events,
        load_persistent=load_persistent,
        store=store,
    )
    db_state = persistent["status"]
    circuit_breakers: list[dict[str, Any]] = []
    if not _db_ready(db_state):
        circuit_breakers.append(_breaker("persistent_db_degraded", True, True, "persistent_db_degraded", _db_reason(db_state)))
        result = _base_result(
            learning_state="paused_by_db_degraded",
            db_state=db_state,
            capital_result={},
            adaptive_result={},
            hygiene_result={},
            tournament_result={},
            learning_result={},
            risk_governor_result={},
            safe_to_learn=False,
            safe_to_open_new_shadow=False,
            paper_rotation_recommendation="db_degraded_no_rotation",
            paper_rotation_applied=False,
            circuit_breakers=circuit_breakers,
            recommended_next_action="NO_TRADE",
            dry_run=dry_run,
            apply_paper_rotation=apply_paper_rotation,
        )
        if persist_events and not dry_run:
            _persist_cycle(result)
        return result

    capital = capital_result or run_capital_protection_governor(
        closed_trades=closed_trades,
        open_trades=open_trades,
        persistent_status=db_state,
        load_shadow_snapshot=load_shadow_snapshot,
        load_persistent=False,
        persist_events=bool(persist_events and not dry_run),
    )
    capital_state = str(capital.get("capital_state") or "")
    capital_controlled = capital_state in {"normal", "caution"}
    if not capital_controlled or not bool(capital.get("safe_to_trade")):
        circuit_breakers.extend(_active_breakers(capital.get("circuit_breakers"), prefix="capital_protection"))
        result = _base_result(
            learning_state="paused_by_capital_protection",
            db_state=db_state,
            capital_result=capital,
            adaptive_result={},
            hygiene_result={},
            tournament_result={},
            learning_result={},
            risk_governor_result={},
            safe_to_learn=False,
            safe_to_open_new_shadow=False,
            paper_rotation_recommendation="capital_protection_no_rotation",
            paper_rotation_applied=False,
            circuit_breakers=circuit_breakers,
            recommended_next_action=capital.get("recommended_action") or "NO_TRADE",
            dry_run=dry_run,
            apply_paper_rotation=apply_paper_rotation,
        )
        if persist_events and not dry_run:
            _persist_cycle(result)
        return result

    adaptive = adaptive_result or run_adaptive_strategy_governor(
        closed_trades=closed_trades,
        open_trades=open_trades,
        load_shadow_snapshot=load_shadow_snapshot,
        load_rotation=False,
        load_intelligence=False,
        persist_events=bool(persist_events and not dry_run),
    )
    adaptive_state = str(adaptive.get("global_state") or "")
    if adaptive_state == "kill_switch":
        circuit_breakers.extend(_active_breakers(adaptive.get("circuit_breakers"), prefix="adaptive_governor"))
        if not circuit_breakers:
            circuit_breakers.append(_breaker("adaptive_governor_kill_switch", True, True, "adaptive_governor:kill_switch", "adaptive governor is in kill_switch"))
        result = _base_result(
            learning_state="paused_by_adaptive_governor",
            db_state=db_state,
            capital_result=capital,
            adaptive_result=adaptive,
            hygiene_result={},
            tournament_result={},
            learning_result={},
            risk_governor_result={},
            safe_to_learn=False,
            safe_to_open_new_shadow=False,
            paper_rotation_recommendation="adaptive_governor_no_rotation",
            paper_rotation_applied=False,
            circuit_breakers=circuit_breakers,
            recommended_next_action=adaptive.get("recommended_next_action") or "NO_TRADE",
            dry_run=dry_run,
            apply_paper_rotation=apply_paper_rotation,
        )
        if persist_events and not dry_run:
            _persist_cycle(result)
        return result

    hygiene = hygiene_result or run_shadow_trade_hygiene(
        open_trades=open_trades,
        load_shadow_snapshot=load_shadow_snapshot,
    )
    safe_to_open = bool(hygiene.get("safe_to_open_new_shadow"))
    if not safe_to_open:
        circuit_breakers.append(
            _breaker(
                "max_open_shadow_trades",
                True,
                True,
                "shadow_hygiene:max_open_shadow_trades",
                f"open_shadow_trades={hygiene.get('open_shadow_trades')}",
            )
        )

    tournament = tournament_result or run_strategy_tournament(
        profile_performance=profile_performance,
        closed_trades=closed_trades,
        persistent_status=db_state,
        load_shadow_snapshot=load_shadow_snapshot,
        load_persistent=False,
        load_rotation=load_rotation,
        persist_events=bool(persist_events and not dry_run),
    )
    risk = risk_governor_result or _safe_risk_governor(clean_symbol, clean_timeframe, open_trades)
    learned = learning_result or {}
    safe_to_learn = bool(_db_ready(db_state) and capital_controlled and adaptive_state != "kill_switch")
    if safe_to_learn and run_trade_learning and not dry_run and learning_result is None:
        learned = _run_trade_learning(clean_symbol, clean_timeframe)

    rotation = _paper_rotation_decision(
        tournament=tournament,
        active_profile=active_profile,
        db_state=db_state,
        capital=capital,
        adaptive=adaptive,
        hygiene=hygiene,
        risk=risk,
        score_margin=score_margin,
        min_auto_rotation_trades=min_auto_rotation_trades,
        max_drawdown_for_rotation=max_drawdown_for_rotation,
    )
    paper_rotation_applied = False
    apply_result: dict[str, Any] = {}
    if apply_paper_rotation and not dry_run and rotation["approved"]:
        paper_rotation_applied = True
        if persist_events:
            apply_result = _apply_paper_rotation_decision(rotation, store=store)
    learning_state = _learning_state(
        safe_to_learn=safe_to_learn,
        safe_to_open=safe_to_open,
        rotation=rotation,
        apply_paper_rotation=apply_paper_rotation,
        paper_rotation_applied=paper_rotation_applied,
        tournament=tournament,
    )
    result = _base_result(
        learning_state=learning_state,
        db_state=db_state,
        capital_result=capital,
        adaptive_result=adaptive,
        hygiene_result=hygiene,
        tournament_result=tournament,
        learning_result=learned,
        risk_governor_result=risk,
        safe_to_learn=safe_to_learn,
        safe_to_open_new_shadow=safe_to_open,
        paper_rotation_recommendation=rotation["recommendation"],
        paper_rotation_applied=paper_rotation_applied,
        circuit_breakers=circuit_breakers,
        recommended_next_action=_recommended_next_action(learning_state, rotation, tournament, hygiene),
        dry_run=dry_run,
        apply_paper_rotation=apply_paper_rotation,
    )
    result["paper_rotation_decision"] = rotation
    result["paper_rotation_apply_result"] = apply_result
    if persist_events and not dry_run:
        _persist_cycle(result)
    return result


def run_autonomous_learning_loop(
    *,
    interval_seconds: int = 300,
    max_cycles: int | None = None,
    lock_path: str | Path | None = None,
    **cycle_kwargs: Any,
) -> dict[str, Any]:
    path = Path(lock_path) if lock_path is not None else DEFAULT_LOCK_PATH
    if path.exists():
        return {
            "ok": False,
            "status": "autonomous_learning_loop_lock_active",
            "lock_active": True,
            "lock_path": str(path),
            "cycles_completed": 0,
            "learning_state": "idle",
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"pid={os.getpid()} started_at={_now()}\n", encoding="utf-8")
    cycles: list[dict[str, Any]] = []
    interrupted = False
    try:
        while True:
            cycle = run_autonomous_learning_orchestrator(**cycle_kwargs)
            cycles.append(_compact_cycle(cycle))
            if max_cycles is not None and len(cycles) >= int(max_cycles):
                break
            time.sleep(max(1, int(interval_seconds)))
    except KeyboardInterrupt:  # pragma: no cover - interactive safety
        interrupted = True
    finally:
        try:
            path.unlink(missing_ok=True)
        except TypeError:  # pragma: no cover - py<3.8 guard
            if path.exists():
                path.unlink()
    last = cycles[-1] if cycles else {}
    return {
        "ok": True,
        "status": "autonomous_learning_loop_completed" if not interrupted else "autonomous_learning_loop_interrupted",
        "lock_active": False,
        "lock_path": str(path),
        "cycles_completed": len(cycles),
        "last_cycle": last,
        "learning_state": last.get("learning_state") or "idle",
        "interrupted": interrupted,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _persistent_context(
    *,
    persistent_status: dict[str, Any] | None,
    recent_events: dict[str, Any] | None,
    load_persistent: bool,
    store: MT5PersistentIntelligenceStore | None,
) -> dict[str, Any]:
    if persistent_status is not None:
        return {"status": persistent_status, "recent_events": recent_events or {}}
    if not load_persistent:
        return {
            "status": {"db_available": True, "db_degraded": False, "tables_ready": True, "provider": "test_disabled"},
            "recent_events": recent_events or {},
        }
    try:
        active_store = store or MT5PersistentIntelligenceStore()
        return {
            "status": active_store.healthcheck(write_test_event=False),
            "recent_events": recent_events if recent_events is not None else active_store.recent_events(limit=50),
        }
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return {
            "status": {
                "db_available": False,
                "db_degraded": True,
                "tables_ready": False,
                "provider": "unavailable",
                "reason": type(exc).__name__,
            },
            "recent_events": recent_events or {},
        }


def _paper_rotation_decision(
    *,
    tournament: dict[str, Any],
    active_profile: dict[str, Any] | None,
    db_state: dict[str, Any],
    capital: dict[str, Any],
    adaptive: dict[str, Any],
    hygiene: dict[str, Any],
    risk: dict[str, Any],
    score_margin: float,
    min_auto_rotation_trades: int,
    max_drawdown_for_rotation: float,
) -> dict[str, Any]:
    top = tournament.get("top_candidate") if isinstance(tournament.get("top_candidate"), dict) else {}
    reasons: list[str] = []
    if not top:
        reasons.append("no_tournament_top_candidate")
    if not _db_ready(db_state):
        reasons.append("db_degraded")
    if str(capital.get("capital_state") or "") not in {"normal", "caution"}:
        reasons.append("capital_state_not_controlled")
    if str(adaptive.get("global_state") or "") == "kill_switch":
        reasons.append("adaptive_governor_kill_switch")
    if not bool(hygiene.get("safe_to_open_new_shadow", True)):
        reasons.append("open_shadow_excess")
    if not bool(risk.get("allowed", risk.get("risk_governor_allowed", False))):
        reasons.append(f"risk_governor_block:{risk.get('reason') or risk.get('risk_governor_reason') or 'blocked'}")
    if top:
        if forward_profile_degradation(top.get("symbol"), top.get("timeframe"), top.get("profile")):
            reasons.append("degradation_registry_block")
        if research_rejection(top.get("symbol"), top.get("timeframe"), top.get("profile"), _infer_family(top.get("profile"))):
            reasons.append("research_rejection_registry_block")
        if top.get("sibling_risk"):
            reasons.append("sibling_risk")
        if int(_float(top.get("trades_forward"))) < int(min_auto_rotation_trades):
            reasons.append("insufficient_trades_for_auto_rotation")
        if _float(top.get("recent_profit_factor")) < 1.05:
            reasons.append("recent_profit_factor_below_1_05")
        if _float(top.get("expectancy")) <= 0:
            reasons.append("expectancy_not_positive")
        if _float(top.get("max_drawdown")) > float(max_drawdown_for_rotation):
            reasons.append("drawdown_too_high")
        if _float(top.get("win_rate")) >= 60.0 and _float(top.get("expectancy")) <= 0:
            reasons.append("high_winrate_negative_expectancy")
    if not active_profile:
        reasons.append("missing_active_profile_context")
        active_score = 0.0
    else:
        active_score = _float(active_profile.get("tournament_score"))
    top_score = _float(top.get("tournament_score")) if top else 0.0
    if top and active_profile and top_score <= active_score + float(score_margin):
        reasons.append("score_margin_not_met")
    approved = bool(top) and not reasons
    recommendation = "paper_rotation_candidate_approved" if approved else _blocked_rotation_recommendation(reasons)
    return {
        "approved": approved,
        "recommendation": recommendation,
        "rejection_reasons": reasons,
        "candidate": top or None,
        "active_profile": active_profile or {},
        "active_score": round(active_score, 6),
        "candidate_score": round(top_score, 6),
        "score_margin": float(score_margin),
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _blocked_rotation_recommendation(reasons: list[str]) -> str:
    if not reasons:
        return "continue_research"
    if any(reason in reasons for reason in ("degradation_registry_block", "research_rejection_registry_block", "sibling_risk")):
        return "paused_by_registry"
    if "db_degraded" in reasons:
        return "db_degraded_no_rotation"
    if "open_shadow_excess" in reasons:
        return "cleanup_open_shadows_before_rotation"
    if "missing_active_profile_context" in reasons:
        return "paper_rotation_review_missing_active_context"
    return "continue_research"


def _apply_paper_rotation_decision(
    rotation: dict[str, Any],
    *,
    store: MT5PersistentIntelligenceStore | None,
) -> dict[str, Any]:
    candidate = rotation.get("candidate") if isinstance(rotation.get("candidate"), dict) else {}
    persist_result = persist_candidate_rotation_run(
        {
            "recommendation": "paper_rotation_applied_review_only",
            "recommended_candidate": candidate,
            "candidate_activated": False,
            "paper_forward_onboarding_started": False,
        },
        store=store,
    )
    profile_state: dict[str, Any] = {}
    try:
        active_store = store or MT5PersistentIntelligenceStore()
        profile_state = active_store.upsert_profile_state(
            {
                "symbol": candidate.get("symbol") or "",
                "timeframe": candidate.get("timeframe") or "",
                "profile": candidate.get("profile") or "",
                "status": "paper_rotation_review",
                "active": False,
                "applies_to_paper_shadow": False,
                "applies_to_real_trading": False,
                "registry_source": "autonomous_learning_orchestrator",
            }
        )
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        profile_state = {"ok": False, "db_degraded": True, "reason": type(exc).__name__, **_safety()}
    return {
        "ok": bool(persist_result.get("ok")) and bool(profile_state.get("ok", True)),
        "candidate_rotation_run": persist_result,
        "profile_state": profile_state,
        "promoted_profile_mutated": False,
        "applies_to_real_trading": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        **_safety(),
    }


def _persist_cycle(result: dict[str, Any]) -> None:
    state = result.get("learning_state") or "idle"
    result["persistent_intelligence_learning_cycle"] = persist_candidate_rotation_run(
        {
            "recommendation": result.get("recommended_next_action") or state,
            "recommended_candidate": result.get("tournament_top_candidate") or {},
            "candidate_activated": False,
            "paper_forward_onboarding_started": False,
        }
    )
    if state not in {"learning", "paper_rotation_review", "paper_rotation_applied"} or result.get("circuit_breakers"):
        result["persistent_intelligence_risk_event"] = persist_risk_event(
            {
                "symbol": "",
                "timeframe": "",
                "risk_state": state,
                "allowed": bool(result.get("safe_to_learn") and result.get("safe_to_open_new_shadow")),
                "reason": result.get("paper_rotation_recommendation") or state,
                "circuit_breaker": (result.get("circuit_breakers") or [{}])[0].get("name") if result.get("circuit_breakers") else "autonomous_learning_orchestrator",
                "open_shadow_count": ((result.get("shadow_hygiene") or {}).get("open_shadow_trades") or 0),
                "recommended_action": result.get("recommended_next_action") or "NO_TRADE",
            }
        )
    for row in (result.get("paused_profiles") or [])[:3] + (result.get("degraded_profiles") or [])[:3]:
        if not isinstance(row, dict):
            continue
        result.setdefault("persistent_intelligence_research_lessons", []).append(
            persist_research_lesson(
                {
                    "family": _infer_family(row.get("profile")),
                    "symbol": row.get("symbol") or "",
                    "timeframe": row.get("timeframe") or "",
                    "lesson_type": "autonomous_learning_profile_action",
                    "failure_pattern": row.get("recommended_action") or "",
                    "summary": f"{row.get('profile')} autonomous action: {row.get('recommended_action')}",
                    "avoid_next": [row.get("profile") or ""],
                    "recommended_next_research_phase": result.get("recommended_next_action") or "continue_research",
                }
            )
        )


def _run_trade_learning(symbol: str, timeframe: str) -> dict[str, Any]:
    try:
        from services.mt5.mt5_trade_memory import MT5TradeMemoryEngine

        return MT5TradeMemoryEngine().run_learning({"symbol": symbol, "timeframe": timeframe, "mode": "paper", "max_trades": 25})
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return {"ok": False, "status": "mt5_trade_learning_unavailable", "reason": type(exc).__name__, **_safety()}


def _safe_risk_governor(symbol: str, timeframe: str, open_trades: list[dict[str, Any]] | None) -> dict[str, Any]:
    try:
        return assess_runtime_risk(
            symbol or "BTCUSD",
            timeframe=timeframe,
            signal={"action": "NO_TRADE", "lot_multiplier": 1.0},
            open_trade=(open_trades or [{}])[0] if open_trades else None,
        )
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return {"allowed": False, "reason": type(exc).__name__, "risk_state": "blocked", **_safety()}


def _base_result(
    *,
    learning_state: str,
    db_state: dict[str, Any],
    capital_result: dict[str, Any],
    adaptive_result: dict[str, Any],
    hygiene_result: dict[str, Any],
    tournament_result: dict[str, Any],
    learning_result: dict[str, Any],
    risk_governor_result: dict[str, Any],
    safe_to_learn: bool,
    safe_to_open_new_shadow: bool,
    paper_rotation_recommendation: str,
    paper_rotation_applied: bool,
    circuit_breakers: list[dict[str, Any]],
    recommended_next_action: str,
    dry_run: bool,
    apply_paper_rotation: bool,
) -> dict[str, Any]:
    return {
        "ok": True,
        "status": "autonomous_learning_orchestrator_ready",
        "orchestrator_version": ORCHESTRATOR_VERSION,
        "mode": "paper_shadow_only",
        "decision": "NO_TRADE",
        "reason": f"autonomous_learning:{learning_state}",
        "learning_state": learning_state,
        "db_state": _compact_db_state(db_state),
        "capital_state": capital_result.get("capital_state") or "",
        "capital_protection": capital_result,
        "adaptive_state": adaptive_result.get("global_state") or "",
        "adaptive_governor": adaptive_result,
        "shadow_hygiene": hygiene_result,
        "risk_governor": risk_governor_result,
        "safe_to_learn": bool(safe_to_learn),
        "safe_to_open_new_shadow": bool(safe_to_open_new_shadow),
        "active_profiles": _profiles(adaptive_result, tournament_result, "active"),
        "paused_profiles": _profiles(adaptive_result, tournament_result, "paused"),
        "degraded_profiles": _profiles(adaptive_result, tournament_result, "degraded"),
        "tournament_top_candidate": tournament_result.get("top_candidate") if isinstance(tournament_result, dict) else None,
        "paper_rotation_recommendation": paper_rotation_recommendation,
        "paper_rotation_applied": bool(paper_rotation_applied),
        "paper_rotation_apply_requested": bool(apply_paper_rotation),
        "dry_run": bool(dry_run),
        "mutations_allowed": bool(not dry_run and safe_to_learn),
        "strategy_tournament": tournament_result,
        "learning_result": learning_result,
        "rejected_candidates": (tournament_result.get("rejected_profiles") or []) if isinstance(tournament_result, dict) else [],
        "circuit_breakers": circuit_breakers,
        "recommended_next_action": recommended_next_action,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "promoted_profile_mutated": False,
        "automatic_real_promotion": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _profiles(adaptive: dict[str, Any], tournament: dict[str, Any], kind: str) -> list[dict[str, Any]]:
    if kind == "active":
        rows = adaptive.get("active_profiles") or []
    elif kind == "paused":
        rows = (adaptive.get("paused_profiles") or []) + (tournament.get("paused_profiles") or [])
    else:
        rows = (adaptive.get("degraded_profiles") or []) + (tournament.get("degraded_profiles") or [])
    return [row for row in rows if isinstance(row, dict)]


def _learning_state(
    *,
    safe_to_learn: bool,
    safe_to_open: bool,
    rotation: dict[str, Any],
    apply_paper_rotation: bool,
    paper_rotation_applied: bool,
    tournament: dict[str, Any],
) -> str:
    if paper_rotation_applied:
        return "paper_rotation_applied"
    if rotation.get("recommendation") == "paused_by_registry":
        return "paused_by_registry"
    if rotation.get("approved"):
        return "paper_rotation_review"
    if not safe_to_learn:
        return "continue_research"
    if not safe_to_open:
        return "continue_research"
    if tournament.get("paused_profiles") or tournament.get("degraded_profiles"):
        return "learning"
    return "continue_research"


def _recommended_next_action(
    learning_state: str,
    rotation: dict[str, Any],
    tournament: dict[str, Any],
    hygiene: dict[str, Any],
) -> str:
    if learning_state in {"paused_by_db_degraded", "paused_by_capital_protection", "paused_by_adaptive_governor", "kill_switch"}:
        return "NO_TRADE"
    if not bool(hygiene.get("safe_to_open_new_shadow", True)):
        return hygiene.get("recommended_cleanup_action") or "cleanup_open_shadows_before_rotation"
    if rotation.get("approved"):
        return "apply_paper_rotation_review" if learning_state == "paper_rotation_applied" else "paper_rotation_review"
    return tournament.get("recommended_action") or "continue_research"


def _active_breakers(rows: Any, *, prefix: str) -> list[dict[str, Any]]:
    active: list[dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict) or not row.get("active"):
            continue
        active.append(
            _breaker(
                f"{prefix}:{row.get('name') or 'breaker'}",
                True,
                bool(row.get("critical")),
                str(row.get("reason") or row.get("name") or ""),
                str(row.get("detail") or ""),
            )
        )
    return active


def _compact_cycle(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "learning_state": result.get("learning_state") or "",
        "safe_to_learn": bool(result.get("safe_to_learn")),
        "safe_to_open_new_shadow": bool(result.get("safe_to_open_new_shadow")),
        "paper_rotation_recommendation": result.get("paper_rotation_recommendation") or "",
        "paper_rotation_applied": bool(result.get("paper_rotation_applied")),
        "recommended_next_action": result.get("recommended_next_action") or "",
        **_safety(),
    }


def _compact_db_state(status: dict[str, Any]) -> dict[str, Any]:
    return {
        "provider": status.get("provider") or "",
        "db_available": bool(status.get("db_available")),
        "tables_ready": bool(status.get("tables_ready")),
        "db_degraded": bool(status.get("db_degraded") or not status.get("db_available") or not status.get("tables_ready")),
        "recommendation": status.get("recommendation") or "",
        "missing_tables": status.get("missing_tables") if isinstance(status.get("missing_tables"), list) else [],
        "queue_depth": int(_float(status.get("queue_depth"))),
        "queued_writes": int(_float(status.get("queued_writes"))),
        "failed_writes": int(_float(status.get("failed_writes"))),
        "backoff_active": bool(status.get("backoff_active")),
    }


def _db_ready(status: dict[str, Any]) -> bool:
    return bool(status.get("db_available") and status.get("tables_ready") and not status.get("db_degraded"))


def _db_reason(status: dict[str, Any]) -> str:
    return str(status.get("recommendation") or status.get("reason") or "persistent intelligence db degraded")


def _breaker(name: str, active: bool, critical: bool, reason: str, detail: str) -> dict[str, Any]:
    return {
        "name": name,
        "active": bool(active),
        "critical": bool(critical),
        "reason": reason if active else "",
        "detail": detail if active else "",
        **_safety(),
    }


def _infer_family(profile: object) -> str:
    text = str(profile or "").casefold()
    if "vol_breakout" in text or "volatility_breakout" in text:
        return "volatility_breakout"
    if "session_vwap" in text:
        return "session_vwap_reclaim"
    if "trend_pullback" in text:
        return "multi_timeframe_trend_pullback"
    if "ema_reclaim" in text:
        return "ema_reclaim"
    return text


def _symbol(value: object) -> str:
    return str(value or "").upper().strip().replace(".B", "")


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _float(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

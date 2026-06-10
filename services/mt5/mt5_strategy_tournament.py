from __future__ import annotations

from collections import defaultdict
from typing import Any

from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation
from services.mt5.mt5_paper_forward_candidate_rotation import run_paper_forward_candidate_rotation
from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore, persist_candidate_rotation_run, persist_research_lesson
from services.mt5.mt5_research_rejection_registry import research_rejection


TOURNAMENT_VERSION = "2026-06-10.mt5_strategy_tournament.v1"

DEFAULT_WEIGHTS = {
    "win_rate_weight": 0.25,
    "profit_factor_weight": 22.0,
    "expectancy_weight": 18.0,
    "drawdown_penalty": 5.0,
    "consecutive_loss_penalty": 9.0,
    "monte_carlo_penalty": 12.0,
    "stale_data_penalty": 15.0,
    "registry_penalty": 1000.0,
    "sibling_risk_penalty": 1000.0,
}


def run_strategy_tournament(
    *,
    profile_performance: list[dict[str, Any]] | None = None,
    closed_trades: list[dict[str, Any]] | None = None,
    decision_events: list[dict[str, Any]] | None = None,
    risk_events: list[dict[str, Any]] | None = None,
    rotation_result: dict[str, Any] | None = None,
    research_lessons: list[dict[str, Any]] | None = None,
    persistent_status: dict[str, Any] | None = None,
    weights: dict[str, Any] | None = None,
    load_shadow_snapshot: bool = True,
    load_persistent: bool = True,
    load_rotation: bool = True,
    persist_events: bool = True,
) -> dict[str, Any]:
    active_weights = {**DEFAULT_WEIGHTS, **(weights or {})}
    persistent = _load_persistent(load_persistent, persistent_status)
    shadow_snapshot = _load_shadow_snapshot(load_shadow_snapshot, closed_trades)
    closed = list(closed_trades if closed_trades is not None else shadow_snapshot.get("closed_trades") or [])
    recent = persistent.get("recent_events") if isinstance(persistent.get("recent_events"), dict) else {}
    decisions = list(decision_events if decision_events is not None else recent.get("recent_decisions") or [])
    risks = list(risk_events if risk_events is not None else recent.get("recent_risk_events") or [])
    lessons = list(research_lessons if research_lessons is not None else recent.get("recent_research_lessons") or [])
    rotation = rotation_result
    if rotation is None and load_rotation:
        rotation = _safe_rotation()
    if rotation is None:
        rotation = _empty_rotation()

    rows = _profile_rows(profile_performance or [], closed, rotation)
    evaluated = [
        _evaluate_profile(
            row,
            decisions=decisions,
            risk_events=risks,
            research_lessons=lessons,
            persistent_status=persistent.get("status") if isinstance(persistent.get("status"), dict) else {},
            weights=active_weights,
        )
        for row in rows
    ]
    evaluated.sort(key=lambda row: row["tournament_score"], reverse=True)
    for index, row in enumerate(evaluated, start=1):
        row["rank"] = index
    eligible = [row for row in evaluated if not _blocked(row)]
    top_candidate = eligible[0] if eligible else None
    paused = [row for row in evaluated if row["recommended_action"] == "pause_profile"]
    degraded = [row for row in evaluated if row["recommended_action"] == "degrade_profile" or row.get("degraded_by_registry")]
    rejected = [row for row in evaluated if row.get("rejected_by_research_registry") or row.get("sibling_risk")]
    recommended_action = _overall_action(top_candidate, paused, degraded, persistent.get("status") if isinstance(persistent.get("status"), dict) else {})
    result = {
        "ok": True,
        "status": "strategy_tournament_ready",
        "tournament_version": TOURNAMENT_VERSION,
        "mode": "paper_shadow_only",
        "decision": "NO_TRADE",
        "reason": f"strategy_tournament:{recommended_action}",
        "ranked_profiles": evaluated,
        "top_candidate": top_candidate,
        "paused_profiles": paused,
        "degraded_profiles": degraded,
        "rejected_profiles": rejected,
        "recommended_action": recommended_action,
        "switching_rules": [
            "pf_below_0_9_and_expectancy_non_positive_after_5_trades_degrades",
            "three_consecutive_losses_pause_profile",
            "recent_win_rate_below_35_after_8_trades_degrades",
            "blocked_registry_rejection_sibling_or_db_degraded_never_rotates",
            "paper_review_only_no_automatic_promotion",
        ],
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "live_runtime_mutated": False,
        **_safety(),
    }
    if persist_events:
        _persist_tournament(result)
    return result


def strategy_tournament_enforcement(
    *,
    symbol: str = "",
    timeframe: str = "",
    profile: str = "",
    tournament_result: dict[str, Any] | None = None,
    load_shadow_snapshot: bool = True,
    load_persistent: bool = True,
    load_rotation: bool = True,
) -> dict[str, Any]:
    tournament = tournament_result or run_strategy_tournament(
        load_shadow_snapshot=load_shadow_snapshot,
        load_persistent=load_persistent,
        load_rotation=load_rotation,
    )
    clean_symbol = _symbol(symbol)
    clean_timeframe = _timeframe(timeframe)
    clean_profile = str(profile or "").strip()
    matching = {}
    for row in tournament.get("ranked_profiles") or []:
        if not isinstance(row, dict):
            continue
        if _symbol(row.get("symbol")) == clean_symbol and _timeframe(row.get("timeframe")) == clean_timeframe and str(row.get("profile") or "") == clean_profile:
            matching = row
            break
    blocked = bool(
        matching
        and (
            matching.get("degraded_by_registry")
            or matching.get("rejected_by_research_registry")
            or matching.get("sibling_risk")
            or matching.get("db_degraded")
            or matching.get("recommended_action") in {"pause_profile", "degrade_profile", "kill_switch", "continue_research"}
        )
    )
    return {
        "ok": True,
        "status": "strategy_tournament_enforcement_ready",
        "symbol": clean_symbol,
        "timeframe": clean_timeframe,
        "profile": clean_profile,
        "allowed": not blocked,
        "blocked": blocked,
        "decision": "NO_TRADE" if blocked else "ALLOW_PAPER_REVIEW",
        "reason": f"strategy_tournament:{matching.get('recommended_action') or 'blocked'}" if blocked else "",
        "matching_profile": matching,
        "strategy_tournament": tournament,
        "paper_exploration_created": False,
        "shadow_trade_id": "",
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _load_persistent(load_persistent: bool, persistent_status: dict[str, Any] | None) -> dict[str, Any]:
    if persistent_status is not None:
        return {"status": persistent_status, "recent_events": {}}
    if not load_persistent:
        return {"status": {"db_available": True, "db_degraded": False, "tables_ready": True}, "recent_events": {}}
    try:
        store = MT5PersistentIntelligenceStore()
        return {"status": store.healthcheck(write_test_event=False), "recent_events": store.recent_events(limit=50)}
    except Exception as exc:  # pragma: no cover
        return {"status": {"db_available": False, "db_degraded": True, "tables_ready": False, "reason": type(exc).__name__}, "recent_events": {}}


def _load_shadow_snapshot(load_shadow_snapshot: bool, closed_trades: list[dict[str, Any]] | None) -> dict[str, Any]:
    if closed_trades is not None or not load_shadow_snapshot:
        return {"closed_trades": closed_trades or []}
    try:
        from services.mt5.mt5_shadow_trading import MT5ShadowTrading

        return MT5ShadowTrading().snapshot(limit=500)
    except Exception:
        return {"closed_trades": []}


def _safe_rotation() -> dict[str, Any]:
    try:
        return run_paper_forward_candidate_rotation()
    except Exception:
        return _empty_rotation()


def _profile_rows(profile_performance: list[dict[str, Any]], closed: list[dict[str, Any]], rotation: dict[str, Any]) -> list[dict[str, Any]]:
    rows: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in profile_performance:
        if not isinstance(row, dict):
            continue
        key = (_symbol(row.get("symbol")), _timeframe(row.get("timeframe")), str(row.get("profile") or row.get("strategy_profile") or "").strip())
        if key[0] and key[2]:
            rows[key] = _normalize_performance_row(row)
    for key, trades in _group_trades(closed).items():
        if key not in rows:
            rows[key] = _row_from_trades(key, trades)
    for row in rotation.get("ranking") or []:
        if not isinstance(row, dict):
            continue
        key = (_symbol(row.get("symbol")), _timeframe(row.get("timeframe")), str(row.get("profile") or row.get("family") or "").strip())
        if key[0] and key[2] and key not in rows:
            rows[key] = _normalize_performance_row(row)
    return list(rows.values())


def _evaluate_profile(
    row: dict[str, Any],
    *,
    decisions: list[dict[str, Any]],
    risk_events: list[dict[str, Any]],
    research_lessons: list[dict[str, Any]],
    persistent_status: dict[str, Any],
    weights: dict[str, Any],
) -> dict[str, Any]:
    symbol = _symbol(row.get("symbol"))
    timeframe = _timeframe(row.get("timeframe"))
    profile = str(row.get("profile") or "")
    trades = int(row.get("trades_forward") or row.get("total_closed") or 0)
    win_rate = _float(row.get("win_rate"))
    recent_win_rate = _float(row.get("recent_win_rate") or row.get("win_rate"))
    pf = _float(row.get("profit_factor") or row.get("total_pf"))
    recent_pf = _float(row.get("recent_profit_factor") or row.get("recent_pf"))
    expectancy = _float(row.get("expectancy"))
    drawdown = _float(row.get("max_drawdown") or row.get("max_drawdown_pct"))
    consecutive_losses = int(_float(row.get("consecutive_losses")))
    mc_pf = _float(row.get("monte_carlo_stressed_pf"))
    degraded = bool(row.get("degraded_by_registry") or forward_profile_degradation(symbol, timeframe, profile))
    rejected = bool(row.get("rejected_by_research_registry") or research_rejection(symbol, timeframe, profile, _infer_family(profile)))
    sibling = bool(row.get("sibling_risk"))
    db_degraded = bool(persistent_status.get("db_degraded"))
    stale = _stale_profile(row, decisions, risk_events, research_lessons)
    action = _profile_action(
        trades=trades,
        pf=pf,
        expectancy=expectancy,
        consecutive_losses=consecutive_losses,
        recent_win_rate=recent_win_rate,
        degraded=degraded,
        rejected=rejected,
        sibling=sibling,
        db_degraded=db_degraded,
    )
    confidence_score = round(min(trades / 30.0, 1.0) * 35.0 + min(max(pf, 0.0), 2.5) / 2.5 * 45.0 + (20.0 if expectancy > 0 else 0.0), 6)
    risk_score = round(drawdown * 8.0 + consecutive_losses * 12.0 + (30.0 if mc_pf and mc_pf < 1.05 else 0.0), 6)
    score = (
        win_rate * float(weights["win_rate_weight"])
        + min(max(pf, 0.0), 3.0) * float(weights["profit_factor_weight"])
        + expectancy * float(weights["expectancy_weight"])
        - drawdown * float(weights["drawdown_penalty"])
        - consecutive_losses * float(weights["consecutive_loss_penalty"])
    )
    if mc_pf and mc_pf < 1.05:
        score -= float(weights["monte_carlo_penalty"])
    if stale:
        score -= float(weights["stale_data_penalty"])
    if degraded or rejected or db_degraded:
        score -= float(weights["registry_penalty"])
    if sibling:
        score -= float(weights["sibling_risk_penalty"])
    if expectancy <= 0:
        score -= 25.0
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "profile": profile,
        "trades_forward": trades,
        "win_rate": round(win_rate, 6),
        "profit_factor": round(pf, 6),
        "expectancy": round(expectancy, 8),
        "max_drawdown": round(drawdown, 6),
        "consecutive_losses": consecutive_losses,
        "recent_win_rate": round(recent_win_rate, 6),
        "recent_profit_factor": round(recent_pf, 6),
        "confidence_score": confidence_score,
        "risk_score": risk_score,
        "tournament_score": round(score, 6),
        "rank": 0,
        "recommended_action": action,
        "degraded_by_registry": degraded,
        "rejected_by_research_registry": rejected,
        "sibling_risk": sibling,
        "db_degraded": db_degraded,
        "stale_data": stale,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        **_safety(),
    }


def _profile_action(
    *,
    trades: int,
    pf: float,
    expectancy: float,
    consecutive_losses: int,
    recent_win_rate: float,
    degraded: bool,
    rejected: bool,
    sibling: bool,
    db_degraded: bool,
) -> str:
    if db_degraded:
        return "continue_research"
    if degraded or rejected or sibling:
        return "continue_research"
    if trades >= 5 and pf < 0.9 and expectancy <= 0:
        return "degrade_profile"
    if consecutive_losses >= 3:
        return "pause_profile"
    if trades >= 8 and recent_win_rate < 35.0:
        return "degrade_profile"
    if trades >= 15 and pf >= 1.15 and expectancy > 0:
        return "allow_paper_probe"
    return "keep_observing"


def _overall_action(top: dict[str, Any] | None, paused: list[dict[str, Any]], degraded: list[dict[str, Any]], persistent_status: dict[str, Any]) -> str:
    if persistent_status.get("db_degraded"):
        return "continue_research"
    if degraded:
        return "degrade_profile"
    if paused:
        return "pause_profile"
    if top and top.get("recommended_action") == "allow_paper_probe":
        return "rotate_to_better_candidate_review"
    return "continue_research"


def _blocked(row: dict[str, Any]) -> bool:
    return bool(
        row.get("degraded_by_registry")
        or row.get("rejected_by_research_registry")
        or row.get("sibling_risk")
        or row.get("db_degraded")
        or row.get("recommended_action") in {"pause_profile", "degrade_profile", "kill_switch", "continue_research"}
    )


def _persist_tournament(result: dict[str, Any]) -> None:
    result["persistent_intelligence_tournament_run"] = persist_candidate_rotation_run(
        {
            "recommendation": result.get("recommended_action") or "continue_research",
            "recommended_candidate": result.get("top_candidate") or {},
            "candidate_activated": False,
            "paper_forward_onboarding_started": False,
        }
    )
    lessons: list[dict[str, Any]] = []
    for row in (result.get("paused_profiles") or []) + (result.get("degraded_profiles") or []):
        if not isinstance(row, dict):
            continue
        lessons.append(
            persist_research_lesson(
                {
                    "family": _infer_family(row.get("profile")),
                    "symbol": row.get("symbol") or "",
                    "timeframe": row.get("timeframe") or "",
                    "lesson_type": "strategy_tournament_recommendation",
                    "failure_pattern": row.get("recommended_action") or "",
                    "summary": f"{row.get('profile')} tournament action: {row.get('recommended_action')}",
                    "avoid_next": [row.get("profile") or ""],
                    "recommended_next_research_phase": result.get("recommended_action") or "continue_research",
                }
            )
        )
        if len(lessons) >= 5:
            break
    if lessons:
        result["persistent_intelligence_research_lessons"] = lessons


def _group_trades(trades: list[dict[str, Any]]) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        key = (_symbol(trade.get("symbol")), _timeframe(trade.get("timeframe")), str(trade.get("strategy_profile") or trade.get("profile") or "unknown_profile").strip())
        if key[0] and key[2]:
            grouped[key].append(trade)
    return grouped


def _row_from_trades(key: tuple[str, str, str], trades: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [_pnl(row) for row in trades]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    total = len(pnls)
    recent = pnls[-10:]
    recent_wins = [value for value in recent if value > 0]
    recent_losses = [value for value in recent if value < 0]
    return {
        "symbol": key[0],
        "timeframe": key[1],
        "profile": key[2],
        "trades_forward": total,
        "win_rate": round(len(wins) / total * 100.0, 6) if total else 0.0,
        "profit_factor": _profit_factor(gross_win, gross_loss),
        "expectancy": round(sum(pnls) / total, 8) if total else 0.0,
        "max_drawdown": _max_drawdown(pnls),
        "consecutive_losses": _consecutive_losses(trades),
        "recent_win_rate": round(len(recent_wins) / len(recent) * 100.0, 6) if recent else 0.0,
        "recent_profit_factor": _profit_factor(sum(recent_wins), abs(sum(recent_losses))),
    }


def _normalize_performance_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": _symbol(row.get("symbol")),
        "timeframe": _timeframe(row.get("timeframe")),
        "profile": str(row.get("profile") or row.get("strategy_profile") or row.get("family") or ""),
        "trades_forward": int(_float(row.get("trades_forward") or row.get("total_closed") or row.get("closed") or row.get("recent_closed"))),
        "win_rate": _float(row.get("win_rate") or row.get("recent_win_rate")),
        "profit_factor": _float(row.get("profit_factor") or row.get("total_pf")),
        "expectancy": _float(row.get("expectancy")),
        "max_drawdown": _float(row.get("max_drawdown") or row.get("max_drawdown_pct")),
        "consecutive_losses": int(_float(row.get("consecutive_losses"))),
        "recent_win_rate": _float(row.get("recent_win_rate") or row.get("win_rate")),
        "recent_profit_factor": _float(row.get("recent_profit_factor") or row.get("recent_pf")),
        "monte_carlo_stressed_pf": _float(row.get("monte_carlo_stressed_pf")),
        "degraded_by_registry": bool(row.get("degraded_by_registry")),
        "rejected_by_research_registry": bool(row.get("rejected_by_research_registry")),
        "sibling_risk": bool(row.get("sibling_risk")),
    }


def _stale_profile(row: dict[str, Any], decisions: list[dict[str, Any]], risks: list[dict[str, Any]], lessons: list[dict[str, Any]]) -> bool:
    if row.get("stale_data"):
        return True
    profile = str(row.get("profile") or "")
    if not profile:
        return False
    observed = any(profile in str(item) for item in decisions + risks + lessons)
    return bool((decisions or risks or lessons) and not observed and int(row.get("trades_forward") or 0) <= 0)


def _empty_rotation() -> dict[str, Any]:
    return {"recommendation": "continue_research", "recommended_candidate": None, "ranking": []}


def _profit_factor(gross_win: float, gross_loss: float) -> float:
    if gross_loss <= 0:
        return 999.0 if gross_win > 0 else 0.0
    return round(gross_win / gross_loss, 6)


def _max_drawdown(pnls: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return round(drawdown, 6)


def _consecutive_losses(trades: list[dict[str, Any]]) -> int:
    count = 0
    for trade in reversed(trades):
        if _pnl(trade) < 0:
            count += 1
            continue
        break
    return count


def _pnl(trade: dict[str, Any]) -> float:
    for key in ("pnl", "profit", "r_multiple", "net_pnl", "pnl_r"):
        if trade.get(key) is not None:
            return _float(trade.get(key))
    status = str(trade.get("status") or "").casefold()
    if status == "win":
        return 1.0
    if status == "loss":
        return -1.0
    return 0.0


def _infer_family(profile: object) -> str:
    text = str(profile or "").casefold()
    if "vol_breakout" in text or "volatility_breakout" in text:
        return "volatility_breakout"
    if "session_vwap" in text:
        return "session_vwap_reclaim"
    if "trend_pullback" in text:
        return "multi_timeframe_trend_pullback"
    return text


def _symbol(value: object) -> str:
    symbol = str(value or "").upper().strip().replace(".B", "")
    if symbol in {"USTECB", "NAS100"}:
        return "USTEC"
    return symbol


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _float(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

from __future__ import annotations

import csv
import json
import time
from pathlib import Path
from typing import Any

from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation
from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore
from services.mt5.mt5_research_rejection_registry import research_rejection


HARVESTER_VERSION = "2026-06-12.mt5_robust_candidate_harvester.v1"

MIN_TOTAL_CLOSED = 50
MIN_RECENT_CLOSED = 20
MIN_RECENT_PF = 1.15
MIN_TOTAL_PF = 1.15
MAX_PROCESSED_SOURCE_BYTES = 2_000_000

_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PROCESSED_SOURCES = (
    _REPO_ROOT / "data" / "backtests" / "multisymbol" / "multi_symbol_recent_first_cost_calibrated_results.csv",
    _REPO_ROOT / "data" / "backtests" / "multisymbol" / "multi_symbol_recent_first_results.csv",
    _REPO_ROOT / "data" / "backtests" / "recent_first_research_results.csv",
    _REPO_ROOT / "data" / "backtests" / "recent_first_hardening_results.csv",
    _REPO_ROOT / "data" / "backtests" / "multisymbol" / "multi_symbol_recent_first_cost_calibrated_results.json",
    _REPO_ROOT / "data" / "backtests" / "multisymbol" / "multi_symbol_recent_first_results.json",
)

_ALIASES = {
    "symbol": ("symbol", "requested_symbol", "resolved_symbol", "normalized_symbol", "Symbol"),
    "timeframe": ("timeframe", "Timeframe"),
    "profile": ("profile", "strategy_profile", "target_name", "Profile"),
    "family": ("family", "strategy_family", "Family"),
    "recent_closed": ("recent_closed", "recent_trades", "closed_recent"),
    "total_closed": ("total_closed", "closed", "trades", "total_trades"),
    "recent_pf": ("recent_pf", "recent_profit_factor"),
    "total_pf": ("total_pf", "profit_factor"),
    "expectancy": ("expectancy", "total_expectancy"),
    "monte_carlo_stressed_pf": ("monte_carlo_stressed_pf", "mc_pf", "stressed_pf"),
    "remove_best_5_pf": ("remove_best_5_pf",),
    "spread_x2_pf": ("spread_x2_pf", "spread_stress_pf"),
    "single_trade_dependency": ("single_trade_dependency", "single_trade_dependent"),
    "fragile_regime_dependency": ("fragile_regime_dependency", "fragile_dependency"),
    "source_identity_resolved": ("source_identity_resolved",),
}


def run_robust_candidate_harvester(
    *,
    rows: list[dict[str, Any]] | None = None,
    processed_source_paths: list[str | Path] | None = None,
    persistent_events: dict[str, Any] | None = None,
    load_persistent: bool = True,
    store: MT5PersistentIntelligenceStore | Any | None = None,
    max_candidates: int = 10,
) -> dict[str, Any]:
    started = time.monotonic()
    loaded_sources: list[str] = []
    missing_sources: list[str] = []
    skipped_sources: list[dict[str, Any]] = []
    raw_rows: list[dict[str, Any]] = []
    if rows is not None:
        raw_rows.extend({**dict(row), "source": str(row.get("source") or "injected_rows")} for row in rows if isinstance(row, dict))
    else:
        paths = _requested_paths(processed_source_paths)
        loaded_rows, loaded_sources, missing_sources, skipped_sources = _load_processed_sources(paths)
        raw_rows.extend(loaded_rows)
    persistent = _load_persistent_events(persistent_events, load_persistent=load_persistent, store=store)
    raw_rows.extend(_rows_from_persistent_events(persistent.get("recent_events") or {}))

    evaluated = [_finalize_row(row) for row in raw_rows]
    useful_rows = [row for row in evaluated if row.get("symbol") and row.get("timeframe") and row.get("profile")]
    top_candidates = [row for row in useful_rows if row["candidate_status"] == "robust_candidate_ready"]
    rejected_candidates = [row for row in useful_rows if row["candidate_status"] != "robust_candidate_ready"]
    top_candidates.sort(key=_ranking_key)
    rejected_candidates.sort(key=_rejected_key)
    recommendation = "paper_observation_review" if top_candidates else "continue_research"
    return {
        "ok": True,
        "status": "robust_candidate_harvester_ready",
        "harvester_version": HARVESTER_VERSION,
        "mode": "processed_sources_and_persistent_memory_only",
        "loaded_sources": loaded_sources,
        "missing_sources": missing_sources,
        "skipped_sources": skipped_sources,
        "persistent_memory_source": persistent.get("source"),
        "persistent_memory_db_degraded": bool(persistent.get("db_degraded")),
        "raw_rows": len(raw_rows),
        "useful_rows": len(useful_rows),
        "top_candidates": top_candidates[: max(1, max_candidates)],
        "rejected_candidates": rejected_candidates[: max(1, max_candidates * 3)],
        "recommendation": recommendation,
        "recommended_candidate": top_candidates[0] if top_candidates else None,
        "recommended_next_research_phase": (
            "human_review_high_sample_low_dependency_candidate"
            if top_candidates
            else "run_research_intelligence_core_for_next_hypothesis"
        ),
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "paper_rotation_applied": False,
        "applies_to_real_trading": False,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "duration_ms": int((time.monotonic() - started) * 1000),
        **_safety(),
    }


def _load_processed_sources(paths: list[Path]) -> tuple[list[dict[str, Any]], list[str], list[str], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    loaded: list[str] = []
    missing: list[str] = []
    skipped: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            missing.append(str(path))
            continue
        size = path.stat().st_size
        if size > MAX_PROCESSED_SOURCE_BYTES:
            skipped.append({"source": str(path), "reason": "processed_source_above_size_cap", "bytes": size})
            continue
        try:
            source_rows = _read_source(path)
        except Exception as exc:  # pragma: no cover - defensive parser guard
            skipped.append({"source": str(path), "reason": type(exc).__name__})
            continue
        for row in source_rows:
            rows.append({**row, "source": str(path)})
        loaded.append(str(path))
    return rows, loaded, missing, skipped


def _read_source(path: Path) -> list[dict[str, Any]]:
    if path.suffix.casefold() == ".csv":
        with path.open("r", newline="", encoding="utf-8") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    if path.suffix.casefold() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            for key in ("results", "rows", "candidates"):
                values = payload.get(key)
                if isinstance(values, list):
                    return [row for row in values if isinstance(row, dict)]
    return []


def _rows_from_persistent_events(events: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not isinstance(events, dict):
        return rows
    for collection in ("recent_decisions", "recent_shadow_events", "recent_research_lessons"):
        values = events.get(collection) if isinstance(events.get(collection), list) else []
        for row in values:
            if isinstance(row, dict):
                rows.append({**row, "source": f"persistent:{collection}"})
    return rows


def _load_persistent_events(
    persistent_events: dict[str, Any] | None,
    *,
    load_persistent: bool,
    store: MT5PersistentIntelligenceStore | Any | None,
) -> dict[str, Any]:
    if persistent_events is not None:
        return {"source": "injected", "recent_events": persistent_events, **_safety()}
    if not load_persistent:
        return {"source": "disabled", "recent_events": {}, **_safety()}
    try:
        active_store = store or MT5PersistentIntelligenceStore()
        recent = active_store.recent_events(limit=50) if hasattr(active_store, "recent_events") else {}
        return {
            "source": "persistent_intelligence",
            "recent_events": recent,
            "db_degraded": bool((recent or {}).get("db_degraded")),
            **_safety(),
        }
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return {"source": "persistent_intelligence", "recent_events": {}, "db_degraded": True, "reason": type(exc).__name__, **_safety()}


def _finalize_row(row: dict[str, Any]) -> dict[str, Any]:
    symbol = _symbol(_pick(row, "symbol"))
    timeframe = _timeframe(_pick(row, "timeframe"))
    family = str(_pick(row, "family") or "").strip()
    profile = str(_pick(row, "profile") or family or "").strip()
    source_identity_resolved = _source_identity_resolved(row, profile, family)
    total_closed = int(_number(_pick(row, "total_closed")) or 0)
    recent_closed = int(_number(_pick(row, "recent_closed")) or 0)
    recent_pf = float(_number(_pick(row, "recent_pf")) or 0.0)
    total_pf = float(_number(_pick(row, "total_pf")) or 0.0)
    expectancy = float(_number(_pick(row, "expectancy")) or 0.0)
    mc_pf = _optional_number(_pick(row, "monte_carlo_stressed_pf"))
    remove_best_5_pf = _optional_number(_pick(row, "remove_best_5_pf"))
    spread_x2_pf = _optional_number(_pick(row, "spread_x2_pf"))
    single_trade_dependency = _optional_bool(_pick(row, "single_trade_dependency"))
    fragile_regime_dependency = _optional_bool(_pick(row, "fragile_regime_dependency"))
    degraded = bool(forward_profile_degradation(symbol, timeframe, profile)) if symbol and timeframe and profile else False
    rejection = research_rejection(symbol, timeframe, profile, family) if symbol and timeframe and profile else {}
    sibling_risk = _optional_bool(row.get("sibling_risk")) is True
    reasons = _rejection_reasons(
        source_identity_resolved=source_identity_resolved,
        total_closed=total_closed,
        recent_closed=recent_closed,
        recent_pf=recent_pf,
        total_pf=total_pf,
        expectancy=expectancy,
        mc_pf=mc_pf,
        remove_best_5_pf=remove_best_5_pf,
        spread_x2_pf=spread_x2_pf,
        single_trade_dependency=single_trade_dependency,
        fragile_regime_dependency=fragile_regime_dependency,
        degraded=degraded,
        rejected=bool(rejection),
        sibling_risk=sibling_risk,
    )
    candidate_status = "robust_candidate_ready" if not reasons else "rejected"
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "profile": profile,
        "family": family,
        "source": row.get("source") or "",
        "source_identity_resolved": source_identity_resolved,
        "total_closed": total_closed,
        "recent_closed": recent_closed,
        "recent_pf": recent_pf,
        "total_pf": total_pf,
        "expectancy": expectancy,
        "monte_carlo_stressed_pf": mc_pf,
        "remove_best_5_pf": remove_best_5_pf,
        "spread_x2_pf": spread_x2_pf,
        "single_trade_dependency": single_trade_dependency,
        "fragile_regime_dependency": fragile_regime_dependency,
        "degraded_by_registry": degraded,
        "rejected_by_research_registry": bool(rejection),
        "research_rejection_reason": str(rejection.get("rejection_reason") or ""),
        "sibling_risk": sibling_risk,
        "rejection_reasons": reasons,
        "candidate_status": candidate_status,
        "recommended_next_action": "paper_observation_review" if candidate_status == "robust_candidate_ready" else "skip_or_continue_research",
        "robust_candidate_score": _score(
            total_closed=total_closed,
            recent_closed=recent_closed,
            recent_pf=recent_pf,
            total_pf=total_pf,
            expectancy=expectancy,
            mc_pf=mc_pf,
            remove_best_5_pf=remove_best_5_pf,
            spread_x2_pf=spread_x2_pf,
            rejection_count=len(reasons),
        ),
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _rejection_reasons(
    *,
    source_identity_resolved: bool,
    total_closed: int,
    recent_closed: int,
    recent_pf: float,
    total_pf: float,
    expectancy: float,
    mc_pf: float | None,
    remove_best_5_pf: float | None,
    spread_x2_pf: float | None,
    single_trade_dependency: bool | None,
    fragile_regime_dependency: bool | None,
    degraded: bool,
    rejected: bool,
    sibling_risk: bool,
) -> list[str]:
    reasons: list[str] = []
    if not source_identity_resolved:
        reasons.append("source_identity_unresolved")
    if total_closed < MIN_TOTAL_CLOSED:
        reasons.append("total_closed_below_50")
    if recent_closed < MIN_RECENT_CLOSED:
        reasons.append("recent_closed_below_20")
    if recent_pf < MIN_RECENT_PF:
        reasons.append("recent_pf_below_1_15")
    if total_pf < MIN_TOTAL_PF:
        reasons.append("total_pf_below_1_15")
    if expectancy <= 0:
        reasons.append("expectancy_not_positive")
    if mc_pf is not None and mc_pf < 1.05:
        reasons.append("monte_carlo_stressed_pf_below_1_05")
    if remove_best_5_pf is not None and remove_best_5_pf < 1.0:
        reasons.append("remove_best_5_pf_below_1")
    if spread_x2_pf is not None and spread_x2_pf < 0.95:
        reasons.append("spread_x2_pf_below_0_95")
    if single_trade_dependency is True:
        reasons.append("single_trade_dependency")
    if fragile_regime_dependency is True:
        reasons.append("fragile_regime_dependency")
    if degraded:
        reasons.append("degraded_by_registry")
    if rejected:
        reasons.append("rejected_by_research_registry")
    if sibling_risk:
        reasons.append("sibling_risk")
    return reasons


def _score(
    *,
    total_closed: int,
    recent_closed: int,
    recent_pf: float,
    total_pf: float,
    expectancy: float,
    mc_pf: float | None,
    remove_best_5_pf: float | None,
    spread_x2_pf: float | None,
    rejection_count: int,
) -> float:
    score = min(total_closed, 250) * 0.7 + min(recent_closed, 100) * 2.5
    score += max(0.0, recent_pf - 1.0) * 90.0
    score += max(0.0, total_pf - 1.0) * 80.0
    score += max(0.0, expectancy) * 180.0
    if mc_pf is not None:
        score += max(0.0, mc_pf - 1.0) * 160.0
    if remove_best_5_pf is not None:
        score += max(0.0, remove_best_5_pf - 1.0) * 120.0
    if spread_x2_pf is not None:
        score += max(0.0, spread_x2_pf - 0.95) * 60.0
    score -= rejection_count * 35.0
    return round(score, 6)


def _source_identity_resolved(row: dict[str, Any], profile: str, family: str) -> bool:
    value = _pick(row, "source_identity_resolved")
    if isinstance(value, bool):
        return value
    if str(value).strip().casefold() in {"true", "1", "yes", "y"}:
        return True
    if str(value).strip().casefold() in {"false", "0", "no", "n"}:
        return False
    status = str(row.get("source_identity_status") or "").casefold()
    if "unresolved" in status:
        return False
    blob = f"{profile} {family}".casefold().strip()
    return bool(blob) and "unknown_profile" not in blob and blob not in {"unknown", "none", "null"}


def _pick(row: dict[str, Any], key: str) -> Any:
    for alias in _ALIASES[key]:
        if alias in row and row.get(alias) not in {None, ""}:
            return row.get(alias)
    return None


def _requested_paths(paths: list[str | Path] | None) -> list[Path]:
    if paths:
        return _dedupe_paths([Path(path) for path in paths])
    return _dedupe_paths([Path(path) for path in DEFAULT_PROCESSED_SOURCES])


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    unique: list[Path] = []
    for path in paths:
        marker = str(path.absolute())
        if marker in seen:
            continue
        seen.add(marker)
        unique.append(path)
    return unique


def _ranking_key(row: dict[str, Any]) -> tuple[float, str, str, str]:
    return (-float(row.get("robust_candidate_score") or 0.0), row.get("symbol") or "", row.get("timeframe") or "", row.get("profile") or "")


def _rejected_key(row: dict[str, Any]) -> tuple[int, float, str]:
    return (len(row.get("rejection_reasons") or []), -float(row.get("robust_candidate_score") or 0.0), row.get("profile") or "")


def _symbol(value: object) -> str:
    text = str(value or "").upper().strip().replace(".B", "")
    if text in {"USTECB", "NAS100"}:
        return "USTEC"
    return text


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _optional_number(value: object) -> float | None:
    if value is None or value == "":
        return None
    return _number(value)


def _optional_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None or value == "":
        return None
    return str(value).strip().casefold() in {"1", "true", "yes", "y"}


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

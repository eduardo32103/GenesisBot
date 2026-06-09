from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from services.mt5.mt5_forward_profile_degradation_registry import (
    forward_profile_degradation,
    forward_profile_degradation_registry_status,
)


MIN_RECENT_CLOSED = 15
MIN_TOTAL_CLOSED = 45
MIN_TOTAL_PF = 1.15
MIN_RECENT_PF = 1.05
MIN_MONTE_CARLO_STRESSED_PF = 1.05
MIN_SPREAD_X2_PF = 0.95
MAX_RESULTS_FILE_BYTES = 2_000_000

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_RESULTS_DIR = _REPO_ROOT / "data" / "backtests" / "multisymbol"

_KNOWN_RESULT_FILENAMES = (
    "multi_symbol_recent_first_cost_calibrated_results.json",
    "multi_symbol_recent_first_cost_calibrated_results.csv",
    "multi_symbol_recent_first_results.json",
    "multi_symbol_recent_first_results.csv",
    "eth_m30_volatility_hardening_results.json",
    "eth_m30_volatility_hardening_results.csv",
    "eth_m30_capital_preservation_results.json",
    "eth_m30_capital_preservation_results.csv",
)

_PRIORITY_CANDIDATES: tuple[dict[str, Any], ...] = (
    {
        "symbol": "ETHUSD",
        "timeframe": "M30",
        "profile": "eth_m30_vol_breakout_chop_guard_v1",
        "family": "recent_volatility_breakout",
        "priority": 0,
        "source": "persistent_degradation_registry_check",
    },
    {
        "symbol": "ETHUSD",
        "timeframe": "M30",
        "profile": "eth_m30_vol_breakout_mae_guard_v1",
        "family": "recent_volatility_breakout",
        "priority": 1,
        "source": "rotation_priority_seed",
    },
    {
        "symbol": "ETHUSD",
        "timeframe": "M30",
        "profile": "eth_m30_vol_breakout_mc_hardened_v1",
        "family": "recent_volatility_breakout",
        "priority": 2,
        "source": "rotation_priority_seed",
    },
    {
        "symbol": "XAUUSD",
        "timeframe": "M15",
        "profile": "xauusd_m15_recent_session_open_continuation",
        "family": "recent_session_open_continuation",
        "priority": 3,
        "source": "rotation_priority_seed",
    },
    {
        "symbol": "US500",
        "timeframe": "H1",
        "profile": "us500_h1_recent_session_open_continuation",
        "family": "recent_session_open_continuation",
        "priority": 4,
        "source": "rotation_priority_seed",
    },
    {
        "symbol": "BTCUSD",
        "timeframe": "M30",
        "profile": "btcusd_m30_recent_liquidity_sweep",
        "family": "recent_liquidity_sweep",
        "priority": 5,
        "source": "rotation_priority_seed",
    },
)

_ALIASES = {
    "symbol": ("symbol", "Symbol", "requested_symbol", "resolved_symbol", "normalized_symbol"),
    "timeframe": ("timeframe", "Timeframe"),
    "profile": ("profile", "Profile", "strategy_profile", "target_name", "experimental_registry_record"),
    "family": ("family", "Family", "strategy_family"),
    "recent_closed": ("recent_closed", "recent_trades", "closed_recent", "closed_recent_holdout"),
    "total_closed": ("total_closed", "closed", "trades", "closed_total", "total_trades"),
    "recent_pf": ("recent_pf", "recent_profit_factor", "pf_recent_holdout"),
    "total_pf": ("total_pf", "profit_factor", "profit_factor_total"),
    "expectancy": ("expectancy", "total_expectancy", "expectancy_total", "recent_expectancy"),
    "monte_carlo_stressed_pf": ("monte_carlo_stressed_pf", "mc_pf", "stressed_pf"),
    "spread_x2_pf": ("spread_x2_pf", "spread_stress_pf"),
}


def run_paper_forward_candidate_rotation(
    *,
    rows: list[dict[str, Any]] | None = None,
    result_paths: list[str | Path] | None = None,
    search_root: str | Path | None = None,
    include_priority_candidates: bool = True,
    load_default_sources: bool = True,
) -> dict[str, Any]:
    discovered_rows, loaded_sources, missing_sources, skipped_sources = _load_existing_result_rows(
        result_paths or [],
        search_root=search_root,
        load_default_sources=load_default_sources,
    )
    source_rows = discovered_rows + list(rows or [])
    useful_rows = len(_merge_rows([_normalize_row(row) for row in source_rows if _is_useful_source_row(row)]))

    normalized_rows: list[dict[str, Any]] = []
    if include_priority_candidates:
        normalized_rows.extend(_normalize_row(row) for row in _PRIORITY_CANDIDATES)
    normalized_rows.extend(_normalize_row(row) for row in discovered_rows)
    normalized_rows.extend(_normalize_row(row) for row in rows or [])

    merged = _merge_rows(normalized_rows)
    ranking = [_evaluate_candidate(row) for row in merged]
    ranking.sort(key=_ranking_key)

    excluded = [row for row in ranking if row["degraded_by_registry"]]
    eligible = [row for row in ranking if not row["degraded_by_registry"]]
    review_ready = [row for row in eligible if row["candidate_status"] == "paper_forward_review_ready"]
    recommended = review_ready[0] if review_ready else None
    if useful_rows == 0:
        recommendation = "repair_data_sources"
        recommended = None
    else:
        recommendation = "paper_forward_candidate_review" if recommended else "continue_research"

    return {
        "ok": True,
        "status": "paper_forward_candidate_rotation_ready",
        "recommendation": recommendation,
        "recommended_candidate": recommended,
        "ranking": ranking,
        "eligible_candidates": eligible,
        "excluded_by_degradation_registry": excluded,
        "loaded_sources": loaded_sources,
        "missing_sources": missing_sources,
        "skipped_sources": skipped_sources,
        "useful_rows": useful_rows,
        "registry": forward_profile_degradation_registry_status(),
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        "automatic_promotion": False,
        "promoted_profile_mutated": False,
        "forward_state_mutated": False,
        "live_runtime_mutated": False,
        "shadow_trades_mutated": False,
        "martingale_enabled": False,
        "grid_enabled": False,
        "averaging_down_enabled": False,
        "increase_size_after_loss_enabled": False,
        **_safety(),
    }


def _evaluate_candidate(row: dict[str, Any]) -> dict[str, Any]:
    symbol = _symbol(row.get("symbol"))
    timeframe = _timeframe(row.get("timeframe"))
    profile = str(row.get("profile") or row.get("family") or "").strip()
    degraded = forward_profile_degradation(symbol, timeframe, profile)
    metrics = {
        "recent_closed": int(_number(row.get("recent_closed")) or 0),
        "total_closed": int(_number(row.get("total_closed")) or 0),
        "recent_pf": float(_number(row.get("recent_pf")) or 0.0),
        "total_pf": float(_number(row.get("total_pf")) or 0.0),
        "expectancy": float(_number(row.get("expectancy") or row.get("total_expectancy") or row.get("recent_expectancy")) or 0.0),
        "monte_carlo_stressed_pf": float(_number(row.get("monte_carlo_stressed_pf")) or 0.0),
        "spread_x2_pf": float(_number(row.get("spread_x2_pf")) or 0.0),
    }
    fragile = _flag(row.get("fragile_regime_dependency") or row.get("fragile_regime") or row.get("fragile"))
    reasons = _gate_reasons(metrics, fragile=fragile, degraded=bool(degraded))
    if degraded:
        candidate_status = "excluded_by_degradation_registry"
        next_action = "skip_degraded_profile"
    elif reasons:
        candidate_status = "gate_failed"
        next_action = "continue_research"
    else:
        candidate_status = "paper_forward_review_ready"
        next_action = "paper_forward_candidate_review"
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "profile": profile,
        "family": str(row.get("family") or _infer_family(profile) or "").strip(),
        **metrics,
        "fragile_regime_dependency": fragile,
        "degraded_by_registry": bool(degraded),
        "degradation_reason": degraded.get("degradation_reason") or "",
        "degradation_registry_version": degraded.get("registry_version") or "",
        "candidate_status": candidate_status,
        "recommended_next_action": next_action,
        "rejection_reasons": reasons,
        "rotation_score": _rotation_score(metrics, fragile=fragile, degraded=bool(degraded)),
        "source": str(row.get("source") or ""),
        "priority": int(_number(row.get("priority")) or 1000),
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }


def _gate_reasons(metrics: dict[str, Any], *, fragile: bool, degraded: bool) -> list[str]:
    reasons: list[str] = []
    if degraded:
        reasons.append("degraded_by_persistent_registry")
    if int(metrics["recent_closed"]) < MIN_RECENT_CLOSED:
        reasons.append("recent_closed_below_15")
    if int(metrics["total_closed"]) < MIN_TOTAL_CLOSED:
        reasons.append("total_closed_below_45")
    if float(metrics["total_pf"]) < MIN_TOTAL_PF:
        reasons.append("total_pf_below_1_15")
    if float(metrics["recent_pf"]) < MIN_RECENT_PF:
        reasons.append("recent_pf_below_1_05")
    if float(metrics["monte_carlo_stressed_pf"]) < MIN_MONTE_CARLO_STRESSED_PF:
        reasons.append("monte_carlo_stressed_pf_below_1_05")
    if float(metrics["spread_x2_pf"]) < MIN_SPREAD_X2_PF:
        reasons.append("spread_x2_pf_below_0_95")
    if float(metrics["expectancy"]) <= 0:
        reasons.append("expectancy_not_positive")
    if fragile:
        reasons.append("fragile_regime_dependency")
    return reasons


def _rotation_score(metrics: dict[str, Any], *, fragile: bool, degraded: bool) -> float:
    if degraded:
        return -10_000.0
    score = 0.0
    score += min(int(metrics["recent_closed"]), 80) * 3.0
    score += min(int(metrics["total_closed"]), 200) * 0.8
    score += float(metrics["recent_pf"]) * 35.0
    score += float(metrics["total_pf"]) * 40.0
    score += float(metrics["monte_carlo_stressed_pf"]) * 45.0
    score += float(metrics["spread_x2_pf"]) * 25.0
    score += float(metrics["expectancy"]) * 100.0
    if fragile:
        score -= 500.0
    return round(score, 4)


def _ranking_key(row: dict[str, Any]) -> tuple[int, float, int, str, str]:
    status_rank = 0 if row["candidate_status"] == "paper_forward_review_ready" else 1 if not row["degraded_by_registry"] else 2
    return (status_rank, -float(row["rotation_score"]), int(row.get("priority") or 1000), row["symbol"], row["profile"])


def _merge_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (_symbol(row.get("symbol")), _timeframe(row.get("timeframe")), str(row.get("profile") or row.get("family") or "").strip())
        if not key[0] or not key[1] or not key[2]:
            continue
        current = merged.get(key, {})
        merged[key] = _prefer_more_complete_row(current, row)
    return list(merged.values())


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    family = _get_alias(row, "family")
    hardening_mode = str(row.get("hardening_mode") or "").strip()
    profile = _get_alias(row, "profile") or row.get("config_id") or ""
    if not profile and family and hardening_mode:
        profile = f"{family}:{hardening_mode}"
    if not profile:
        profile = family or ""
    family = family or _infer_family(profile)
    symbol = _get_alias(row, "symbol")
    return {
        **row,
        "symbol": _symbol(symbol),
        "timeframe": _timeframe(_get_alias(row, "timeframe")),
        "profile": str(profile or "").strip(),
        "family": str(family or "").strip(),
        "recent_closed": int(_number(_get_alias(row, "recent_closed")) or 0),
        "total_closed": int(_number(_get_alias(row, "total_closed")) or 0),
        "recent_pf": float(_number(_get_alias(row, "recent_pf")) or 0.0),
        "total_pf": float(_number(_get_alias(row, "total_pf")) or 0.0),
        "expectancy": float(_number(_get_alias(row, "expectancy")) or 0.0),
        "monte_carlo_stressed_pf": float(_number(_get_alias(row, "monte_carlo_stressed_pf")) or 0.0),
        "spread_x2_pf": float(_number(_get_alias(row, "spread_x2_pf")) or 0.0),
    }


def _load_existing_result_rows(
    result_paths: list[str | Path],
    *,
    search_root: str | Path | None,
    load_default_sources: bool,
) -> tuple[list[dict[str, Any]], list[str], list[str], list[dict[str, str]]]:
    paths = [Path(path) for path in result_paths]
    if search_root:
        root = Path(search_root)
        paths.extend(root / name for name in _KNOWN_RESULT_FILENAMES)
    elif load_default_sources:
        paths.extend(_DEFAULT_RESULTS_DIR / name for name in _KNOWN_RESULT_FILENAMES)
    paths = _dedupe_paths(paths)

    rows: list[dict[str, Any]] = []
    loaded: list[str] = []
    missing: list[str] = []
    skipped: list[dict[str, str]] = []
    for path in paths:
        if not path.exists():
            missing.append(str(path))
            continue
        if not _is_processed_results_path(path):
            skipped.append({"path": str(path), "reason": "not_processed_results_file"})
            continue
        if path.stat().st_size > MAX_RESULTS_FILE_BYTES:
            skipped.append({"path": str(path), "reason": "result_file_too_large"})
            continue
        try:
            loaded_rows = _read_result_file(path)
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            skipped.append({"path": str(path), "reason": type(exc).__name__})
            continue
        for row in loaded_rows:
            row.setdefault("source", str(path))
        rows.extend(loaded_rows)
        loaded.append(str(path))
    return rows, loaded, missing, skipped


def _read_result_file(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            rows: list[dict[str, Any]] = []
            for key in ("results", "candidates", "passed", "top_3_for_paper_forward_review", "top_3_for_capital_optimizer", "rows", "ranking"):
                value = payload.get(key)
                if isinstance(value, list):
                    rows.extend(row for row in value if isinstance(row, dict))
            return rows
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _prefer_more_complete_row(current: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    if not current:
        return {k: v for k, v in candidate.items() if v not in (None, "")}
    current_score = _row_data_score(current)
    candidate_score = _row_data_score(candidate)
    if candidate_score >= current_score:
        return {**current, **{k: v for k, v in candidate.items() if v not in (None, "")}}
    return {**candidate, **{k: v for k, v in current.items() if v not in (None, "")}}


def _row_data_score(row: dict[str, Any]) -> float:
    score = 0.0
    for key in (
        "recent_closed",
        "total_closed",
        "recent_pf",
        "total_pf",
        "expectancy",
        "monte_carlo_stressed_pf",
        "spread_x2_pf",
    ):
        score += abs(_number(row.get(key)) or 0.0)
    return score


def _is_useful_source_row(row: dict[str, Any]) -> bool:
    normalized = _normalize_row(row)
    if not normalized.get("symbol") or not normalized.get("timeframe") or not normalized.get("profile"):
        return False
    return _row_data_score(normalized) > 0.0


def _get_alias(row: dict[str, Any], canonical: str) -> Any:
    for key in _ALIASES[canonical]:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return ""


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    unique: list[Path] = []
    for path in paths:
        marker = str(path.resolve()) if path.exists() else str(path.absolute())
        if marker in seen:
            continue
        seen.add(marker)
        unique.append(path)
    return unique


def _is_processed_results_path(path: Path) -> bool:
    if path.suffix.lower() not in {".csv", ".json"}:
        return False
    name = path.name.casefold()
    return "results" in name or "summary" in name


def _infer_family(profile: object) -> str:
    raw = str(profile or "")
    for family in ("recent_volatility_breakout", "recent_session_open_continuation", "recent_liquidity_sweep"):
        if family in raw:
            return family
    if "vol_breakout" in raw:
        return "recent_volatility_breakout"
    if "liquidity_sweep" in raw:
        return "recent_liquidity_sweep"
    return ""


def _symbol(value: object) -> str:
    return str(value or "").upper().strip().replace(".B", "")


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _number(value: object) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _flag(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").casefold().strip() in {"1", "true", "yes", "y", "fragile"}


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

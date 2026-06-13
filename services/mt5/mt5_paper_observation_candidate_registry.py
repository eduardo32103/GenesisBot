from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore


REGISTRY_VERSION = "2026-06-12.mt5_paper_observation_candidate_registry.v1"
DEFAULT_PAYLOAD_PATH = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "research_outputs"
    / "xau_m15_volatility_compression_observation_candidate.json"
)

_REQUIRED_TOP_LEVEL = (
    "candidate_profile",
    "symbol",
    "timeframe",
    "family",
    "mode",
    "validation_metrics",
    "gates",
    "recommendation",
    "paper_observation_ready",
    "requires_human_approval",
    "candidate_activated",
    "paper_forward_onboarding_started",
    "applies_to_real_trading",
    "broker_touched",
    "order_executed",
    "order_policy",
)

_RAW_ARTIFACT_KEYS = {
    "bars",
    "candles",
    "csv_rows",
    "ohlc",
    "ohlc_rows",
    "raw_ohlc",
    "raw_ohlc_rows",
    "raw_ticks",
    "raw_trades",
    "tick_rows",
    "ticks",
    "trade_list",
    "trades",
}


def run_paper_observation_candidate_registry(
    *,
    payload_path: str | Path | None = None,
    apply: bool = False,
    payload: dict[str, Any] | None = None,
    store: MT5PersistentIntelligenceStore | Any | None = None,
) -> dict[str, Any]:
    started = time.monotonic()
    path = Path(payload_path) if payload_path else DEFAULT_PAYLOAD_PATH
    load_result = _load_payload(path, payload=payload)
    candidate = load_result.get("payload") if isinstance(load_result.get("payload"), dict) else {}
    validation = validate_paper_observation_payload(candidate)
    prepared = _prepare_rows(candidate) if validation["payload_valid"] else _empty_prepared_rows()
    active_store = store or MT5PersistentIntelligenceStore()
    write_results: dict[str, Any] = {}
    healthcheck = {"attempted": False, "ok": False, "reason": "dry_run_no_healthcheck"}
    applied = False
    rows_written = 0

    if apply and validation["payload_valid"]:
        healthcheck = _safe_healthcheck(active_store)
        if _health_ready(healthcheck):
            write_results = _write_rows(active_store, prepared)
            rows_written = sum(1 for item in write_results.values() if isinstance(item, dict) and item.get("ok"))
            applied = rows_written == len(prepared["rows_to_write"])
        else:
            write_results = {
                "skipped": {
                    "ok": False,
                    "reason": "persistent_intelligence_not_ready",
                    "healthcheck": _compact_healthcheck(healthcheck),
                    **_safety(),
                }
            }

    status = "paper_observation_candidate_registry_valid" if validation["payload_valid"] else "paper_observation_candidate_registry_invalid"
    if apply and validation["payload_valid"]:
        status = "paper_observation_candidate_registry_applied" if applied else "paper_observation_candidate_registry_apply_skipped_or_failed"
    result = {
        "ok": bool(validation["payload_valid"] and (not apply or applied)),
        "status": status,
        "registry_version": REGISTRY_VERSION,
        "payload_path": str(path),
        "payload_valid": bool(validation["payload_valid"]),
        "validation_errors": validation["validation_errors"],
        "candidate_profile": str(candidate.get("candidate_profile") or ""),
        "symbol": _symbol(candidate.get("symbol")),
        "timeframe": _timeframe(candidate.get("timeframe")),
        "dry_run": not bool(apply),
        "applied": bool(applied),
        "rows_to_write": prepared["rows_to_write"],
        "rows_written": rows_written,
        "write_results": write_results,
        "healthcheck": _compact_healthcheck(healthcheck),
        "research_lesson_prepared": bool(prepared["research_lesson_prepared"]),
        "profile_state_prepared": bool(prepared["profile_state_prepared"]),
        "strategy_registry_prepared": bool(prepared["strategy_registry_prepared"]),
        "candidate_rotation_review_prepared": bool(prepared["candidate_rotation_review_prepared"]),
        "prepared_rows": prepared["prepared_rows"],
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        "duration_ms": int((time.monotonic() - started) * 1000),
        **_safety(),
    }
    if load_result.get("error"):
        result["validation_errors"] = [*result["validation_errors"], str(load_result.get("error"))]
        result["payload_valid"] = False
        result["ok"] = False
    return result


def validate_paper_observation_payload(payload: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    if not isinstance(payload, dict) or not payload:
        errors.append("payload_missing_or_not_object")
        return {"payload_valid": False, "validation_errors": errors, **_safety()}
    for key in _REQUIRED_TOP_LEVEL:
        if key not in payload:
            errors.append(f"missing_{key}")
    if _symbol(payload.get("symbol")) != "XAUUSD":
        errors.append("symbol_must_be_xauusd")
    if _timeframe(payload.get("timeframe")) != "M15":
        errors.append("timeframe_must_be_m15")
    if str(payload.get("candidate_profile") or "").strip() == "":
        errors.append("candidate_profile_missing")
    if bool(payload.get("candidate_activated")):
        errors.append("candidate_activated_must_be_false")
    if bool(payload.get("paper_forward_onboarding_started")):
        errors.append("paper_forward_onboarding_started_must_be_false")
    if bool(payload.get("applies_to_real_trading")):
        errors.append("applies_to_real_trading_must_be_false")
    if bool(payload.get("broker_touched")):
        errors.append("broker_touched_must_be_false")
    if bool(payload.get("order_executed")):
        errors.append("order_executed_must_be_false")
    if str(payload.get("order_policy") or "") != "journal_only_no_broker":
        errors.append("order_policy_must_be_journal_only_no_broker")
    if not bool(payload.get("paper_observation_ready")):
        errors.append("paper_observation_ready_must_be_true")
    if not bool(payload.get("requires_human_approval")):
        errors.append("requires_human_approval_must_be_true")
    if str(payload.get("recommendation") or "") != "paper_observation_review":
        errors.append("recommendation_must_be_paper_observation_review")
    gates = payload.get("gates")
    if not isinstance(gates, dict) or not gates:
        errors.append("gates_missing")
    elif any(not _gate_passed(value) for value in gates.values()):
        errors.append("all_gates_must_pass")
    metrics = payload.get("validation_metrics")
    if not isinstance(metrics, dict) or not metrics:
        errors.append("validation_metrics_missing")
    elif not bool(metrics.get("source_identity_resolved")):
        errors.append("source_identity_must_be_resolved")
    raw_hits = sorted(_raw_artifact_hits(payload))
    if raw_hits:
        errors.append(f"raw_artifacts_not_allowed:{','.join(raw_hits)}")
    return {"payload_valid": not errors, "validation_errors": errors, **_safety()}


def _prepare_rows(payload: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    symbol = _symbol(payload.get("symbol"))
    timeframe = _timeframe(payload.get("timeframe"))
    profile = str(payload.get("candidate_profile") or "").strip()
    family = str(payload.get("family") or "").strip()
    metrics = dict(payload.get("validation_metrics") or {})
    compact_candidate = {
        "symbol": symbol,
        "broker_symbol": str(payload.get("broker_symbol") or ""),
        "timeframe": timeframe,
        "family": family,
        "mode": payload.get("mode"),
        "profile": profile,
        "source_csv_basename": payload.get("source_csv_basename"),
        "status": "paper_observation_review",
        "recommendation": "paper_observation_review",
        "paper_observation_ready": True,
        "requires_human_approval": True,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        "validation_metrics": _compact_metrics(metrics),
        **_safety(),
    }
    strategy_registry = {
        "symbol": symbol,
        "timeframe": timeframe,
        "profile": profile,
        "family": family,
        "status": "paper_observation_review",
        "source": "paper_observation_candidate_registry",
        "created_at": now,
        "updated_at": now,
    }
    profile_state = {
        "symbol": symbol,
        "timeframe": timeframe,
        "profile": profile,
        "status": "paper_observation_review",
        "active": False,
        "applies_to_paper_shadow": False,
        "applies_to_real_trading": False,
        "registry_source": "paper_observation_candidate_registry",
        "updated_at": now,
    }
    research_lesson = {
        "timestamp": now,
        "family": family,
        "symbol": symbol,
        "timeframe": timeframe,
        "lesson_type": "deep_validation_passed",
        "failure_pattern": "",
        "summary": _lesson_summary(payload, metrics),
        "avoid_next": [],
        "recommended_next_research_phase": "paper_observation_review",
    }
    candidate_rotation = {
        "run_id": "paper-observation-review-xauusd-m15-volatility-compression-nr7-trailing-defensive",
        "timestamp": now,
        "recommendation": "paper_observation_review",
        "recommended_candidate": compact_candidate,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        **_safety(),
    }
    rows = [
        {"table": "mt5_strategy_registry", "operation": "upsert", "payload": strategy_registry},
        {"table": "mt5_profile_state", "operation": "upsert", "payload": profile_state},
        {"table": "mt5_research_lessons", "operation": "insert", "payload": research_lesson},
        {"table": "mt5_candidate_rotation_runs", "operation": "upsert", "payload": candidate_rotation},
    ]
    return {
        "rows_to_write": [
            {"table": row["table"], "operation": row["operation"]}
            for row in rows
        ],
        "prepared_rows": rows,
        "strategy_registry_prepared": True,
        "profile_state_prepared": True,
        "research_lesson_prepared": True,
        "candidate_rotation_review_prepared": True,
    }


def _empty_prepared_rows() -> dict[str, Any]:
    return {
        "rows_to_write": [],
        "prepared_rows": [],
        "strategy_registry_prepared": False,
        "profile_state_prepared": False,
        "research_lesson_prepared": False,
        "candidate_rotation_review_prepared": False,
    }


def _write_rows(store: Any, prepared: dict[str, Any]) -> dict[str, Any]:
    results: dict[str, Any] = {}
    for row in prepared.get("prepared_rows") or []:
        table = row.get("table")
        payload = row.get("payload") or {}
        try:
            if table == "mt5_strategy_registry":
                result = store.upsert_strategy_registry(payload, critical=False)
            elif table == "mt5_profile_state":
                result = store.upsert_profile_state(payload, critical=False)
            elif table == "mt5_research_lessons":
                result = store.record_research_lesson(payload, critical=False)
            elif table == "mt5_candidate_rotation_runs":
                result = store.record_candidate_rotation_run(payload, critical=False)
            else:
                result = {"ok": False, "reason": "unsupported_table", **_safety()}
        except Exception as exc:  # pragma: no cover - defensive runtime guard
            result = {"ok": False, "db_degraded": True, "reason": type(exc).__name__, **_safety()}
        results[str(table)] = dict(result or {})
    return results


def _load_payload(path: Path, *, payload: dict[str, Any] | None) -> dict[str, Any]:
    if payload is not None:
        return {"payload": payload}
    try:
        return {"payload": json.loads(path.read_text(encoding="utf-8"))}
    except FileNotFoundError:
        return {"payload": {}, "error": "payload_file_missing"}
    except json.JSONDecodeError:
        return {"payload": {}, "error": "payload_json_invalid"}


def _safe_healthcheck(store: Any) -> dict[str, Any]:
    try:
        health = store.healthcheck(write_test_event=False)
        return {"attempted": True, **dict(health or {})}
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return {"attempted": True, "ok": False, "db_degraded": True, "reason": type(exc).__name__, **_safety()}


def _health_ready(healthcheck: dict[str, Any]) -> bool:
    return bool(
        healthcheck.get("db_available")
        and healthcheck.get("tables_ready")
        and not healthcheck.get("db_degraded")
    )


def _compact_healthcheck(healthcheck: dict[str, Any]) -> dict[str, Any]:
    return {
        "attempted": bool(healthcheck.get("attempted")),
        "provider": healthcheck.get("provider") or "",
        "db_available": bool(healthcheck.get("db_available")),
        "tables_ready": bool(healthcheck.get("tables_ready")),
        "db_degraded": bool(healthcheck.get("db_degraded")),
        "recommendation": healthcheck.get("recommendation") or healthcheck.get("reason") or "",
        **_safety(),
    }


def _lesson_summary(payload: dict[str, Any], metrics: dict[str, Any]) -> str:
    summary = {
        "candidate_profile": payload.get("candidate_profile"),
        "source_csv_basename": payload.get("source_csv_basename"),
        "total_closed": metrics.get("total_closed"),
        "recent_closed": metrics.get("recent_closed"),
        "total_pf": metrics.get("total_pf"),
        "recent_pf": metrics.get("recent_pf"),
        "monte_carlo_stressed_pf": metrics.get("monte_carlo_stressed_pf"),
        "spread_x2_pf": metrics.get("spread_x2_pf"),
        "remove_best_5_pf": metrics.get("remove_best_5_pf"),
        "paper_observation_ready": True,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
    }
    return json.dumps(summary, ensure_ascii=True, sort_keys=True)


def _compact_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "total_closed",
        "recent_closed",
        "win_rate",
        "recent_win_rate",
        "total_pf",
        "recent_pf",
        "expectancy",
        "recent_expectancy",
        "max_drawdown",
        "consecutive_losses",
        "monte_carlo_stressed_pf",
        "monte_carlo_stressed_expectancy",
        "monte_carlo_p95_drawdown",
        "spread_x2_pf",
        "remove_best_5_pf",
        "single_trade_dependency",
        "fragile_regime_dependency",
        "sample_stability_score",
        "cost_model_confidence",
        "source_identity_resolved",
    }
    return {key: metrics.get(key) for key in sorted(allowed) if key in metrics}


def _raw_artifact_hits(value: Any) -> set[str]:
    hits: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            clean_key = str(key or "").casefold()
            if clean_key in _RAW_ARTIFACT_KEYS:
                hits.add(clean_key)
            hits.update(_raw_artifact_hits(item))
    elif isinstance(value, list):
        for item in value:
            hits.update(_raw_artifact_hits(item))
    return hits


def _gate_passed(value: Any) -> bool:
    if isinstance(value, dict):
        return bool(value.get("passed"))
    return bool(value)


def _symbol(value: object) -> str:
    symbol = str(value or "").upper().strip().replace(".B", "")
    if symbol == "XAUUSDB":
        return "XAUUSD"
    return symbol


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

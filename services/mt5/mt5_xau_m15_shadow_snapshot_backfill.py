from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore
from services.mt5.mt5_xau_m15_paper_observation_readiness import BROKER_SYMBOL, CANDIDATE_PROFILE, SYMBOL, TIMEFRAME


DEFAULT_SNAPSHOT_PATH = Path("data/research_outputs/xau_m15_open_shadow_snapshot.json")
BACKFILL_VERSION = "2026-06-14.mt5_xau_m15_shadow_snapshot_backfill.v1"


def run_xau_m15_shadow_snapshot_backfill(
    *,
    snapshot_path: str | Path = DEFAULT_SNAPSHOT_PATH,
    apply: bool = False,
    store: MT5PersistentIntelligenceStore | Any | None = None,
) -> dict[str, Any]:
    path = Path(snapshot_path)
    payload_result = _load_snapshot(path)
    if not payload_result.get("ok"):
        return _result(
            payload_valid=False,
            dry_run=not apply,
            applied=False,
            status="xau_m15_shadow_snapshot_backfill_invalid",
            reason=str(payload_result.get("reason") or "snapshot_load_failed"),
            validation_errors=[str(payload_result.get("reason") or "snapshot_load_failed")],
        )

    snapshot = payload_result["payload"]
    validation = validate_xau_m15_shadow_snapshot(snapshot)
    if not validation["payload_valid"]:
        return _result(
            payload_valid=False,
            dry_run=not apply,
            applied=False,
            status="xau_m15_shadow_snapshot_backfill_invalid",
            reason="snapshot_validation_failed",
            validation_errors=validation["validation_errors"],
            shadow_trade=validation.get("shadow_trade") or {},
        )

    shadow = validation["shadow_trade"]
    active_store = store or MT5PersistentIntelligenceStore()
    existing = _existing_open_shadows(active_store)
    same_existing = [row for row in existing if str(row.get("shadow_trade_id") or "") == str(shadow.get("shadow_trade_id") or "")]
    different_existing = [row for row in existing if str(row.get("shadow_trade_id") or "") != str(shadow.get("shadow_trade_id") or "")]
    if different_existing:
        return _result(
            payload_valid=True,
            dry_run=not apply,
            applied=False,
            status="xau_m15_shadow_snapshot_backfill_blocked",
            reason="blocked_multiple_open_shadows",
            shadow_trade=shadow,
            existing_shadow_found=bool(existing),
            existing_open_shadow_count=len(existing),
            duplicate_prevented=True,
        )
    if same_existing:
        return _result(
            payload_valid=True,
            dry_run=not apply,
            applied=False,
            status="xau_m15_shadow_snapshot_backfill_duplicate_prevented",
            reason="shadow_already_persisted",
            shadow_trade=shadow,
            existing_shadow_found=True,
            existing_open_shadow_count=len(existing),
            duplicate_prevented=True,
        )
    if not apply:
        return _result(
            payload_valid=True,
            dry_run=True,
            applied=False,
            status="xau_m15_shadow_snapshot_backfill_dry_run_ready",
            reason="dry_run_no_write",
            shadow_trade=shadow,
            existing_shadow_found=False,
            existing_open_shadow_count=0,
            duplicate_prevented=False,
        )

    write_result = _write_shadow(active_store, shadow)
    rows_written = 1 if write_result.get("ok") else 0
    return _result(
        payload_valid=True,
        dry_run=False,
        applied=bool(write_result.get("ok")),
        status="xau_m15_shadow_snapshot_backfill_applied" if write_result.get("ok") else "xau_m15_shadow_snapshot_backfill_write_failed",
        reason="backfill_applied" if write_result.get("ok") else str(write_result.get("reason") or write_result.get("status") or "write_failed"),
        shadow_trade=shadow,
        existing_shadow_found=False,
        existing_open_shadow_count=0,
        rows_written=rows_written,
        write_result=write_result,
    )


def validate_xau_m15_shadow_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    shadow = _extract_shadow(payload)
    open_count = _open_count(payload, shadow)
    if open_count != 1:
        errors.append("open_count_must_equal_1")
    if not shadow:
        errors.append("shadow_trade_missing")
        return {"payload_valid": False, "validation_errors": errors, "shadow_trade": {}, **_safety()}
    clean = _normalize_shadow(shadow)
    if not clean.get("shadow_trade_id"):
        errors.append("shadow_trade_id_required")
    if _symbol(clean.get("symbol")) != SYMBOL and _symbol(clean.get("broker_symbol")) != SYMBOL:
        errors.append("symbol_must_be_xauusd")
    if _timeframe(clean.get("timeframe")) != TIMEFRAME:
        errors.append("timeframe_must_be_m15")
    if str(clean.get("status") or "").casefold() != "open":
        errors.append("status_must_be_open")
    if bool(shadow.get("broker_touched")):
        errors.append("broker_touched_must_be_false")
    if bool(shadow.get("order_executed")):
        errors.append("order_executed_must_be_false")
    if str(shadow.get("order_policy") or "journal_only_no_broker") != "journal_only_no_broker":
        errors.append("order_policy_must_be_journal_only_no_broker")
    if bool(clean.get("applies_to_real_trading")):
        errors.append("applies_to_real_trading_must_be_false")
    return {"payload_valid": not errors, "validation_errors": errors, "shadow_trade": clean, **_safety()}


def _load_snapshot(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError:
        return {"ok": False, "reason": "snapshot_file_missing", **_safety()}
    except json.JSONDecodeError:
        return {"ok": False, "reason": "snapshot_json_invalid", **_safety()}
    if not isinstance(payload, dict):
        return {"ok": False, "reason": "snapshot_payload_must_be_object", **_safety()}
    return {"ok": True, "payload": payload, **_safety()}


def _extract_shadow(payload: dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload.get("open_shadow_trade"), dict):
        return dict(payload["open_shadow_trade"])
    if isinstance(payload.get("shadow_trade"), dict):
        return dict(payload["shadow_trade"])
    trades = payload.get("trades") if isinstance(payload.get("trades"), list) else []
    dict_trades = [dict(row) for row in trades if isinstance(row, dict)]
    if len(dict_trades) == 1:
        return dict_trades[0]
    if _looks_like_shadow(payload):
        return dict(payload)
    return {}


def _open_count(payload: dict[str, Any], shadow: dict[str, Any]) -> int:
    raw = _int(payload.get("open_count"))
    if raw is not None:
        return raw
    trades = payload.get("trades") if isinstance(payload.get("trades"), list) else []
    if trades:
        return len([row for row in trades if isinstance(row, dict)])
    return 1 if shadow else 0


def _normalize_shadow(shadow: dict[str, Any]) -> dict[str, Any]:
    profile = str(shadow.get("strategy_profile") or shadow.get("candidate_profile") or shadow.get("profile") or CANDIDATE_PROFILE)
    symbol = _symbol(shadow.get("symbol") or shadow.get("broker_symbol"))
    broker_symbol = str(shadow.get("broker_symbol") or (BROKER_SYMBOL if symbol == SYMBOL else "")).strip() or BROKER_SYMBOL
    return {
        "shadow_trade_id": shadow.get("shadow_trade_id") or shadow.get("trade_id") or "",
        "symbol": SYMBOL if symbol == SYMBOL else symbol,
        "broker_symbol": broker_symbol,
        "timeframe": _timeframe(shadow.get("timeframe")),
        "profile": profile,
        "strategy_profile": profile,
        "source": shadow.get("source") or "paper_observation_shadow_once",
        "side": str(shadow.get("side") or shadow.get("action") or "").lower(),
        "entry_price": _number(shadow.get("entry_price") or shadow.get("entry")),
        "stop_loss": _number(shadow.get("stop_loss") or shadow.get("virtual_stop_loss")),
        "take_profit": _number(shadow.get("take_profit")),
        "status": str(shadow.get("status") or shadow.get("lifecycle_status") or "open").lower(),
        "opened_at": shadow.get("opened_at") or shadow.get("created_at") or "",
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "applies_to_real_trading": bool(shadow.get("applies_to_real_trading")),
    }


def _existing_open_shadows(store: Any) -> list[dict[str, Any]]:
    if not hasattr(store, "_safe_select"):
        return []
    try:
        result = store._safe_select(
            "mt5_shadow_trades",
            params={
                "select": "shadow_trade_id,symbol,timeframe,status",
                "symbol": f"eq.{SYMBOL}",
                "timeframe": f"eq.{TIMEFRAME}",
                "status": "eq.open",
                "limit": "10",
            },
        )
    except Exception:
        return []
    rows = result.get("rows") if isinstance(result, dict) else []
    return [
        dict(row)
        for row in rows or []
        if isinstance(row, dict)
        and _symbol(row.get("symbol")) == SYMBOL
        and _timeframe(row.get("timeframe")) == TIMEFRAME
        and str(row.get("status") or "").casefold() == "open"
    ]


def _write_shadow(store: Any, shadow: dict[str, Any]) -> dict[str, Any]:
    if not hasattr(store, "record_shadow_trade"):
        return {"ok": False, "reason": "store_missing_record_shadow_trade", **_safety()}
    try:
        result = store.record_shadow_trade(shadow, critical=False)
    except Exception as exc:
        return {"ok": False, "reason": type(exc).__name__, **_safety()}
    return dict(result or {"ok": False, "reason": "empty_record_shadow_trade_result", **_safety()})


def _result(
    *,
    payload_valid: bool,
    dry_run: bool,
    applied: bool,
    status: str,
    reason: str,
    validation_errors: list[str] | None = None,
    shadow_trade: dict[str, Any] | None = None,
    existing_shadow_found: bool = False,
    existing_open_shadow_count: int = 0,
    duplicate_prevented: bool = False,
    rows_written: int = 0,
    write_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    shadow = shadow_trade or {}
    return {
        "ok": True,
        "status": status,
        "backfill_version": BACKFILL_VERSION,
        "reason": reason,
        "payload_valid": bool(payload_valid),
        "validation_errors": validation_errors or [],
        "dry_run": bool(dry_run),
        "applied": bool(applied),
        "shadow_trade_id": shadow.get("shadow_trade_id") or "",
        "symbol": shadow.get("symbol") or SYMBOL,
        "broker_symbol": shadow.get("broker_symbol") or BROKER_SYMBOL,
        "timeframe": shadow.get("timeframe") or TIMEFRAME,
        "status_after": shadow.get("status") or "",
        "source": shadow.get("source") or "paper_observation_shadow_once",
        "strategy_profile": shadow.get("strategy_profile") or CANDIDATE_PROFILE,
        "existing_shadow_found": bool(existing_shadow_found),
        "existing_open_shadow_count": int(existing_open_shadow_count),
        "rows_written": int(rows_written),
        "shadow_source": "persistent_intelligence_backfill",
        "duplicate_prevented": bool(duplicate_prevented),
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        "write_result": write_result or {"ok": True, "skipped": True},
        **_safety(),
    }


def _looks_like_shadow(payload: dict[str, Any]) -> bool:
    return bool(payload.get("shadow_trade_id") or payload.get("trade_id")) and bool(payload.get("symbol") or payload.get("broker_symbol"))


def _symbol(value: object) -> str:
    symbol = str(value or "").upper().strip().replace(".B", "")
    if symbol == "XAUUSDB":
        return "XAUUSD"
    return symbol


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _int(value: object) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


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

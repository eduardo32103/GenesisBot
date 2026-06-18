from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore
from services.mt5.mt5_xau_m15_paper_observation_readiness import BROKER_SYMBOL, CANDIDATE_PROFILE, SYMBOL, TIMEFRAME


BACKFILL_VERSION = "2026-06-18.xau_m15_runtime_open_shadow_backfill.v1"
ALLOWED_OPEN_SOURCES = {"runtime_memory", "merged"}


def run_xau_m15_runtime_open_shadow_backfill(
    *,
    snapshot: dict[str, Any] | None = None,
    snapshot_file: str | Path | None = None,
    confirm_paper_only_backfill: bool = False,
    store: Any | None = None,
) -> dict[str, Any]:
    if not confirm_paper_only_backfill:
        return _result(
            "xau_m15_runtime_open_shadow_backfill_confirmation_required",
            payload_valid=False,
            reason="confirm_paper_only_backfill_required",
        )
    payload = _load_snapshot(snapshot=snapshot, snapshot_file=snapshot_file)
    validation = _validate_snapshot(payload)
    if not validation["payload_valid"]:
        return _result(
            "xau_m15_runtime_open_shadow_backfill_rejected",
            payload_valid=False,
            reason=validation["reason"],
            snapshot_summary=_snapshot_summary(payload),
        )
    trade = validation["trade"]
    active_store = store or MT5PersistentIntelligenceStore()
    existing = _safe_open_trades(active_store)
    existing_rows = [row for row in existing.get("open_trades", []) if isinstance(row, dict) and _is_xau_m15(row)]
    same_id = [row for row in existing_rows if str(row.get("shadow_trade_id") or "") == trade["shadow_trade_id"]]
    different = [row for row in existing_rows if str(row.get("shadow_trade_id") or "") != trade["shadow_trade_id"]]
    if different:
        return _result(
            "blocked_duplicate_open_shadow",
            payload_valid=True,
            persistent_open_ready=False,
            existing_shadow_found=True,
            duplicate_prevented=True,
            shadow_trade_id=trade["shadow_trade_id"],
            rows_written=0,
            reason="different_persistent_open_shadow_exists",
            existing_open_count=len(existing_rows),
        )
    if same_id:
        return _result(
            "xau_m15_runtime_open_shadow_backfill_already_present",
            payload_valid=True,
            persistent_open_ready=True,
            existing_shadow_found=True,
            duplicate_prevented=True,
            shadow_trade_id=trade["shadow_trade_id"],
            rows_written=0,
            rows_updated=0,
            reason="same_shadow_already_persisted_open",
            existing_open_count=len(existing_rows),
        )
    write = _safe_record_shadow_trade(active_store, trade)
    ok = bool(write.get("ok"))
    return _result(
        "xau_m15_runtime_open_shadow_backfill_applied" if ok else "xau_m15_runtime_open_shadow_backfill_failed",
        payload_valid=True,
        persistent_open_ready=ok,
        existing_shadow_found=False,
        duplicate_prevented=False,
        shadow_trade_id=trade["shadow_trade_id"],
        rows_written=1 if ok else 0,
        rows_updated=0,
        reason="" if ok else str(write.get("reason") or "persistent_write_failed"),
        persistent_shadow_write=write,
    )


def _load_snapshot(*, snapshot: dict[str, Any] | None, snapshot_file: str | Path | None) -> dict[str, Any]:
    if isinstance(snapshot, dict):
        return dict(snapshot)
    if snapshot_file:
        try:
            text = Path(snapshot_file).read_text(encoding="utf-8-sig")
            return json.loads(text)
        except Exception as exc:
            return {"_load_error": type(exc).__name__}
    return {}


def _validate_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(snapshot, dict) or snapshot.get("_load_error"):
        return {"payload_valid": False, "reason": snapshot.get("_load_error") or "invalid_snapshot_payload", "trade": {}}
    if bool(snapshot.get("broker_touched")) or bool(snapshot.get("order_executed")) or str(snapshot.get("order_policy") or "") != "journal_only_no_broker":
        return {"payload_valid": False, "reason": "snapshot_safety_flags_invalid", "trade": {}}
    open_count = int(_num(snapshot.get("open_count") or snapshot.get("merged_open_count")) or 0)
    if open_count != 1:
        return {"payload_valid": False, "reason": "snapshot_open_count_not_one", "trade": {}}
    open_source = str(snapshot.get("open_source") or "").strip()
    if open_source not in ALLOWED_OPEN_SOURCES:
        return {"payload_valid": False, "reason": "snapshot_open_source_not_runtime_or_merged", "trade": {}}
    rows = snapshot.get("trades") if isinstance(snapshot.get("trades"), list) else snapshot.get("open_trades")
    trade = dict(rows[0]) if isinstance(rows, list) and rows and isinstance(rows[0], dict) else {}
    if not trade:
        return {"payload_valid": False, "reason": "snapshot_missing_open_trade", "trade": {}}
    if bool(trade.get("broker_touched")) or bool(trade.get("order_executed")) or str(trade.get("order_policy") or "") != "journal_only_no_broker":
        return {"payload_valid": False, "reason": "trade_safety_flags_invalid", "trade": {}}
    normalized = _normalize_trade(trade)
    if not normalized.get("shadow_trade_id"):
        return {"payload_valid": False, "reason": "shadow_trade_id_missing", "trade": {}}
    if normalized["symbol"] != SYMBOL or normalized["timeframe"] != TIMEFRAME:
        return {"payload_valid": False, "reason": "symbol_or_timeframe_mismatch", "trade": {}}
    if str(normalized.get("status") or "") != "open":
        return {"payload_valid": False, "reason": "shadow_status_not_open", "trade": {}}
    return {"payload_valid": True, "reason": "", "trade": normalized}


def _normalize_trade(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "shadow_trade_id": str(row.get("shadow_trade_id") or "").strip(),
        "symbol": _symbol(row.get("symbol") or row.get("broker_symbol")),
        "broker_symbol": str(row.get("broker_symbol") or BROKER_SYMBOL).upper().strip(),
        "timeframe": str(row.get("timeframe") or "").upper().strip(),
        "side": str(row.get("side") or "").lower().strip(),
        "entry_price": _num(row.get("entry_price")),
        "last_price": _num(row.get("last_price") or row.get("entry_price")),
        "opened_at": row.get("opened_at") or "",
        "stop_loss": _num(row.get("stop_loss")),
        "take_profit": _num(row.get("take_profit")),
        "status": "open",
        "source": row.get("source") or "paper_observation_shadow_once",
        "strategy_profile": row.get("strategy_profile") or row.get("profile") or CANDIDATE_PROFILE,
        "profile": row.get("profile") or row.get("strategy_profile") or CANDIDATE_PROFILE,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
        "applies_to_real_trading": False,
    }


def _safe_open_trades(store: Any) -> dict[str, Any]:
    if not hasattr(store, "open_shadow_trades"):
        return {"ok": True, "open_trades": [], **_safety()}
    try:
        result = store.open_shadow_trades(limit=50)
    except TypeError:
        result = store.open_shadow_trades()
    except Exception as exc:
        return {"ok": False, "open_trades": [], "reason": type(exc).__name__, **_safety()}
    payload = dict(result or {"ok": False, "open_trades": [], "reason": "empty_open_shadow_trades", **_safety()})
    rows = payload.get("open_trades")
    if not isinstance(rows, list):
        rows = payload.get("trades")
    payload["open_trades"] = rows if isinstance(rows, list) else []
    return payload


def _safe_record_shadow_trade(store: Any, trade: dict[str, Any]) -> dict[str, Any]:
    if not hasattr(store, "record_shadow_trade"):
        return {"ok": False, "reason": "store_missing_record_shadow_trade", **_safety()}
    try:
        return dict(store.record_shadow_trade(trade, critical=True) or {"ok": False, "reason": "empty_record_shadow_trade", **_safety()})
    except Exception as exc:
        return {"ok": False, "reason": type(exc).__name__, **_safety()}


def _is_xau_m15(row: dict[str, Any]) -> bool:
    return _symbol(row.get("symbol") or row.get("broker_symbol")) == SYMBOL and str(row.get("timeframe") or "").upper().strip() == TIMEFRAME


def _symbol(value: object) -> str:
    text = str(value or "").upper().strip()
    if text.endswith(".B"):
        text = text[:-2]
    return text


def _num(value: object) -> float:
    try:
        if value in (None, ""):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _snapshot_summary(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "open_count": int(_num(snapshot.get("open_count") or snapshot.get("merged_open_count")) or 0) if isinstance(snapshot, dict) else 0,
        "open_source": snapshot.get("open_source") if isinstance(snapshot, dict) else "",
        "broker_touched": bool(snapshot.get("broker_touched")) if isinstance(snapshot, dict) else False,
        "order_executed": bool(snapshot.get("order_executed")) if isinstance(snapshot, dict) else False,
        "order_policy": snapshot.get("order_policy") if isinstance(snapshot, dict) else "",
    }


def _result(status: str, **fields: Any) -> dict[str, Any]:
    return {
        "ok": status
        not in {
            "xau_m15_runtime_open_shadow_backfill_confirmation_required",
            "xau_m15_runtime_open_shadow_backfill_failed",
            "xau_m15_runtime_open_shadow_backfill_rejected",
            "blocked_duplicate_open_shadow",
        },
        "status": status,
        "backfill_version": BACKFILL_VERSION,
        "symbol": SYMBOL,
        "broker_symbol": BROKER_SYMBOL,
        "timeframe": TIMEFRAME,
        "shadow_source": "persistent_intelligence_backfill",
        "dry_run": False,
        "applied": status == "xau_m15_runtime_open_shadow_backfill_applied",
        "payload_valid": bool(fields.pop("payload_valid", False)),
        "rows_written": int(fields.pop("rows_written", 0) or 0),
        "rows_updated": int(fields.pop("rows_updated", 0) or 0),
        "persistent_open_ready": bool(fields.pop("persistent_open_ready", False)),
        "existing_shadow_found": bool(fields.pop("existing_shadow_found", False)),
        "duplicate_prevented": bool(fields.pop("duplicate_prevented", False)),
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **fields,
        **_safety(),
    }


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

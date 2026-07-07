from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from services.mt5.mt5_persistent_intelligence_store import persist_shadow_trade
from services.mt5.mt5_shadow_trade_hygiene import run_shadow_trade_hygiene


CLEANUP_VERSION = "2026-06-11.mt5_shadow_trade_cleanup.v1"

Closer = Callable[[dict[str, Any], str], dict[str, Any]]


def run_shadow_trade_cleanup(
    *,
    apply_paper_cleanup: bool = False,
    open_trades: list[dict[str, Any]] | None = None,
    max_open_shadow_trades: int = 3,
    max_profile_open_shadows: int = 1,
    stale_hours: float = 12.0,
    load_shadow_snapshot: bool = True,
    load_persistent_db: bool = True,
    closer: Closer | None = None,
    require_live_db: bool = False,
    expected_live_capital_count: int | None = None,
    confirm_source_fingerprint: str = "",
    source_fingerprint: dict[str, Any] | None = None,
) -> dict[str, Any]:
    hygiene = run_shadow_trade_hygiene(
        open_trades=open_trades,
        max_open_shadow_trades=max_open_shadow_trades,
        max_profile_open_shadows=max_profile_open_shadows,
        stale_hours=stale_hours,
        load_shadow_snapshot=load_shadow_snapshot,
        load_persistent_db=load_persistent_db,
        persist_events=False,
    )
    candidates = [row for row in hygiene.get("safe_to_close_paper_only") or [] if isinstance(row, dict)]
    unsafe = [row for row in hygiene.get("unsafe_to_close") or [] if isinstance(row, dict)]
    closed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = list(unsafe)
    source_guard = _cleanup_source_guard(
        apply_paper_cleanup=apply_paper_cleanup,
        hygiene=hygiene,
        require_live_db=require_live_db,
        expected_live_capital_count=expected_live_capital_count,
        confirm_source_fingerprint=confirm_source_fingerprint,
        source_fingerprint=source_fingerprint,
    )
    if apply_paper_cleanup and not bool(source_guard.get("allowed")):
        return {
            "ok": False,
            "status": "shadow_trade_cleanup_blocked_source_mismatch",
            "reason": source_guard.get("reason") or "source_guard_blocked",
            "cleanup_version": CLEANUP_VERSION,
            "mode": "blocked_no_mutation",
            "apply_paper_cleanup": bool(apply_paper_cleanup),
            "open_shadow_trades_before": hygiene.get("open_shadow_trades_total", hygiene.get("open_shadow_trades", 0)),
            "open_shadow_trades_after": hygiene.get("open_shadow_trades_total", hygiene.get("open_shadow_trades", 0)),
            "cleanup_candidates": candidates,
            "closed_paper_only": 0,
            "closed": [],
            "skipped_unsafe": skipped,
            "source_guard": source_guard,
            "history_deleted": False,
            "metrics_reset": False,
            "losses_reset": False,
            "capital_protection_relaxed": False,
            "risk_governor_relaxed": False,
            "shadow_hygiene": hygiene,
            "paper_rotation_applied": False,
            "candidate_activated": False,
            "paper_forward_onboarding_started": False,
            "applies_to_real_trading": False,
            **_safety(),
        }
    close_fn = closer or _default_close
    if apply_paper_cleanup:
        indexed = {_trade_id(row): row for row in open_trades or [] if isinstance(row, dict)}
        for candidate in candidates:
            trade_id = _trade_id(candidate)
            source_trade = indexed.get(trade_id) or candidate
            safety_reasons = _unsafe_reasons(source_trade)
            if safety_reasons:
                skipped.append({**candidate, "skip_reasons": safety_reasons, **_safety()})
                continue
            reason = _exit_reason(candidate)
            result = close_fn(source_trade, reason)
            if result.get("ok"):
                closed.append(
                    {
                        "shadow_trade_id": trade_id,
                        "symbol": candidate.get("symbol") or source_trade.get("symbol") or "",
                        "timeframe": candidate.get("timeframe") or source_trade.get("timeframe") or "",
                        "exit_reason": reason,
                        "closed_at": result.get("closed_at") or (result.get("closed_trade") or {}).get("closed_at") or _now(),
                        **_safety(),
                    }
                )
            else:
                skipped.append({**candidate, "skip_reasons": [result.get("status") or result.get("reason") or "close_failed"], **_safety()})
    return {
        "ok": True,
        "status": "shadow_trade_cleanup_completed" if apply_paper_cleanup else "shadow_trade_cleanup_dry_run",
        "cleanup_version": CLEANUP_VERSION,
        "mode": "apply_paper_cleanup" if apply_paper_cleanup else "dry_run_report_only",
        "apply_paper_cleanup": bool(apply_paper_cleanup),
        "open_shadow_trades_before": hygiene.get("open_shadow_trades_total", hygiene.get("open_shadow_trades", 0)),
        "open_shadow_trades_after": max(0, int(hygiene.get("open_shadow_trades_total", 0) or 0) - len(closed)) if apply_paper_cleanup else hygiene.get("open_shadow_trades_total", hygiene.get("open_shadow_trades", 0)),
        "cleanup_candidates": candidates,
        "closed_paper_only": len(closed),
        "closed": closed,
        "skipped_unsafe": skipped,
        "source_guard": source_guard,
        "history_deleted": False,
        "metrics_reset": False,
        "losses_reset": False,
        "capital_protection_relaxed": False,
        "risk_governor_relaxed": False,
        "shadow_hygiene": hygiene,
        "paper_rotation_applied": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }


def _default_close(trade: dict[str, Any], reason: str) -> dict[str, Any]:
    trade_id = _trade_id(trade)
    symbol = str(trade.get("symbol") or "").upper().strip()
    if not trade_id or not symbol:
        return {"ok": False, "status": "missing_trade_identity", **_safety()}
    try:
        from services.mt5.mt5_shadow_trading import MT5ShadowTrading

        result = MT5ShadowTrading().close_shadow_trade(shadow_trade_id=trade_id, reason=reason, symbol=symbol)
        if result.get("ok"):
            return result
    except Exception:
        pass
    closed = {
        **trade,
        "status": "closed",
        "closed_at": _now(),
        "exit_reason": reason,
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }
    persist = persist_shadow_trade(closed)
    return {
        "ok": bool(persist.get("ok", True)),
        "status": "persistent_paper_shadow_marked_closed",
        "closed_trade": closed,
        "persistent_intelligence_shadow_trade": persist,
        **_safety(),
    }


def _exit_reason(candidate: dict[str, Any]) -> str:
    reasons = [str(reason) for reason in candidate.get("reasons") or []]
    if "duplicate_shadow_trade" in reasons:
        return "duplicate_paper_shadow_cleanup"
    if "stale_shadow_trade" in reasons:
        return "stale_paper_shadow_cleanup"
    if "degraded_profile" in reasons or "research_rejected_profile" in reasons:
        return "registry_rejected_paper_shadow_cleanup"
    if "paper_only_impossible_state" in reasons:
        return "impossible_state_paper_shadow_cleanup"
    return "safe_paper_shadow_cleanup"


def _unsafe_reasons(trade: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if bool(trade.get("broker_touched")):
        reasons.append("broker_touched")
    if bool(trade.get("order_executed")):
        reasons.append("order_executed")
    if bool(trade.get("applies_to_real_trading")):
        reasons.append("applies_to_real_trading")
    if str(trade.get("order_policy") or "journal_only_no_broker") != "journal_only_no_broker":
        reasons.append("non_journal_order_policy")
    if str(trade.get("status") or "open").casefold() != "open":
        reasons.append("not_open")
    return reasons


def _cleanup_source_guard(
    *,
    apply_paper_cleanup: bool,
    hygiene: dict[str, Any],
    require_live_db: bool,
    expected_live_capital_count: int | None,
    confirm_source_fingerprint: str,
    source_fingerprint: dict[str, Any] | None,
) -> dict[str, Any]:
    dry_run_count = int(hygiene.get("open_shadow_trades_total", hygiene.get("open_shadow_trades", 0)) or 0)
    if not apply_paper_cleanup:
        return {
            "allowed": True,
            "reason": "dry_run_no_mutation",
            "dry_run_count": dry_run_count,
            "expected_live_capital_count": expected_live_capital_count,
            "source_matches_capital_protection": False,
            **_safety(),
        }
    source = source_fingerprint if isinstance(source_fingerprint, dict) else _inspect_cleanup_source(require_live_db=require_live_db)
    source_count = int(source.get("open_shadow_trades_count") or 0) if isinstance(source, dict) else 0
    source_hash = str(source.get("source_fingerprint") or "") if isinstance(source, dict) else ""
    guard = {
        "allowed": False,
        "dry_run_count": dry_run_count,
        "source_open_shadow_trades_count": source_count,
        "expected_live_capital_count": expected_live_capital_count,
        "require_live_db": bool(require_live_db),
        "backend_type": source.get("backend_type") if isinstance(source, dict) else "unknown",
        "live_db_detected": bool(source.get("live_db_detected")) if isinstance(source, dict) else False,
        "source_matches_capital_protection": bool(source.get("source_matches_capital_protection")) if isinstance(source, dict) else False,
        "source_fingerprint": source_hash,
        "confirmed_source_fingerprint": str(confirm_source_fingerprint or ""),
        **_safety(),
    }
    if not str(confirm_source_fingerprint or "").strip():
        return {**guard, "reason": "missing_confirm_source_fingerprint"}
    if source_hash != str(confirm_source_fingerprint or "").strip():
        return {**guard, "reason": "confirm_source_fingerprint_mismatch"}
    if require_live_db and not bool(guard["live_db_detected"]):
        return {**guard, "reason": "source_is_local_sqlite_but_live_required"}
    if not bool(guard["source_matches_capital_protection"]):
        return {**guard, "reason": "source_matches_capital_protection_false"}
    if expected_live_capital_count is not None and dry_run_count != int(expected_live_capital_count):
        return {**guard, "reason": "dry_run_count_mismatches_live_capital_count"}
    if expected_live_capital_count is not None and source_count != int(expected_live_capital_count):
        return {**guard, "reason": "source_count_mismatches_live_capital_count"}
    if source_count != dry_run_count:
        return {**guard, "reason": "dry_run_count_mismatches_source_count"}
    return {**guard, "allowed": True, "reason": "source_fingerprint_confirmed"}


def _inspect_cleanup_source(*, require_live_db: bool) -> dict[str, Any]:
    try:
        from services.mt5.mt5_legacy_shadow_inspector import inspect_legacy_open_shadows

        return inspect_legacy_open_shadows(
            limit=500,
            status="open",
            require_live_db=require_live_db,
            redact_ids=True,
        )
    except Exception:
        return {
            "ok": False,
            "status": "source_inspection_failed",
            "backend_type": "unknown",
            "live_db_detected": False,
            "source_matches_capital_protection": False,
            "open_shadow_trades_count": 0,
            "source_fingerprint": "",
            **_safety(),
        }


def _trade_id(trade: dict[str, Any]) -> str:
    return str(trade.get("shadow_trade_id") or trade.get("trade_id") or "").strip()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

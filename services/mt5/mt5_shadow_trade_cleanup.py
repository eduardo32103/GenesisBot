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

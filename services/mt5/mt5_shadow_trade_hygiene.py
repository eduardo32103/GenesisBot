from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from services.mt5.mt5_forward_profile_degradation_registry import forward_profile_degradation
from services.mt5.mt5_persistent_intelligence_store import MT5PersistentIntelligenceStore, persist_risk_event
from services.mt5.mt5_research_rejection_registry import research_rejection


DEFAULT_MAX_OPEN_SHADOW_TRADES = 3
DEFAULT_MAX_PROFILE_OPEN_SHADOWS = 1
DEFAULT_STALE_HOURS = 12.0


def run_shadow_trade_hygiene(
    *,
    open_trades: list[dict[str, Any]] | None = None,
    max_open_shadow_trades: int = DEFAULT_MAX_OPEN_SHADOW_TRADES,
    max_profile_open_shadows: int = DEFAULT_MAX_PROFILE_OPEN_SHADOWS,
    stale_hours: float = DEFAULT_STALE_HOURS,
    load_shadow_snapshot: bool = True,
    load_persistent_db: bool = True,
    persist_events: bool = True,
) -> dict[str, Any]:
    trades = _normalize_trades(
        list(open_trades) if open_trades is not None else _load_open_trades(load_shadow_snapshot, load_persistent_db)
    )
    duplicate_clusters = _duplicate_clusters(trades)
    profile_clusters = _profiles_with_too_many_open_shadows(trades, max_profile_open_shadows)
    stale = _stale_trades(trades, stale_hours)
    diagnostics = _diagnostics(trades, duplicate_clusters=duplicate_clusters, stale=stale, stale_hours=stale_hours)
    safe_to_open = len(trades) <= int(max_open_shadow_trades)
    recommended_cleanup_action = _recommended_cleanup_action(
        open_count=len(trades),
        safe_to_open=safe_to_open,
        stale=stale,
        duplicate_clusters=duplicate_clusters,
        profile_clusters=profile_clusters,
    )
    result = {
        "ok": True,
        "status": "shadow_trade_hygiene_ready",
        "mode": "report_only_no_auto_close",
        "open_shadow_trades": len(trades),
        "open_shadow_trades_total": len(trades),
        "by_symbol": diagnostics["by_symbol"],
        "by_timeframe": diagnostics["by_timeframe"],
        "by_profile": diagnostics["by_profile"],
        "by_age_bucket": diagnostics["by_age_bucket"],
        "by_source": diagnostics["by_source"],
        "max_profile_exposure": diagnostics["max_profile_exposure"],
        "duplicates": diagnostics["duplicates"],
        "stale_trades": diagnostics["stale_trades"],
        "degraded_profile_open_trades": diagnostics["degraded_profile_open_trades"],
        "missing_price_trades": diagnostics["missing_price_trades"],
        "impossible_state_trades": diagnostics["impossible_state_trades"],
        "safe_to_close_paper_only": diagnostics["safe_to_close_paper_only"],
        "unsafe_to_close": diagnostics["unsafe_to_close"],
        "stale_shadow_trades": stale,
        "duplicate_shadow_clusters": duplicate_clusters,
        "profiles_with_too_many_open_shadows": profile_clusters,
        "safe_to_open_new_shadow": safe_to_open,
        "recommended_cleanup_action": recommended_cleanup_action,
        "shadow_trades_mutated": False,
        "closed_shadow_trades": False,
        "deleted_shadow_trades": False,
        "candidate_activated": False,
        "paper_forward_onboarding_started": False,
        "applies_to_real_trading": False,
        **_safety(),
    }
    if persist_events and (recommended_cleanup_action not in {"monitor_open_shadow_trades", "no_open_shadow_cleanup_needed"} or not safe_to_open):
        result["persistent_intelligence_risk_event"] = persist_risk_event(
            {
                "symbol": "",
                "timeframe": "",
                "risk_state": "shadow_hygiene",
                "allowed": bool(safe_to_open and not duplicate_clusters and not profile_clusters),
                "reason": recommended_cleanup_action,
                "circuit_breaker": "shadow_trade_hygiene",
                "open_shadow_count": len(trades),
                "recommended_action": recommended_cleanup_action,
            }
        )
    return result


def _load_open_trades(load_shadow_snapshot: bool, load_persistent_db: bool) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        if load_shadow_snapshot:
            from services.mt5.mt5_shadow_trading import MT5ShadowTrading

            snapshot = MT5ShadowTrading().snapshot(limit=500)
            for row in snapshot.get("open_trades") or []:
                if isinstance(row, dict):
                    rows.append({**row, "diagnostic_source": row.get("diagnostic_source") or "runtime_shadow_store"})
    except Exception:
        pass
    try:
        if load_persistent_db:
            result = MT5PersistentIntelligenceStore().open_shadow_trades(limit=500)
            for row in result.get("open_trades") or []:
                if isinstance(row, dict):
                    rows.append({**row, "diagnostic_source": row.get("diagnostic_source") or "persistent_intelligence_db"})
    except Exception:
        pass
    return _dedupe_rows(rows)


def _normalize_trades(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for trade in trades:
        if not isinstance(trade, dict):
            continue
        row = dict(trade)
        row["symbol"] = _symbol(row.get("symbol"))
        row["timeframe"] = _timeframe(row.get("timeframe"))
        row.setdefault("diagnostic_source", row.get("source") or "input")
        normalized.append(row)
    return normalized


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for row in rows:
        key = str(row.get("shadow_trade_id") or row.get("trade_id") or "")
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        unique.append(row)
    return unique


def _diagnostics(
    trades: list[dict[str, Any]],
    *,
    duplicate_clusters: list[dict[str, Any]],
    stale: list[dict[str, Any]],
    stale_hours: float,
) -> dict[str, Any]:
    duplicate_close_ids = _duplicate_close_ids(trades)
    stale_ids = {str(row.get("shadow_trade_id") or "") for row in stale}
    by_symbol = _counts(trades, lambda row: _symbol(row.get("symbol")) or "UNKNOWN")
    by_timeframe = _counts(trades, lambda row: _timeframe(row.get("timeframe")) or "UNKNOWN")
    by_profile = _counts(trades, lambda row: _profile(row) or "UNKNOWN")
    by_source = _counts(trades, lambda row: str(row.get("diagnostic_source") or row.get("source") or "unknown"))
    by_age_bucket = _age_bucket_counts(trades)
    max_profile_exposure = max((row.get("open_count") or 0 for row in _profiles_with_too_many_open_shadows(trades, 0)), default=0)
    degraded_rows: list[dict[str, Any]] = []
    missing_price_rows: list[dict[str, Any]] = []
    impossible_rows: list[dict[str, Any]] = []
    safe: list[dict[str, Any]] = []
    unsafe: list[dict[str, Any]] = []
    for trade in trades:
        trade_id = _trade_id(trade)
        symbol = _symbol(trade.get("symbol"))
        timeframe = _timeframe(trade.get("timeframe"))
        profile = _profile(trade)
        degraded = bool(forward_profile_degradation(symbol, timeframe, profile))
        rejected = bool(research_rejection(symbol, timeframe, profile, _infer_family(profile)))
        if degraded or rejected:
            degraded_rows.append(_trade_summary(trade, reasons=["degraded_profile" if degraded else "research_rejected_profile"]))
        if _price(trade) is None:
            missing_price_rows.append(_trade_summary(trade, reasons=["missing_reliable_price"]))
        impossible = _impossible_state_reasons(trade)
        if impossible:
            impossible_rows.append(_trade_summary(trade, reasons=impossible))
        safe_reasons: list[str] = []
        if degraded:
            safe_reasons.append("degraded_profile")
        if rejected:
            safe_reasons.append("research_rejected_profile")
        if trade_id in stale_ids:
            safe_reasons.append("stale_shadow_trade")
        if trade_id in duplicate_close_ids:
            safe_reasons.append("duplicate_shadow_trade")
        if impossible and _paper_only(trade):
            safe_reasons.append("paper_only_impossible_state")
        unsafe_reasons = _unsafe_reasons(trade)
        if safe_reasons and not unsafe_reasons:
            safe.append(_trade_summary(trade, reasons=safe_reasons))
        elif unsafe_reasons:
            unsafe.append(_trade_summary(trade, reasons=unsafe_reasons))
    return {
        "by_symbol": by_symbol,
        "by_timeframe": by_timeframe,
        "by_profile": by_profile,
        "by_age_bucket": by_age_bucket,
        "by_source": by_source,
        "max_profile_exposure": int(max_profile_exposure),
        "duplicates": duplicate_clusters,
        "stale_trades": stale,
        "degraded_profile_open_trades": degraded_rows,
        "missing_price_trades": missing_price_rows,
        "impossible_state_trades": impossible_rows,
        "safe_to_close_paper_only": safe,
        "unsafe_to_close": unsafe,
        "stale_hours": float(stale_hours),
    }


def _duplicate_clusters(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        key = (
            _symbol(trade.get("symbol")),
            _timeframe(trade.get("timeframe")),
            str(trade.get("strategy_profile") or trade.get("profile") or trade.get("family") or "").strip(),
            str(trade.get("side") or trade.get("action") or "").lower().strip(),
        )
        grouped[key].append(trade)
    clusters: list[dict[str, Any]] = []
    for (symbol, timeframe, profile, side), rows in grouped.items():
        if len(rows) <= 1:
            continue
        clusters.append(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "profile": profile,
                "side": side,
                "open_count": len(rows),
                "shadow_trade_ids": _trade_ids(rows),
                **_safety(),
            }
        )
    clusters.sort(key=lambda row: (-int(row["open_count"]), row["symbol"], row["profile"]))
    return clusters


def _profiles_with_too_many_open_shadows(trades: list[dict[str, Any]], max_profile_open_shadows: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        key = (
            _symbol(trade.get("symbol")),
            _timeframe(trade.get("timeframe")),
            str(trade.get("strategy_profile") or trade.get("profile") or trade.get("family") or "").strip(),
        )
        grouped[key].append(trade)
    rows: list[dict[str, Any]] = []
    for (symbol, timeframe, profile), items in grouped.items():
        if len(items) <= int(max_profile_open_shadows):
            continue
        rows.append(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "profile": profile,
                "open_count": len(items),
                "max_profile_open_shadows": int(max_profile_open_shadows),
                "shadow_trade_ids": _trade_ids(items),
                **_safety(),
            }
        )
    rows.sort(key=lambda row: (-int(row["open_count"]), row["symbol"], row["profile"]))
    return rows


def _stale_trades(trades: list[dict[str, Any]], stale_hours: float) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    stale: list[dict[str, Any]] = []
    for trade in trades:
        opened = _parse_time(trade.get("opened_at") or trade.get("created_at") or trade.get("timestamp"))
        if opened is None:
            continue
        age_hours = (now - opened).total_seconds() / 3600.0
        if age_hours < float(stale_hours):
            continue
        stale.append(
            {
                "symbol": _symbol(trade.get("symbol")),
                "timeframe": _timeframe(trade.get("timeframe")),
                "profile": str(trade.get("strategy_profile") or trade.get("profile") or trade.get("family") or "").strip(),
                "shadow_trade_id": str(trade.get("shadow_trade_id") or trade.get("trade_id") or ""),
                "opened_at": str(trade.get("opened_at") or ""),
                "age_hours": round(age_hours, 4),
                **_safety(),
            }
        )
    stale.sort(key=lambda row: -float(row["age_hours"]))
    return stale


def _recommended_cleanup_action(
    *,
    open_count: int,
    safe_to_open: bool,
    stale: list[dict[str, Any]],
    duplicate_clusters: list[dict[str, Any]],
    profile_clusters: list[dict[str, Any]],
) -> str:
    if not safe_to_open:
        return "review_open_shadow_over_limit_before_new_entries"
    if duplicate_clusters:
        return "review_duplicate_shadow_clusters"
    if profile_clusters:
        return "review_profiles_with_too_many_open_shadows"
    if stale:
        return "review_stale_shadow_trades"
    if open_count == 0:
        return "no_open_shadow_cleanup_needed"
    return "monitor_open_shadow_trades"


def _trade_ids(trades: list[dict[str, Any]]) -> list[str]:
    return [str(row.get("shadow_trade_id") or row.get("trade_id") or "") for row in trades if str(row.get("shadow_trade_id") or row.get("trade_id") or "").strip()]


def _duplicate_close_ids(trades: list[dict[str, Any]]) -> set[str]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        if _unsafe_reasons(trade):
            continue
        grouped[(
            _symbol(trade.get("symbol")),
            _timeframe(trade.get("timeframe")),
            _profile(trade),
            str(trade.get("side") or trade.get("action") or "").lower().strip(),
        )].append(trade)
    close_ids: set[str] = set()
    for items in grouped.values():
        if len(items) <= 1:
            continue
        ordered = sorted(items, key=lambda row: str(row.get("opened_at") or row.get("created_at") or ""))
        for duplicate in ordered[1:]:
            trade_id = _trade_id(duplicate)
            if trade_id:
                close_ids.add(trade_id)
    return close_ids


def _counts(trades: list[dict[str, Any]], key_fn: Any) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for trade in trades:
        counts[str(key_fn(trade) or "UNKNOWN")] += 1
    return dict(sorted(counts.items()))


def _age_bucket_counts(trades: list[dict[str, Any]]) -> dict[str, int]:
    buckets = {"unknown": 0, "lt_1h": 0, "1h_6h": 0, "6h_12h": 0, "12h_24h": 0, "gt_24h": 0}
    now = datetime.now(timezone.utc)
    for trade in trades:
        opened = _parse_time(trade.get("opened_at") or trade.get("created_at") or trade.get("timestamp"))
        if opened is None:
            buckets["unknown"] += 1
            continue
        age_hours = (now - opened).total_seconds() / 3600.0
        if age_hours < 1:
            buckets["lt_1h"] += 1
        elif age_hours < 6:
            buckets["1h_6h"] += 1
        elif age_hours < 12:
            buckets["6h_12h"] += 1
        elif age_hours < 24:
            buckets["12h_24h"] += 1
        else:
            buckets["gt_24h"] += 1
    return buckets


def _trade_summary(trade: dict[str, Any], *, reasons: list[str]) -> dict[str, Any]:
    return {
        "shadow_trade_id": _trade_id(trade),
        "symbol": _symbol(trade.get("symbol")),
        "timeframe": _timeframe(trade.get("timeframe")),
        "profile": _profile(trade),
        "side": str(trade.get("side") or trade.get("action") or "").lower().strip(),
        "opened_at": str(trade.get("opened_at") or trade.get("created_at") or ""),
        "diagnostic_source": str(trade.get("diagnostic_source") or trade.get("source") or "unknown"),
        "reasons": reasons,
        **_safety(),
    }


def _unsafe_reasons(trade: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if bool(trade.get("broker_touched")):
        reasons.append("broker_touched")
    if bool(trade.get("order_executed")):
        reasons.append("order_executed")
    if str(trade.get("order_policy") or "journal_only_no_broker") != "journal_only_no_broker":
        reasons.append("non_journal_order_policy")
    if bool(trade.get("applies_to_real_trading")):
        reasons.append("applies_to_real_trading")
    if str(trade.get("status") or "").casefold() not in {"open", ""}:
        reasons.append("not_open")
    if not _trade_id(trade):
        reasons.append("missing_shadow_trade_id")
    if not _symbol(trade.get("symbol")):
        reasons.append("missing_symbol")
    if not _paper_only(trade):
        reasons.append("not_confirmed_paper_shadow")
    return reasons


def _paper_only(trade: dict[str, Any]) -> bool:
    source = str(trade.get("source") or trade.get("diagnostic_source") or "").casefold()
    if bool(trade.get("broker_touched")) or bool(trade.get("order_executed")) or bool(trade.get("applies_to_real_trading")):
        return False
    if str(trade.get("order_policy") or "journal_only_no_broker") != "journal_only_no_broker":
        return False
    if any(token in source for token in ("paper", "shadow", "runtime", "persistent_intelligence", "mt5_bridge", "input")):
        return True
    return bool(trade.get("paper_exploration") or trade.get("auto_forward") or str(trade.get("shadow_trade_id") or "").startswith(("paper-", "shadow-")))


def _impossible_state_reasons(trade: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    side = str(trade.get("side") or trade.get("action") or "").lower().strip()
    if side and side not in {"buy", "sell"}:
        reasons.append("invalid_side")
    if not _symbol(trade.get("symbol")):
        reasons.append("missing_symbol")
    if not _trade_id(trade):
        reasons.append("missing_shadow_trade_id")
    return reasons


def _trade_id(trade: dict[str, Any]) -> str:
    return str(trade.get("shadow_trade_id") or trade.get("trade_id") or "").strip()


def _profile(trade: dict[str, Any]) -> str:
    return str(trade.get("strategy_profile") or trade.get("profile") or trade.get("family") or "").strip()


def _price(trade: dict[str, Any]) -> float | None:
    for key in ("last_price", "current_price", "exit_price", "entry_price", "entry", "last"):
        try:
            value = trade.get(key)
            if value not in (None, ""):
                return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _infer_family(profile: object) -> str:
    text = str(profile or "").casefold()
    if "session_vwap" in text:
        return "session_vwap_reclaim"
    if "trend_pullback" in text:
        return "multi_timeframe_trend_pullback"
    if "vol_breakout" in text or "volatility_breakout" in text:
        return "volatility_breakout"
    if "ema_reclaim" in text:
        return "ema_reclaim"
    if "london_us_breakout" in text or "opening_range_fakeout" in text:
        return "opening_range_fakeout"
    return text


def _parse_time(value: object) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _symbol(value: object) -> str:
    return str(value or "").upper().strip().replace(".B", "")


def _timeframe(value: object) -> str:
    return str(value or "").upper().strip()


def _safety() -> dict[str, Any]:
    return {
        "broker_touched": False,
        "order_executed": False,
        "order_policy": "journal_only_no_broker",
    }

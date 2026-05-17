from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.genesis.genesis_brain import GenesisBrain
from services.genesis.memory_store import MemoryStore
from services.genesis.ticker_parser import normalize_ticker


_SECRET_KEY_PARTS = ("api", "key", "secret", "token", "password", "credential")
_BULLISH_TOKENS = ("bullish", "long", "buy", "compra", "comprar", "alcista", "ruptura", "inflow", "accumulation")
_BEARISH_TOKENS = ("bearish", "short", "sell", "venta", "vender", "bajista", "breakdown", "outflow", "distribution")
_VALID_ACTIONS = {
    "long_signal",
    "short_signal",
    "exit_signal",
    "stop_hit",
    "take_profit_hit",
    "strategy_invalidated",
    "watch_only",
    "long_exit",
    "short_exit",
    "hedge_alert",
    "protect_profit",
    "reduce_exposure",
    "risk_off",
}


def get_trading_context(ticker: str, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    """Build the external Genesis context Pine can read manually through inputs."""

    store = memory or MemoryStore()
    return GenesisBrain(memory=store).build_trading_context(ticker)


def receive_tradingview_webhook(
    payload: dict[str, Any] | None,
    *,
    memory: MemoryStore | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Receive TradingView alerts and store them for journal/learning only."""

    body = payload if isinstance(payload, dict) else {}
    normalized = normalize_ticker(body.get("ticker") or body.get("symbol") or body.get("asset") or "")
    if not normalized:
        return {
            "ok": False,
            "status": "missing_ticker",
            "message": "TradingView webhook requiere ticker.",
            "order_executed": False,
        }

    clock = now or datetime.now(timezone.utc)
    store = memory or MemoryStore()
    action = _clean_action(body.get("action"))
    confidence = _confidence_from_score(body.get("score"))
    canonical = _canonical_signal(normalized, body, action, confidence, clock)

    alert_event = store.save_event("tradingview_alert", canonical, source="tradingview", confidence=confidence)
    strategy_signal = store.save_signal_event(
        normalized,
        {
            **canonical,
            "event_type": "strategy_signal",
            "collection": "strategy_signals",
            "expected_direction": _expected_direction(action),
            "expected_impact": str(body.get("notes") or body.get("setup") or action)[:120],
        },
        source="tradingview",
        confidence=confidence,
    )
    outcome = store.save_outcome_tracking(
        normalized,
        {
            **canonical,
            "event_type": "strategy_outcome",
            "collection": "strategy_outcomes",
            "signal_event_id": strategy_signal.get("event_id"),
            "status": "open" if action in {"long_signal", "short_signal", "watch_only"} else "observed",
        },
        source="tradingview",
        confidence=confidence,
    )
    journal = store.save_asset_memory(
        normalized,
        {
            **canonical,
            "event_type": "trade_journal",
            "collection": "trade_journal",
            "journal_note": _journal_note(canonical),
        },
        source="tradingview",
        confidence=confidence,
    )
    backtest_note = store.save_hypothesis(
        normalized,
        {
            **canonical,
            "event_type": "backtest_note",
            "collection": "backtest_notes",
            "hypothesis": "Validar si este setup conserva ventaja en backtesting, paper trading y forward testing.",
        },
        source="tradingview",
        confidence=confidence,
    )
    benchmark_comparison = None
    if canonical.get("benchmark_capture_ratio") is not None or canonical.get("strategy_vs_hold") is not None:
        benchmark_comparison = store.save_event(
            "benchmark_comparison",
            {
                **canonical,
                "event_type": "benchmark_comparison",
                "collection": "benchmark_comparison",
                "benchmark_capture_ratio": canonical.get("benchmark_capture_ratio"),
                "strategy_vs_hold": canonical.get("strategy_vs_hold"),
                "benchmark_warning": canonical.get("benchmark_warning"),
                "order_policy": "journal_only_no_broker",
                "broker_touched": False,
            },
            "tradingview",
            confidence,
        )
    hedge_event = None
    hedge_recommendation = None
    protected_trade = None
    no_edge_decision = None
    btc_edge_result = None
    btc_backtest_result = None
    if canonical.get("hedge_score") is not None or canonical.get("hedge_needed") is not None:
        hedge_payload = {
            **canonical,
            "event_type": "hedge_event",
            "collection": "hedge_events",
            "hedge_score": canonical.get("hedge_score"),
            "hedge_type": canonical.get("hedge_type"),
            "hedge_ratio": canonical.get("hedge_ratio"),
            "action_suggested": canonical.get("suggested_action"),
            "reason": canonical.get("block_reason") or canonical.get("notes"),
            "expected_risk": "capital_protection_watch",
            "status": "open",
            "order_policy": "journal_only_no_broker",
            "broker_touched": False,
        }
        hedge_event = store.save_event("hedge_event", hedge_payload, "tradingview", confidence)
        hedge_recommendation = store.save_event(
            "hedge_recommendation",
            {**hedge_payload, "event_type": "hedge_recommendation", "collection": "hedge_recommendations"},
            "tradingview",
            confidence,
        )
        if canonical.get("protect_open_profit"):
            protected_trade = store.save_event(
                "protected_trade",
                {**hedge_payload, "event_type": "protected_trade", "collection": "protected_trades"},
                "tradingview",
                confidence,
            )
    if canonical.get("no_trade_block") or (canonical.get("no_trade_score") is not None and canonical.get("no_trade_score") >= 70):
        no_edge_decision = store.save_no_edge_decision(
            normalized,
            {
                **canonical,
                "event_type": "no_edge_decision",
                "collection": "no_edge_decisions",
                "edge_status": canonical.get("edge_status") or "no_edge",
                "reason": canonical.get("block_reason") or "Genesis marco sin edge para este activo/timeframe/setup.",
                "order_policy": "journal_only_no_broker",
                "broker_touched": False,
            },
            "tradingview",
            confidence,
        )
    if canonical.get("btc_regime") or canonical.get("crypto_v4_mode"):
        btc_payload = {
            **canonical,
            "event_type": "btc_edge_result",
            "collection": "btc_edge_results",
            "strategy_version": canonical.get("strategy_version") or "Genesis Advantage v10.13 BTC Edge",
            "order_policy": "journal_only_no_broker",
            "broker_touched": False,
        }
        btc_edge_result = store.save_btc_edge_result(normalized, btc_payload, "tradingview", confidence)
        btc_backtest_result = store.save_btc_backtest_result(
            normalized,
            {**btc_payload, "event_type": "btc_backtest_result", "collection": "btc_backtest_results"},
            "tradingview",
            confidence,
        )
    store.save_learned_context(
        f"tradingview:last_signal:{normalized}",
        {
            "ticker": normalized,
            "strategy_version": canonical.get("strategy_version"),
            "action": canonical["action"],
            "setup": canonical["setup"],
            "score": canonical["score"],
            "asset_profile": canonical.get("asset_profile"),
            "core_tactical_mode": canonical.get("core_tactical_mode"),
            "core_entry": canonical.get("core_entry"),
            "tactical_entry": canonical.get("tactical_entry"),
            "btc_regime": canonical.get("btc_regime"),
            "btc_long_term_mode": canonical.get("btc_long_term_mode"),
            "crypto_v4_mode": canonical.get("crypto_v4_mode"),
            "crypto_v3_mode": canonical.get("crypto_v3_mode"),
            "bot_status": canonical.get("bot_status"),
            "recommended_timeframe": canonical.get("recommended_timeframe"),
            "entry_quality_score": canonical.get("entry_quality_score"),
            "benchmark_capture_ratio": canonical.get("benchmark_capture_ratio"),
            "strategy_vs_hold": canonical.get("strategy_vs_hold"),
            "hedge_score": canonical.get("hedge_score"),
            "hedge_overlay": canonical.get("hedge_overlay"),
            "no_trade_score": canonical.get("no_trade_score"),
            "edge_status": canonical.get("edge_status"),
            "timestamp": canonical["timestamp"],
            "status": canonical["status"],
        },
        source="tradingview",
        confidence=confidence,
    )

    return {
        "ok": True,
        "status": "tradingview_alert_recorded",
        "message": "Alerta guardada para journal y aprendizaje. No se ejecuto ninguna orden.",
        "ticker": normalized,
        "action": action,
        "collections_saved": [
            "strategy_signals",
            "tradingview_alerts",
            "strategy_outcomes",
            "backtest_notes",
            "trade_journal",
            *(["benchmark_comparison"] if benchmark_comparison is not None else []),
            *(
                ["hedge_events", "hedge_recommendations"]
                if hedge_event is not None
                else []
            ),
            *(["protected_trades"] if protected_trade is not None else []),
            *(["no_edge_decisions"] if no_edge_decision is not None else []),
            *(["btc_edge_results", "btc_backtest_results"] if btc_edge_result is not None else []),
        ],
        "memory": {
            "backend": store.backend,
            "alert_event": alert_event,
            "strategy_signal_event_id": strategy_signal.get("event_id"),
            "outcome_event_id": outcome.get("event_id"),
            "journal_event_id": journal.get("event_id"),
            "backtest_note_event_id": backtest_note.get("event_id"),
            "benchmark_comparison": benchmark_comparison,
            "hedge_event": hedge_event,
            "hedge_recommendation": hedge_recommendation,
            "protected_trade": protected_trade,
            "no_edge_decision": no_edge_decision,
            "btc_edge_result": btc_edge_result,
            "btc_backtest_result": btc_backtest_result,
        },
        "order_executed": False,
        "broker_touched": False,
        "real_money": False,
        "execution_policy": "journal_only_no_broker",
        "signal": canonical,
    }


def _context_evidence(summary: dict[str, Any]) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for row in summary.get("signals") or []:
        evidence.append({"source": "signals", "score": _direction_score(_row_text(row), 18)})
    for row in summary.get("decisions") or []:
        evidence.append({"source": "decisions", "score": _direction_score(_row_text(row), 20)})
    for row in summary.get("alerts") or []:
        evidence.append({"source": "alerts", "score": _direction_score(_row_text(row), 12)})
    for row in summary.get("whales") or []:
        evidence.append({"source": "whales", "score": _direction_score(_row_text(row), 14)})
    for row in summary.get("news") or []:
        evidence.append({"source": "news", "score": _direction_score(_row_text(row), 8)})
    for row in summary.get("outcomes") or []:
        text = _row_text(row)
        if "miss" in text or "fallo" in text:
            evidence.append({"source": "outcomes", "score": -8})
        elif "hit" in text or "acierto" in text:
            evidence.append({"source": "outcomes", "score": 8})
    return [item for item in evidence if item["score"] != 0]


def _canonical_signal(
    ticker: str,
    body: dict[str, Any],
    action: str,
    confidence: str,
    clock: datetime,
) -> dict[str, Any]:
    score = _safe_float(body.get("score"))
    price = _safe_float(body.get("price") or body.get("close"))
    stop = _safe_float(body.get("stop"))
    target = _safe_float(body.get("target"))
    risk_reward = _safe_float(body.get("risk_reward") or body.get("rr"))
    if risk_reward is None and price and stop is not None and target is not None:
        risk = abs(price - stop)
        reward = abs(target - price)
        risk_reward = round(reward / risk, 4) if risk else None

    timestamp = str(body.get("time") or body.get("timestamp") or clock.isoformat())[:80]
    return {
        "ticker": ticker,
        "asset_name": str(body.get("asset_name") or body.get("name") or "")[:240],
        "source": "tradingview",
        "strategy": str(body.get("strategy") or "Genesis Advantage Strategy v1")[:160],
        "strategy_version": str(body.get("strategy_version") or "")[:160],
        "setup": str(body.get("setup") or "unspecified")[:120],
        "action": action,
        "score": score,
        "price": price,
        "stop": stop,
        "target": target,
        "risk": str(body.get("risk") or "paper_only")[:120],
        "risk_reward": risk_reward,
        "market_regime": str(body.get("regime") or body.get("market_regime") or "")[:120],
        "genesis_context": _safe_float(body.get("genesis_context") or body.get("genesis_context_score")),
        "preset": str(body.get("preset") or "")[:80],
        "auto_profile_mode": _safe_bool(body.get("auto_profile_mode")),
        "safe_mode": _safe_bool(body.get("safe_mode")),
        "validation_mode": _safe_bool(body.get("validation_mode")),
        "validation_warning": str(body.get("validation_warning") or "")[:180],
        "bot_status": str(body.get("bot_status") or "")[:120],
        "recommended_timeframe": str(body.get("recommended_timeframe") or "")[:80],
        "asset_profile": str(body.get("asset_profile") or "")[:120],
        "profile_warning": str(body.get("profile_warning") or "")[:240],
        "no_trade_mode": _safe_bool(body.get("no_trade_mode")),
        "no_trade_score": _safe_float(body.get("no_trade_score")),
        "no_trade_block": _safe_bool(body.get("no_trade_block")),
        "edge_status": str(body.get("edge_status") or "")[:80],
        "suggested_mode": str(body.get("suggested_mode") or "")[:80],
        "core_tactical_mode": _safe_bool(body.get("core_tactical_mode")),
        "core_position_active": _safe_bool(body.get("core_position_active")),
        "core_entry": _safe_bool(body.get("core_entry")),
        "tactical_entry": _safe_bool(body.get("tactical_entry")),
        "entry_quality": str(body.get("entry_quality") or "")[:80],
        "entry_quality_score": _safe_float(body.get("entry_quality_score")),
        "trend_runner_mode": _safe_bool(body.get("trend_runner_mode")),
        "btc_long_term_mode": _safe_bool(body.get("btc_long_term_mode")),
        "crypto_v4_mode": _safe_bool(body.get("crypto_v4_mode")),
        "crypto_v3_mode": _safe_bool(body.get("crypto_v3_mode")),
        "btc_regime": str(body.get("btc_regime") or "")[:80],
        "crypto_avoid_chop": _safe_bool(body.get("crypto_avoid_chop")),
        "crypto_breakout_retest": _safe_bool(body.get("crypto_breakout_retest")),
        "crypto_vol_expansion": _safe_bool(body.get("crypto_vol_expansion")),
        "crypto_chop_block": _safe_bool(body.get("crypto_chop_block")),
        "trend_alignment": str(body.get("trend_alignment") or "")[:80],
        "benchmark_warning": str(body.get("benchmark_warning") or "")[:240],
        "benchmark_capture_ratio": _safe_float(body.get("benchmark_capture_ratio")),
        "strategy_vs_hold": _safe_float(body.get("strategy_vs_hold")),
        "runner_active": _safe_bool(body.get("runner_active")),
        "breakeven_active": _safe_bool(body.get("breakeven_active")),
        "block_reason": str(body.get("block_reason") or "")[:240],
        "hedge_mode": _safe_bool(body.get("hedge_mode")),
        "hedge_overlay": _safe_bool(body.get("hedge_overlay")),
        "hedge_short_allowed": _safe_bool(body.get("hedge_short_allowed")),
        "hedge_impact_mode": str(body.get("hedge_impact_mode") or "")[:80],
        "hedge_score": _safe_float(body.get("hedge_score")),
        "hedge_needed": _safe_bool(body.get("hedge_needed")),
        "hedge_type": str(body.get("hedge_type") or "")[:120],
        "hedge_ratio": _safe_float(body.get("hedge_ratio")),
        "capital_protection_mode": _safe_bool(body.get("capital_protection_mode")),
        "protect_open_profit": _safe_bool(body.get("protect_open_profit")),
        "no_trade_reason": str(body.get("no_trade_reason") or "")[:240],
        "suggested_action": str(body.get("suggested_action") or "")[:160],
        "timestamp": timestamp,
        "created_at": clock.isoformat(),
        "confidence": confidence,
        "status": "validation_only" if _safe_bool(body.get("safe_mode")) or _safe_bool(body.get("validation_mode")) else "watching",
        "outcome_1h": None,
        "outcome_24h": None,
        "outcome_7d": None,
        "actual_outcome_1h": None,
        "actual_outcome_24h": None,
        "actual_outcome_7d": None,
        "notes": _redact_text(body.get("notes"))[:500],
        "raw_data_sanitized": _clean_webhook_payload(body),
        "broker_execution": False,
        "order_executed": False,
        "real_money": False,
        "execution_policy": "journal_only_no_broker",
        "genesis_reading": "Senal de TradingView guardada para backtesting, paper trading, alertas y aprendizaje; no es orden real.",
    }


def _clean_webhook_payload(body: dict[str, Any]) -> dict[str, Any]:
    clean: dict[str, Any] = {}
    for key, value in body.items():
        key_text = str(key or "")[:120]
        if any(part in key_text.casefold() for part in _SECRET_KEY_PARTS):
            continue
        if isinstance(value, (str, int, float, bool)) or value is None:
            text = str(value)
            if any(secret in text.casefold() for secret in ("fmp_api_key", "openai_api_key", "apikey=", "bearer ")):
                clean[key_text] = "[redacted]"
            else:
                clean[key_text] = value
        elif isinstance(value, dict):
            clean[key_text] = _clean_webhook_payload(value)
        elif isinstance(value, list):
            clean[key_text] = value[:20]
        else:
            clean[key_text] = str(value)[:500]
    return clean


def _redact_text(value: Any) -> str:
    text = str(value or "")
    if any(secret in text.casefold() for secret in ("fmp_api_key", "openai_api_key", "apikey=", "bearer ")):
        return "[redacted]"
    return text


def _compact_rows(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for row in rows[:limit]:
        if not isinstance(row, dict):
            continue
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else row
        compact.append(
            {
                "event_type": row.get("event_type") or payload.get("event_type") or "",
                "title": payload.get("title_es") or payload.get("title") or payload.get("verdict") or payload.get("setup") or "",
                "summary": payload.get("summary_es") or payload.get("summary") or payload.get("genesis_reading") or payload.get("notes") or "",
                "source": row.get("source") or payload.get("source") or "",
                "confidence": row.get("confidence") or payload.get("confidence") or "",
                "timestamp": row.get("created_at") or payload.get("timestamp") or payload.get("created_at") or "",
            }
        )
    return compact


def _asset_name(ticker: str, summary: dict[str, Any]) -> str:
    for group in ("asset_memory", "signals", "decisions", "news"):
        for row in summary.get(group) or []:
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else row
            name = payload.get("asset_name") or payload.get("name")
            if name:
                return str(name)[:240]
    return ticker


def _risk_flags(summary: dict[str, Any], evidence: list[dict[str, Any]]) -> list[str]:
    flags: list[str] = []
    if not evidence:
        flags.append("Sin evidencia suficiente en memoria; usar score 0 y validar solo por chart.")
    if not summary.get("outcomes"):
        flags.append("Aun no hay outcomes 1h/24h/7d suficientes para medir ventaja.")
    bearish = sum(1 for item in evidence if item["score"] < 0)
    bullish = sum(1 for item in evidence if item["score"] > 0)
    if bearish and bullish:
        flags.append("Evidencia mixta; exigir confirmacion de precio, volumen y nivel.")
    return flags[:5]


def _what_to_watch(ticker: str, bias: str, risk_flags: list[str]) -> list[str]:
    direction = "alcista" if bias == "bullish" else "bajista" if bias == "bearish" else "neutral"
    items = [
        f"{ticker}: validar que el setup tecnico confirme el sesgo {direction}.",
        "Volumen relativo y ruptura/rechazo del nivel clave.",
        "RR minimo 1.5 antes de elevar conviccion.",
        "Webhook a Genesis para guardar journal y outcomes.",
    ]
    if risk_flags:
        items.append("Si la evidencia sigue incompleta, tratarlo como watch only.")
    return items


def _direction_score(text: str, weight: int) -> int:
    folded = text.casefold()
    bullish = any(token in folded for token in _BULLISH_TOKENS)
    bearish = any(token in folded for token in _BEARISH_TOKENS)
    if bullish and not bearish:
        return weight
    if bearish and not bullish:
        return -weight
    return 0


def _row_text(row: dict[str, Any]) -> str:
    return str(row)[:3000]


def _confidence(score: int, evidence: list[dict[str, Any]]) -> str:
    groups = {item["source"] for item in evidence}
    if abs(score) >= 60 and len(groups) >= 3:
        return "high"
    if abs(score) >= 25 or len(groups) >= 2:
        return "medium"
    return "low"


def _confidence_from_score(value: Any) -> str:
    score = _safe_float(value)
    if score is None:
        return "media"
    if score >= 80:
        return "alta"
    if score >= 60:
        return "media"
    return "baja"


def _clean_action(value: Any) -> str:
    action = str(value or "watch_only").strip().lower().replace(" ", "_")[:80]
    return action if action in _VALID_ACTIONS else "watch_only"


def _expected_direction(action: str) -> str:
    if action in {"long_signal"}:
        return "bullish"
    if action in {"short_signal"}:
        return "bearish"
    return "watch"


def _journal_note(payload: dict[str, Any]) -> str:
    return (
        f"{payload['ticker']} {payload['action']} via {payload['strategy']} "
        f"score={payload.get('score')} setup={payload.get('setup')} RR={payload.get('risk_reward')}. "
        "Paper/journal only."
    )


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "na", "NaN"):
        return None
    try:
        return float(str(value).replace("$", "").replace("%", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _safe_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().casefold() in {"1", "true", "yes", "si", "sí", "on"}


def _clamp_score(value: int | float) -> int:
    return int(max(-100, min(100, round(value))))

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from services.genesis.memory_store import MemoryStore
from services.genesis.price_agent import get_price_agent
from services.genesis.ticker_parser import normalize_ticker


QuoteLoader = Callable[[str], dict[str, Any]]

_NON_ASSET_TICKERS = {
    "CAUTELA",
    "VALIDACION",
    "VALIDADO",
    "VALIDADA",
    "VALIDADOS",
    "VALIDADAS",
    "OPORTUNIDAD",
    "OPORTUNIDADES",
    "PRECIOS",
    "CREE",
    "CREES",
    "DIME",
    "ESTA",
    "ESTAS",
    "HOLA",
    "GENESIS",
}


def build_genesis_performance_report(
    message: str = "",
    *,
    memory: MemoryStore | None = None,
    limit: int = 80,
    quote_loader: QuoteLoader | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    store = memory or MemoryStore()
    clock = now or datetime.now(timezone.utc)
    decision_rows = store.get_decision_notes(limit=limit)
    available_tickers = {
        normalize_ticker(row.get("ticker") or row.get("payload", {}).get("ticker"))
        for row in decision_rows
        if normalize_ticker(row.get("ticker") or row.get("payload", {}).get("ticker"))
    }
    ticker_filter = _ticker_from_message(message, available_tickers)
    if ticker_filter:
        decision_rows = [row for row in decision_rows if normalize_ticker(row.get("ticker") or row.get("payload", {}).get("ticker")) == ticker_filter]

    decisions = _dedupe_decisions(decision_rows)
    loader = quote_loader or get_price_agent().quote
    quote_cache: dict[str, dict[str, Any]] = {}
    evaluated: list[dict[str, Any]] = []
    missing_price = 0
    ignored_rows = 0

    for row in decisions[:40]:
        payload = _payload(row)
        ticker = normalize_ticker(row.get("ticker") or payload.get("ticker"))
        if not ticker or _is_non_asset_ticker(ticker):
            ignored_rows += 1
            continue
        entry_price = _entry_price(payload)
        if entry_price is None or entry_price <= 0:
            missing_price += 1
            continue
        quote = _quote_for(ticker, loader, quote_cache)
        current_price = _number(quote.get("current_price") or quote.get("price") or quote.get("close"))
        if current_price is None or current_price <= 0:
            missing_price += 1
            continue
        return_pct = ((current_price - entry_price) / entry_price) * 100
        direction = _expected_direction(payload)
        outcome = _outcome_label(direction, return_pct)
        status = "resolved" if outcome in {"hit", "miss"} else "watching"
        created_at = _parse_dt(payload.get("created_at") or row.get("created_at")) or clock
        evaluated_item = {
            "id": f"outcome:{row.get('event_id') or payload.get('event_id') or _semantic_decision_key(row)}",
            "event_type": "decision_outcome",
            "decision_event_id": row.get("event_id") or payload.get("event_id"),
            "ticker": ticker,
            "asset_name": payload.get("asset_name") or quote.get("name") or ticker,
            "verdict": payload.get("verdict") or payload.get("decision") or payload.get("action") or "vigilar",
            "price_at_decision": entry_price,
            "current_price": current_price,
            "return_pct": round(return_pct, 3),
            "expected_direction": direction,
            "expected_impact": payload.get("expected_impact") or payload.get("impact") or "",
            "outcome_label": outcome,
            "status": status,
            "source": "genesis_performance_tracker",
            "confidence": payload.get("confidence") or row.get("confidence") or "media",
            "created_at": created_at.isoformat(),
            "timestamp": clock.isoformat(),
            "actual_outcome_1h": payload.get("actual_outcome_1h"),
            "actual_outcome_24h": round(return_pct, 3),
            "actual_outcome_7d": payload.get("actual_outcome_7d"),
            "genesis_reading": _outcome_reading(ticker, outcome, return_pct, direction),
        }
        evaluated.append(evaluated_item)
        store.save_outcome_tracking(ticker, evaluated_item, "performance_tracker", evaluated_item["confidence"])

    hits = [item for item in evaluated if item["outcome_label"] == "hit"]
    misses = [item for item in evaluated if item["outcome_label"] == "miss"]
    watching = [item for item in evaluated if item["outcome_label"] == "watching"]
    today_items = [item for item in evaluated if _parse_dt(item.get("created_at")) and _parse_dt(item.get("created_at")).date() == clock.date()]
    today_hits = len([item for item in today_items if item["outcome_label"] == "hit"])
    today_misses = len([item for item in today_items if item["outcome_label"] == "miss"])
    today_watching = len([item for item in today_items if item["outcome_label"] == "watching"])
    scored = len(hits) + len(misses)
    accuracy = round((len(hits) / scored) * 100, 1) if scored else None
    best = max(evaluated, key=lambda item: item["return_pct"], default=None)
    worst = min(evaluated, key=lambda item: item["return_pct"], default=None)
    learning = _learning_lines(len(hits), len(misses), len(watching), missing_price, accuracy)
    answer = _answer(today_hits, today_misses, today_watching, len(hits), len(misses), len(watching), accuracy, missing_price, ignored_rows)
    report = {
        "answer": answer,
        "ticker": ticker_filter,
        "metrics": {
            "total_decisions": len(decisions),
            "priced_decisions": len(evaluated),
            "hits": len(hits),
            "misses": len(misses),
            "watching": len(watching),
            "missing_price": missing_price,
            "ignored_rows": ignored_rows,
            "accuracy": accuracy,
        },
        "today": {
            "hits": today_hits,
            "misses": today_misses,
            "watching": today_watching,
            "total": len(today_items),
        },
        "recent": evaluated[:8],
        "best": best,
        "worst": worst,
        "learning": learning,
        "source": "memory_store + price_truth",
        "generated_at": clock.isoformat(),
    }
    store.save_learned_context(
        f"genesis_performance:last_review:{ticker_filter or 'global'}",
        {
            "metrics": report["metrics"],
            "today": report["today"],
            "learning": learning,
            "generated_at": report["generated_at"],
        },
        "performance_tracker",
        "media",
    )
    store.save_event(
        "performance_review",
        {
            "message": message,
            "metrics": report["metrics"],
            "today": report["today"],
            "ticker": ticker_filter,
        },
        "performance_tracker",
        "media",
    )
    return report


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("payload")
    return payload if isinstance(payload, dict) else {}


def _dedupe_decisions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for row in rows:
        key = _semantic_decision_key(row)
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def _semantic_decision_key(row: dict[str, Any]) -> str:
    payload = _payload(row)
    ticker = normalize_ticker(row.get("ticker") or payload.get("ticker"))
    entry_price = _entry_price(payload)
    if entry_price is not None and entry_price > 0:
        price_bucket = f"{entry_price:.2f}"
    else:
        price_bucket = "no-price"
    return "|".join(
        str(part or "").strip().casefold()
        for part in (
            ticker,
            payload.get("verdict") or payload.get("decision") or payload.get("action"),
            _expected_direction(payload),
            payload.get("support"),
            payload.get("resistance"),
            price_bucket,
        )
    )


def _entry_price(payload: dict[str, Any]) -> float | None:
    direct_candidates = (
        payload.get("price_at_decision"),
        payload.get("decision_price"),
        payload.get("entry_price"),
        payload.get("price"),
        payload.get("current_price"),
        payload.get("confirmed_price"),
    )
    for candidate in direct_candidates:
        parsed = _number(candidate)
        if parsed is not None and parsed > 0:
            return parsed
    for key in ("quote", "snapshot", "metrics", "market"):
        nested = payload.get(key)
        if not isinstance(nested, dict):
            continue
        for nested_key in ("price_at_decision", "current_price", "price", "close", "last_price"):
            parsed = _number(nested.get(nested_key))
            if parsed is not None and parsed > 0:
                return parsed
    return None


def _is_non_asset_ticker(ticker: str) -> bool:
    normalized = normalize_ticker(ticker)
    if not normalized:
        return True
    if normalized in _NON_ASSET_TICKERS:
        return True
    if len(normalized) > 10 and "-" not in normalized and "=" not in normalized and "." not in normalized:
        return True
    return False


def _quote_for(ticker: str, loader: QuoteLoader, cache: dict[str, dict[str, Any]]) -> dict[str, Any]:
    if ticker in cache:
        return cache[ticker]
    try:
        quote = loader(ticker) or {}
    except Exception:
        quote = {}
    cache[ticker] = quote if isinstance(quote, dict) else {}
    return cache[ticker]


def _ticker_from_message(message: str, available_tickers: set[str] | None = None) -> str:
    text = f" {str(message or '').upper()} "
    stopwords = {
        "QUE",
        "TANTO",
        "ESTA",
        "ESTAS",
        "ESTE",
        "COMO",
        "GENESIS",
        "ACIERTOS",
        "FALLOS",
        "PRECISION",
        "RENDIMIENTO",
        "SCORE",
        "HOY",
    }
    for token in text.replace(",", " ").replace("?", " ").split():
        ticker = normalize_ticker(token)
        if available_tickers and ticker not in available_tickers:
            continue
        if ticker and len(ticker) <= 12 and ticker not in stopwords:
            return ticker
    return ""


def _expected_direction(payload: dict[str, Any]) -> str:
    joined = " ".join(
        str(payload.get(key) or "").casefold()
        for key in ("expected_direction", "action", "verdict", "expected_impact", "genesis_reading", "reason")
    )
    if any(token in joined for token in ("bearish", "sell", "vender", "bajista", "reducir", "short", "evitar")):
        return "down"
    if any(token in joined for token in ("bullish", "buy", "comprar", "alcista", "subir", "ruptura", "long")):
        return "up"
    return "watch"


def _outcome_label(direction: str, return_pct: float) -> str:
    threshold = 0.25
    if direction == "up":
        if return_pct >= threshold:
            return "hit"
        if return_pct <= -threshold:
            return "miss"
    if direction == "down":
        if return_pct <= -threshold:
            return "hit"
        if return_pct >= threshold:
            return "miss"
    return "watching"


def _outcome_reading(ticker: str, outcome: str, return_pct: float, direction: str) -> str:
    if outcome == "hit":
        return f"{ticker}: la tesis {direction} va a favor ({return_pct:+.2f}%). Genesis suma acierto y guarda patron."
    if outcome == "miss":
        return f"{ticker}: la tesis {direction} fue en contra ({return_pct:+.2f}%). Genesis lo marca como error para ajustar filtros."
    return f"{ticker}: sigue en vigilancia ({return_pct:+.2f}%). Aun no cuenta como acierto ni fallo."


def _learning_lines(hits: int, misses: int, watching: int, missing_price: int, accuracy: float | None) -> list[str]:
    lines: list[str] = []
    if accuracy is not None:
        lines.append(f"Precision medida: {accuracy:.1f}% sobre decisiones con resultado claro.")
    if misses > hits:
        lines.append("Ajuste: subir exigencia de volumen/nivel antes de elevar conviccion.")
    elif hits > misses and hits:
        lines.append("Ajuste: conservar setups que combinan precio confirmado, nivel e invalidacion clara.")
    if watching:
        lines.append(f"{watching} tesis siguen abiertas; no se cuentan como ganadas ni perdidas todavia.")
    if missing_price:
        lines.append(f"{missing_price} tesis validas quedaron sin score porque no guardaban precio base o precio actual.")
    return lines or ["Aun falta historial suficiente; Genesis seguira guardando decisiones y resultados."]


def _answer(
    today_hits: int,
    today_misses: int,
    today_watching: int,
    hits: int,
    misses: int,
    watching: int,
    accuracy: float | None,
    missing_price: int,
    ignored_rows: int,
) -> str:
    accuracy_text = f"{accuracy:.1f}%" if accuracy is not None else "pendiente"
    ignored_text = f" Ignoro {ignored_rows} registros que no eran activos reales." if ignored_rows else ""
    return (
        f"Genesis ya mide su precision. Hoy: {today_hits} aciertos, {today_misses} fallos y {today_watching} en vigilancia. "
        f"Historial evaluado: {hits} aciertos, {misses} fallos, {watching} abiertas; precision {accuracy_text}. "
        f"{missing_price} lecturas quedaron fuera por falta de precio confirmado.{ignored_text} Lo guardo en memoria para ajustar los filtros diarios."
    )


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace("$", "").replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return None


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None

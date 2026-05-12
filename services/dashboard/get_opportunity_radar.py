from __future__ import annotations

import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any

from app.settings import load_settings
from integrations.fmp.client import FmpClient
from services.genesis.memory_store import MemoryStore
from services.genesis.trading_strategy import build_signal_strategy

_LOGGER = logging.getLogger("genesis.dashboard.opportunities")
_CACHE: dict[str, Any] = {"expires_at": 0.0, "payload": None}
_CACHE_TTL_SECONDS = 55
_DEFAULT_UNIVERSE = (
    "NVDA",
    "MSFT",
    "AAPL",
    "META",
    "AMZN",
    "TSLA",
    "NFLX",
    "AMD",
    "AVGO",
    "PLTR",
    "SMCI",
    "SPY",
    "QQQ",
    "IAU",
    "BNO",
    "BTC-USD",
)


def get_opportunity_radar_snapshot(
    *,
    force_refresh: bool = False,
    limit: int = 12,
    client: FmpClient | None = None,
    store: MemoryStore | None = None,
    settings: Any | None = None,
) -> dict[str, Any]:
    now = time.time()
    if not force_refresh and _CACHE.get("payload") and now < float(_CACHE.get("expires_at") or 0):
        payload = dict(_CACHE["payload"])
        payload["source_status"] = {**(payload.get("source_status") or {}), "cache_hit": True}
        payload["summary"] = {**(payload.get("summary") or {}), "cache_hit": True}
        return payload

    started = time.perf_counter()
    settings = settings or load_settings()
    client = client or FmpClient(settings.fmp_api_key)
    store = store or MemoryStore()
    source_status: dict[str, Any] = {
        "fmp": {
            "key_configured": bool(getattr(settings, "fmp_api_key", "")),
            "live_enabled": bool(getattr(settings, "fmp_live_enabled", False)),
            "quote_ok": False,
            "movers_ok": False,
            "screener_ok": False,
            "news_ok": False,
            "historical_ok": False,
            "last_error_safe": "",
        },
        "memory": {"ok": False, "backend": getattr(store, "backend", "unknown")},
        "cache_hit": False,
    }

    try:
        universe = _build_universe(client, store)
        items = _scan_universe(client, universe, max_items=limit * 2)
        ranked = sorted(items, key=lambda item: float(item.get("opportunity_score") or 0), reverse=True)[: max(1, limit)]
        source_status["fmp"]["quote_ok"] = any(_num(item.get("price")) is not None for item in ranked)
        source_status["fmp"]["news_ok"] = any(int(item.get("news_count") or 0) > 0 for item in ranked)
        source_status["fmp"]["historical_ok"] = any(item.get("support") is not None or item.get("resistance") is not None for item in ranked)
        source_status["fmp"]["movers_ok"] = bool(universe.get("movers"))
        source_status["fmp"]["screener_ok"] = bool(universe.get("screener"))
        _remember_opportunities(store, ranked[:5])
        source_status["memory"]["ok"] = True
    except Exception as exc:
        _LOGGER.warning("Opportunity radar failed", exc_info=True)
        ranked = []
        source_status["fmp"]["last_error_safe"] = _safe_error(exc)

    elapsed_ms = int((time.perf_counter() - started) * 1000)
    summary = _build_summary(ranked)
    payload = {
        "ok": True,
        "kind": "opportunity_radar",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            **summary,
            "cache_hit": False,
            "provider_used": "fmp_live_opportunity_engine",
            "engine_summary": (
                "Genesis cruza FMP movers, screener, quotes, volumen, noticias, earnings y memoria. "
                "El resultado es radar paper: no broker, no compra real."
            ),
        },
        "items": ranked,
        "source_status": {**source_status, "elapsed_ms": elapsed_ms},
    }
    _CACHE["payload"] = payload
    _CACHE["expires_at"] = time.time() + _CACHE_TTL_SECONDS
    return payload


def _build_universe(client: FmpClient, store: MemoryStore) -> dict[str, list[str]]:
    tracked: list[str] = []
    try:
        for row in store.get_tracked_entities(limit=40):
            ticker = str(row.get("ticker") or "").strip().upper()
            if ticker:
                tracked.append(ticker)
    except Exception:
        tracked = []

    movers: list[str] = []
    for kind in ("gainers", "actives", "losers"):
        try:
            for row in client.get_market_movers(kind, limit=12):
                ticker = str(row.get("symbol") or row.get("ticker") or "").strip().upper()
                if ticker:
                    movers.append(ticker)
        except Exception:
            continue

    screener: list[str] = []
    try:
        for row in client.get_company_screener(limit=24, min_market_cap=5_000_000_000):
            ticker = str(row.get("symbol") or row.get("ticker") or "").strip().upper()
            if ticker:
                screener.append(ticker)
    except Exception:
        screener = []

    ordered: list[str] = []
    for ticker in [*tracked, *_DEFAULT_UNIVERSE, *movers, *screener]:
        ticker = _normalize_ticker(ticker)
        if ticker and ticker not in ordered:
            ordered.append(ticker)
    return {
        "symbols": ordered[:30],
        "tracked": tracked,
        "movers": movers,
        "screener": screener,
    }


def _scan_universe(client: FmpClient, universe: dict[str, list[str]], max_items: int) -> list[dict[str, Any]]:
    symbols = universe.get("symbols") or list(_DEFAULT_UNIVERSE)
    earnings_by_symbol = _earnings_map(client)
    batch_quotes = _quote_map(client, symbols)

    def worker(ticker: str) -> dict[str, Any] | None:
        quote = batch_quotes.get(_quote_symbol(ticker)) or client.get_quote(ticker) or {}
        if not quote:
            return None
        history = client.get_historical_eod(ticker, limit=90) or []
        profile = client.get_profile(ticker) or {}
        news = client.get_stock_news(ticker, limit=2) or []
        smart_money = client.get_smart_money_activity(ticker, limit=2) or []
        analyst = client.get_analyst_signal(ticker) or {}
        return _shape_opportunity_item(
            ticker=ticker,
            quote=quote,
            history=history,
            profile=profile,
            news=news,
            smart_money=smart_money,
            analyst=analyst,
            earnings=earnings_by_symbol.get(_quote_symbol(ticker), {}),
        )

    items: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(worker, ticker): ticker for ticker in symbols}
        for future in as_completed(futures):
            try:
                item = future.result()
            except Exception as exc:
                _LOGGER.debug("Opportunity worker failed for %s: %s", futures[future], exc)
                item = None
            if item:
                items.append(item)
            if len(items) >= max_items:
                break
    return items


def _quote_map(client: FmpClient, symbols: list[str]) -> dict[str, dict]:
    rows = client.get_batch_quotes(symbols) if hasattr(client, "get_batch_quotes") else []
    mapped: dict[str, dict] = {}
    for row in rows or []:
        symbol = _quote_symbol(str(row.get("symbol") or row.get("ticker") or ""))
        if symbol:
            mapped[symbol] = row
    return mapped


def _earnings_map(client: FmpClient) -> dict[str, dict]:
    today = datetime.now(timezone.utc).date()
    try:
        rows = client.get_earnings_calendar(today.isoformat(), (today + timedelta(days=21)).isoformat(), limit=200)
    except Exception:
        rows = []
    mapped: dict[str, dict] = {}
    for row in rows or []:
        symbol = _quote_symbol(str(row.get("symbol") or row.get("ticker") or ""))
        if symbol and symbol not in mapped:
            mapped[symbol] = row
    return mapped


def _shape_opportunity_item(
    *,
    ticker: str,
    quote: dict,
    history: list[dict],
    profile: dict,
    news: list[dict],
    smart_money: list[dict],
    analyst: dict,
    earnings: dict,
) -> dict[str, Any]:
    price = _first_num(quote, "price", "current_price")
    change = _first_num(quote, "change", "daily_change")
    change_pct = _first_num(quote, "changesPercentage", "change_pct", "percent_change", "daily_change_pct")
    volume = _first_num(quote, "volume", "vol")
    avg_volume = _first_num(quote, "avgVolume", "avg_volume")
    if avg_volume is None:
        avg_volume = _average([_first_num(row, "volume") for row in history[:30]])
    relative_volume = volume / avg_volume if volume is not None and avg_volume and avg_volume > 0 else None
    dollar_volume = _safe_dollar_volume(ticker, price, volume, _first_num(quote, "dollarVolume", "dollar_volume"))
    levels = _levels_from_history(history, price)
    profile_name = str(profile.get("companyName") or profile.get("company_name") or profile.get("name") or "").strip()
    quote_name = str(quote.get("name") or quote.get("companyName") or "").strip()
    asset_name = _display_name(ticker, profile_name or quote_name or ticker)
    strategy = build_signal_strategy(
        ticker,
        {
            "price": price,
            "change_pct": change_pct,
            "volume": volume,
            "avg_volume": avg_volume,
            "relative_volume": relative_volume,
            "dollar_volume": dollar_volume,
            "support": levels.get("support"),
            "resistance": levels.get("resistance"),
        },
    )
    catalyst_score = _catalyst_score(news, smart_money, analyst, earnings, relative_volume)
    base_score = float(strategy.get("score") or 0)
    opportunity_score = max(0.0, min(100.0, base_score + catalyst_score))
    grade = _grade(opportunity_score)
    decision = _decision_from_score(strategy, opportunity_score, change_pct)
    confidence = _confidence(opportunity_score, relative_volume, news, history)
    item_id = f"opp-{_quote_symbol(ticker)}-{datetime.now(timezone.utc).strftime('%Y%m%d')}"
    what_to_watch = _watch_points(ticker, price, levels, relative_volume, earnings)
    return {
        "id": item_id,
        "ticker": ticker,
        "asset_name": asset_name,
        "price": price,
        "change": change,
        "change_pct": change_pct,
        "volume": volume,
        "avg_volume": avg_volume,
        "relative_volume": relative_volume,
        "dollar_volume": dollar_volume,
        "support": levels.get("support"),
        "resistance": levels.get("resistance"),
        "trend": _trend(change_pct, levels.get("position")),
        "momentum": _momentum(relative_volume, change_pct),
        "catalyst_count": len(news or []) + len(smart_money or []) + (1 if earnings else 0),
        "news_count": len(news or []),
        "smart_money_count": len(smart_money or []),
        "analyst_bias": _analyst_bias(analyst, price),
        "earnings_window": earnings.get("date") or earnings.get("epsDate") or "",
        "opportunity_score": round(opportunity_score, 1),
        "grade": grade,
        "decision": decision["decision"],
        "decision_label_es": decision["label"],
        "confidence": confidence,
        "entry_condition": strategy.get("entry_condition"),
        "invalidation": strategy.get("invalidation"),
        "what_to_watch_es": what_to_watch,
        "genesis_reading_es": _reading(ticker, asset_name, decision["label"], opportunity_score, what_to_watch),
        "source": "fmp_opportunity_engine",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "strategy": {**strategy, "score": round(opportunity_score, 1), "grade": grade},
        "memory_saved": False,
    }


def _remember_opportunities(store: MemoryStore, items: list[dict[str, Any]]) -> None:
    for item in items:
        ticker = str(item.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        payload = {
            "event_type": "opportunity_radar",
            "ticker": ticker,
            "asset_name": item.get("asset_name"),
            "price_at_signal": item.get("price"),
            "change_pct": item.get("change_pct"),
            "volume": item.get("volume"),
            "relative_volume": item.get("relative_volume"),
            "dollar_volume": item.get("dollar_volume"),
            "support": item.get("support"),
            "resistance": item.get("resistance"),
            "expected_direction": item.get("trend"),
            "expected_impact": item.get("decision_label_es"),
            "opportunity_score": item.get("opportunity_score"),
            "status": "watching",
            "genesis_reading": item.get("genesis_reading_es"),
            "created_at": item.get("timestamp"),
        }
        confidence = item.get("confidence") or "media"
        try:
            store.save_signal_event(ticker, payload, source="opportunity_radar", confidence=confidence)
            store.save_hypothesis(ticker, payload, source="opportunity_radar", confidence=confidence)
            store.save_decision_note(
                ticker,
                str(item.get("decision_label_es") or "vigilar"),
                payload,
                source="opportunity_radar",
                confidence=confidence,
            )
            store.save_asset_memory(ticker, payload, source="opportunity_radar", confidence=confidence)
            item["memory_saved"] = True
        except Exception:
            _LOGGER.debug("Opportunity memory save failed for %s", ticker, exc_info=True)


def _build_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    top = items[0] if items else {}
    actionable = [item for item in items if item.get("decision") in {"buy_cautiously", "watch_confirmation"}]
    defensive = [item for item in items if item.get("decision") == "reduce_or_sell_risk"]
    return {
        "count": len(items),
        "actionable_count": len(actionable),
        "defensive_count": len(defensive),
        "top_ticker": top.get("ticker", ""),
        "top_action": top.get("decision_label_es", "sin oportunidad prioritaria"),
        "top_score": top.get("opportunity_score"),
        "watchlist": [item.get("ticker") for item in items[:5]],
    }


def _levels_from_history(history: list[dict], price: float | None) -> dict[str, Any]:
    highs = [_first_num(row, "high", "dayHigh") for row in history[:45]]
    lows = [_first_num(row, "low", "dayLow") for row in history[:45]]
    closes = [_first_num(row, "close", "adjClose", "price") for row in history[:45]]
    highs = [value for value in highs if value is not None and value > 0]
    lows = [value for value in lows if value is not None and value > 0]
    closes = [value for value in closes if value is not None and value > 0]
    support = min(lows or closes) if lows or closes else None
    resistance = max(highs or closes) if highs or closes else None
    position = None
    if price is not None and support is not None and resistance is not None and resistance > support:
        position = (price - support) / (resistance - support)
    return {
        "support": round(support, 4) if support is not None else None,
        "resistance": round(resistance, 4) if resistance is not None else None,
        "position": position,
    }


def _catalyst_score(news: list[dict], smart_money: list[dict], analyst: dict, earnings: dict, relative_volume: float | None) -> float:
    score = 0.0
    if news:
        score += min(8.0, len(news) * 3.0)
    if smart_money:
        score += min(8.0, len(smart_money) * 4.0)
    if analyst:
        score += 4.0
    if earnings:
        score += 5.0
    if relative_volume is not None and relative_volume >= 1.5:
        score += min(10.0, (relative_volume - 1.0) * 6.0)
    return score


def _decision_from_score(strategy: dict, score: float, change_pct: float | None) -> dict[str, str]:
    if score >= 76 and (change_pct is None or change_pct > -1.5):
        return {"decision": "buy_cautiously", "label": "Comprar con cautela"}
    if score >= 62:
        return {"decision": "watch_confirmation", "label": "Vigilar confirmacion"}
    if change_pct is not None and change_pct < -2.5 and score >= 55:
        return {"decision": "reduce_or_sell_risk", "label": "Reducir riesgo"}
    return {"decision": str(strategy.get("decision") or "wait"), "label": str(strategy.get("decision_label_es") or "Esperar")}


def _watch_points(ticker: str, price: float | None, levels: dict[str, Any], relative_volume: float | None, earnings: dict) -> str:
    points: list[str] = []
    resistance = levels.get("resistance")
    support = levels.get("support")
    if resistance is not None:
        points.append(f"ruptura limpia arriba de ${resistance:,.2f}")
    if support is not None:
        points.append(f"perdida de soporte en ${support:,.2f}")
    if relative_volume is not None:
        points.append(f"volumen relativo {relative_volume:.2f}x")
    else:
        points.append("confirmar volumen relativo")
    if earnings:
        points.append(f"earnings cerca de {earnings.get('date') or earnings.get('epsDate')}")
    return f"{ticker}: " + "; ".join(points[:4])


def _reading(ticker: str, asset_name: str, decision_label: str, score: float, watch_points: str) -> str:
    return (
        f"{asset_name} ({ticker}) queda en {decision_label.lower()} con score {score:.0f}/100. "
        f"Genesis no ejecuta compra real; valida precio, volumen y catalizador. Vigilar: {watch_points}."
    )


def _analyst_bias(analyst: dict, price: float | None) -> str:
    target = _first_num(analyst, "targetHigh", "targetMean", "priceTarget", "targetConsensus")
    if target is None or price is None or price <= 0:
        return "sin consenso"
    upside = ((target - price) / price) * 100
    if upside >= 12:
        return "alcista"
    if upside <= -8:
        return "bajista"
    return "neutral"


def _confidence(score: float, relative_volume: float | None, news: list[dict], history: list[dict]) -> str:
    if score >= 76 and (relative_volume or 0) >= 1.1 and history:
        return "alta"
    if score >= 60 and (news or history):
        return "media"
    return "baja"


def _trend(change_pct: float | None, position: float | None) -> str:
    if change_pct is not None and change_pct >= 1.0:
        return "alcista"
    if change_pct is not None and change_pct <= -1.0:
        return "bajista"
    if position is not None and position >= 0.75:
        return "cerca de resistencia"
    if position is not None and position <= 0.25:
        return "cerca de soporte"
    return "neutral"


def _momentum(relative_volume: float | None, change_pct: float | None) -> str:
    if relative_volume is not None and relative_volume >= 1.8:
        return "volumen inusual"
    if change_pct is not None and abs(change_pct) >= 2.5:
        return "movimiento fuerte"
    return "normal"


def _grade(score: float) -> str:
    if score >= 80:
        return "A"
    if score >= 68:
        return "B"
    if score >= 55:
        return "C"
    return "D"


def _safe_dollar_volume(ticker: str, price: float | None, volume: float | None, direct: float | None) -> float | None:
    limit = 1_000_000_000_000
    if direct is not None:
        return direct if 0 < direct <= limit else None
    if price is None or volume is None:
        return None
    computed = price * volume
    if ticker.upper().endswith("-USD") and computed > limit:
        return volume if 0 < volume <= limit else None
    return computed if 0 < computed <= limit else None


def _first_num(row: dict, *keys: str) -> float | None:
    for key in keys:
        value = _num(row.get(key) if isinstance(row, dict) else None)
        if value is not None:
            return value
    return None


def _average(values: list[float | None]) -> float | None:
    nums = [value for value in values if value is not None and math.isfinite(value) and value > 0]
    return sum(nums) / len(nums) if nums else None


def _num(value: Any) -> float | None:
    try:
        if value in (None, "", "None"):
            return None
        number = float(value)
        return number if math.isfinite(number) else None
    except Exception:
        return None


def _normalize_ticker(value: str) -> str:
    ticker = str(value or "").strip().upper()
    if ticker == "BTCUSD":
        return "BTC-USD"
    return ticker


def _quote_symbol(value: str) -> str:
    ticker = str(value or "").strip().upper()
    if ticker.endswith("-USD"):
        return ticker.replace("-USD", "USD")
    return ticker


def _display_name(ticker: str, fallback: str) -> str:
    symbol = str(ticker or "").strip().upper()
    if symbol == "BZ=F":
        return "Brent Crude Oil"
    if symbol == "BTC-USD":
        return "Bitcoin"
    return fallback or symbol


def _safe_error(exc: Exception) -> str:
    text = " ".join(str(exc or "").split())
    return text[:180] or exc.__class__.__name__

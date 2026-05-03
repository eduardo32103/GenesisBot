from __future__ import annotations

from typing import Any

from services.genesis.chart_intent import detect_chart_intent
from services.genesis.market_briefing import get_portfolio_briefing
from services.genesis.market_format import format_signed_money, format_signed_percent
from services.genesis.memory_store import MemoryStore
from services.genesis.price_truth import get_verified_market_quote
from services.genesis.time_tool import detect_time_request, get_time_answer
from services.genesis.ticker_parser import extract_tickers_from_prompt, normalize_ticker
from services.genesis.weather_tool import detect_weather_request, get_weather_answer
from services.genesis.whale_learning import learn_whale_events


def route_message(message: str, context: str = "general", ticker: str = "", panel_context: Any | None = None, memory: MemoryStore | None = None) -> dict[str, Any]:
    store = memory or MemoryStore()
    clean = str(message or "").strip()
    tickers = extract_tickers_from_prompt(clean, context=panel_context if isinstance(panel_context, dict) else None)
    explicit_ticker = normalize_ticker(tickers[0] if tickers else ticker)

    if _is_greeting(clean):
        answer = "Hola. Genesis activo. Puedo revisar mercado, cartera, seguimiento, ballenas, alertas, clima o graficas con precio confirmado."
        store.save_event("greeting", {"message": clean}, "genesis", "alta")
        return _payload("greeting", answer, tickers, memory=store)

    if detect_time_request(clean):
        time_payload = get_time_answer()
        store.save_event("time_request", {"message": clean, "timezone": time_payload["timezone"]}, "time", "alta")
        return _payload("time", time_payload["answer"], [], extra={"time": time_payload}, memory=store)

    if detect_weather_request(clean):
        weather = get_weather_answer(clean)
        store.save_event("weather_request", {"message": clean, "city": weather.get("city"), "source": weather.get("source")}, "weather", "media")
        return _payload("weather", weather["answer"], tickers, extra={"weather": weather}, memory=store)

    if _mentions_portfolio(clean):
        briefing = get_portfolio_briefing()
        store.save_event("portfolio_briefing", {"summary": briefing["answer"]}, "portfolio", "media")
        return _payload("portfolio", briefing["answer"], tickers, extra={"portfolio": briefing}, memory=store)

    if _mentions_whales(clean):
        learned = learn_whale_events(explicit_ticker or None, memory=store)
        return _payload("whales", learned["answer"], tickers, extra={"whales": learned}, memory=store)

    if len(tickers) >= 2 and _mentions_comparison(clean):
        quotes = [get_verified_market_quote(item) for item in tickers[:2]]
        store.save_event("comparison", {"tickers": tickers[:2], "quotes": [_safe_quote_memory(item) for item in quotes]}, "price_truth", "media")
        return _payload("comparison", _comparison_answer(quotes), tickers[:2], extra={"quotes": quotes}, memory=store)

    chart = detect_chart_intent(clean, context=panel_context if isinstance(panel_context, dict) else None)
    if chart["is_chart"]:
        if not chart["ticker"]:
            return _payload("chart", "Que activo quieres revisar?", tickers, extra={"chart": chart}, memory=store)
        quote = get_verified_market_quote(chart["ticker"])
        store.save_event("chart_request", {"ticker": chart["ticker"], "range": chart["range"], "quote": _safe_quote_memory(quote)}, "chart", "alta" if quote.get("current_price") else "baja")
        answer = _chart_answer(chart["ticker"], quote)
        return _payload("chart", answer, [chart["ticker"], *[item for item in tickers if item != chart["ticker"]]], extra={"chart": chart, "quote": quote}, memory=store)

    if explicit_ticker:
        quote = get_verified_market_quote(explicit_ticker)
        technical = _technical_payload(explicit_ticker) if _mentions_technical(clean) else None
        store.save_event("ticker_analysis", {"ticker": explicit_ticker, "quote": _safe_quote_memory(quote), "technical_requested": bool(technical)}, "price_truth", "alta" if quote.get("current_price") else "baja")
        extra = {"quote": quote}
        if technical:
            extra["technical"] = technical
        return _payload("ticker_analysis", _ticker_answer(explicit_ticker, quote, technical), tickers or [explicit_ticker], extra=extra, memory=store)

    answer = "Puedo ayudarte con mercado, cartera, seguimiento, ballenas, alertas, clima o una grafica. Dime el activo o el tema que quieres revisar."
    store.save_event("general_question", {"message": clean}, "genesis", "media")
    return _payload("general", answer, tickers, memory=store)


def _payload(intent: str, answer: str, tickers: list[str], *, extra: dict[str, Any] | None = None, memory: MemoryStore) -> dict[str, Any]:
    payload = {
        "ok": True,
        "status": "genesis_intelligence_ready",
        "intent": intent,
        "answer": answer,
        "tickers": tickers,
        "memory": {
            "backend": memory.backend,
            "recent_events": memory.get_recent_events(5),
            "durable_on_railway": memory.backend == "postgres",
        },
        "source_policy": "Los precios salen de FMP, snapshot validado o referencia paper. Genesis no inventa precios.",
    }
    if extra:
        payload.update(extra)
    return payload


def _chart_answer(ticker: str, quote: dict[str, Any]) -> str:
    if not quote.get("current_price"):
        return f"{ticker}: no tengo precio confirmado para ese activo. Puedo mostrar la grafica solo si FMP devuelve OHLC suficiente."
    change = format_signed_money(quote.get("daily_change"))
    pct = format_signed_percent(quote.get("daily_change_pct"))
    return (
        f"{ticker}: precio confirmado {quote.get('formatted_price')} ({change}, {pct}). "
        "Cargo velas japonesas con retornos por temporalidad. La lectura usa datos confirmados, no precios inventados."
    )


def _ticker_answer(ticker: str, quote: dict[str, Any], technical: dict[str, Any] | None = None) -> str:
    if not quote.get("current_price"):
        return f"{ticker}: no tengo precio confirmado para ese activo."
    answer = (
        f"{ticker}: {quote.get('formatted_price')} confirmado por {quote.get('source_label')}. "
        f"Cambio diario {format_signed_money(quote.get('daily_change'))} / {format_signed_percent(quote.get('daily_change_pct'))}. "
        "Veredicto: vigilar con contexto; entrada solo con confirmacion de precio, volumen y riesgo."
    )
    if technical and technical.get("ok"):
        indicators = technical.get("indicators") or {}
        answer += (
            f" Indicadores pedidos: RSI {indicators.get('rsi')}, "
            f"MACD {indicators.get('macd', {}).get('line')}, "
            f"soporte {indicators.get('support')}, resistencia {indicators.get('resistance')}, "
            f"golden pocket {indicators.get('golden_pocket')}."
        )
    return answer


def _comparison_answer(quotes: list[dict[str, Any]]) -> str:
    parts = []
    for quote in quotes:
        ticker = quote.get("ticker") or "Activo"
        if not quote.get("current_price"):
            parts.append(f"{ticker}: sin precio confirmado")
        else:
            parts.append(f"{ticker}: {quote.get('formatted_price')} ({format_signed_percent(quote.get('daily_change_pct'))})")
    return "Comparacion con precio confirmado: " + " | ".join(parts) + ". No uso precios inventados."


def _technical_payload(ticker: str) -> dict[str, Any]:
    try:
        from services.dashboard.get_asset_chart_series import get_asset_chart_series

        chart = get_asset_chart_series(ticker, "1Y")
        return {
            "ok": bool(chart.get("ok")),
            "ticker": chart.get("ticker") or ticker,
            "range": chart.get("range") or "1Y",
            "indicators": chart.get("indicators") or {},
        }
    except Exception as exc:
        return {"ok": False, "ticker": ticker, "range": "1Y", "message": str(exc)}


def _safe_quote_memory(quote: dict[str, Any]) -> dict[str, Any]:
    return {
        "ticker": quote.get("ticker"),
        "current_price": quote.get("current_price"),
        "previous_close": quote.get("previous_close"),
        "daily_change_pct": quote.get("daily_change_pct"),
        "source": quote.get("source"),
        "is_live": quote.get("is_live"),
        "sanity": quote.get("sanity"),
    }


def _is_greeting(message: str) -> bool:
    text = str(message or "").casefold().strip(" .!?")
    return text in {"hola", "buen dia", "buenos dias", "buenas", "hey", "hello"}


def _mentions_portfolio(message: str) -> bool:
    text = str(message or "").casefold()
    return any(token in text for token in ("cartera", "portfolio", "posiciones", "paper"))


def _mentions_whales(message: str) -> bool:
    text = str(message or "").casefold()
    return any(token in text for token in ("ballena", "ballenas", "dinero grande", "smart money"))


def _mentions_comparison(message: str) -> bool:
    text = str(message or "").casefold()
    return any(token in text for token in ("contra", "versus", " vs ", "compara", "comparar"))


def _mentions_technical(message: str) -> bool:
    text = str(message or "").casefold()
    return any(
        token in text
        for token in (
            "rsi",
            "macd",
            "fibonacci",
            "fib",
            "indicador",
            "indicadores",
            "media movil",
            "medias moviles",
            "ema",
            "sma",
            "vwap",
            "bollinger",
            "atr",
        )
    )

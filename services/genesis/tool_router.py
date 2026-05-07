from __future__ import annotations

from typing import Any

from services.genesis.agent_router import AgentRouter
from services.genesis.alerts_agent import get_alerts_agent
from services.genesis.llm_orchestrator import get_llm_orchestrator
from services.genesis.market_format import format_signed_money, format_signed_percent
from services.genesis.memory_store import MemoryStore
from services.genesis.news_macro_agent import get_news_macro_agent
from services.genesis.portfolio_agent import get_portfolio_agent
from services.genesis.price_agent import get_price_agent
from services.genesis.response_composer import get_response_composer
from services.genesis.technical_agent import get_technical_agent
from services.genesis.time_tool import detect_date_request, detect_time_request, get_date_answer, get_time_answer
from services.genesis.ticker_parser import normalize_ticker
from services.genesis.tracking_agent import get_tracking_agent
from services.genesis.weather_agent import get_weather_agent
from services.genesis.weather_tool import detect_weather_request
from services.genesis.whale_agent import get_whale_agent


def route_message(
    message: str,
    context: str = "general",
    ticker: str = "",
    panel_context: Any | None = None,
    memory: MemoryStore | None = None,
    conversation_id: str = "default",
) -> dict[str, Any]:
    store = memory or MemoryStore()
    clean = str(message or "").strip()
    clean_conversation_id = str(conversation_id or "default").strip()[:120] or "default"
    route = AgentRouter().route(clean, context=panel_context if isinstance(panel_context, dict) else None, ticker=ticker)
    if clean:
        store.save_message(clean_conversation_id, "user", clean, {"context": context, "intent": route.intent})
        store.save_recent_topic(route.intent, {"message": clean, "tickers": route.tickers})
    tickers = route.tickers
    explicit_ticker = normalize_ticker(route.primary_ticker)
    price_agent = get_price_agent()
    composer = get_response_composer()

    if route.intent == "greeting":
        answer = composer.greeting()
        store.save_event("greeting", {"message": clean}, "genesis", "alta")
        return _payload("greeting", answer, tickers, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "time" or detect_time_request(clean):
        time_payload = get_time_answer()
        store.save_event("time_request", {"message": clean, "timezone": time_payload["timezone"]}, "time", "alta")
        return _payload("time", time_payload["answer"], [], extra={"time": time_payload}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "date" or detect_date_request(clean):
        date_payload = get_date_answer()
        store.save_event("date_request", {"message": clean, "timezone": date_payload["timezone"], "date": date_payload["date"]}, "time", "alta")
        return _payload("date", date_payload["answer"], [], extra={"date": date_payload}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "weather" or detect_weather_request(clean):
        weather = get_weather_agent().answer(clean)
        store.save_event("weather_request", {"message": clean, "city": weather.get("city"), "source": weather.get("source")}, "weather", "media")
        return _payload("weather", weather["answer"], [], extra={"weather": weather}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "daily_briefing":
        briefing = get_news_macro_agent().daily_briefing(clean)
        store.save_event("daily_briefing", {"summary": briefing["answer"]}, "macro", "media")
        structured = composer.market_briefing(briefing)
        return _payload("daily_briefing", briefing["answer"], [], extra={"briefing": briefing, "structured": structured, "kind": structured["kind"]}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "market_overview":
        overview = get_news_macro_agent().market_overview(clean)
        store.save_event("market_overview", {"summary": overview["answer"]}, "macro", "media")
        structured = composer.market_briefing(overview)
        return _payload("market_overview", overview["answer"], [], extra={"overview": overview, "structured": structured, "kind": structured["kind"]}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "macro_news":
        overview = get_news_macro_agent().market_overview(clean)
        store.save_event("news_brief", {"summary": overview["answer"], "news_count": len(overview.get("news") or [])}, "macro", "media")
        structured = composer.news_brief(overview)
        return _payload(
            "macro_news",
            overview["answer"],
            tickers,
            extra={"overview": overview, "briefing": overview, "structured": structured, "kind": structured["kind"]},
            memory=store,
            prompt=clean,
            conversation_id=clean_conversation_id,
        )

    if route.intent == "portfolio_summary":
        briefing = get_portfolio_agent().summary()
        store.save_event("portfolio_briefing", {"summary": briefing["answer"]}, "portfolio", "media")
        return _payload("portfolio_summary", briefing["answer"], tickers, extra={"portfolio": briefing}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "tracking_summary":
        tracking = get_tracking_agent().summary()
        store.save_event("tracking_summary", {"count": len(tracking.get("items", []))}, "tracking", "media")
        return _payload("tracking_summary", tracking["answer"], tickers, extra={"tracking": tracking}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "memory_query":
        memory_summary = store.get_memory_summary(clean)
        answer = _memory_answer(memory_summary)
        store.save_event("memory_query", {"message": clean}, "memory", "media")
        return _payload("memory_query", answer, [], extra={"memory_summary": memory_summary}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "whale_activity":
        learned = get_whale_agent().activity(explicit_ticker or None, memory=store)
        return _payload("whale_activity", learned["answer"], tickers, extra={"whales": learned}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "alerts":
        alerts = get_alerts_agent().summary()
        store.save_event("alerts_summary", {"count": len(alerts.get("items", [])), "answer": alerts.get("answer")}, "alerts", "media")
        return _payload("alerts", alerts["answer"], [], extra={"alerts": alerts}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "comparison":
        quotes = [price_agent.quote(item) for item in tickers[:2]]
        store.save_event("comparison", {"tickers": tickers[:2], "quotes": [_safe_quote_memory(item) for item in quotes]}, "price_truth", "media")
        for item in tickers[:2]:
            store.track_entity(item, "asset", {"reason": "comparison"})
        return _payload("comparison", _comparison_answer(quotes), tickers[:2], extra={"quotes": quotes}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "chart_request":
        chart = route.chart or {"is_chart": True, "ticker": "", "range": "1Y"}
        if not chart["ticker"]:
            return _payload("chart_request", "Que activo quieres revisar?", tickers, extra={"chart": chart}, memory=store, prompt=clean, conversation_id=clean_conversation_id)
        quote = price_agent.quote(chart["ticker"])
        store.save_event("chart_request", {"ticker": chart["ticker"], "range": chart["range"], "quote": _safe_quote_memory(quote)}, "chart", "alta" if quote.get("current_price") else "baja")
        store.track_entity(chart["ticker"], "asset", {"reason": "chart_request", "range": chart["range"]})
        store.save_learned_context(f"asset_interest:{chart['ticker']}", {"ticker": chart["ticker"], "last_intent": "chart_request"}, "genesis", "media")
        technical = get_technical_agent().for_ticker(chart["ticker"], chart["range"])
        answer = _chart_answer(chart["ticker"], quote, chart.get("overlays") or [])
        structured = composer.asset_analysis(chart["ticker"], quote=quote, technical=technical)
        extra = {"chart": chart, "quote": quote, "technical": technical, "structured": structured, "kind": structured["kind"]}
        return _payload("chart_request", answer, [chart["ticker"], *[item for item in tickers if item != chart["ticker"]]], extra=extra, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent in {"ticker_analysis", "technical_indicators"} and explicit_ticker:
        quote = price_agent.quote(explicit_ticker)
        technical = get_technical_agent().for_ticker(explicit_ticker, "1Y")
        store.save_event("ticker_analysis", {"ticker": explicit_ticker, "quote": _safe_quote_memory(quote), "technical_requested": bool(technical)}, "price_truth", "alta" if quote.get("current_price") else "baja")
        store.track_entity(explicit_ticker, "asset", {"reason": route.intent})
        store.save_learned_context(f"asset_interest:{explicit_ticker}", {"ticker": explicit_ticker, "last_intent": route.intent}, "genesis", "media")
        structured = composer.asset_analysis(explicit_ticker, quote=quote, technical=technical)
        extra = {"quote": quote, "technical": technical, "structured": structured, "kind": structured["kind"]}
        return _payload(route.intent, _ticker_answer(explicit_ticker, quote, technical), tickers or [explicit_ticker], extra=extra, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    answer = composer.general()
    store.save_event("general_question", {"message": clean}, "genesis", "media")
    return _payload("general", answer, tickers, memory=store, prompt=clean, conversation_id=clean_conversation_id)


def _payload(
    intent: str,
    answer: str,
    tickers: list[str],
    *,
    extra: dict[str, Any] | None = None,
    memory: MemoryStore,
    prompt: str = "",
    conversation_id: str = "default",
) -> dict[str, Any]:
    response_type = _response_type_for_intent(intent)
    memory_context = memory.get_memory_summary(answer)
    llm_result = get_llm_orchestrator().compose(
        prompt or answer,
        {
            "intent": intent,
            "response_type": response_type,
            "tickers": tickers,
            "deterministic_answer": answer,
            "data": extra or {},
            "memory": memory_context,
            "source_policy": "verified_backend_only",
        },
        answer,
    )
    answer = llm_result["answer"]
    memory.save_message(conversation_id, "assistant", answer, {"intent": intent, "tickers": tickers})
    payload = {
        "ok": True,
        "status": "genesis_intelligence_ready",
        "intent": intent,
        "response_type": response_type,
        "answer": answer,
        "tickers": tickers,
        "memory": {
            "backend": memory.backend,
            "recent_events": memory_context["recent_events"][:5],
            "recent_messages": memory_context["recent_messages"][-5:],
            "tracked_entities": memory_context["tracked_entities"][:5],
            "durable_on_railway": memory.backend == "postgres",
        },
        "llm": {"used": llm_result["used_llm"], "reason": llm_result["reason"]},
        "source_policy": "Los precios salen de FMP, snapshot validado o referencia paper. Genesis no inventa precios.",
    }
    if extra:
        payload.update(extra)
    return payload


def _response_type_for_intent(intent: str) -> str:
    return {
        "daily_briefing": "market_summary",
        "market_overview": "market_summary",
        "ticker_analysis": "asset_analysis",
        "technical_indicators": "asset_analysis",
        "chart_request": "chart_analysis",
        "comparison": "comparison",
        "weather": "weather",
        "alerts": "alerts_digest",
        "whale_activity": "whale_flow",
        "macro_news": "news_brief",
        "portfolio_summary": "general_assistant",
        "tracking_summary": "general_assistant",
        "image_chart_analysis": "chart_analysis",
    }.get(intent, "general_assistant")


def _chart_answer(ticker: str, quote: dict[str, Any], overlays: list[str] | None = None) -> str:
    if not quote.get("current_price"):
        return (
            f"{ticker}: no tengo precio confirmado para ese activo en la fuente activa. "
            "No doy precio ni entrada operativa sin confirmacion; puedo revisar velas, retornos o contexto si FMP devuelve OHLC suficiente."
        )
    change = format_signed_money(quote.get("daily_change"))
    pct = format_signed_percent(quote.get("daily_change_pct"))
    overlay_text = f" Incluyo indicadores solicitados: {', '.join(overlays)}." if overlays else ""
    return (
        f"{ticker}: precio confirmado {quote.get('formatted_price')} ({change}, {pct}). "
        "Cargo velas japonesas con retornos por temporalidad. La lectura usa datos confirmados, no precios inventados."
        f"{overlay_text}"
    )


def _ticker_answer(ticker: str, quote: dict[str, Any], technical: dict[str, Any] | None = None) -> str:
    if not quote.get("current_price"):
        return (
            f"{ticker}: no tengo precio confirmado en FMP o snapshot validado. "
            "Lectura: no conviene tomar decision con dato incompleto. Siguiente paso: reconfirmar fuente, revisar chart OHLC y esperar precio directo."
        )
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


def _memory_answer(memory_summary: dict[str, Any]) -> str:
    entities = [item.get("ticker") for item in memory_summary.get("tracked_entities", []) if item.get("ticker")]
    topics = [item.get("topic") for item in memory_summary.get("recent_topics", []) if item.get("topic")]
    if not entities and not topics:
        return "Todavia tengo poca memoria util. A partir de tus analisis, graficas y cartera voy guardando activos, temas y preferencias sin guardar secretos."
    entity_text = ", ".join(entities[:5]) if entities else "sin activos recurrentes todavia"
    topic_text = ", ".join(topics[:4]) if topics else "sin temas recientes claros"
    return f"Recuerdo como contexto reciente: activos {entity_text}. Temas: {topic_text}. Uso esta memoria como apoyo, no como fuente de precios."


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

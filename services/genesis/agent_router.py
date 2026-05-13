from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from typing import Any

from services.genesis.chart_intent import detect_chart_intent
from services.genesis.ticker_parser import extract_tickers_from_prompt, normalize_ticker
from services.genesis.time_tool import detect_date_request, detect_time_request
from services.genesis.weather_tool import detect_weather_request


@dataclass(frozen=True)
class AgentRoute:
    intent: str
    agent: str
    tickers: list[str]
    primary_ticker: str
    chart: dict[str, Any] | None = None


class AgentRouter:
    def route(self, message: str, context: object | None = None, ticker: str = "") -> AgentRoute:
        clean = str(message or "").strip()
        tickers = extract_tickers_from_prompt(clean, context=context)
        primary_ticker = normalize_ticker(tickers[0] if tickers else ticker)
        chart = detect_chart_intent(clean, context=context)
        intent = self.detect_intent(clean, tickers=tickers, chart=chart)
        if intent == "memory_query":
            tickers = [item for item in tickers if item not in {"CONSULTAS", "RECIENTES", "ACTIVOS", "MEMORIA", "APRENDISTE"}]
            primary_ticker = normalize_ticker(tickers[0] if tickers else ticker)
        if intent in {"greeting", "time", "date", "weather", "daily_briefing", "market_overview", "opportunities", "general_question"}:
            tickers = []
            primary_ticker = ""
        if intent in {"whale_activity", "alerts", "macro_news"} and not tickers:
            primary_ticker = ""
        return AgentRoute(
            intent=intent,
            agent=_AGENT_BY_INTENT.get(intent, "response_composer"),
            tickers=tickers,
            primary_ticker=primary_ticker,
            chart=chart if chart.get("is_chart") else None,
        )

    def detect_intent(self, message: str, *, tickers: list[str] | None = None, chart: dict[str, Any] | None = None) -> str:
        text = _fold(message).strip(" .!?")
        tickers = tickers or []
        chart = chart or {}
        if text in {"hola", "buen dia", "buenos dias", "buenas", "hey", "hello"}:
            return "greeting"
        if _mentions_personal_support(text):
            return "general_question"
        if _mentions_performance_review(text):
            return "performance_review"
        if _mentions_casual_chat(text):
            return "greeting"
        if detect_time_request(message):
            return "time"
        if detect_date_request(message):
            return "date"
        if detect_weather_request(message):
            return "weather"
        if "imagen" in text or "foto" in text:
            return "image_chart_analysis"
        if _mentions_daily_briefing(text):
            return "daily_briefing"
        if _mentions_market_overview(text):
            return "market_overview"
        if _mentions_portfolio(text):
            return "portfolio_summary"
        if _mentions_tracking(text):
            return "tracking_summary"
        if _mentions_memory(text):
            return "memory_query"
        if _mentions_whales(text):
            return "whale_activity"
        if _mentions_alerts(text):
            return "alerts"
        if _mentions_macro(text):
            return "macro_news"
        if _mentions_opportunities(text) and not tickers:
            return "opportunities"
        if len(tickers) >= 2 and _mentions_comparison(text):
            return "comparison"
        if chart.get("is_chart"):
            return "chart_request"
        if tickers and _mentions_trade_decision(text):
            return "trade_decision"
        if _mentions_technical(text) and tickers:
            return "technical_indicators"
        if tickers:
            return "ticker_analysis"
        return "general_question"


_AGENT_BY_INTENT = {
    "greeting": "response_composer",
    "time": "time_agent",
    "date": "time_agent",
    "weather": "weather_agent",
    "daily_briefing": "news_macro_agent",
    "market_overview": "news_macro_agent",
    "ticker_analysis": "price_agent",
    "chart_request": "chart_agent",
    "image_chart_analysis": "image_chart_agent",
    "portfolio_summary": "portfolio_agent",
    "tracking_summary": "tracking_agent",
    "memory_query": "memory_agent",
    "performance_review": "memory_agent",
    "whale_activity": "whale_agent",
    "alerts": "alerts_agent",
    "macro_news": "news_macro_agent",
    "opportunities": "opportunity_agent",
    "comparison": "price_agent",
    "technical_indicators": "technical_agent",
    "trade_decision": "asset_analysis_agent",
    "general_question": "response_composer",
}


def _mentions_daily_briefing(text: str) -> bool:
    return any(token in text for token in ("resumen del dia", "briefing", "resumen diario"))


def _mentions_market_overview(text: str) -> bool:
    if "mercado libre" in text:
        return False
    return (
        ("mercado" in text and not any(token in text for token in ("seguimiento", "cartera")))
        or "como esta el mercado" in text
        or "como va el mercado" in text
        or "mercado el dia de hoy" in text
        or "mercado hoy" in text
        or "que esta pasando hoy" in text
        or "viernes pasado" in text
    )


def _mentions_casual_chat(text: str) -> bool:
    return any(
        token in text
        for token in (
            "como estas",
            "como vas",
            "que tal",
            "todo bien",
            "estas listo",
            "estas activa",
            "estas funcionando",
            "como te sientes",
            "buenas tardes",
            "buenas noches",
        )
    )


def _mentions_personal_support(text: str) -> bool:
    personal_terms = (
        "mi novia",
        "mi novio",
        "mi esposa",
        "mi esposo",
        "mi pareja",
        "relacion",
        "enojada",
        "enojado",
        "molesta",
        "molesto",
        "triste",
        "ansioso",
        "ansiosa",
        "necesito consejo",
        "dame consejo",
        "problema personal",
        "que le digo",
        "como le digo",
        "disculparme",
        "pedir perdon",
    )
    return any(token in text for token in personal_terms)


def _mentions_portfolio(text: str) -> bool:
    return any(token in text for token in ("cartera", "portfolio", "posiciones", "paper"))


def _mentions_tracking(text: str) -> bool:
    return any(token in text for token in ("seguimiento", "watchlist", "vigilancia"))


def _mentions_memory(text: str) -> bool:
    return any(token in text for token in ("recuerdame", "que vimos", "que hicimos", "que sabes", "aprendiste", "consultas recientes", "activos reviso"))


def _mentions_performance_review(text: str) -> bool:
    return any(
        token in text
        for token in (
            "acertando",
            "aciertos",
            "fallos",
            "fallaste",
            "equivocaste",
            "equivocaciones",
            "precision de genesis",
            "rendimiento de genesis",
            "score de genesis",
            "que tanto aciertas",
            "que tan bien vas",
            "alertas funcionaron",
            "senales funcionaron",
            "aprende de tus errores",
            "aprendiendo de tus errores",
            "como va genesis",
            "como vamos genesis",
        )
    )


def _mentions_whales(text: str) -> bool:
    return any(
        token in text
        for token in (
            "ballena",
            "ballenas",
            "ballnea",
            "ballneas",
            "balena",
            "balenas",
            "dinero grande",
            "smart money",
            "flujo institucional",
            "flujo de ballenas",
            "whale",
            "whales",
        )
    )


def _mentions_alerts(text: str) -> bool:
    return "alerta" in text or "alertas" in text


def _mentions_macro(text: str) -> bool:
    return any(token in text for token in ("macro", "noticia", "noticias"))


def _mentions_opportunities(text: str) -> bool:
    return any(
        token in text
        for token in (
            "oportunidad",
            "oportunidades",
            "buen precio",
            "buenos precios",
            "acciones buenas",
            "que hay para comprar",
            "que puedo comprar",
            "que podemos comprar",
            "que podria comprar",
            "que hay bueno para comprar",
            "que compro",
            "que comprar",
            "que deberia comprar",
            "comprar con cautela",
            "compra con cautela",
            "ideas de compra",
            "ideas para comprar",
            "lista de compra",
            "watchlist de compra",
            "oportunidades de compra",
            "oportunidades para comprar",
            "donde hay compra",
            "donde hay entrada",
            "comprar hoy",
            "compra hoy",
            "que acciones compro",
            "que activo compro",
            "buena validacion",
            "entrada validada",
            "entradas validas",
            "buen setup",
            "setups de compra",
            "oportunidad de entrada",
            "cazar",
            "caza",
            "cazame",
            "aguila",
            "setup",
            "setups",
        )
    )


def _mentions_comparison(text: str) -> bool:
    return any(token in text for token in ("contra", "versus", " vs ", "compara", "comparar"))


def _mentions_trade_decision(text: str) -> bool:
    decision_tokens = (
        "deberia comprar",
        "debo comprar",
        "conviene comprar",
        "comprar con",
        "buena idea comprar",
        "seria buena idea comprar",
        "vale la pena comprar",
        "entro a",
        "entrada en",
        "compro",
        "compraria",
        "vender",
        "vendo",
        "salirme",
        "tomar ganancia",
    )
    return any(token in text for token in decision_tokens)


def _mentions_technical(text: str) -> bool:
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


def _fold(value: object) -> str:
    normalized = unicodedata.normalize("NFD", str(value or "").casefold())
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn")

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from services.genesis.chart_intent import detect_chart_intent
from services.genesis.ticker_parser import extract_tickers_from_prompt, normalize_ticker
from services.genesis.time_tool import detect_time_request
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
        return AgentRoute(
            intent=intent,
            agent=_AGENT_BY_INTENT.get(intent, "response_composer"),
            tickers=tickers,
            primary_ticker=primary_ticker,
            chart=chart if chart.get("is_chart") else None,
        )

    def detect_intent(self, message: str, *, tickers: list[str] | None = None, chart: dict[str, Any] | None = None) -> str:
        text = str(message or "").casefold().strip(" .!?")
        tickers = tickers or []
        chart = chart or {}
        if text in {"hola", "buen dia", "buenos dias", "buenas", "hey", "hello"}:
            return "greeting"
        if detect_time_request(message):
            return "time"
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
        if _mentions_whales(text):
            return "whale_activity"
        if _mentions_alerts(text):
            return "alerts"
        if _mentions_macro(text):
            return "macro_news"
        if len(tickers) >= 2 and _mentions_comparison(text):
            return "comparison"
        if _mentions_technical(text) and tickers:
            return "technical_indicators"
        if chart.get("is_chart"):
            return "chart_request"
        if tickers:
            return "ticker_analysis"
        return "general_question"


_AGENT_BY_INTENT = {
    "greeting": "response_composer",
    "time": "time_agent",
    "weather": "weather_agent",
    "daily_briefing": "news_macro_agent",
    "market_overview": "news_macro_agent",
    "ticker_analysis": "price_agent",
    "chart_request": "chart_agent",
    "image_chart_analysis": "image_chart_agent",
    "portfolio_summary": "portfolio_agent",
    "tracking_summary": "tracking_agent",
    "whale_activity": "whale_agent",
    "alerts": "news_macro_agent",
    "macro_news": "news_macro_agent",
    "comparison": "price_agent",
    "technical_indicators": "technical_agent",
    "general_question": "response_composer",
}


def _mentions_daily_briefing(text: str) -> bool:
    return any(token in text for token in ("resumen del dia", "resumen del día", "briefing", "resumen diario"))


def _mentions_market_overview(text: str) -> bool:
    return "mercado" in text and not any(token in text for token in ("seguimiento", "cartera"))


def _mentions_portfolio(text: str) -> bool:
    return any(token in text for token in ("cartera", "portfolio", "posiciones", "paper"))


def _mentions_tracking(text: str) -> bool:
    return any(token in text for token in ("seguimiento", "watchlist", "vigilancia"))


def _mentions_whales(text: str) -> bool:
    return any(token in text for token in ("ballena", "ballenas", "dinero grande", "smart money"))


def _mentions_alerts(text: str) -> bool:
    return "alerta" in text or "alertas" in text


def _mentions_macro(text: str) -> bool:
    return any(token in text for token in ("macro", "noticia", "noticias"))


def _mentions_comparison(text: str) -> bool:
    return any(token in text for token in ("contra", "versus", " vs ", "compara", "comparar"))


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

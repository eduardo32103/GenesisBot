from __future__ import annotations

from services.genesis.ticker_parser import extract_tickers_from_prompt

_CHART_WORDS = (
    "grafica",
    "graficas",
    "grafico",
    "graficos",
    "chart",
    "candles",
    "velas",
    "vela",
)


def detect_chart_intent(message: str, context: object | None = None) -> dict:
    text = str(message or "").casefold()
    wants_chart = any(word in text for word in _CHART_WORDS)
    tickers = extract_tickers_from_prompt(message, context=context)
    return {
        "is_chart": wants_chart,
        "ticker": tickers[0] if tickers else "",
        "tickers": tickers,
        "range": "1Y",
    }


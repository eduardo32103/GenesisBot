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
        "overlays": _detect_overlays(text),
    }


def _detect_overlays(text: str) -> list[str]:
    overlays: list[str] = []
    for token, label in (
        ("sma 20", "SMA20"),
        ("sma 50", "SMA50"),
        ("sma 200", "SMA200"),
        ("ema", "EMA"),
        ("rsi", "RSI"),
        ("macd", "MACD"),
        ("vwap", "VWAP"),
        ("fibonacci", "FIBONACCI"),
        ("fib", "FIBONACCI"),
        ("volumen", "VOLUME"),
    ):
        if token in text and label not in overlays:
            overlays.append(label)
    return overlays

from __future__ import annotations

from typing import Any

from services.dashboard.get_asset_chart_series import get_asset_chart_series
from services.genesis.technical_analysis import compute_technical_indicators


class TechnicalAgent:
    def from_ohlc(self, ohlc: list[dict[str, Any]]) -> dict[str, Any]:
        return compute_technical_indicators(ohlc)

    def for_ticker(self, ticker: str, timeframe: str = "1Y") -> dict[str, Any]:
        chart = get_asset_chart_series(ticker, timeframe=timeframe)
        return {
            "ok": bool(chart.get("ok")),
            "ticker": chart.get("ticker") or ticker,
            "range": chart.get("range") or timeframe,
            "indicators": chart.get("indicators") or {},
        }


def get_technical_agent() -> TechnicalAgent:
    return TechnicalAgent()

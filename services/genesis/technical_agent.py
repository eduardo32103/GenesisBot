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
            "summary": chart.get("summary") or {},
            "returns": chart.get("returns") or {},
            "history_points": chart.get("history_points") or 0,
            "selected_range_points": chart.get("selected_range_points") or 0,
            "source": chart.get("source") or {},
            "chart": {
                "ticker": chart.get("ticker") or ticker,
                "range": chart.get("range") or timeframe,
                "ohlc": chart.get("ohlc") or chart.get("points") or [],
                "points": chart.get("points") or chart.get("ohlc") or [],
                "summary": chart.get("summary") or {},
                "returns": chart.get("returns") or {},
                "price_only": bool(chart.get("price_only") or (chart.get("source") or {}).get("price_only")),
                "source": chart.get("source") or {},
            },
        }


def get_technical_agent() -> TechnicalAgent:
    return TechnicalAgent()

from __future__ import annotations

from typing import Any

from services.dashboard.get_asset_chart_series import get_asset_chart_series


class ChartAgent:
    def get_chart(self, ticker: str, timeframe: str = "1Y") -> dict[str, Any]:
        return get_asset_chart_series(ticker, timeframe=timeframe)


def get_chart_agent() -> ChartAgent:
    return ChartAgent()

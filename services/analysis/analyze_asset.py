from __future__ import annotations

from dataclasses import dataclass

from core.entities.analysis_report import AnalysisReport
from core.ports.market_data_port import MarketDataPort
from core.ports.news_port import NewsPort


@dataclass
class AnalyzeAssetRequest:
    ticker: str
    timeframe: str


class AnalyzeAssetService:
    def __init__(self, market_data: MarketDataPort, news: NewsPort) -> None:
        self.market_data = market_data
        self.news = news

    def execute(self, request: AnalyzeAssetRequest) -> AnalysisReport:
        quote = self.market_data.get_quote(request.ticker)
        news_items = self.news.get_market_news([request.ticker])
        summary = f"Pending migration with {len(news_items)} news items"
        return AnalysisReport(
            ticker=request.ticker,
            timeframe=request.timeframe,
            summary=summary,
            orientation="pending",
            confidence=0.0,
            key_levels={"last_price": float(quote.get("price", 0.0))},
        )

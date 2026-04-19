from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Iterable

import requests


@dataclass
class GoogleNewsClient:
    timeout: int = 5
    language: str = "en"
    country: str = "US"
    session: requests.Session = field(default_factory=requests.Session)

    def get_rss_news(self, queries: Iterable[str], limit: int = 8) -> list[str]:
        all_news: list[str] = []
        for query in queries:
            safe_query = str(query or "").strip()
            if not safe_query:
                continue
            url = f"https://news.google.com/rss/search?q={safe_query}&hl={self.language}-{self.country}&gl={self.country}&ceid={self.country}:{self.language}"
            try:
                response = self.session.get(url, timeout=self.timeout)
                if response.status_code != 200:
                    continue
                root = ET.fromstring(response.text)
                for item in root.findall(".//item"):
                    title = (item.findtext("title") or "").strip()
                    if title and title not in all_news:
                        all_news.append(title)
                        if len(all_news) >= limit:
                            return all_news
            except Exception:
                continue
        return all_news[:limit]


class NewsClient:
    def get_market_news(self, tickers: list[str] | None = None) -> list[dict]:
        return [{"tickers": tickers or [], "title": "pending migration"}]

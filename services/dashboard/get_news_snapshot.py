from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.genesis.news_feed import get_news_source_status, get_recent_market_news
from services.genesis.ticker_parser import normalize_ticker


def get_news_snapshot(limit: int = 24) -> dict[str, Any]:
    focus = _focus_tickers()
    items = get_recent_market_news(focus, limit=limit, max_age_days=30)
    important = [item for item in items if item.get("is_important")][:8]
    latest = sorted(items, key=lambda item: str(item.get("published_at") or ""), reverse=True)[:16]
    mine = [item for item in items if set(item.get("tickers") or item.get("assets") or []) & set(focus)]
    global_items = [item for item in items if not (set(item.get("tickers") or item.get("assets") or []) & set(focus))]
    return {
        "ok": True,
        "kind": "news_snapshot",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "focus_tickers": focus,
        "items": items,
        "important": important,
        "latest": latest,
        "sections": {
            "important": important,
            "latest": latest,
            "mine": mine[:12],
            "global": global_items[:12],
        },
        "source_status": get_news_source_status(),
        "policy": "FMP primero; si no alcanza, RSS publico con timeout y cache. No mezcla alertas ni ballenas como noticias.",
    }


def _focus_tickers() -> list[str]:
    tickers: list[str] = []
    try:
        from services.dashboard.get_radar_snapshot import get_radar_snapshot

        snapshot = get_radar_snapshot()
        for item in snapshot.get("items") or []:
            if not isinstance(item, dict):
                continue
            ticker = normalize_ticker(item.get("ticker") or item.get("symbol"))
            if ticker and ticker not in tickers:
                tickers.append(ticker)
    except Exception:
        pass
    for ticker in ("SPY", "QQQ", "BTC-USD", "BZ=F", "NVDA", "MSFT"):
        if ticker not in tickers:
            tickers.append(ticker)
    return tickers[:12]

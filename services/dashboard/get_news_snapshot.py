from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.genesis.news_feed import get_news_source_status, get_recent_market_news
from services.genesis.ticker_parser import normalize_ticker


def get_news_snapshot(limit: int = 24, *, force_refresh: bool = False) -> dict[str, Any]:
    focus = _focus_tickers()
    items = get_recent_market_news(focus, limit=limit, max_age_days=30, force_refresh=force_refresh)
    important = sorted(
        [item for item in items if item.get("is_important")],
        key=lambda item: (_bucket_rank(item), int(item.get("relevance_score") or 0), int(item.get("published_ts") or _news_ts(item))),
        reverse=True,
    )[:8]
    latest = sorted(items, key=_news_ts, reverse=True)[:16]
    mine = sorted([item for item in items if _touches_focus(item, focus)], key=_news_ts, reverse=True)
    global_items = sorted([item for item in items if not _touches_focus(item, focus)], key=_news_ts, reverse=True)
    recency_windows = {
        "24h": sum(1 for item in items if _news_bucket(item) == "24h"),
        "7d": sum(1 for item in items if _news_bucket(item) in {"24h", "7d"}),
        "30d": sum(1 for item in items if _news_bucket(item) in {"24h", "7d", "30d"}),
    }
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
        "recency_windows": recency_windows,
        "force_refresh": force_refresh,
        "source_status": get_news_source_status(),
        "policy": "FMP primero; si no alcanza, RSS publico con timeout. Últimas se ordena por timestamp real 24h/7d/30d. No mezcla alertas ni ballenas como noticias.",
    }


def _touches_focus(item: dict[str, Any], focus: list[str]) -> bool:
    normalized_focus = {normalize_ticker(ticker) for ticker in focus if normalize_ticker(ticker)}
    tickers = _news_item_tickers(item)
    if tickers & normalized_focus:
        return True
    text = " ".join(
        str(value or "")
        for value in (
            item.get("title"),
            item.get("title_es"),
            item.get("original_title"),
            item.get("summary"),
            item.get("summary_es"),
            item.get("category"),
            item.get("source"),
            " ".join(item.get("asset_names_affected") or []),
        )
    ).lower()
    return any(alias in text for ticker in normalized_focus for alias in _ticker_aliases(ticker))


def _news_item_tickers(item: dict[str, Any]) -> set[str]:
    tickers: set[str] = set()
    for key in ("tickers", "assets", "tickers_affected"):
        values = item.get(key) or []
        if not isinstance(values, list):
            values = [values]
        for value in values:
            ticker = normalize_ticker(value)
            if ticker:
                tickers.add(ticker)
    return tickers


def _ticker_aliases(ticker: str) -> tuple[str, ...]:
    aliases = {
        "BTC": ("bitcoin", "btc", "crypto"),
        "BTC-USD": ("bitcoin", "btc", "crypto"),
        "BZ=F": ("brent", "oil", "crude", "petroleo", "petróleo", "energia", "energy"),
        "BNO": ("brent", "oil", "crude", "petroleo", "petróleo", "energia", "energy"),
        "IXC": ("energy", "energia", "oil", "crude", "petroleo", "petróleo"),
        "IAU": ("gold", "oro", "metales", "refugio"),
        "NVDA": ("nvidia", "nvda", "chip", "semiconductor", "blackwell", "ai", "ia"),
        "MSFT": ("microsoft", "msft", "cloud", "nube", "ai", "ia"),
        "NFLX": ("netflix", "nflx", "streaming"),
        "META": ("meta", "facebook", "instagram", "ai", "ia"),
        "MARA": ("marathon", "mara", "bitcoin", "btc", "crypto", "mineria"),
        "BIP": ("brookfield", "infrastructure", "infraestructura", "bip"),
        "ENH": ("endurance", "enh"),
        "NFE": ("new fortress", "lng", "gas natural", "energy", "energia"),
        "SPY": ("s&p 500", "sp500", "spy", "stock market", "mercado accionario"),
        "QQQ": ("nasdaq", "qqq", "technology", "tecnologia"),
    }
    return aliases.get(ticker, (ticker.lower(),))


def _news_ts(item: dict[str, Any]) -> int:
    direct = item.get("published_ts")
    if isinstance(direct, (int, float)) and direct > 0:
        return int(direct)
    text = str(item.get("published_at") or item.get("publishedDate") or item.get("date") or "").strip()
    if not text:
        return 0
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return int((parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).timestamp())
    except Exception:
        return 0


def _news_bucket(item: dict[str, Any]) -> str:
    bucket = str(item.get("recency_bucket") or "").strip().lower()
    if bucket in {"24h", "7d", "30d"}:
        return bucket
    ts = _news_ts(item)
    if not ts:
        return "unknown"
    age_seconds = max(int(datetime.now(timezone.utc).timestamp()) - ts, 0)
    if age_seconds <= 86_400:
        return "24h"
    if age_seconds <= 7 * 86_400:
        return "7d"
    if age_seconds <= 30 * 86_400:
        return "30d"
    return "old"


def _bucket_rank(item: dict[str, Any]) -> int:
    return {"24h": 3, "7d": 2, "30d": 1}.get(_news_bucket(item), 0)


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

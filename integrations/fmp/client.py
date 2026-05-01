from __future__ import annotations

import logging
import math
import urllib.parse
from dataclasses import dataclass, field

import requests


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value in (None, "", "None"):
            return default
        numeric = float(value)
        return numeric if math.isfinite(numeric) else default
    except Exception:
        return default


def _first_float(quote: dict, keys: tuple[str, ...]) -> float:
    for key in keys:
        value = _safe_float(quote.get(key))
        if value != 0:
            return value
    return 0.0


@dataclass
class FmpClient:
    api_key: str
    logger: logging.Logger | None = None
    session: requests.Session = field(default_factory=requests.Session)
    last_error_by_ticker: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.logger is None:
            self.logger = logging.getLogger("genesis.fmp")

    @staticmethod
    def is_crypto_ticker(ticker: str) -> bool:
        return str(ticker or "").upper().endswith("-USD")

    @staticmethod
    def is_profile_excluded(ticker: str) -> bool:
        tk = str(ticker or "").upper()
        return tk in {"BZ=F", "GC=F"} or tk.endswith("-USD")

    def resolve_symbol(self, ticker: str, symbol_map: dict[str, str] | None = None) -> str:
        tk = str(ticker or "").strip().upper()
        if symbol_map and tk in symbol_map:
            return str(symbol_map[tk]).strip().upper()
        if self.is_crypto_ticker(tk):
            return tk.replace("-USD", "USD")
        return tk

    def _request_json(self, url: str, timeout: int = 10) -> tuple[int, object | None]:
        response = self.session.get(url, timeout=timeout)
        if response.status_code != 200:
            return response.status_code, None
        try:
            return response.status_code, response.json()
        except Exception:
            return response.status_code, None

    @staticmethod
    def _parse_historical_payload(raw: object) -> list[dict]:
        history: list[dict] = []
        if isinstance(raw, list):
            history = raw
        elif isinstance(raw, dict):
            for key in ("historical", "data", "results", "prices"):
                if isinstance(raw.get(key), list):
                    history = raw[key]
                    break

        cleaned = []
        for row in history:
            if not isinstance(row, dict):
                continue
            if row.get("date") or row.get("label"):
                cleaned.append(row)
        cleaned.sort(key=lambda item: str(item.get("date") or item.get("label") or ""), reverse=True)
        return cleaned

    def get_last_error(self, ticker: str) -> str:
        return self.last_error_by_ticker.get(str(ticker or "").upper(), "")

    def get_quote(self, ticker: str, symbol_map: dict[str, str] | None = None) -> dict | None:
        tk = str(ticker or "").strip().upper()
        if not self.api_key:
            self.last_error_by_ticker[tk] = "FMP_API_KEY no detectada."
            return None

        resolved = self.resolve_symbol(tk, symbol_map)
        symbols_to_try = [resolved]
        if self.is_crypto_ticker(tk) or tk in {"BTC", "ETH", "SOL", "MARA", "XRP", "DOGE"}:
            base = tk.replace("-USD", "")
            symbols_to_try = [f"{base}USD", base, tk]
        elif tk == "BZ=F":
            symbols_to_try = ["BZUSD", "BCOUSD"]
        elif tk == "GC=F":
            symbols_to_try = ["GCUSD", "XAUUSD"]

        for symbol in symbols_to_try:
            url = f"https://financialmodelingprep.com/stable/quote?symbol={urllib.parse.quote(symbol)}&apikey={self.api_key}"
            try:
                status, payload = self._request_json(url, timeout=10)
                self.logger.debug("FMP quote status %s para %s", status, symbol)
                if status in (401, 403):
                    self.last_error_by_ticker[tk] = f"{status} - Key rechazada o plan insuficiente"
                    return None
                if status != 200 or payload is None:
                    continue

                if isinstance(payload, list):
                    quote = payload[0] if payload else {}
                elif isinstance(payload, dict):
                    quote = payload
                else:
                    quote = {}

                price = _safe_float(quote.get("price"))
                if price <= 0:
                    continue

                result = {
                    "price": price,
                    "vol": _safe_float(quote.get("volume")),
                    "volume": _safe_float(quote.get("volume")),
                    "avgVolume": _safe_float(quote.get("avgVolume")),
                    "change": _safe_float(quote.get("change")),
                    "changesPercentage": _safe_float(quote.get("changesPercentage")),
                    "pe": _safe_float(quote.get("pe")),
                    "marketCap": _safe_float(quote.get("marketCap")),
                    "name": quote.get("name") or quote.get("companyName") or "",
                    "open": _safe_float(quote.get("open")),
                    "dayHigh": _safe_float(quote.get("dayHigh") or quote.get("high")),
                    "dayLow": _safe_float(quote.get("dayLow") or quote.get("low")),
                    "previousClose": _safe_float(quote.get("previousClose")),
                    "extendedHoursPrice": _first_float(quote, ("extendedHoursPrice", "afterHoursPrice", "postMarketPrice", "preMarketPrice")),
                    "extendedHoursChange": _first_float(quote, ("extendedHoursChange", "afterHoursChange", "postMarketChange", "preMarketChange")),
                    "extendedHoursChangePct": _first_float(quote, ("extendedHoursChangePct", "afterHoursChangePercent", "postMarketChangePercent", "preMarketChangePercent")),
                    "marketSession": quote.get("marketSession") or quote.get("session") or quote.get("marketState") or "",
                    "timestamp": quote.get("timestamp") or quote.get("lastUpdated") or quote.get("date") or "",
                }
                self.last_error_by_ticker.pop(tk, None)
                return result
            except Exception as exc:
                self.logger.error("FMP error fetching %s: %s", symbol, exc)

        self.last_error_by_ticker[tk] = "Activo no encontrado en FMP"
        return None

    def get_historical_eod(self, ticker: str, limit: int | None = None, symbol_map: dict[str, str] | None = None) -> list[dict] | None:
        tk = str(ticker or "").strip().upper()
        if not self.api_key:
            self.last_error_by_ticker[tk] = "FMP_API_KEY no detectada."
            return None

        symbol = self.resolve_symbol(tk, symbol_map)
        endpoints = [
            ("stable", f"https://financialmodelingprep.com/stable/historical-price-eod/full?symbol={urllib.parse.quote(symbol)}&apikey={self.api_key}"),
            ("legacy", f"https://financialmodelingprep.com/api/v3/historical-price-full/{urllib.parse.quote(symbol)}?apikey={self.api_key}"),
        ]
        last_status = None

        for endpoint_name, url in endpoints:
            try:
                status, payload = self._request_json(url, timeout=10)
                last_status = status
                self.logger.debug("FMP historical %s status %s para %s", endpoint_name, status, tk)
                if status != 200 or payload is None:
                    continue
                rows = self._parse_historical_payload(payload)
                if rows:
                    self.last_error_by_ticker.pop(tk, None)
                    return rows[: int(limit)] if limit else rows
            except Exception as exc:
                self.logger.debug("FMP historical %s error para %s: %s", endpoint_name, tk, exc)

        if last_status in (401, 403):
            self.last_error_by_ticker[tk] = f"Historico FMP no disponible para {tk}: HTTP {last_status}"
        else:
            self.last_error_by_ticker[tk] = f"Historico FMP no disponible para {tk}"
        return None

    def get_intraday_history(
        self,
        ticker: str,
        interval: str = "1hour",
        limit: int | None = None,
        symbol_map: dict[str, str] | None = None,
    ) -> list[dict] | None:
        tk = str(ticker or "").strip().upper()
        if not self.api_key:
            self.last_error_by_ticker[tk] = "FMP_API_KEY no detectada."
            return None

        symbol = self.resolve_symbol(tk, symbol_map)
        endpoints = [
            ("stable", f"https://financialmodelingprep.com/stable/historical-chart/{interval}?symbol={urllib.parse.quote(symbol)}&apikey={self.api_key}"),
            ("legacy", f"https://financialmodelingprep.com/api/v3/historical-chart/{interval}/{urllib.parse.quote(symbol)}?apikey={self.api_key}"),
        ]
        last_status = None

        for endpoint_name, url in endpoints:
            try:
                status, payload = self._request_json(url, timeout=12)
                last_status = status
                self.logger.debug("FMP intraday %s %s status %s para %s", interval, endpoint_name, status, tk)
                if status != 200 or payload is None:
                    continue
                rows = self._parse_historical_payload(payload)
                if rows:
                    self.last_error_by_ticker.pop(tk, None)
                    return rows[: int(limit)] if limit else rows
            except Exception as exc:
                self.logger.debug("FMP intraday %s %s error para %s: %s", interval, endpoint_name, tk, exc)

        if last_status in (401, 403):
            self.last_error_by_ticker[tk] = f"Historico intradia FMP no disponible para {tk}: HTTP {last_status}"
        else:
            self.last_error_by_ticker[tk] = f"Historico intradia FMP no disponible para {tk}"
        return None

    def get_candles(self, ticker: str, timeframe: str) -> list[dict]:
        normalized = str(timeframe or "").strip().upper()
        intraday_map = {
            "1M": "1min",
            "5M": "5min",
            "15M": "15min",
            "30M": "30min",
            "1H": "1hour",
            "4H": "4hour",
        }
        if normalized in intraday_map:
            return self.get_intraday_history(ticker, interval=intraday_map[normalized]) or []
        return self.get_historical_eod(ticker, limit=260) or []

    def get_profile(self, ticker: str, symbol_map: dict[str, str] | None = None) -> dict | None:
        tk = str(ticker or "").strip().upper()
        if not self.api_key or self.is_profile_excluded(tk):
            return None

        symbol = self.resolve_symbol(tk, symbol_map)
        urls = [
            f"https://financialmodelingprep.com/stable/profile?symbol={urllib.parse.quote(symbol)}&apikey={self.api_key}",
            f"https://financialmodelingprep.com/api/v3/profile/{urllib.parse.quote(symbol)}?apikey={self.api_key}",
            f"https://financialmodelingprep.com/stable/sec-profile?symbol={urllib.parse.quote(symbol)}&apikey={self.api_key}",
        ]

        for url in urls:
            try:
                status, payload = self._request_json(url, timeout=10)
                if status != 200 or payload is None:
                    continue
                if isinstance(payload, list) and payload:
                    row = payload[0]
                elif isinstance(payload, dict):
                    row = payload
                else:
                    row = None
                if row:
                    return row
            except Exception as exc:
                self.logger.debug("FMP profile error para %s: %s", tk, exc)
        return None

    def get_stock_news(self, ticker: str, limit: int = 3, symbol_map: dict[str, str] | None = None) -> list[dict]:
        tk = str(ticker or "").strip().upper()
        if not self.api_key:
            return []
        if tk in {"BZ=F", "GC=F"}:
            return []

        symbol = self.resolve_symbol(tk, symbol_map)
        url = f"https://financialmodelingprep.com/stable/stock-news?symbol={urllib.parse.quote(symbol)}&limit={int(limit)}&apikey={self.api_key}"
        try:
            status, payload = self._request_json(url, timeout=10)
            if status != 200 or payload is None:
                return []
            return payload if isinstance(payload, list) else []
        except Exception as exc:
            self.logger.debug("FMP ticker news error para %s: %s", tk, exc)
            return []

    def get_market_news(
        self,
        tracked_tickers: list[str] | None = None,
        limit: int = 10,
        symbol_map: dict[str, str] | None = None,
    ) -> list[dict]:
        if not self.api_key:
            return []

        tracked_tickers = tracked_tickers or []
        tracked_symbols: list[str] = []
        for raw_ticker in tracked_tickers:
            tk = str(raw_ticker or "").strip().upper()
            if not tk:
                continue
            if self.is_crypto_ticker(tk):
                tracked_symbols.append(tk.replace("-USD", "") + "USD")
            elif tk not in {"BZ=F", "GC=F"}:
                tracked_symbols.append(self.resolve_symbol(tk, symbol_map))

        default_tickers = ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "BTCUSD"]
        symbols: list[str] = []
        seen: set[str] = set()
        for ticker in tracked_symbols + default_tickers:
            safe_ticker = str(ticker or "").strip().upper()
            if not safe_ticker or safe_ticker in seen:
                continue
            seen.add(safe_ticker)
            symbols.append(safe_ticker)

        all_news: list[dict] = []
        for ticker in symbols:
            url = f"https://financialmodelingprep.com/stable/stock-news?symbol={ticker}&limit=2&apikey={self.api_key}"
            try:
                status, payload = self._request_json(url, timeout=5)
                if status == 200 and isinstance(payload, list):
                    all_news.extend(payload)
                    if len(all_news) >= limit:
                        break
            except Exception as exc:
                self.logger.debug("FMP stock-news fallback error para %s: %s", ticker, exc)

        unique: list[dict] = []
        seen_titles: set[str] = set()
        for article in all_news:
            title = str(article.get("title") or "").strip().lower()
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)
            unique.append(article)
        return unique[:limit]

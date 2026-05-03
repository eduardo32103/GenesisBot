from __future__ import annotations

import re
import unicodedata
from typing import Iterable

_TOKEN_PATTERN = re.compile(r"\b[A-Z0-9]{1,12}(?:[.\-=][A-Z0-9]{1,8})?\b")

_STOPWORDS = {
    "A",
    "ACTIVO",
    "ACTIVOS",
    "AHORA",
    "ANALISIS",
    "ANALIZA",
    "ANALIZAR",
    "ALERTA",
    "ALERTAS",
    "ATR",
    "BALLENA",
    "BALLENAS",
    "BOLLINGER",
    "BUEN",
    "BUENA",
    "BUENAS",
    "BUENO",
    "CARTERA",
    "CLIMA",
    "COMO",
    "COMPARA",
    "COMPARAR",
    "COMPRA",
    "COMPRAR",
    "CON",
    "CONTRA",
    "CUAL",
    "CUANDO",
    "DAME",
    "DE",
    "DEBO",
    "DEBERIA",
    "DEL",
    "DICE",
    "DICEN",
    "DINERO",
    "DONDE",
    "EL",
    "EN",
    "EMA",
    "ES",
    "ESTA",
    "ESTAN",
    "ESTADO",
    "ESTE",
    "ESTO",
    "FIB",
    "FIBONACCI",
    "GRAFICA",
    "GRAFICAS",
    "GRAFICO",
    "GRAFICOS",
    "GRANDE",
    "HAZ",
    "HAZME",
    "HOLA",
    "HORA",
    "HORARIO",
    "IDEA",
    "INDICADOR",
    "INDICADORES",
    "LA",
    "LAS",
    "LO",
    "LOS",
    "MACRO",
    "MERCADO",
    "MUNDO",
    "MUESTRA",
    "MUESTRAME",
    "OPINA",
    "OPINAS",
    "OPINION",
    "PARA",
    "PASA",
    "PASANDO",
    "PENA",
    "PUEDES",
    "PUEDO",
    "QUE",
    "QUIERO",
    "REVISA",
    "REVISAR",
    "RSI",
    "SEGUIMIENTO",
    "SMA",
    "SOBRE",
    "UN",
    "UNA",
    "VELA",
    "VELAS",
    "VER",
    "VENDER",
    "VENTA",
    "VERSUS",
    "VWAP",
    "VS",
    "Y",
}

_ALIASES = {
    "BTC": "BTC-USD",
    "BITCOIN": "BTC-USD",
    "ETH": "ETH-USD",
    "ETHEREUM": "ETH-USD",
    "SOL": "SOL-USD",
    "SOLANA": "SOL-USD",
    "BRENT": "BZ=F",
    "BZ": "BZ=F",
    "ORO": "IAU",
    "GOLD": "IAU",
    "PLATA": "SLV",
    "SILVER": "SLV",
}


def normalize_text(value: object) -> str:
    normalized = unicodedata.normalize("NFD", str(value or "").strip())
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn")


def normalize_ticker(value: object) -> str:
    raw = normalize_text(value).upper().strip().rstrip(".,;:!?")
    raw = raw.replace("/", "-")
    return _ALIASES.get(raw, raw)


def extract_tickers_from_prompt(message: str, context: object | None = None) -> list[str]:
    text = normalize_text(message).upper()
    tickers: list[str] = []

    for alias, ticker in _ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", text):
            _append_unique(tickers, ticker)

    for raw in _TOKEN_PATTERN.findall(text):
        token = normalize_ticker(raw)
        if not _looks_like_ticker(token):
            continue
        _append_unique(tickers, token)

    if not tickers and isinstance(context, dict):
        for raw in _context_ticker_candidates(context):
            token = normalize_ticker(raw)
            if _looks_like_ticker(token):
                _append_unique(tickers, token)

    return tickers


def _context_ticker_candidates(context: dict) -> Iterable[object]:
    yield context.get("ticker")
    selected = context.get("selectedAsset") or context.get("selected_asset")
    yield selected
    for key in ("asset", "position", "watchlist_item"):
        value = context.get(key)
        if isinstance(value, dict):
            yield value.get("ticker") or value.get("symbol")


def _looks_like_ticker(token: str) -> bool:
    if not token or token in _STOPWORDS:
        return False
    if token in _ALIASES.values():
        return True
    if token in _ALIASES:
        return False
    if len(token) < 2 or len(token) > 15:
        return False
    if token.isdigit():
        return False
    if not re.fullmatch(r"[A-Z0-9.\-=]+", token):
        return False
    return any(char.isalpha() for char in token)


def _append_unique(values: list[str], value: str) -> None:
    if value and value not in values:
        values.append(value)

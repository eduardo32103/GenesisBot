from __future__ import annotations

import re
import unicodedata
from typing import Iterable

_TOKEN_PATTERN = re.compile(r"\b[A-Z0-9]{1,12}(?:[.\-=][A-Z0-9]{1,8})?\b")
_TOKEN_PATTERN_ORIGINAL = re.compile(r"\b[A-Za-z0-9]{1,12}(?:[.\-=][A-Za-z0-9]{1,8})?\b")
_INTENT_NEAR_TICKER = {
    "analiza",
    "analizar",
    "aprende",
    "aprendi",
    "aprendiste",
    "aprender",
    "compra",
    "comprar",
    "compro",
    "vender",
    "vendo",
    "opina",
    "opinas",
    "opinion",
    "precio",
    "grafica",
    "graficas",
    "grafico",
    "chart",
    "soporte",
    "resistencia",
    "rsi",
    "macd",
    "ema",
    "sma",
    "vwap",
    "fib",
    "fibonacci",
    "ticker",
    "activo",
    "memoria",
    "historial",
    "ver",
    "revisa",
    "comparar",
    "compara",
}
_CONNECTOR_NEAR_TICKER = {"de", "con", "sobre", "vs", "contra"}

_STOPWORDS = {
    "A",
    "AFECTA",
    "AFECTAN",
    "AFECTAR",
    "ACTIVO",
    "ACTIVOS",
    "AHORA",
    "APRENDISTE",
    "ANALISIS",
    "ANALIZA",
    "ANALIZAR",
    "ALERTA",
    "ALERTAS",
    "ATR",
    "AYER",
    "BALLENA",
    "BALLENAS",
    "BALLNEA",
    "BALLNEAS",
    "BALENA",
    "BALENAS",
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
    "DIA",
    "DIARIO",
    "DICE",
    "DICEN",
    "DIME",
    "DINERO",
    "DONDE",
    "EL",
    "EN",
    "EMA",
    "ES",
    "ESTA",
    "ESTAN",
    "ESTAS",
    "ESTOY",
    "ESTAMOS",
    "ESTADO",
    "ESTE",
    "ESTO",
    "ESTUVO",
    "EDGE",
    "FECHA",
    "FIB",
    "FIBONACCI",
    "FOTO",
    "FLOW",
    "FLUJO",
    "GRAFICA",
    "GRAFICAS",
    "GRAFICO",
    "GRAFICOS",
    "GRANDE",
    "GENESIS",
    "HACEN",
    "HACER",
    "HACIENDO",
    "HICIMOS",
    "HAZ",
    "HAZME",
    "HOLA",
    "HORA",
    "HORARIO",
    "HOY",
    "IDEA",
    "BIEN",
    "IMAGEN",
    "INDICADOR",
    "INDICADORES",
    "INSTITUCIONAL",
    "INSTITUCIONALES",
    "LA",
    "LAS",
    "LEE",
    "LEER",
    "LO",
    "LOS",
    "MACRO",
    "MERCADO",
    "MI",
    "MONEY",
    "MUNDO",
    "MIS",
    "NOVIA",
    "NOVIO",
    "ENOJADA",
    "ENOJADO",
    "AYUDA",
    "AYUDAME",
    "RELACION",
    "CONSEJO",
    "PROBLEMA",
    "PERSONAL",
    "TRISTE",
    "MOLESTA",
    "MOLESTO",
    "DISCULPA",
    "DISCULPARME",
    "NOTICIA",
    "NOTICIAS",
    "MUESTRA",
    "MUESTRAME",
    "OPINA",
    "OPINAS",
    "OPINION",
    "OYE",
    "PARA",
    "PASA",
    "PASANDO",
    "PASADO",
    "PASO",
    "PENA",
    "PRECIO",
    "PUEDES",
    "PUEDO",
    "RECIENTES",
    "RECUERDAME",
    "QUE",
    "TAL",
    "TIENE",
    "TODO",
    "LISTO",
    "GRACIAS",
    "QUIERO",
    "RESUMEN",
    "REVISA",
    "REVISAR",
    "RSI",
    "SE",
    "SEGUIMIENTO",
    "SENAL",
    "SENALES",
    "SMA",
    "SMART",
    "SABES",
    "SOBRE",
    "SU",
    "SUS",
    "TU",
    "TUS",
    "UN",
    "UNA",
    "VELA",
    "VELAS",
    "VER",
    "VE",
    "VA",
    "VAS",
    "VOY",
    "VENDER",
    "VENTA",
    "VERSUS",
    "VIERNES",
    "VIMOS",
    "VWAP",
    "VS",
    "Y",
}

_STOPWORDS.update(
    {
        "CREE",
        "CREER",
        "CREES",
        "CREO",
        "MAS",
        "MACD",
        "ME",
        "MEJOR",
        "MUY",
        "NO",
        "SER",
        "SERA",
        "SERIA",
        "SI",
        "SIN",
    }
)

_STOPWORDS.update(
    {
        "CALOR",
        "DESPEJADO",
        "FRIO",
        "HUMEDAD",
        "LLUEVE",
        "LLUVIA",
        "MOCHIS",
        "NUBLADO",
        "PRONOSTICO",
        "TEMPERATURA",
        "TIEMPO",
        "VIENTO",
        "WEATHER",
    }
)

_STOPWORDS.update(
    {
        "AGUILA",
        "ALCISTA",
        "BAJISTA",
        "CAUTELA",
        "CAZA",
        "CAZADOR",
        "CAZANDO",
        "CAZAR",
        "CAZAME",
        "CONVIENE",
        "CONVENDRIA",
        "ENTRADA",
        "ENTRADAS",
        "OPORTUNIDAD",
        "OPORTUNIDADES",
        "PRECIOS",
        "RUPTURA",
        "RUPTURAS",
        "SETUP",
        "SETUPS",
        "VALIDACION",
        "VALIDACIONES",
        "VALIDADA",
        "VALIDADAS",
        "VALIDADO",
        "VALIDADOS",
        "VALIDAR",
    }
)

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
    original_text = normalize_text(message)
    text = original_text.upper()
    folded_words = [normalize_text(word).casefold() for word in re.findall(r"\b[\w.\-=]+\b", original_text)]
    has_market_intent = any(word in _INTENT_NEAR_TICKER for word in folded_words)
    tickers: list[str] = []

    for alias, ticker in _ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", text):
            _append_unique(tickers, ticker)

    for match in _TOKEN_PATTERN_ORIGINAL.finditer(original_text):
        raw = match.group(0)
        token = normalize_ticker(raw)
        if not _looks_like_ticker(token):
            continue
        if token in _ALIASES.values():
            continue
        if not _token_is_intended_ticker(original_text, match.start(), raw, has_market_intent):
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


def _token_is_intended_ticker(original_text: str, start: int, raw: str, has_market_intent: bool) -> bool:
    normalized_raw = normalize_text(raw).strip()
    if not normalized_raw:
        return False
    token = normalize_ticker(normalized_raw)
    if token in _ALIASES.values():
        return True
    if any(separator in normalized_raw for separator in (".", "-", "=")):
        return True
    if any(char.isdigit() for char in normalized_raw):
        return True
    if normalized_raw.isupper() and 1 < len(token) <= 6:
        return True
    if not has_market_intent:
        return False

    before = normalize_text(original_text[:start]).casefold()
    before_words = re.findall(r"\b[\w.\-=]+\b", before)
    recent_words = before_words[-6:]
    previous = before_words[-1] if before_words else ""
    if any(word in _INTENT_NEAR_TICKER for word in recent_words):
        return True
    if previous in _CONNECTOR_NEAR_TICKER and any(word in _INTENT_NEAR_TICKER for word in before_words[-10:]):
        return True
    return False


def _append_unique(values: list[str], value: str) -> None:
    if value and value not in values:
        values.append(value)

from __future__ import annotations

from dataclasses import dataclass

from services.genesis.ticker_parser import normalize_ticker


MEGA_CAP_GROWTH = "Mega-cap Growth"
INDEX_ETF = "Index ETF"
CRYPTO = "Crypto"
COMMODITY = "Commodity"
GOLD_DEFENSIVE = "Gold Defensive"
HIGH_BETA = "High Beta"
UNKNOWN = "Unknown"


_MEGA_CAP_GROWTH = {"NVDA", "MSFT", "AAPL", "AMZN", "META", "GOOGL", "GOOG", "AMD"}
_INDEX_ETF = {"SPY", "VOO", "QQQ", "DIA", "IWM", "IVV", "VTI", "XLK"}
_CRYPTO = {"BTC", "BTC-USD", "ETH-USD", "SOL-USD", "BTCUSD", "BTCUSDT", "ETHUSD", "ETHUSDT", "GBTC", "IBIT", "ETHE", "SOL1!", "SOLUSD", "SOLUSDT"}
_COMMODITY = {"BNO", "USO", "BZ=F", "CL=F", "BRENT", "OIL", "DBC"}
_GOLD = {"IAU", "GLD", "SLV", "GC=F", "GOLD"}
_HIGH_BETA = {"MARA", "RIOT", "COIN", "TSLA", "PLTR", "SOFI", "RIVN", "LCID"}


@dataclass(frozen=True)
class AssetClassification:
    ticker: str
    asset_class: str
    recommended_profile: str
    recommended_preset: str
    recommended_timeframe: str
    reason: str
    risk_flags: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "ticker": self.ticker,
            "asset_class": self.asset_class,
            "recommended_profile": self.recommended_profile,
            "recommended_preset": self.recommended_preset,
            "recommended_timeframe": self.recommended_timeframe,
            "reason": self.reason,
            "risk_flags": list(self.risk_flags),
        }


class AssetClassifier:
    """Classifies assets so Genesis does not force one strategy on every ticker."""

    def classify(self, ticker: str) -> dict[str, object]:
        normalized = normalize_ticker(ticker)
        if normalized in _MEGA_CAP_GROWTH:
            result = AssetClassification(
                normalized,
                MEGA_CAP_GROWTH,
                "Trend Pullback",
                "Core Tactical",
                "1H/4H",
                "Activo growth liquido: favorece pullbacks, breakouts con volumen y runner; evitar shorts contra tendencia.",
                ("Evitar perseguir extension extrema.", "Comparar contra buy & hold en tendencias fuertes."),
            )
        elif normalized in _INDEX_ETF:
            result = AssetClassification(
                normalized,
                INDEX_ETF,
                "Defensive ETF Core",
                "Defensive ETF Core",
                "4H/1D",
                "ETF de indice: conviene core long-only, filtro EMA200 y menos operaciones; 1H suele sobreoperar.",
                ("No sobreoperar ruido intradia.", "Benchmark buy & hold es la referencia principal."),
            )
        elif normalized in _CRYPTO or "BTC" in normalized or "ETH" in normalized:
            result = AssetClassification(
                normalized,
                CRYPTO,
                "Crypto Momentum V4",
                "Crypto Momentum V4",
                "4H/1D",
                "Cripto liquido: requiere Crypto Momentum V4 Long Term Edge, HTF, ATR amplio, breakout/retest, hedge overlay y no-trade real en chop o 1H sin edge.",
                ("Volatilidad alta.", "No usar stops estrechos ni tamanos grandes sin forward testing.", "Si 1H no tiene edge, preferir 4H/1D o No-Trade."),
            )
        elif normalized in _COMMODITY:
            result = AssetClassification(
                normalized,
                COMMODITY,
                "Commodity Regime",
                "Commodity Regime",
                "4H/1D",
                "Commodity: necesita regimen macro, noticias/geopolitica y confirmacion de tendencia.",
                ("Evitar senales sin catalizador.", "Revisar dolar, geopolitica e inventarios si aplica."),
            )
        elif normalized in _GOLD:
            result = AssetClassification(
                normalized,
                GOLD_DEFENSIVE,
                "Gold Defensive",
                "Gold Defensive",
                "4H/1D",
                "Oro/defensivo: se mueve por tasas, dolar y risk-off; usar regimen defensivo.",
                ("Vigilar DXY y tasas.", "No tratarlo como growth momentum."),
            )
        elif normalized in _HIGH_BETA:
            result = AssetClassification(
                normalized,
                HIGH_BETA,
                "Breakout Volume",
                "Aggressive",
                "1H/4H",
                "Activo high beta: solo paper, volumen confirmado y defensa estricta.",
                ("Riesgo de gaps y whipsaw.", "Reducir tamano y exigir liquidez."),
            )
        else:
            result = AssetClassification(
                normalized,
                UNKNOWN,
                "Trend Following",
                "Paper Quality",
                "4H",
                "Activo sin perfil fuerte en memoria; usar perfil generico y medir antes de operar.",
                ("Muestra insuficiente.", "Tratar como watch only hasta tener evidencia."),
            )
        return result.to_dict()


def classify_asset(ticker: str) -> dict[str, object]:
    return AssetClassifier().classify(ticker)

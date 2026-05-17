from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from services.trading_intelligence.asset_classifier import (
    COMMODITY,
    CRYPTO,
    GOLD_DEFENSIVE,
    HIGH_BETA,
    INDEX_ETF,
    MEGA_CAP_GROWTH,
    UNKNOWN,
)


@dataclass(frozen=True)
class StrategyProfile:
    name: str
    family: str
    description: str
    ideal_asset_classes: tuple[str, ...]
    default_preset: str
    default_timeframe: str
    default_inputs: dict[str, Any] = field(default_factory=dict)
    risk_notes: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "family": self.family,
            "description": self.description,
            "ideal_asset_classes": list(self.ideal_asset_classes),
            "default_preset": self.default_preset,
            "default_timeframe": self.default_timeframe,
            "default_inputs": dict(self.default_inputs),
            "risk_notes": list(self.risk_notes),
        }


PROFILES: tuple[StrategyProfile, ...] = (
    StrategyProfile(
        "Trend Following",
        "trend",
        "EMA 20/50/200, ADX y trailing ATR para activos con tendencia persistente.",
        (MEGA_CAP_GROWTH, CRYPTO, COMMODITY, UNKNOWN),
        "Trend Runner",
        "4H",
        {"trendRunnerMode": True, "useHTFConfirmation": True, "coreTrailMode": "ATR"},
        ("No perseguir extension extrema.",),
    ),
    StrategyProfile(
        "Trend Pullback",
        "pullback",
        "Pullback a EMA20/EMA50/golden pocket con RSI rebotando y MACD mejorando.",
        (MEGA_CAP_GROWTH, HIGH_BETA),
        "Core Tactical",
        "1H/4H",
        {"coreTacticalMode": True, "trendRunnerMode": True, "avoidShortsInBullTrend": True},
        ("Funciona mejor con tendencia ya confirmada.",),
    ),
    StrategyProfile(
        "Breakout Volume",
        "breakout",
        "Ruptura de resistencia con cierre confirmado y volumen relativo alto.",
        (MEGA_CAP_GROWTH, HIGH_BETA, CRYPTO),
        "Trend Runner",
        "1H/4H",
        {"useVolumeFilter": True, "minRelativeVolume": 1.2},
        ("Fragil si se compra ruptura extendida sin volumen real.",),
    ),
    StrategyProfile(
        "Mean Reversion",
        "mean_reversion",
        "Bollinger Bands, RSI extremo recuperando y soporte cercano en mercado lateral.",
        (UNKNOWN, INDEX_ETF),
        "Paper Quality",
        "4H",
        {"useMarketRegimeFilter": True, "chopFilterEnabled": False},
        ("No usar contra tendencia fuerte.",),
    ),
    StrategyProfile(
        "Defensive ETF Core",
        "defensive_core",
        "Core long-only para ETFs: EMA200, pendiente EMA50, baja frecuencia y hedge risk-off.",
        (INDEX_ETF,),
        "Defensive ETF Core",
        "4H/1D",
        {
            "tradeMode": "Long Only",
            "assetProfile": "Index ETF",
            "coreTacticalMode": True,
            "avoidShortsInBullTrend": True,
            "coreTrailMode": "EMA50",
            "maxTradesPerDay": 1,
        },
        ("1H puede sobreoperar ETFs.", "Benchmark buy & hold manda."),
    ),
    StrategyProfile(
        "Crypto Momentum V4",
        "crypto_momentum",
        "BTC/cripto con HTF, ATR amplio, regimen V4 long-term, ruptura/retest, expansion de volatilidad, hedge overlay y No-Trade real.",
        (CRYPTO,),
        "Crypto Momentum V4",
        "4H/1D",
        {
            "assetProfile": "Crypto",
            "tradeMode": "Long & Short",
            "enableShorts": True,
            "strategyVersion": "Genesis Advantage v10.13 BTC Edge",
            "coreATRMultiplier": 3.5,
            "tacticalATRMultiplier": 2.6,
            "cryptoAtrMultiplier": 3.0,
            "cryptoTrailATR": 3.8,
            "cryptoMinAdx": 20,
            "cryptoMinVolRel": 1.1,
            "cryptoV4Mode": True,
            "btcLongTermMode": True,
            "cryptoAvoidChop": True,
            "cryptoUseBreakoutRetest": True,
            "cryptoUseVolExpansion": True,
            "cryptoV3Mode": True,
            "cryptoUseRegimeSwitch": True,
            "cryptoUseTrendContinuation": True,
            "cryptoUseMeanReversionOnlyInRange": True,
            "cryptoUseHTF": True,
            "cryptoNoTradeInChop": True,
            "useActiveHedgeOverlay": True,
            "hedgeShortAllowed": True,
            "btcMaxTradesPerDay": 2,
            "btcCooldownBars": 24,
            "btcMinBarsAfterExit": 12,
        },
        ("Volatilidad alta; primero paper trading.", "Safe/validation no mide rentabilidad."),
    ),
    StrategyProfile(
        "BTC Breakout Retest",
        "breakout_retest",
        "Ruptura BTC confirmada, retest del nivel y recuperacion con volumen.",
        (CRYPTO,),
        "Crypto Momentum V4",
        "4H",
        {"assetProfile": "Crypto", "tradeMode": "Long & Short", "cryptoV4Mode": True, "cryptoV3Mode": True, "btcLongTermMode": True, "cryptoUseBreakoutRetest": True, "cryptoMinVolRel": 1.1, "cryptoAtrMultiplier": 3.0, "cryptoTrailATR": 3.8, "btcCooldownBars": 24, "useActiveHedgeOverlay": True},
        ("No comprar solo una mecha; esperar cierre/retest.",),
    ),
    StrategyProfile(
        "BTC Volatility Expansion",
        "volatility_expansion",
        "Compresion de Bollinger/ATR y expansion direccional con volumen.",
        (CRYPTO,),
        "Crypto Momentum V4",
        "4H",
        {"assetProfile": "Crypto", "tradeMode": "Long & Short", "cryptoV4Mode": True, "cryptoV3Mode": True, "btcLongTermMode": True, "cryptoUseVolExpansion": True, "cryptoTrailATR": 3.8, "cryptoNoTradeInChop": True, "btcCooldownBars": 24, "useActiveHedgeOverlay": True},
        ("Riesgo de falso breakout si no confirma volumen.",),
    ),
    StrategyProfile(
        "Commodity Regime",
        "commodity_regime",
        "Tendencia + contexto macro/geopolitico para petroleo, oro y materias primas.",
        (COMMODITY,),
        "Commodity Regime",
        "4H/1D",
        {"assetProfile": "Commodity", "useGenesisContext": True, "useMarketRegimeFilter": True},
        ("No operar sin catalizador o contexto macro claro.",),
    ),
    StrategyProfile(
        "Gold Defensive",
        "gold_defensive",
        "Perfil defensivo/risk-off sensible a dolar, tasas y volatilidad.",
        (GOLD_DEFENSIVE,),
        "Gold Defensive",
        "4H/1D",
        {"assetProfile": "Gold Defensive", "useGenesisContext": True, "hedgeImpactMode": "Balanced"},
        ("Confirmar DXY/tasas antes de subir conviccion.",),
    ),
    StrategyProfile(
        "Hedge / Capital Protection",
        "hedge",
        "Reduccion, stop protector, cash hedge o inverse ETF sugerido; no ejecuta broker.",
        (MEGA_CAP_GROWTH, INDEX_ETF, CRYPTO, COMMODITY, GOLD_DEFENSIVE, HIGH_BETA, UNKNOWN),
        "Conservative",
        "4H/1D",
        {"useHedgeMode": True, "capitalProtectionMode": True, "hedgeImpactMode": "Defensive"},
        ("Reduce riesgo, pero no elimina perdidas.",),
    ),
)


def get_strategy_profiles() -> list[dict[str, Any]]:
    return [profile.to_dict() for profile in PROFILES]


def profile_by_name(name: str) -> StrategyProfile:
    clean = str(name or "").casefold()
    for profile in PROFILES:
        if profile.name.casefold() == clean:
            return profile
    return PROFILES[0]


def default_profile_for_asset_class(asset_class: str) -> StrategyProfile:
    for profile in PROFILES:
        if asset_class in profile.ideal_asset_classes and profile.family != "hedge":
            return profile
    return profile_by_name("Trend Following")

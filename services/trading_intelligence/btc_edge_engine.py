from __future__ import annotations

from typing import Any

from services.genesis.ticker_parser import normalize_ticker


class BTCEdgeEngine:
    """BTC/crypto-specific context engine; it does not execute orders."""

    def evaluate(
        self,
        ticker: str,
        *,
        bars: list[dict[str, Any]] | None = None,
        hedge_score: float | int = 0,
        memory_failures: int = 0,
        market_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized = normalize_ticker(ticker) or str(ticker or "BTC-USD").upper()
        hedge_value = float(hedge_score or 0)
        clean = _normalize_bars(bars or [])
        if len(clean) < 80:
            return _pending_context(normalized, hedge_value, memory_failures, "Sin suficientes barras backend para medir BTC edge V4.")

        closes = [bar["close"] for bar in clean]
        highs = [bar["high"] for bar in clean]
        lows = [bar["low"] for bar in clean]
        volumes = [bar["volume"] for bar in clean]
        ema20 = _ema(closes, 20)
        ema50 = _ema(closes, 50)
        ema200 = _ema(closes, 200)
        atr = _atr(clean, 14)
        adx = _adx(clean, 14)
        avg_volume = _sma(volumes, 20)
        bb_width = _bb_width(closes, 20)
        rsi = _rsi(closes, 14)
        _macd_line, _macd_signal, macd_hist = _macd(closes)
        idx = len(clean) - 1
        close = closes[idx]
        open_ = clean[idx]["open"]
        high = highs[idx]
        low = lows[idx]
        atr_value = atr[idx] or max(close * 0.02, 0.01)
        atr_pct = _safe_ratio(atr_value, close) * 100.0
        rel_vol = _safe_ratio(volumes[idx], avg_volume[idx], fallback=1.0)
        slope = _safe_ratio((ema50[idx] or close) - (ema50[idx - 10] or close), ema50[idx - 10] or close) * 100.0 if idx >= 10 else 0.0
        swing_start = max(0, idx - 55)
        resistance = max(highs[swing_start:idx] or [close])
        support = min(lows[swing_start:idx] or [close])
        range_pct = _safe_ratio(resistance - support, close) * 100.0
        candle_body_strength = _safe_ratio(abs(close - open_), high - low, fallback=0.0)
        higher_lows = lows[idx] > lows[idx - 5] > lows[idx - 10] if idx >= 10 else False
        higher_highs = highs[idx] > highs[idx - 5] > highs[idx - 10] if idx >= 10 else False
        lower_highs = highs[idx] < highs[idx - 5] < highs[idx - 10] if idx >= 10 else False
        lower_lows = lows[idx] < lows[idx - 5] < lows[idx - 10] if idx >= 10 else False
        ema20_now = ema20[idx] or close
        ema50_now = ema50[idx] or close
        ema200_now = ema200[idx] or close
        rsi_now = rsi[idx] or 50.0
        adx_now = adx[idx] or 0.0
        macd_now = macd_hist[idx] or 0.0
        macd_prev = macd_hist[idx - 1] or 0.0 if idx >= 1 else 0.0

        bull_trend = close > ema200_now and ema20_now >= ema50_now and slope > 0 and rsi_now >= 50 and macd_now >= macd_prev
        bear_trend = close < ema200_now and ema20_now <= ema50_now and slope < 0 and rsi_now <= 50 and macd_now <= macd_prev
        squeeze = bb_width[idx] < 3.8 and atr_pct < 3.8
        volatility_expansion = idx >= 3 and bb_width[idx] > bb_width[idx - 3] * 1.08 and atr_pct > 1.2 and rel_vol >= 1.0
        breakout = close > resistance and candle_body_strength >= 0.45 and rel_vol >= 1.1
        breakdown = close < support and candle_body_strength >= 0.45 and rel_vol >= 1.1
        liquidity_sweep = (high > resistance and close < resistance) or (low < support and close > support)
        recovery = higher_lows and close > ema50_now and rsi_now >= 45 and macd_now >= macd_prev
        range_market = range_pct <= 10 and adx_now < 18 and not breakout and not breakdown
        chop = abs(slope) < 0.08 and bb_width[idx] < 5.0 and adx_now < 18 and rel_vol < 1.05 and not breakout and not breakdown
        risk_off = hedge_value >= 65 or bool((market_context or {}).get("risk_off"))

        regime = (
            "risk_off"
            if risk_off and not (breakdown or bear_trend)
            else "breakout"
            if breakout
            else "breakdown"
            if breakdown
            else "volatility_expansion"
            if volatility_expansion and not squeeze
            else "squeeze"
            if squeeze
            else "liquidity_sweep"
            if liquidity_sweep
            else "bull_trend"
            if bull_trend
            else "bear_trend"
            if bear_trend
            else "chop"
            if chop
            else "range"
            if range_market
            else "recovery"
            if recovery
            else "range"
        )

        no_trade_score = 0
        reasons: list[str] = []
        if chop:
            no_trade_score += 38
            reasons.append("BTC en chop: ADX bajo, EMAs planas, compresion y volumen debil.")
        if range_market and not liquidity_sweep:
            no_trade_score += 14
            reasons.append("BTC atrapado entre soporte/resistencia; esperar ruptura, retest o mean reversion controlada.")
        if rel_vol < 0.85:
            no_trade_score += 12
            reasons.append("Volumen relativo debil para confirmar impulso BTC.")
        if risk_off:
            no_trade_score += 25
            reasons.append("Riesgo externo/hedge alto: priorizar defensa, reduce exposure o cash hedge.")
        if memory_failures >= 3:
            no_trade_score += 20
            reasons.append("Memoria marca fallos repetidos en BTC/setup.")
        if not (bull_trend or bear_trend or breakout or breakdown or volatility_expansion or liquidity_sweep):
            no_trade_score += 14
            reasons.append("Sin tendencia, ruptura, breakdown o expansion clara.")

        btc_edge_score = _clamp(
            45
            + (18 if bull_trend or bear_trend else 0)
            + (16 if breakout or breakdown else 0)
            + (12 if volatility_expansion else 0)
            + (8 if recovery else 0)
            + (8 if rel_vol >= 1.1 else -6 if rel_vol < 0.85 else 0)
            + (6 if adx_now >= 20 else -8 if adx_now < 15 else 0)
            - (22 if chop else 0)
            - (18 if risk_off and not breakdown else 0)
            - min(20, memory_failures * 5),
            0,
            100,
        )
        no_trade_score = int(_clamp(no_trade_score, 0, 100))
        hedge_mode = hedge_value >= 55 or risk_off or breakdown or bear_trend
        hedge_reason = (
            "Hedge activo: BTC pierde soporte o contexto risk-off; proteger ganancias o usar short tactico solo en paper."
            if breakdown or bear_trend or risk_off
            else "Hedge en vigilancia: no bloquear tendencia, solo subir calidad de entrada."
            if hedge_value >= 55
            else "Sin hedge activo: vigilar estructura y mantener stops amplios."
        )
        profile = "BTC Breakout Retest" if breakout or liquidity_sweep else "BTC Volatility Expansion" if squeeze or volatility_expansion else "Crypto Momentum V4"
        recommended_timeframe = "4H/1D" if chop or no_trade_score >= 45 or memory_failures >= 3 else "4H"
        return {
            "ok": True,
            "ticker": normalized,
            "asset_class": "Crypto",
            "strategy_version": "Genesis Advantage v10.13 BTC Edge",
            "btc_regime": regime,
            "btc_edge_score": int(btc_edge_score),
            "recommended_strategy_profile": profile,
            "recommended_preset": "Crypto Momentum V4",
            "recommended_timeframe": recommended_timeframe,
            "no_trade_recommendation": no_trade_score >= 70,
            "no_trade_score": no_trade_score,
            "hedge_mode": hedge_mode,
            "hedge_reason": hedge_reason,
            "hedge_score": int(_clamp(hedge_value, 0, 100)),
            "volatility": {
                "atr_pct": round(atr_pct, 4),
                "bb_width": round(bb_width[idx], 4),
                "squeeze": squeeze,
                "expansion": volatility_expansion,
            },
            "momentum": {
                "trend_up": bull_trend,
                "trend_down": bear_trend,
                "higher_highs": higher_highs,
                "higher_lows": higher_lows,
                "lower_highs": lower_highs,
                "lower_lows": lower_lows,
                "ema50_slope_pct": round(slope, 4),
                "rsi": round(rsi_now, 4),
                "macd_hist": round(macd_now, 6),
                "adx": round(adx_now, 4),
            },
            "volume": {"relative_volume": round(rel_vol, 4), "breakout_confirmed": breakout, "breakdown_confirmed": breakdown},
            "risk_external": {"hedge_score": hedge_value, "risk_off": risk_off},
            "reason": "BTC requiere Crypto Momentum V4: long-term edge en 4H/1D, trend continuation, breakout/retest, volatility expansion, No-Trade real en chop y hedge activo cuando el riesgo sube.",
            "what_to_watch": [
                "HTF bullish/bearish antes de operar BTC.",
                "Breakout + retest confirmado, no solo mecha.",
                "Expansion de volatilidad con volumen relativo.",
                "Si BTC 1H no tiene edge, pasar a 4H/1D o No-Trade.",
                "Hedge score alto: proteger ganancia, reducir exposicion o watch-only.",
            ],
            "suggested_tradingview_inputs": {
                "assetProfile": "Crypto",
                "preset": "Crypto Momentum V4",
                "strategyVersion": "Genesis Advantage v10.13 BTC Edge",
                "tradeMode": "Long & Short",
                "enableShorts": True,
                "safeMode": False,
                "validationMode": False,
                "cryptoV4Mode": True,
                "cryptoV3Mode": True,
                "btcLongTermMode": True,
                "cryptoUseRegimeSwitch": True,
                "cryptoUseBreakoutRetest": True,
                "cryptoUseVolExpansion": True,
                "cryptoUseTrendContinuation": True,
                "cryptoUseMeanReversionOnlyInRange": True,
                "cryptoAvoidChop": True,
                "cryptoNoTradeInChop": True,
                "cryptoAtrMultiplier": 3.0,
                "cryptoTrailATR": 3.8,
                "cryptoMinAdx": 20,
                "cryptoMinVolRel": 1.1,
                "useActiveHedgeOverlay": True,
                "useHedgeMode": True,
                "hedgeShortAllowed": True,
                "hedgeRiskOffThreshold": 65,
                "hedgeHardBlockThreshold": 80,
                "hedgeReduceExposureThreshold": 55,
                "btcMaxTradesPerDay": 2,
                "btcCooldownBars": 24,
                "btcMinBarsAfterExit": 12,
                "blockIfNoEdge": True,
                "noTradeScoreInput": no_trade_score,
                "minSignalScore": 62,
                "recommended_timeframe": recommended_timeframe,
            },
            "notes": reasons or ["BTC edge candidato; validar con backtesting, paper y forward testing."],
            "policy": "Paper/journal only; no broker real ni promesa de rentabilidad.",
        }


def build_btc_edge_context(ticker: str, **kwargs: Any) -> dict[str, Any]:
    return BTCEdgeEngine().evaluate(ticker, **kwargs)


def _pending_context(ticker: str, hedge_score: float | int, memory_failures: int, reason: str) -> dict[str, Any]:
    no_trade_score = int(_clamp(25 + (20 if memory_failures >= 3 else 0) + (25 if float(hedge_score or 0) >= 70 else 0), 0, 100))
    hedge_mode = float(hedge_score or 0) >= 55
    return {
        "ok": True,
        "ticker": ticker,
        "asset_class": "Crypto",
        "strategy_version": "Genesis Advantage v10.13 BTC Edge",
        "btc_regime": "pending_bars",
        "btc_edge_score": 40,
        "recommended_strategy_profile": "Crypto Momentum V4",
        "recommended_preset": "Crypto Momentum V4",
        "recommended_timeframe": "4H/1D",
        "no_trade_recommendation": no_trade_score >= 70,
        "no_trade_score": no_trade_score,
        "hedge_mode": hedge_mode,
        "hedge_reason": "Sin barras suficientes: usar BTC 4H, no-trade si el contexto no confirma y no medir edge con validation.",
        "hedge_score": int(_clamp(float(hedge_score or 0), 0, 100)),
        "reason": reason,
        "what_to_watch": ["Usar BTC 4H primero.", "No medir performance en safeMode/validationMode.", "Bloquear si noTradeScoreInput >= 70."],
        "suggested_tradingview_inputs": {
            "assetProfile": "Crypto",
            "preset": "Crypto Momentum V4",
            "strategyVersion": "Genesis Advantage v10.13 BTC Edge",
            "tradeMode": "Long & Short",
            "enableShorts": True,
            "safeMode": False,
            "validationMode": False,
            "cryptoV4Mode": True,
            "cryptoV3Mode": True,
            "btcLongTermMode": True,
            "cryptoUseRegimeSwitch": True,
            "cryptoUseBreakoutRetest": True,
            "cryptoUseVolExpansion": True,
            "cryptoUseTrendContinuation": True,
            "cryptoUseMeanReversionOnlyInRange": True,
            "cryptoAvoidChop": True,
            "cryptoNoTradeInChop": True,
            "cryptoAtrMultiplier": 3.0,
            "cryptoTrailATR": 3.8,
            "cryptoMinAdx": 20,
            "cryptoMinVolRel": 1.1,
            "useActiveHedgeOverlay": True,
            "useHedgeMode": True,
            "hedgeShortAllowed": True,
            "btcMaxTradesPerDay": 2,
            "btcCooldownBars": 24,
            "btcMinBarsAfterExit": 12,
            "blockIfNoEdge": True,
            "noTradeScoreInput": no_trade_score,
            "minSignalScore": 62,
            "recommended_timeframe": "4H/1D",
        },
        "policy": "Paper/journal only; no broker real ni promesa de rentabilidad.",
    }


def _normalize_bars(bars: list[dict[str, Any]]) -> list[dict[str, float]]:
    clean: list[dict[str, float]] = []
    for bar in bars:
        close = _num(bar.get("close"))
        if close is None:
            continue
        clean.append(
            {
                "open": _num(bar.get("open")) or close,
                "high": _num(bar.get("high")) or close,
                "low": _num(bar.get("low")) or close,
                "close": close,
                "volume": _num(bar.get("volume")) or 0.0,
            }
        )
    return clean


def _ema(values: list[float], length: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if len(values) < length:
        return result
    multiplier = 2 / (length + 1)
    ema = sum(values[:length]) / length
    result[length - 1] = ema
    for idx in range(length, len(values)):
        ema = (values[idx] - ema) * multiplier + ema
        result[idx] = ema
    return result


def _sma(values: list[float], length: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    for idx in range(length - 1, len(values)):
        result[idx] = sum(values[idx - length + 1 : idx + 1]) / length
    return result


def _atr(bars: list[dict[str, float]], length: int) -> list[float | None]:
    ranges: list[float] = []
    previous_close = bars[0]["close"]
    for bar in bars:
        ranges.append(max(bar["high"] - bar["low"], abs(bar["high"] - previous_close), abs(bar["low"] - previous_close)))
        previous_close = bar["close"]
    return _sma(ranges, length)


def _adx(bars: list[dict[str, float]], length: int) -> list[float | None]:
    if len(bars) < length + 2:
        return [None] * len(bars)
    tr: list[float] = [0.0]
    plus_dm: list[float] = [0.0]
    minus_dm: list[float] = [0.0]
    for idx in range(1, len(bars)):
        up_move = bars[idx]["high"] - bars[idx - 1]["high"]
        down_move = bars[idx - 1]["low"] - bars[idx]["low"]
        tr.append(max(bars[idx]["high"] - bars[idx]["low"], abs(bars[idx]["high"] - bars[idx - 1]["close"]), abs(bars[idx]["low"] - bars[idx - 1]["close"])))
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)
    atr = _sma(tr, length)
    plus_avg = _sma(plus_dm, length)
    minus_avg = _sma(minus_dm, length)
    dx: list[float] = [0.0] * len(bars)
    for idx in range(len(bars)):
        plus_di = _safe_ratio(plus_avg[idx], atr[idx]) * 100.0
        minus_di = _safe_ratio(minus_avg[idx], atr[idx]) * 100.0
        dx[idx] = _safe_ratio(abs(plus_di - minus_di), plus_di + minus_di) * 100.0
    return _sma(dx, length)


def _bb_width(values: list[float], length: int) -> list[float]:
    basis = _sma(values, length)
    result: list[float] = [0.0] * len(values)
    for idx in range(length - 1, len(values)):
        window = values[idx - length + 1 : idx + 1]
        mean = basis[idx] or values[idx]
        variance = sum((value - mean) ** 2 for value in window) / length
        width = (variance**0.5) * 4
        result[idx] = _safe_ratio(width, values[idx]) * 100.0
    return result


def _rsi(values: list[float], length: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    if len(values) <= length:
        return result
    gains = [0.0]
    losses = [0.0]
    for idx in range(1, len(values)):
        change = values[idx] - values[idx - 1]
        gains.append(max(change, 0.0))
        losses.append(abs(min(change, 0.0)))
    avg_gain = sum(gains[1 : length + 1]) / length
    avg_loss = sum(losses[1 : length + 1]) / length
    result[length] = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
    for idx in range(length + 1, len(values)):
        avg_gain = (avg_gain * (length - 1) + gains[idx]) / length
        avg_loss = (avg_loss * (length - 1) + losses[idx]) / length
        result[idx] = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
    return result


def _macd(values: list[float]) -> tuple[list[float | None], list[float | None], list[float | None]]:
    ema12 = _ema(values, 12)
    ema26 = _ema(values, 26)
    line: list[float | None] = [None] * len(values)
    for idx in range(len(values)):
        if ema12[idx] is not None and ema26[idx] is not None:
            line[idx] = float(ema12[idx]) - float(ema26[idx])
    signal_source = [value if value is not None else 0.0 for value in line]
    signal = _ema(signal_source, 9)
    hist: list[float | None] = [None] * len(values)
    for idx in range(len(values)):
        if line[idx] is not None and signal[idx] is not None:
            hist[idx] = float(line[idx]) - float(signal[idx])
    return line, signal, hist


def _safe_ratio(a: float | None, b: float | None, *, fallback: float = 0.0) -> float:
    return float(a) / float(b) if b not in (None, 0) and a is not None else fallback


def _num(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.genesis.hedge_engine import HedgeEngine
from services.genesis.memory_store import MemoryStore
from services.genesis.ticker_parser import normalize_ticker
from services.trading_intelligence.strategy_research_lab import StrategyResearchLab


_BULLISH_TOKENS = ("bullish", "long", "buy", "compra", "comprar", "alcista", "ruptura", "inflow", "accumulation")
_BEARISH_TOKENS = ("bearish", "short", "sell", "venta", "vender", "bajista", "breakdown", "outflow", "distribution")


class GenesisBrain:
    """Unifies market memory, portfolio context and hedge context for decision support only."""

    def __init__(self, *, memory: MemoryStore | None = None, hedge_engine: HedgeEngine | None = None) -> None:
        self.memory = memory or MemoryStore()
        self.hedge_engine = hedge_engine or HedgeEngine(memory=self.memory)

    def build_trading_context(self, ticker: str) -> dict[str, Any]:
        normalized = normalize_ticker(ticker)
        if not normalized:
            return {
                "ok": False,
                "status": "missing_ticker",
                "message": "ticker requerido",
                "genesis_context_score": 0,
                "bias": "neutral",
                "confidence": "low",
            }

        summary = self.memory.get_asset_learning_summary(normalized, limit=12)
        evidence = _context_evidence(summary)
        score = _clamp_score(sum(item["score"] for item in evidence))
        bias = "bullish" if score >= 25 else "bearish" if score <= -25 else "neutral"
        confidence = _confidence(score, evidence)
        hedge_context = self.hedge_engine.build_hedge_context(normalized)
        strategy_research = StrategyResearchLab(memory=self.memory).research(normalized, save=True)
        risk_flags = _risk_flags(summary, evidence, hedge_context)
        technical_context = _technical_context(summary)
        macro_context = _macro_context(summary, hedge_context)
        news_context = {"items": _compact_rows(summary.get("news") or [], limit=5), "risk": hedge_context.get("news_risk") or {}}
        whale_context = {"items": _compact_rows(summary.get("whales") or [], limit=5), "risk": hedge_context.get("whale_risk") or {}}
        alerts_context = {"items": _compact_rows(summary.get("alerts") or summary.get("signals") or [], limit=5)}
        memory_context = {
            "summary_lines": list(summary.get("summary_lines") or [])[:6],
            "counts": summary.get("counts") or {},
            "recent_outcomes": _compact_rows(summary.get("outcomes") or [], limit=5),
            "learned_patterns": _compact_rows(summary.get("patterns") or [], limit=5),
        }
        portfolio_context = hedge_context.get("portfolio_risk") if isinstance(hedge_context.get("portfolio_risk"), dict) else {}
        news_score = _source_score(evidence, "news")
        whale_score = _source_score(evidence, "whales")
        macro_risk_score = int(max(0, min(100, (hedge_context.get("hedge_score") or 0))))
        tradingview_inputs = _suggest_tradingview_inputs(
            bias,
            score,
            hedge_context,
            strategy_research,
            news_score=news_score,
            whale_score=whale_score,
            macro_risk_score=macro_risk_score,
            confidence=confidence,
        )
        return {
            "ok": True,
            "status": "genesis_brain_context_ready",
            "ticker": normalized,
            "asset_name": _asset_name(normalized, summary),
            "genesis_context_score": score,
            "bias": bias,
            "confidence": confidence,
            "market_regime": macro_context.get("market_regime") or "memory_driven_context",
            "technical_context": technical_context,
            "macro_context": macro_context,
            "news_context": news_context,
            "whale_context": whale_context,
            "alerts_context": alerts_context,
            "memory_context": memory_context,
            "portfolio_context": portfolio_context,
            "hedge_context": hedge_context,
            "strategy_research": strategy_research,
            "strategy_version": "Genesis Advantage v10.13 BTC Edge",
            "asset_class": strategy_research.get("asset_class"),
            "recommended_strategy_profile": strategy_research.get("recommended_strategy_profile"),
            "recommended_preset": strategy_research.get("recommended_preset"),
            "recommended_timeframe": strategy_research.get("recommended_timeframe"),
            "no_trade_recommendation": strategy_research.get("no_trade_recommendation"),
            "no_trade_score": strategy_research.get("no_trade_score"),
            "no_trade_decision": strategy_research.get("no_trade_decision"),
            "edge_status": strategy_research.get("edge_status"),
            "edge_finder": strategy_research.get("edge_finder"),
            "btc_regime": strategy_research.get("btc_regime"),
            "btc_edge_score": strategy_research.get("btc_edge_score"),
            "backtest_summary": strategy_research.get("backtest_summary"),
            "benchmark_summary": strategy_research.get("benchmark_summary"),
            "strategy_health": strategy_research.get("strategy_health"),
            "hedge_score": hedge_context.get("hedge_score", 0),
            "hedge_needed": bool(hedge_context.get("hedge_needed")),
            "suggested_hedge_type": hedge_context.get("hedge_type") or "none",
            "suggested_hedge_ratio": hedge_context.get("suggested_hedge_ratio") or 0.0,
            "capital_protection_mode": bool((hedge_context.get("hedge_score") or 0) >= 31),
            "portfolio_risk": portfolio_context,
            "protection_notes": [
                "La cobertura reduce riesgo, pero no elimina perdidas.",
                "Todo queda en paper/journal: no hay orden real ni broker.",
            ],
            "relevant_news": news_context["items"],
            "active_alerts": alerts_context["items"],
            "whale_flow": whale_context["items"],
            "memory_notes": memory_context["summary_lines"],
            "risk_flags": risk_flags,
            "what_to_watch": _what_to_watch(normalized, bias, risk_flags, hedge_context),
            "genesis_reading": _genesis_reading(normalized, bias, confidence, hedge_context),
            "suggested_preset": tradingview_inputs["preset"],
            "suggested_mode": tradingview_inputs["suggested_mode"],
            "suggested_core_tactical_mode": tradingview_inputs["coreTacticalMode"],
            "suggested_hedge_impact_mode": tradingview_inputs["hedgeImpactMode"],
            "avoid_shorts": tradingview_inputs["avoidShortsInBullTrend"],
            "suggested_min_signal_score": tradingview_inputs["minSignalScore"],
            "suggested_trailing_mode": tradingview_inputs["coreTrailMode"],
            "reason": tradingview_inputs["reason"],
            "suggested_tradingview_inputs": tradingview_inputs,
            "evidence_count": len(evidence),
            "source": "GenesisBrain + MemoryStore + HedgeEngine + paper portfolio",
            "policy": "Contexto para TradingView input manual; no es orden, no ejecuta broker.",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }


def build_genesis_brain_context(ticker: str, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return GenesisBrain(memory=memory).build_trading_context(ticker)


def _suggest_tradingview_inputs(
    bias: str,
    score: int,
    hedge_context: dict[str, Any],
    strategy_research: dict[str, Any] | None = None,
    *,
    news_score: int = 0,
    whale_score: int = 0,
    macro_risk_score: int = 0,
    confidence: str = "medium",
) -> dict[str, Any]:
    hedge_score = int(hedge_context.get("hedge_score") or 0)
    hedge_needed = bool(hedge_context.get("hedge_needed"))
    research = strategy_research or {}
    research_inputs = dict(research.get("suggested_tradingview_inputs") or {})
    no_trade_decision = research.get("no_trade_decision") if isinstance(research.get("no_trade_decision"), dict) else {}
    research_mode = str(research.get("recommended_preset") or research_inputs.get("preset") or "")
    suggested_mode = "Defensive" if hedge_score >= 76 else research_mode or ("Core Tactical" if bias == "bullish" and hedge_score < 56 else "Paper Quality")
    preset = "Conservative" if hedge_score >= 76 else research_inputs.get("preset") or ("Core Tactical" if suggested_mode == "Core Tactical" else suggested_mode if suggested_mode else "Paper Quality")
    min_signal = int(research_inputs.get("minSignalScore") or (70 if suggested_mode == "Defensive" else 62 if suggested_mode == "Core Tactical" else 65))
    asset_class = str(research.get("asset_class") or research_inputs.get("assetProfile") or "")
    allow_crypto_shorts = asset_class == "Crypto" and bool(research_inputs.get("enableShorts"))
    trade_mode = "Long Only" if bias == "bullish" and hedge_score < 76 else "Long & Short"
    hedge_impact_mode = "Defensive" if hedge_score >= 76 else "Balanced" if hedge_needed else "Light"
    reason = (
        "Contexto bullish y hedge bajo: favorecer exposicion core, evitar shorts contra tendencia y trailing EMA50."
        if suggested_mode == "Core Tactical"
        else "Hedge score alto: priorizar defensa, reducir agresividad y no ejecutar broker."
        if suggested_mode == "Defensive"
        else "Contexto mixto: usar Paper Quality y esperar confirmacion tecnica."
    )
    return {
        **research_inputs,
        "autopilotMode": True,
        "enableShorts": allow_crypto_shorts,
        "useGenesisSync": True,
        "genesisBiasInput": "Bullish" if bias == "bullish" else "Bearish" if bias == "bearish" else "Neutral",
        "genesisConfidenceInput": "High" if confidence == "high" else "Low" if confidence == "low" else "Medium",
        "genesisMarketRegimeInput": "Risk Off" if hedge_score >= 76 else "Bearish" if bias == "bearish" and hedge_score >= 56 else "Bullish" if bias == "bullish" and hedge_score < 56 else "Neutral",
        "genesisNewsScore": news_score,
        "genesisWhaleScore": whale_score,
        "genesisMacroRiskScore": macro_risk_score,
        "safeMode": False,
        "validationMode": False,
        "preset": preset,
        "suggested_mode": suggested_mode,
        "coreTacticalMode": bool(research_inputs.get("coreTacticalMode")) or suggested_mode == "Core Tactical",
        "minSignalScore": min_signal,
        "useHedgeMode": True,
        "hedgeScoreInput": hedge_score,
        "avoidTradeIfHedgeScoreAbove": 75,
        "reduceSizeIfHedgeScoreAbove": 55,
        "hedgeImpactMode": hedge_impact_mode,
        "noTradeMode": True,
        "noTradeScoreInput": int(no_trade_decision.get("no_trade_score") or research_inputs.get("noTradeScoreInput") or 0),
        "blockIfNoEdge": True,
        "trendRunnerMode": bool(research_inputs.get("trendRunnerMode")) or suggested_mode in {"Core Tactical", "Paper Quality", "Defensive ETF Core", "Crypto Momentum", "Crypto Momentum V3", "Crypto Momentum V4"},
        "avoidShortsInBullTrend": True,
        "useHTFConfirmation": True,
        "useVolumeFilter": hedge_score < 76,
        "useMarketRegimeFilter": True,
        "tradeMode": research_inputs.get("tradeMode") or trade_mode,
        "coreTrailMode": research_inputs.get("coreTrailMode") or "EMA50",
        "coreATRMultiplier": research_inputs.get("coreATRMultiplier") or 3.0,
        "tacticalATRMultiplier": research_inputs.get("tacticalATRMultiplier") or 1.8,
        "notes": (
            "Hedge activo: priorizar proteccion y evitar entradas sin ventaja."
            if hedge_needed
            else "Usar como input manual en TradingView; primero paper trading."
        ),
        "reason": f"{reason} {(research.get('reason') or '').strip()}".strip(),
        "genesisContextScore": score,
    }


def _source_score(evidence: list[dict[str, Any]], source: str) -> int:
    total = sum(int(item.get("score") or 0) for item in evidence if item.get("source") == source)
    return _clamp_score(total * 4)


def _technical_context(summary: dict[str, Any]) -> dict[str, Any]:
    latest_signal = _latest_payload(summary.get("signals") or [])
    latest_decision = _latest_payload(summary.get("decisions") or [])
    source = latest_signal or latest_decision
    keys = (
        "price",
        "volume",
        "relative_volume",
        "dollar_volume",
        "support",
        "resistance",
        "trend",
        "momentum",
        "rsi",
        "macd",
        "ema20",
        "ema50",
        "ema200",
        "sma20",
        "sma50",
        "vwap",
        "fib618",
        "fib65",
        "golden_pocket",
        "atr",
        "adx",
        "bollinger_width",
        "market_regime",
    )
    return {key: source.get(key) for key in keys if source.get(key) is not None} | {
        "source": "latest_strategy_signal_or_decision",
        "available": bool(source),
    }


def _macro_context(summary: dict[str, Any], hedge_context: dict[str, Any]) -> dict[str, Any]:
    return {
        "market_regime": _latest_payload(summary.get("signals") or []).get("market_regime") or "not_confirmed",
        "risk_level": hedge_context.get("risk_level") or "low",
        "risk_off_detected": bool((hedge_context.get("macro_risk") or {}).get("risk_off_detected")),
        "items": _compact_rows(summary.get("patterns") or [], limit=4),
    }


def _context_evidence(summary: dict[str, Any]) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for row in summary.get("signals") or []:
        evidence.append({"source": "signals", "score": _direction_score(_row_text(row), 18)})
    for row in summary.get("decisions") or []:
        evidence.append({"source": "decisions", "score": _direction_score(_row_text(row), 20)})
    for row in summary.get("alerts") or []:
        evidence.append({"source": "alerts", "score": _direction_score(_row_text(row), 12)})
    for row in summary.get("whales") or []:
        evidence.append({"source": "whales", "score": _direction_score(_row_text(row), 14)})
    for row in summary.get("news") or []:
        evidence.append({"source": "news", "score": _direction_score(_row_text(row), 8)})
    for row in summary.get("outcomes") or []:
        text = _row_text(row)
        if "miss" in text or "fallo" in text:
            evidence.append({"source": "outcomes", "score": -8})
        elif "hit" in text or "acierto" in text:
            evidence.append({"source": "outcomes", "score": 8})
    return [item for item in evidence if item["score"] != 0]


def _compact_rows(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for row in rows[:limit]:
        if not isinstance(row, dict):
            continue
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else row
        compact.append(
            {
                "event_type": row.get("event_type") or payload.get("event_type") or "",
                "title": payload.get("title_es") or payload.get("title") or payload.get("verdict") or payload.get("setup") or "",
                "summary": payload.get("summary_es") or payload.get("summary") or payload.get("genesis_reading") or payload.get("notes") or "",
                "source": row.get("source") or payload.get("source") or "",
                "confidence": row.get("confidence") or payload.get("confidence") or "",
                "timestamp": row.get("created_at") or payload.get("timestamp") or payload.get("created_at") or "",
            }
        )
    return compact


def _latest_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    for row in rows:
        if isinstance(row, dict):
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else row
            if isinstance(payload, dict):
                return payload
    return {}


def _asset_name(ticker: str, summary: dict[str, Any]) -> str:
    for group in ("asset_memory", "signals", "decisions", "news"):
        for row in summary.get(group) or []:
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else row
            name = payload.get("asset_name") or payload.get("name")
            if name:
                return str(name)[:240]
    return ticker


def _risk_flags(summary: dict[str, Any], evidence: list[dict[str, Any]], hedge_context: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    if not evidence:
        flags.append("Sin evidencia suficiente en memoria; usar score 0 y validar solo por chart.")
    if not summary.get("outcomes"):
        flags.append("Aun no hay outcomes 1h/24h/7d suficientes para medir ventaja.")
    bearish = sum(1 for item in evidence if item["score"] < 0)
    bullish = sum(1 for item in evidence if item["score"] > 0)
    if bearish and bullish:
        flags.append("Evidencia mixta; exigir confirmacion de precio, volumen y nivel.")
    if (hedge_context.get("hedge_score") or 0) >= 56:
        flags.append("Hedge score elevado; reducir agresividad o proteger ganancias en paper.")
    return flags[:6]


def _what_to_watch(ticker: str, bias: str, risk_flags: list[str], hedge_context: dict[str, Any]) -> list[str]:
    direction = "alcista" if bias == "bullish" else "bajista" if bias == "bearish" else "neutral"
    items = [
        f"{ticker}: validar que el setup tecnico confirme el sesgo {direction}.",
        "Volumen relativo y ruptura/rechazo del nivel clave.",
        "RR minimo antes de elevar conviccion.",
        "Webhook a Genesis para guardar journal y outcomes.",
    ]
    items.extend(hedge_context.get("what_to_watch") or [])
    if risk_flags:
        items.append("Si la evidencia sigue incompleta, tratarlo como watch only.")
    return _unique(items)[:8]


def _genesis_reading(ticker: str, bias: str, confidence: str, hedge_context: dict[str, Any]) -> str:
    hedge_score = hedge_context.get("hedge_score") or 0
    return (
        f"{ticker}: sesgo Genesis {bias} con confianza {confidence}; hedge_score {hedge_score}/100. "
        "Usar como filtro de contexto para TradingView. No es orden real ni promesa de rentabilidad."
    )


def _direction_score(text: str, weight: int) -> int:
    folded = text.casefold()
    bullish = any(token in folded for token in _BULLISH_TOKENS)
    bearish = any(token in folded for token in _BEARISH_TOKENS)
    if bullish and not bearish:
        return weight
    if bearish and not bullish:
        return -weight
    return 0


def _confidence(score: int, evidence: list[dict[str, Any]]) -> str:
    groups = {item["source"] for item in evidence}
    if abs(score) >= 60 and len(groups) >= 3:
        return "high"
    if abs(score) >= 25 or len(groups) >= 2:
        return "medium"
    return "low"


def _row_text(row: dict[str, Any]) -> str:
    return str(row)[:3000]


def _clamp_score(value: int | float) -> int:
    return int(max(-100, min(100, round(value))))


def _unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    clean: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text and text not in seen:
            clean.append(text)
            seen.add(text)
    return clean

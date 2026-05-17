from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.genesis.memory_store import MemoryStore
from services.genesis.ticker_parser import normalize_ticker
from services.portfolio.get_portfolio_snapshot import normalize_portfolio_positions
from services.portfolio.portfolio_store import PortfolioStore


_BEARISH_TOKENS = (
    "bearish",
    "bajista",
    "sell",
    "venta",
    "outflow",
    "distribution",
    "distribucion",
    "support_break",
    "soporte perdido",
    "risk-off",
    "riesgo alto",
    "recession",
    "inflacion",
    "fed hawkish",
    "geopolitical",
    "geopolitico",
)
_BULLISH_TOKENS = (
    "bullish",
    "alcista",
    "long",
    "buy",
    "compra",
    "inflow",
    "accumulation",
    "acumulacion",
    "breakout",
    "ruptura",
)
_INDEX_HEDGES = {
    "SPY": ("SH", "SPXU"),
    "VOO": ("SH", "SPXU"),
    "QQQ": ("PSQ", "SQQQ"),
    "NVDA": ("QQQ hedge / PSQ", "SQQQ"),
    "MSFT": ("QQQ hedge / PSQ", "SQQQ"),
    "AAPL": ("QQQ hedge / PSQ", "SQQQ"),
    "META": ("QQQ hedge / PSQ", "SQQQ"),
    "TSLA": ("QQQ hedge / PSQ", "SQQQ"),
}


class HedgeEngine:
    """Builds paper-only capital-protection context. It never routes broker orders."""

    def __init__(
        self,
        *,
        memory: MemoryStore | None = None,
        portfolio_store: PortfolioStore | None = None,
    ) -> None:
        self.memory = memory or MemoryStore()
        self.portfolio_store = portfolio_store or PortfolioStore()

    def build_hedge_context(self, ticker: str | None = None, portfolio_mode: bool = False) -> dict[str, Any]:
        normalized = normalize_ticker(ticker or "")
        positions = self._portfolio_positions()
        portfolio_context = self._portfolio_context(positions)
        if portfolio_mode or not normalized:
            plans = [
                self.recommend_hedge_plan(
                    str(item.get("ticker") or ""),
                    item,
                    {"portfolio_context": portfolio_context, "memory_context": self.memory.get_asset_learning_summary(str(item.get("ticker") or ""), limit=8)},
                )
                for item in positions
                if normalize_ticker(item.get("ticker") or "")
            ]
            top_score = max([_safe_float(item.get("hedge_score")) or 0 for item in plans], default=0)
            return {
                "ok": True,
                "ticker": normalized,
                "portfolio_mode": True,
                "hedge_needed": top_score >= 56,
                "hedge_score": int(top_score),
                "risk_level": _risk_level(top_score),
                "portfolio_risk": portfolio_context,
                "plans": plans,
                "suggested_reduction_pct": _ratio_from_score(top_score, "Moderado"),
                "what_to_watch": _unique(
                    ["SPY/QQQ bajo EMA50/200, VIX, DXY y concentracion de cartera."]
                    + [line for plan in plans for line in plan.get("what_to_watch", [])[:2]]
                )[:6],
                "genesis_reading": (
                    "Cartera paper sin posiciones compradas; Genesis solo puede sugerir vigilancia."
                    if not positions
                    else "Genesis revisa exposicion paper y propone defensa parcial si el contexto se deteriora. No ejecuta orden real."
                ),
                "order_policy": "journal_only_no_broker",
                "broker_touched": False,
                "generated_at": _now(),
            }
        position_context = _find_position(positions, normalized)
        memory_context = self.memory.get_asset_learning_summary(normalized, limit=10)
        return self.recommend_hedge_plan(
            normalized,
            position_context,
            {
                "portfolio_context": portfolio_context,
                "memory_context": memory_context,
            },
        )

    def recommend_hedge_plan(
        self,
        ticker: str,
        position_context: dict[str, Any] | None,
        market_context: dict[str, Any] | None,
    ) -> dict[str, Any]:
        normalized = normalize_ticker(ticker or "")
        context = market_context if isinstance(market_context, dict) else {}
        position = position_context if isinstance(position_context, dict) else {}
        portfolio_context = context.get("portfolio_context") if isinstance(context.get("portfolio_context"), dict) else {}
        memory_context = context.get("memory_context") if isinstance(context.get("memory_context"), dict) else {}
        score_details = self.score_hedge_need(normalized, {**context, "position_context": position})
        hedge_score = score_details["hedge_score"]
        hedge_type = _hedge_type(normalized, hedge_score, position, portfolio_context)
        ratio = _ratio_from_score(hedge_score, str(position.get("hedgeRiskProfile") or "Moderado"))
        price = _safe_float(position.get("current_price") or position.get("reference_price") or position.get("price"))
        suggested_stop = round(price * (0.94 if hedge_score >= 76 else 0.96), 4) if price else None
        conservative, aggressive = _INDEX_HEDGES.get(normalized, ("sector_or_index_hedge", "inverse_index_hedge"))
        inverse_symbol = aggressive if hedge_score >= 76 else conservative
        risk_level = _risk_level(hedge_score)
        hedge_needed = hedge_score >= 56
        what_to_watch = _unique(
            [
                "VIX/risk-off, SPY/QQQ y DXY antes de elevar exposicion.",
                "Si el precio pierde soporte con volumen, priorizar proteccion de capital.",
                "Si la cobertura erosiona rendimiento sin bajar drawdown, reducirla en paper.",
            ]
            + score_details["reasons"][:4]
        )[:6]
        reading = (
            f"{normalized}: hedge_score {hedge_score}/100 ({risk_level}). "
            "La cobertura reduce riesgo, pero no elimina perdidas. Plan paper/journal only."
        )
        return {
            "ok": True,
            "ticker": normalized,
            "asset_name": str(position.get("display_name") or position.get("asset_name") or normalized),
            "hedge_needed": hedge_needed,
            "hedge_score": hedge_score,
            "hedge_type": hedge_type,
            "reason": "; ".join(score_details["reasons"][:3]) or "Sin deterioro fuerte confirmado; mantener vigilancia.",
            "risk_level": risk_level,
            "market_risk": score_details["market_risk"],
            "technical_risk": score_details["technical_risk"],
            "news_risk": score_details["news_risk"],
            "macro_risk": score_details["macro_risk"],
            "whale_risk": score_details["whale_risk"],
            "portfolio_risk": portfolio_context,
            "suggested_hedge_ratio": ratio,
            "suggested_reduction_pct": ratio if hedge_type in {"reduce_exposure", "cash_hedge"} else round(ratio * 0.5, 2),
            "suggested_stop": suggested_stop,
            "suggested_trailing": round((_safe_float(position.get("atr")) or (price * 0.025 if price else 0)) * 2.2, 4) if price else None,
            "suggested_inverse_symbol": inverse_symbol,
            "suggested_option": {
                "options_data_confirmed": False,
                "note": "No se inventa cadena de opciones; protective put/covered call solo si existe options chain real.",
            },
            "what_to_watch": what_to_watch,
            "genesis_reading": reading,
            "source_status": {
                "memory_backend": self.memory.backend,
                "portfolio_backend": self.portfolio_store.status().get("backend"),
                "options_data_confirmed": False,
                "broker_touched": False,
                "order_policy": "journal_only_no_broker",
            },
            "generated_at": _now(),
        }

    def score_hedge_need(self, ticker: str, context: dict[str, Any] | None) -> dict[str, Any]:
        normalized = normalize_ticker(ticker or "")
        data = context if isinstance(context, dict) else {}
        memory_context = data.get("memory_context") if isinstance(data.get("memory_context"), dict) else {}
        portfolio_context = data.get("portfolio_context") if isinstance(data.get("portfolio_context"), dict) else {}
        position_context = data.get("position_context") if isinstance(data.get("position_context"), dict) else {}

        alerts = memory_context.get("alerts") or []
        whales = memory_context.get("whales") or []
        news = memory_context.get("news") or []
        signals = memory_context.get("signals") or []
        outcomes = memory_context.get("outcomes") or []
        text = " ".join(_row_text(item) for item in [*alerts, *whales, *news, *signals, *outcomes, position_context, portfolio_context])
        bearish_hits = _token_hits(text, _BEARISH_TOKENS)
        bullish_hits = _token_hits(text, _BULLISH_TOKENS)
        concentration = _safe_float(position_context.get("weight_pct") or position_context.get("portfolio_weight_pct"))
        concentration = concentration if concentration is not None else _safe_float(portfolio_context.get("top_weight_pct"))
        drawdown = abs(_safe_float(position_context.get("drawdown_pct") or portfolio_context.get("drawdown_pct")) or 0)
        open_profit = _safe_float(position_context.get("unrealized_pnl_pct") or position_context.get("pnl_pct")) or 0
        base = 18
        score = base
        reasons: list[str] = []
        if bearish_hits:
            score += min(28, bearish_hits * 7)
            reasons.append("memoria/noticias/alertas con sesgo de riesgo")
        if bullish_hits and not bearish_hits:
            score -= min(12, bullish_hits * 4)
            reasons.append("contexto favorable reduce necesidad de cobertura")
        if concentration and concentration >= 35:
            score += 16
            reasons.append("concentracion alta en cartera paper")
        elif concentration and concentration >= 20:
            score += 8
            reasons.append("concentracion moderada en cartera paper")
        if drawdown >= 8:
            score += 14
            reasons.append("drawdown actual elevado")
        elif drawdown >= 4:
            score += 7
            reasons.append("drawdown actual a vigilar")
        if open_profit >= 8:
            score += 8
            reasons.append("hay ganancia abierta que puede protegerse")
        if normalized in {"BTC", "BTC-USD", "ETH", "ETH-USD"}:
            score += 5
            reasons.append("activo cripto con volatilidad estructural")
        score = int(_clamp(score, 0, 100))
        if not reasons:
            reasons.append("sin alerta defensiva fuerte confirmada")
        return {
            "ticker": normalized,
            "hedge_score": score,
            "reasons": reasons,
            "market_risk": {"score": score, "bearish_hits": bearish_hits, "bullish_hits": bullish_hits},
            "technical_risk": {"support_break_detected": "support_break" in text.casefold() or "soporte perdido" in text.casefold()},
            "news_risk": {"risk_tokens": bearish_hits, "items": _compact_memory(news, 3)},
            "macro_risk": {"risk_off_detected": "risk-off" in text.casefold() or "riesgo alto" in text.casefold()},
            "whale_risk": {"outflow_detected": "outflow" in text.casefold() or "distribution" in text.casefold(), "items": _compact_memory(whales, 3)},
        }

    def track_hedge_outcome(self, event_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.memory.save_event(
            "hedge_outcome",
            {"event_id": str(event_id or "")[:160], **(payload or {}), "order_policy": "journal_only_no_broker"},
            "hedge_engine",
            "media",
        )

    def _portfolio_positions(self) -> list[dict[str, Any]]:
        try:
            return normalize_portfolio_positions(self.portfolio_store.read_raw())
        except Exception:
            return []

    def _portfolio_context(self, positions: list[dict[str, Any]]) -> dict[str, Any]:
        valued = [_shape_position(item) for item in positions if not item.get("removed_watchlist")]
        paper = [item for item in valued if (_safe_float(item.get("units")) or 0) > 0 or str(item.get("mode") or "").lower() == "paper"]
        total_value = sum(_safe_float(item.get("market_value") or item.get("amount_usd") or item.get("value")) or 0 for item in paper)
        if not total_value:
            total_value = sum((_safe_float(item.get("units")) or 0) * (_safe_float(item.get("reference_price") or item.get("current_price")) or 0) for item in paper)
        weights = []
        for item in paper:
            value = _safe_float(item.get("market_value") or item.get("amount_usd") or item.get("value"))
            if value is None:
                value = (_safe_float(item.get("units")) or 0) * (_safe_float(item.get("reference_price") or item.get("current_price")) or 0)
            weight = (value / total_value * 100) if total_value else 0
            item["weight_pct"] = round(weight, 2)
            weights.append(weight)
        return {
            "positions_count": len(paper),
            "watchlist_count": len([item for item in valued if item.get("watchlist")]),
            "total_value": round(total_value, 2),
            "top_weight_pct": round(max(weights), 2) if weights else 0,
            "concentration_risk": "high" if weights and max(weights) >= 35 else "medium" if weights and max(weights) >= 20 else "low",
            "positions": paper[:12],
            "source": "PortfolioStore",
            "broker_touched": False,
        }


def build_hedge_context(ticker: str | None = None, portfolio_mode: bool = False, *, memory: MemoryStore | None = None) -> dict[str, Any]:
    return HedgeEngine(memory=memory).build_hedge_context(ticker, portfolio_mode=portfolio_mode)


def _hedge_type(ticker: str, score: float, position: dict[str, Any], portfolio: dict[str, Any]) -> str:
    if score < 31:
        return "none"
    if ticker in {"BTC", "BTC-USD", "ETH", "ETH-USD"}:
        return "crypto_hedge" if score >= 56 else "cash_hedge"
    if score >= 76:
        return "reduce_exposure" if portfolio.get("concentration_risk") == "high" else "protective_stop"
    if score >= 56:
        return "protective_stop"
    if score >= 31:
        return "cash_hedge"
    return "none"


def _ratio_from_score(score: float, profile: str) -> float:
    base = 0.0
    if score >= 76:
        base = 0.5
    elif score >= 56:
        base = 0.3
    elif score >= 31:
        base = 0.15
    profile_text = str(profile or "").casefold()
    if "conserv" in profile_text:
        base *= 1.2
    elif "agres" in profile_text:
        base *= 0.75
    return round(_clamp(base, 0.0, 1.0), 2)


def _risk_level(score: float) -> str:
    if score >= 76:
        return "high"
    if score >= 56:
        return "medium"
    if score >= 31:
        return "medium"
    return "low"


def _find_position(positions: list[dict[str, Any]], ticker: str) -> dict[str, Any]:
    normalized = normalize_ticker(ticker)
    for item in positions:
        if normalize_ticker(item.get("ticker") or "") == normalized:
            return _shape_position(item)
    return {"ticker": normalized, "watchlist": False, "paper_position": False}


def _shape_position(item: dict[str, Any]) -> dict[str, Any]:
    shaped = dict(item or {})
    units = _safe_float(shaped.get("units")) or 0
    price = _safe_float(shaped.get("current_price") or shaped.get("reference_price") or shaped.get("entry_price"))
    value = _safe_float(shaped.get("market_value") or shaped.get("amount_usd") or shaped.get("value"))
    if value is None and price is not None:
        value = units * price
    shaped["market_value"] = round(value or 0, 2)
    shaped["paper_position"] = units > 0 or str(shaped.get("mode") or "").lower() == "paper"
    return shaped


def _compact_memory(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    items = []
    for row in rows[:limit]:
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else row
        items.append(
            {
                "event_type": row.get("event_type") or payload.get("event_type") or "",
                "summary": payload.get("summary") or payload.get("genesis_reading") or payload.get("notes") or payload.get("title") or "",
                "confidence": row.get("confidence") or payload.get("confidence") or "",
            }
        )
    return items


def _token_hits(text: str, tokens: tuple[str, ...]) -> int:
    folded = text.casefold()
    return sum(1 for token in tokens if token in folded)


def _row_text(row: Any) -> str:
    return str(row or "")[:5000]


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "na", "NaN"):
        return None
    try:
        return float(str(value).replace("$", "").replace("%", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    clean: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text and text not in seen:
            clean.append(text)
            seen.add(text)
    return clean


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

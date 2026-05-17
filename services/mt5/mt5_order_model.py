from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


_SECRET_KEY_PARTS = ("password", "pass", "secret", "token", "api_key", "apikey", "credential")


@dataclass(frozen=True)
class MT5OrderIntent:
    symbol: str
    action: str
    entry: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None
    trailing_stop: float | None = None
    risk_pct: float = 0.0
    lot_size_hint: float | None = None
    confidence: str = "low"
    strategy_profile: str = ""
    timeframe: str = ""
    hedge_score: int = 0
    no_trade_score: int = 0
    genesis_context_score: int = 0
    spread_points: float | None = None
    open_trades: int = 0
    daily_loss_pct: float = 0.0
    source: str = "genesis"
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: dict[str, Any] | None) -> "MT5OrderIntent":
        body = sanitize_payload(payload or {})
        return cls(
            symbol=str(body.get("symbol") or body.get("ticker") or "").upper().strip(),
            action=str(body.get("action") or body.get("decision") or "WAIT").upper().strip(),
            entry=_to_float(body.get("entry") or body.get("price")),
            stop_loss=_to_float(body.get("stop_loss") or body.get("sl") or body.get("stop")),
            take_profit=_to_float(body.get("take_profit") or body.get("tp") or body.get("target")),
            trailing_stop=_to_float(body.get("trailing_stop")),
            risk_pct=_to_float(body.get("risk_pct") or body.get("riskPerTradePct")) or 0.0,
            lot_size_hint=_to_float(body.get("lot_size_hint") or body.get("lot")),
            confidence=str(body.get("confidence") or "low").lower(),
            strategy_profile=str(body.get("strategy_profile") or body.get("strategy") or ""),
            timeframe=str(body.get("timeframe") or ""),
            hedge_score=int(_to_float(body.get("hedge_score")) or 0),
            no_trade_score=int(_to_float(body.get("no_trade_score")) or 0),
            genesis_context_score=int(_to_float(body.get("genesis_context_score")) or 0),
            spread_points=_to_float(body.get("spread_points") or body.get("spread")),
            open_trades=int(_to_float(body.get("open_trades")) or 0),
            daily_loss_pct=_to_float(body.get("daily_loss_pct")) or 0.0,
            source=str(body.get("source") or "genesis"),
            raw=body,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "action": self.action,
            "entry": self.entry,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "trailing_stop": self.trailing_stop,
            "risk_pct": self.risk_pct,
            "lot_size_hint": self.lot_size_hint,
            "confidence": self.confidence,
            "strategy_profile": self.strategy_profile,
            "timeframe": self.timeframe,
            "hedge_score": self.hedge_score,
            "no_trade_score": self.no_trade_score,
            "genesis_context_score": self.genesis_context_score,
            "spread_points": self.spread_points,
            "open_trades": self.open_trades,
            "daily_loss_pct": self.daily_loss_pct,
            "source": self.source,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


def sanitize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    clean: dict[str, Any] = {}
    for key, value in (payload or {}).items():
        key_text = str(key or "").strip()[:120]
        if not key_text:
            continue
        folded = key_text.casefold()
        if any(part in folded for part in _SECRET_KEY_PARTS):
            clean[key_text] = "[redacted]"
            continue
        if isinstance(value, dict):
            clean[key_text] = sanitize_payload(value)
        elif isinstance(value, list):
            clean[key_text] = [sanitize_payload(item) if isinstance(item, dict) else item for item in value[:100]]
        else:
            clean[key_text] = value
    return clean


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from services.genesis.memory_store import MemoryStore
from services.mt5.instrument_resolver import enrich_payload
from services.mt5.mt5_auto_forward import MT5AutoForward
from services.mt5.mt5_journal import MT5Journal
from services.mt5.mt5_order_model import sanitize_payload
from services.mt5.mt5_performance import MT5Performance
from services.mt5.mt5_risk_guard import MT5BridgeConfig
from services.mt5.mt5_shadow_trading import MT5ShadowTrading
from services.mt5.mt5_symbol_mapper import MT5SymbolMapper


class MT5ForwardTestEngine:
    def __init__(
        self,
        *,
        memory: MemoryStore | None = None,
        config: MT5BridgeConfig | None = None,
        symbol_mapper: MT5SymbolMapper | None = None,
    ) -> None:
        self.memory = memory or MemoryStore()
        self.journal = MT5Journal(memory=self.memory)
        self.shadow = MT5ShadowTrading(memory=self.memory)
        self.performance = MT5Performance(memory=self.memory)
        self.auto_forward = MT5AutoForward(memory=self.memory, config=config, symbol_mapper=symbol_mapper)

    def record_tick(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        clean = sanitize_payload(payload or {})
        symbol = str(clean.get("symbol") or clean.get("ticker") or "").upper().strip()
        last = _price(clean)
        if not symbol or last is None:
            return {
                "ok": False,
                "status": "tick_missing_symbol_or_price",
                "broker_touched": False,
                "order_executed": False,
                "order_policy": "journal_only_no_broker",
            }
        tick = enrich_payload({
            **clean,
            "symbol": symbol,
            "last": last,
            "timeframe": str(clean.get("timeframe") or "").upper(),
            "source": str(clean.get("source") or "mt5_bridge"),
            "timestamp": str(clean.get("timestamp") or clean.get("bar_time") or _now()),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        })
        logging.getLogger("genesis.mt5").info("MT5_TICK_RECEIVED symbol=%s source=%s", symbol, tick.get("source"))
        event = self.journal.save("mt5_ticks", symbol, tick)
        updates = self.shadow.update_with_tick(tick)
        auto_forward = self.auto_forward.process_tick(tick)
        return {
            "ok": True,
            "status": "mt5_tick_recorded",
            "symbol": symbol,
            "tick_saved": True,
            "auto_forward_checked": True,
            "tick": tick,
            "event": event,
            "shadow_updates": updates,
            "auto_forward": auto_forward,
            "auto_shadow_trade_created": bool(auto_forward.get("shadow_trade_created")),
            "broker_touched": False,
            "order_executed": False,
            "order_policy": "journal_only_no_broker",
        }

    def forward_test(self, *, symbol: str = "", timeframe: str = "") -> dict[str, Any]:
        report = self.performance.report(symbol=symbol, timeframe=timeframe)
        return {
            **report,
            "status": "mt5_forward_test_ready",
            "forward_test": {
                "mode": "shadow_trading",
                "journal_only": True,
                "broker_touched": False,
                "order_executed": False,
                "latest_summary": report["summary"],
            },
        }

    def outcomes_recent(self, *, symbol: str = "", limit: int = 25) -> dict[str, Any]:
        return self.performance.outcomes_recent(symbol=symbol, limit=limit)

    def auto_forward_status(self, *, symbol: str = "") -> dict[str, Any]:
        return self.auto_forward.status(symbol=symbol)


def _price(payload: dict[str, Any]) -> float | None:
    for key in ("last", "price"):
        value = _number(payload.get(key))
        if value is not None:
            return value
    bid = _number(payload.get("bid"))
    ask = _number(payload.get("ask"))
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    return bid if bid is not None else ask


def _number(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()

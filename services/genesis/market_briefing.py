from __future__ import annotations

from services.dashboard.get_radar_snapshot import get_radar_snapshot
from services.genesis.market_format import format_signed_money, format_signed_percent


def get_portfolio_briefing() -> dict:
    snapshot = get_radar_snapshot()
    summary = snapshot.get("summary") if isinstance(snapshot, dict) else {}
    total = summary.get("total_value")
    pnl = summary.get("total_unrealized_pnl") or summary.get("daily_pnl")
    pnl_pct = summary.get("total_unrealized_pnl_pct") or summary.get("daily_pnl_pct")
    positions = int(summary.get("number_of_positions") or 0)
    return {
        "intent": "portfolio",
        "answer": f"Cartera paper: {positions} posiciones, valor calculado {format_signed_money(total).replace('+', '')}, P/L {format_signed_money(pnl)} {format_signed_percent(pnl_pct)}.",
        "snapshot": snapshot,
    }


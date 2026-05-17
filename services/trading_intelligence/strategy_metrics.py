from __future__ import annotations

from statistics import mean, pstdev
from typing import Any


def calculate_strategy_metrics(
    trades: list[dict[str, Any]] | None = None,
    *,
    equity_curve: list[float] | None = None,
    benchmark_return: float | None = None,
) -> dict[str, Any]:
    trades = trades or []
    pnls = [_trade_pnl_pct(trade) for trade in trades]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    total_trades = len(pnls)
    net_profit = round(sum(pnls), 4)
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    profit_factor = round(gross_profit / gross_loss, 4) if gross_loss > 0 else (round(gross_profit, 4) if gross_profit > 0 else 0.0)
    win_rate = round(len(wins) / total_trades * 100, 2) if total_trades else 0.0
    avg_trade = round(mean(pnls), 4) if pnls else 0.0
    average_win = round(mean(wins), 4) if wins else 0.0
    average_loss = round(mean(losses), 4) if losses else 0.0
    win_loss_ratio = round(average_win / abs(average_loss), 4) if average_loss else (average_win if average_win else 0.0)
    expectancy = round((win_rate / 100.0) * average_win - (1 - win_rate / 100.0) * abs(average_loss), 4) if total_trades else 0.0
    max_drawdown = round(calculate_max_drawdown(equity_curve or _equity_from_trades(pnls)), 4)
    benchmark = round(float(benchmark_return or 0.0), 4)
    benchmark_capture_ratio = round(net_profit / benchmark, 4) if benchmark > 0 else 0.0
    return_over_drawdown = round(net_profit / max_drawdown, 4) if max_drawdown > 0 else (net_profit if net_profit > 0 else 0.0)
    stability_score = _stability_score(pnls, profit_factor, max_drawdown)
    out_of_sample_score = _out_of_sample_score(pnls)
    regime_score = _regime_score(net_profit, profit_factor, max_drawdown, total_trades)
    flags = _quality_flags(total_trades, profit_factor, win_rate, max_drawdown, benchmark_capture_ratio)
    no_trade_score = _no_trade_score(
        profit_factor=profit_factor,
        expectancy=expectancy,
        max_drawdown=max_drawdown,
        net_profit=net_profit,
        total_trades=total_trades,
        benchmark_capture_ratio=benchmark_capture_ratio,
    )
    return {
        "net_profit": net_profit,
        "profit_factor": profit_factor,
        "win_rate": win_rate,
        "max_drawdown": max_drawdown,
        "total_trades": total_trades,
        "avg_trade": avg_trade,
        "expectancy": expectancy,
        "average_win": average_win,
        "average_loss": average_loss,
        "win_loss_ratio": win_loss_ratio,
        "benchmark_return": benchmark,
        "benchmark_capture_ratio": benchmark_capture_ratio,
        "return_over_drawdown": return_over_drawdown,
        "stability_score": stability_score,
        "out_of_sample_score": out_of_sample_score,
        "regime_score": regime_score,
        "no_trade_score": no_trade_score,
        "quality_flags": flags,
        "status": "sample_insufficient" if total_trades < 10 else "evaluated",
    }


def calculate_max_drawdown(equity_curve: list[float]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        if peak:
            max_dd = max(max_dd, (peak - value) / peak * 100.0)
    return max_dd


def score_metrics(metrics: dict[str, Any]) -> float:
    profit_factor = float(metrics.get("profit_factor") or 0)
    expectancy = float(metrics.get("expectancy") or 0)
    drawdown = float(metrics.get("max_drawdown") or 0)
    trades = int(metrics.get("total_trades") or 0)
    stability = float(metrics.get("stability_score") or 0)
    sample_score = min(trades / 40.0, 1.0) * 15.0
    return round(max(0.0, min(100.0, profit_factor * 22.0 + expectancy * 3.0 + stability + sample_score - drawdown * 1.4)), 2)


def _trade_pnl_pct(trade: dict[str, Any]) -> float:
    for key in ("pnl_pct", "return_pct", "pnl", "profit_pct"):
        if trade.get(key) is not None:
            try:
                return float(trade[key])
            except (TypeError, ValueError):
                return 0.0
    entry = _num(trade.get("entry"))
    exit_price = _num(trade.get("exit"))
    side = str(trade.get("side") or "long").casefold()
    if entry and exit_price is not None:
        raw = (exit_price - entry) / entry * 100.0
        return -raw if side == "short" else raw
    return 0.0


def _equity_from_trades(pnls: list[float]) -> list[float]:
    equity = 100.0
    curve = [equity]
    for pnl in pnls:
        equity *= 1 + pnl / 100.0
        curve.append(equity)
    return curve


def _stability_score(pnls: list[float], profit_factor: float, max_drawdown: float) -> float:
    if not pnls:
        return 0.0
    volatility = pstdev(pnls) if len(pnls) > 1 else abs(pnls[0])
    score = profit_factor * 18.0 - volatility * 2.0 - max_drawdown
    return round(max(0.0, min(25.0, score)), 2)


def _out_of_sample_score(pnls: list[float]) -> float:
    if len(pnls) < 12:
        return 0.0
    midpoint = len(pnls) // 2
    train = sum(pnls[:midpoint])
    test = sum(pnls[midpoint:])
    if train > 0 and test > 0:
        return round(min(100.0, 50.0 + min(test / max(abs(train), 1.0), 1.0) * 50.0), 2)
    if train > 0 and test <= 0:
        return 25.0
    return 0.0


def _regime_score(net_profit: float, profit_factor: float, max_drawdown: float, trades: int) -> float:
    base = profit_factor * 25 + net_profit * 0.7 - max_drawdown * 1.2
    if trades < 10:
        base -= 20
    return round(max(0.0, min(100.0, base)), 2)


def _quality_flags(total_trades: int, profit_factor: float, win_rate: float, max_drawdown: float, capture: float) -> list[str]:
    flags: list[str] = []
    if total_trades < 10:
        flags.append("muestra_insuficiente")
    if profit_factor < 1.2:
        flags.append("profit_factor_debil")
    if win_rate >= 60 and profit_factor < 1.25:
        flags.append("win_rate_fragil")
    if max_drawdown > 12:
        flags.append("drawdown_alto")
    if 0 < capture < 0.25:
        flags.append("captura_benchmark_baja")
    if profit_factor < 1:
        flags.append("no_edge")
    if total_trades >= 10 and profit_factor < 1.15:
        flags.append("edge_insuficiente")
    return flags


def _no_trade_score(
    *,
    profit_factor: float,
    expectancy: float,
    max_drawdown: float,
    net_profit: float,
    total_trades: int,
    benchmark_capture_ratio: float,
) -> int:
    score = 0.0
    if profit_factor < 1:
        score += 40
    elif profit_factor < 1.15:
        score += 28
    elif profit_factor < 1.2:
        score += 14
    if expectancy < 0:
        score += 24
    if total_trades < 10:
        score += 12
    if max_drawdown > 0 and net_profit <= max_drawdown * 0.75:
        score += 16
    if 0 < benchmark_capture_ratio < 0.25:
        score += 16
    return int(max(0, min(100, round(score))))


def _num(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

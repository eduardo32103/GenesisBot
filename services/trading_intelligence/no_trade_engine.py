from __future__ import annotations

from typing import Any


class NoTradeEngine:
    """Turns weak evidence into a valid no-trade decision."""

    def evaluate(
        self,
        *,
        ticker: str,
        asset_class: str,
        timeframe: str,
        profile: str,
        metrics: dict[str, Any],
        benchmark: dict[str, Any] | None = None,
        hedge_score: float | int | None = None,
        memory_failures: int = 0,
    ) -> dict[str, Any]:
        reasons: list[str] = []
        score = 0.0
        profit_factor = _num(metrics.get("profit_factor"))
        expectancy = _num(metrics.get("expectancy"))
        max_drawdown = _num(metrics.get("max_drawdown"))
        net_profit = _num(metrics.get("net_profit"))
        win_rate = _num(metrics.get("win_rate"))
        total_trades = int(_num(metrics.get("total_trades")) or 0)
        benchmark_return = _num((benchmark or {}).get("return") or metrics.get("benchmark_return"))
        benchmark_capture = _num(metrics.get("benchmark_capture_ratio"))
        hedge_value = _num(hedge_score)

        if profit_factor < 1.0:
            score += 70
            reasons.append("NO EDGE: profit factor menor a 1.")
        elif profit_factor < 1.15:
            score += 28
            reasons.append("Profit factor menor a 1.15; edge insuficiente.")
        elif profit_factor < 1.2:
            score += 14
            reasons.append("Profit factor debajo del minimo sugerido 1.20.")

        if expectancy < 0:
            score += 28
            reasons.append("Expectancy negativa.")

        if max_drawdown > 0 and net_profit <= max_drawdown * 0.75:
            score += 18
            reasons.append("Drawdown alto frente al retorno.")

        if win_rate < 42 and expectancy <= 0:
            score += 12
            reasons.append("Win rate bajo sin expectancy positiva.")

        if total_trades and total_trades < 10:
            score += 16
            reasons.append("Muestra insuficiente para validar edge.")
        elif total_trades == 0 and metrics.get("status") != "profile_heuristic":
            score += 18
            reasons.append("No hay operaciones suficientes para validar.")

        if benchmark_return > 8 and benchmark_capture < 0.25:
            score += 18
            reasons.append("Benchmark supera por mucho a la estrategia.")

        if asset_class == "Index ETF" and _is_intraday_1h(timeframe) and profile != "Defensive ETF Core":
            score += 25
            reasons.append("ETF en 1H con perfil tactico tiende a sobreoperar; usar 4H/1D o Defensive ETF Core.")
        elif asset_class == "Index ETF" and _is_intraday_1h(timeframe):
            score += 10
            reasons.append("ETF core en 1H requiere senales de alta calidad; 4H/1D es preferible.")

        if asset_class == "Crypto" and _is_intraday_1h(timeframe):
            score += 30
            reasons.append("BTC/cripto en 1H puede sobreoperar; Genesis prefiere 4H/1D salvo edge medido.")

        if hedge_value >= 76:
            score += 24
            reasons.append("Hedge score alto: riesgo contextual defensivo.")
        elif hedge_value >= 56:
            score += 10
            reasons.append("Hedge score medio: reducir agresividad.")

        if memory_failures >= 3:
            score += 20
            reasons.append("Memoria indica fallos repetidos de este setup.")

        no_trade_score = int(max(0, min(100, round(score))))
        no_trade = no_trade_score >= 70
        if no_trade:
            action = "no_trade"
            edge_status = "no_edge" if profit_factor < 1 or expectancy < 0 else "insufficient_edge"
        elif no_trade_score >= 45:
            action = "wait_or_reduce"
            edge_status = "fragile"
        elif total_trades < 10 and metrics.get("status") != "profile_heuristic":
            action = "paper_only"
            edge_status = "insufficient_sample"
        else:
            action = "paper_trade_candidate"
            edge_status = "edge_candidate"

        return {
            "no_trade": no_trade,
            "no_trade_score": no_trade_score,
            "edge_status": edge_status,
            "action": action,
            "reasons": reasons or ["No hay bloqueo critico; validar con paper/forward testing."],
            "policy": "No-trade es una decision valida; no ejecuta broker.",
        }


def evaluate_no_trade(**kwargs: Any) -> dict[str, Any]:
    return NoTradeEngine().evaluate(**kwargs)


def _num(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _is_intraday_1h(timeframe: str) -> bool:
    clean = str(timeframe or "").strip().upper()
    return clean in {"1H", "60", "60M"} or clean.endswith("/1H")

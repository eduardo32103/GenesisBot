from __future__ import annotations

from typing import Any

from services.trading_intelligence.backtest_engine import BacktestEngine
from services.trading_intelligence.no_trade_engine import NoTradeEngine
from services.trading_intelligence.parameter_search import ParameterSearch
from services.trading_intelligence.strategy_metrics import score_metrics
from services.trading_intelligence.walk_forward import WalkForwardValidator


class EdgeFinder:
    """Searches for measurable edge without broad curve fitting."""

    def __init__(
        self,
        *,
        backtest_engine: BacktestEngine | None = None,
        parameter_search: ParameterSearch | None = None,
        walk_forward: WalkForwardValidator | None = None,
        no_trade_engine: NoTradeEngine | None = None,
    ) -> None:
        self.backtest_engine = backtest_engine or BacktestEngine()
        self.parameter_search = parameter_search or ParameterSearch()
        self.walk_forward = walk_forward or WalkForwardValidator()
        self.no_trade_engine = no_trade_engine or NoTradeEngine()

    def find_edge(
        self,
        ticker: str,
        *,
        classification: dict[str, Any],
        profiles: list[dict[str, Any]],
        bars: list[dict[str, Any]] | None = None,
        timeframes: list[str] | None = None,
        hedge_score: float | int = 0,
        memory_failures: int = 0,
    ) -> dict[str, Any]:
        asset_class = str(classification.get("asset_class") or "Unknown")
        if not bars:
            return {
                "status": "pending_backend_bars",
                "accepted": False,
                "no_trade": False,
                "no_trade_score": 0,
                "best_candidate": None,
                "candidate_results": [],
                "timeframes_tested": [],
                "rejected_count": 0,
                "notes": ["Sin barras backend: Genesis usa clasificacion, memoria y contexto hasta ejecutar backtest real."],
            }

        tested_timeframes = _dedupe(timeframes or [str(classification.get("recommended_timeframe") or "4H")])
        candidate_results: list[dict[str, Any]] = []
        rejected_count = 0
        for profile in profiles:
            if profile.get("family") == "hedge":
                continue
            profile_candidates = self.parameter_search.candidates_for(profile, asset_class=asset_class)
            for timeframe in tested_timeframes:
                for parameters in profile_candidates:
                    profiled = {**profile, "parameters": parameters}
                    result = self.backtest_engine.run(bars, profiled, timeframe=timeframe)
                    walk_forward = self.walk_forward.evaluate(result.get("trades") or [])
                    metrics = {
                        **(result.get("metrics") or {}),
                        "out_of_sample_score": walk_forward.get("out_of_sample_score", 0.0),
                    }
                    result = {**result, "metrics": metrics, "walk_forward": walk_forward, "parameters": parameters}
                    decision = self.no_trade_engine.evaluate(
                        ticker=ticker,
                        asset_class=asset_class,
                        timeframe=timeframe,
                        profile=str(result.get("profile") or ""),
                        metrics=metrics,
                        benchmark=result.get("benchmark") or {},
                        hedge_score=hedge_score,
                        memory_failures=memory_failures,
                    )
                    result = {
                        **result,
                        "metrics": {
                            **metrics,
                            "no_trade_score": decision["no_trade_score"],
                            "edge_status": decision["edge_status"],
                        },
                        "no_trade_decision": decision,
                        "edge_score": _candidate_score(metrics, decision, walk_forward),
                    }
                    if _accepted(metrics, decision, walk_forward):
                        candidate_results.append(result)
                    else:
                        rejected_count += 1
                        if _is_interesting_reject(result):
                            candidate_results.append(result)

        if not candidate_results:
            decision = self.no_trade_engine.evaluate(
                ticker=ticker,
                asset_class=asset_class,
                timeframe=tested_timeframes[0] if tested_timeframes else "",
                profile=str(classification.get("recommended_profile") or ""),
                metrics={"profit_factor": 0, "expectancy": 0, "net_profit": 0, "total_trades": 0},
                benchmark={"return": 0},
                hedge_score=hedge_score,
                memory_failures=memory_failures,
            )
            return {
                "status": "no_candidates",
                "accepted": False,
                "no_trade": True,
                "no_trade_score": decision["no_trade_score"],
                "no_trade_decision": decision,
                "best_candidate": None,
                "candidate_results": [],
                "timeframes_tested": tested_timeframes,
                "rejected_count": rejected_count,
                "notes": ["Ninguna familia produjo muestra util; no forzar operaciones."],
            }

        best = max(candidate_results, key=lambda item: float(item.get("edge_score") or 0.0))
        decision = best.get("no_trade_decision") or {}
        accepted = _accepted(best.get("metrics") or {}, decision, best.get("walk_forward") or {})
        return {
            "status": "edge_found" if accepted else "no_edge_found",
            "accepted": accepted,
            "no_trade": bool(decision.get("no_trade")) or not accepted,
            "no_trade_score": int(decision.get("no_trade_score") or 0),
            "no_trade_decision": decision,
            "best_candidate": best,
            "candidate_results": sorted(candidate_results, key=lambda item: float(item.get("edge_score") or 0.0), reverse=True)[:24],
            "timeframes_tested": tested_timeframes,
            "rejected_count": rejected_count,
            "notes": [
                "Busqueda acotada: pocas combinaciones razonables, walk-forward y no-trade si no hay edge.",
                "No se acepta un perfil por funcionar solo en una ventana o un activo.",
            ],
        }


def find_edge(ticker: str, **kwargs: Any) -> dict[str, Any]:
    return EdgeFinder().find_edge(ticker, **kwargs)


def _accepted(metrics: dict[str, Any], decision: dict[str, Any], walk_forward: dict[str, Any]) -> bool:
    return (
        float(metrics.get("profit_factor") or 0) >= 1.2
        and float(metrics.get("expectancy") or 0) > 0
        and float(metrics.get("net_profit") or 0) > 0
        and int(metrics.get("total_trades") or 0) >= 10
        and not bool(decision.get("no_trade"))
        and bool(walk_forward.get("accepted"))
    )


def _candidate_score(metrics: dict[str, Any], decision: dict[str, Any], walk_forward: dict[str, Any]) -> float:
    score = score_metrics(metrics)
    score += float(walk_forward.get("out_of_sample_score") or 0) * 0.35
    score -= float(decision.get("no_trade_score") or 0) * 0.45
    return round(score, 4)


def _is_interesting_reject(result: dict[str, Any]) -> bool:
    metrics = result.get("metrics") or {}
    return (
        float(metrics.get("profit_factor") or 0) >= 1.05
        or float(metrics.get("quality_score") or 0) >= 35
        or str(result.get("profile") or "") in {"Defensive ETF Core", "Crypto Momentum", "Crypto Momentum V2", "Crypto Momentum V3", "Crypto Momentum V4", "BTC Breakout Retest", "BTC Volatility Expansion"}
    )


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            out.append(text)
            seen.add(text)
    return out or ["4H"]

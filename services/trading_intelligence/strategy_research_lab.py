from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from services.genesis.memory_store import MemoryStore
from services.genesis.ticker_parser import normalize_ticker
from services.trading_intelligence.asset_classifier import CRYPTO, AssetClassifier, INDEX_ETF
from services.trading_intelligence.backtest_engine import BacktestEngine
from services.trading_intelligence.btc_edge_engine import BTCEdgeEngine
from services.trading_intelligence.edge_finder import EdgeFinder
from services.trading_intelligence.no_trade_engine import NoTradeEngine
from services.trading_intelligence.strategy_metrics import score_metrics
from services.trading_intelligence.strategy_profiles import get_strategy_profiles, profile_by_name


class StrategyResearchLab:
    """Compares strategy families and recommends a profile per asset/timeframe."""

    def __init__(self, *, memory: MemoryStore | None = None, backtest_engine: BacktestEngine | None = None) -> None:
        self.memory = memory or MemoryStore()
        self.backtest_engine = backtest_engine or BacktestEngine()
        self.edge_finder = EdgeFinder(backtest_engine=self.backtest_engine)
        self.btc_edge_engine = BTCEdgeEngine()
        self.classifier = AssetClassifier()
        self.no_trade_engine = NoTradeEngine()

    def research(
        self,
        ticker: str,
        *,
        timeframe: str = "",
        bars: list[dict[str, Any]] | None = None,
        save: bool = True,
    ) -> dict[str, Any]:
        normalized = normalize_ticker(ticker)
        if not normalized:
            return {"ok": False, "status": "missing_ticker", "message": "ticker requerido"}

        classification = self.classifier.classify(normalized)
        effective_timeframe = timeframe or str(classification.get("recommended_timeframe") or "4H")
        profiles = get_strategy_profiles()
        memory_failures = _memory_failures(self.memory, normalized)
        btc_edge_context = self.btc_edge_engine.evaluate(normalized, bars=bars, hedge_score=0, memory_failures=memory_failures) if classification.get("asset_class") == CRYPTO else {}
        if bars:
            edge_summary = self.edge_finder.find_edge(
                normalized,
                classification=classification,
                profiles=profiles,
                bars=bars,
                timeframes=_candidate_timeframes(effective_timeframe, classification),
                hedge_score=0,
                memory_failures=memory_failures,
            )
            raw_results = edge_summary.get("candidate_results") or []
        else:
            edge_summary = {
                "status": "pending_backend_bars",
                "accepted": False,
                "no_trade": False,
                "no_trade_score": 0,
                "timeframes_tested": [effective_timeframe],
                "notes": ["Sin barras backend en esta llamada; usar clasificacion, memoria y paper testing."],
            }
            raw_results = [_heuristic_profile_result(profile, classification, effective_timeframe) for profile in profiles if profile["family"] != "hedge"]
        profile_results = [
            result if isinstance(result.get("no_trade_decision"), dict) else _attach_no_trade(
                self.no_trade_engine,
                result,
                ticker=normalized,
                classification=classification,
                timeframe=effective_timeframe,
                hedge_score=0,
                memory_failures=memory_failures,
            )
            for result in raw_results
        ]

        recommendation = _select_recommendation(profile_results, classification)
        no_trade_decision = recommendation["no_trade_decision"]
        benchmark_summary = {
            "benchmark": "buy_and_hold",
            "return": recommendation.get("benchmark", {}).get("return", 0.0),
            "warning": _benchmark_warning(recommendation, classification),
        }
        risk_flags = _risk_flags(recommendation, classification, bool(bars))
        suggested_inputs = _suggested_inputs(classification, recommendation)
        if btc_edge_context:
            suggested_inputs.update(btc_edge_context.get("suggested_tradingview_inputs") or {})
        suggested_inputs.update(
            {
                "noTradeMode": True,
                "noTradeScoreInput": max(int(suggested_inputs.get("noTradeScoreInput") or 0), int(no_trade_decision["no_trade_score"])),
                "blockIfNoEdge": True,
            }
        )
        strategy_health = _strategy_health(recommendation, no_trade_decision)
        payload = {
            "ok": True,
            "status": "strategy_research_ready",
            "ticker": normalized,
            "asset_class": classification["asset_class"],
            "recommended_strategy_profile": recommendation["profile"],
            "recommended_preset": suggested_inputs["preset"],
            "recommended_timeframe": suggested_inputs["recommended_timeframe"],
            "no_trade_recommendation": bool(no_trade_decision["no_trade"]),
            "no_trade_score": no_trade_decision["no_trade_score"],
            "no_trade_decision": no_trade_decision,
            "edge_finder": edge_summary,
            "btc_edge_context": btc_edge_context,
            "btc_regime": btc_edge_context.get("btc_regime"),
            "btc_edge_score": btc_edge_context.get("btc_edge_score"),
            "hedge_mode": btc_edge_context.get("hedge_mode"),
            "hedge_reason": btc_edge_context.get("hedge_reason"),
            "edge_status": no_trade_decision["edge_status"],
            "reason": _reason(classification, recommendation),
            "backtest_summary": {
                "source": "backend_bars" if bars else "profile_heuristic_until_backend_bars_available",
                "profile": recommendation["profile"],
                "metrics": recommendation["metrics"],
                "status": recommendation["status"],
                "parameters": recommendation.get("parameters") or {},
                "walk_forward": recommendation.get("walk_forward") or {},
                "edge_status": no_trade_decision["edge_status"],
                "notes": recommendation.get("notes") or [],
            },
            "benchmark_summary": benchmark_summary,
            "strategy_health": strategy_health,
            "risk_flags": risk_flags,
            "profiles_tested": profile_results,
            "failed_profiles": [item["profile"] for item in profile_results if "profit_factor_debil" in item["metrics"].get("quality_flags", [])],
            "fragile_profiles": [item["profile"] for item in profile_results if _is_fragile(item)],
            "no_trade_recommendations": [item["profile"] for item in profile_results if item.get("no_trade_decision", {}).get("no_trade")],
            "best_profile_by_asset": recommendation["profile"],
            "best_profile_by_timeframe": {effective_timeframe: recommendation["profile"]},
            "recommended_profile": recommendation["profile"],
            "recommended_parameters": recommendation.get("parameters") or {},
            "suggested_tradingview_inputs": suggested_inputs,
            "policy": "Research para backtesting, paper trading y journal; no ejecuta broker ni promete rentabilidad.",
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        if save:
            self.save_research_result(payload)
        return payload

    def save_research_result(self, payload: dict[str, Any]) -> None:
        ticker = str(payload.get("ticker") or "")
        self.memory.save_asset_strategy_recommendation(ticker, payload, "strategy_research_lab", "media")
        self.memory.save_backtest_run(
            ticker,
            {
                "ticker": ticker,
                "asset_class": payload.get("asset_class"),
                "timeframe": payload.get("recommended_timeframe"),
                "profile": payload.get("recommended_strategy_profile"),
                "preset": payload.get("recommended_preset"),
                "parameters": payload.get("suggested_tradingview_inputs"),
                "metrics": (payload.get("backtest_summary") or {}).get("metrics"),
                "benchmark": payload.get("benchmark_summary"),
                "edge_finder": payload.get("edge_finder"),
                "btc_edge_context": payload.get("btc_edge_context"),
                "edge_status": (payload.get("backtest_summary") or {}).get("edge_status"),
                "status": (payload.get("backtest_summary") or {}).get("status"),
                "notes": (payload.get("backtest_summary") or {}).get("notes"),
            },
        )
        if payload.get("asset_class") == CRYPTO:
            btc_payload = {
                "ticker": ticker,
                "strategy_version": "Genesis Advantage v10.13 BTC Edge",
                "profile": payload.get("recommended_strategy_profile"),
                "preset": payload.get("recommended_preset"),
                "timeframe": payload.get("recommended_timeframe"),
                "btc_regime": payload.get("btc_regime"),
                "btc_edge_score": payload.get("btc_edge_score"),
                "no_trade_score": payload.get("no_trade_score"),
                "no_trade_recommendation": payload.get("no_trade_recommendation"),
                "hedge_mode": payload.get("hedge_mode"),
                "hedge_reason": payload.get("hedge_reason"),
                "metrics": (payload.get("backtest_summary") or {}).get("metrics"),
                "benchmark": payload.get("benchmark_summary"),
                "suggested_tradingview_inputs": payload.get("suggested_tradingview_inputs"),
                "order_policy": "journal_only_no_broker",
                "broker_touched": False,
            }
            self.memory.save_btc_edge_result(ticker, btc_payload, "strategy_research_lab", "media")
            self.memory.save_btc_backtest_result(ticker, btc_payload, "strategy_research_lab", "media")
        if payload.get("no_trade_recommendation"):
            self.memory.save_no_edge_decision(
                ticker,
                {
                    "ticker": ticker,
                    "asset_class": payload.get("asset_class"),
                    "profile": payload.get("recommended_strategy_profile"),
                    "timeframe": payload.get("recommended_timeframe"),
                    "no_trade_decision": payload.get("no_trade_decision"),
                    "edge_status": (payload.get("backtest_summary") or {}).get("edge_status"),
                    "order_policy": "journal_only_no_broker",
                    "broker_touched": False,
                },
            )
        for result in payload.get("profiles_tested") or []:
            self.memory.save_strategy_profile_result(ticker, {"ticker": ticker, **result}, "strategy_research_lab", "media")
            if _is_fragile(result):
                self.memory.save_event("fragile_setup", {"ticker": ticker, **result}, "strategy_research_lab", "media")
            if result.get("profile") in payload.get("failed_profiles", []):
                self.memory.save_event("failed_setup", {"ticker": ticker, **result}, "strategy_research_lab", "media")
        self.memory.save_event("best_setup", {"ticker": ticker, "profile": payload.get("recommended_strategy_profile"), "preset": payload.get("recommended_preset")}, "strategy_research_lab", "media")
        self.memory.save_learned_context(
            f"strategy_research:recommended:{ticker}",
            {
                "ticker": ticker,
                "asset_class": payload.get("asset_class"),
                "profile": payload.get("recommended_strategy_profile"),
                "preset": payload.get("recommended_preset"),
                "timeframe": payload.get("recommended_timeframe"),
                "edge_status": (payload.get("backtest_summary") or {}).get("edge_status"),
            "no_trade_recommendation": payload.get("no_trade_recommendation"),
            "no_trade_score": payload.get("no_trade_score"),
            "reason": payload.get("reason"),
            },
            "strategy_research_lab",
            "media",
        )

    def answer(self, message: str, ticker: str = "") -> dict[str, Any]:
        normalized = normalize_ticker(ticker)
        if not normalized:
            normalized = _extract_ticker_fallback(message)
        research = self.research(normalized or "SPY", save=True)
        answer = (
            f"{research['ticker']}: el perfil recomendado es {research['recommended_strategy_profile']} "
            f"con preset {research['recommended_preset']} en {research['recommended_timeframe']}. "
            f"Edge status: {research['strategy_health']['edge_status']}. "
            f"Razon: {research['reason']} "
            "No es promesa de rentabilidad; primero backtesting, paper trading y forward testing."
        )
        return {"ok": True, "intent": "strategy_research", "answer": answer, "research": research}


def build_strategy_research(
    ticker: str,
    *,
    memory: MemoryStore | None = None,
    timeframe: str = "",
    bars: list[dict[str, Any]] | None = None,
    save: bool = True,
) -> dict[str, Any]:
    return StrategyResearchLab(memory=memory).research(ticker, timeframe=timeframe, bars=bars, save=save)


def _heuristic_profile_result(profile: dict[str, Any], classification: dict[str, Any], timeframe: str) -> dict[str, Any]:
    asset_class = str(classification.get("asset_class") or "")
    base = 42.0
    if asset_class in profile.get("ideal_asset_classes", []):
        base += 28.0
    if profile["name"] == classification.get("recommended_profile"):
        base += 18.0
    if asset_class == INDEX_ETF and timeframe in {"1H", "60"} and profile["name"] != "Defensive ETF Core":
        base -= 18.0
    if asset_class == CRYPTO and profile["name"] in {"Crypto Momentum V4", "Crypto Momentum V3", "Crypto Momentum V2", "BTC Breakout Retest", "BTC Volatility Expansion"}:
        base += 10.0
    if profile["name"] == "Hedge / Capital Protection":
        base -= 10.0
    quality_score = max(0.0, min(100.0, base))
    profit_factor = round(0.75 + quality_score / 100.0 * 0.85, 3)
    benchmark_capture = round(quality_score / 100.0, 3)
    metrics = {
        "net_profit": round(quality_score / 10.0, 3),
        "profit_factor": profit_factor,
        "win_rate": round(38.0 + quality_score / 3.0, 2),
        "max_drawdown": round(max(2.0, 14.0 - quality_score / 10.0), 2),
        "total_trades": 0,
        "avg_trade": 0.0,
        "expectancy": 0.0,
        "average_win": 0.0,
        "average_loss": 0.0,
        "win_loss_ratio": 0.0,
        "benchmark_return": 0.0,
        "benchmark_capture_ratio": benchmark_capture,
        "return_over_drawdown": 0.0,
        "stability_score": round(quality_score / 4.0, 2),
        "out_of_sample_score": 0.0,
        "regime_score": round(quality_score, 2),
        "no_trade_score": 30,
        "quality_score": round(quality_score, 2),
        "quality_flags": ["research_pending_backend_bars"],
        "status": "profile_heuristic",
    }
    if profit_factor < 1.2:
        metrics["quality_flags"].append("profit_factor_debil")
    return {
        "profile": profile["name"],
        "timeframe": timeframe,
        "status": "profile_heuristic",
        "metrics": metrics,
        "benchmark": {"return": 0.0, "type": "buy_and_hold"},
        "walk_forward": {"status": "pending_backend_bars", "accepted": False, "rolling_windows": []},
        "trades": [],
        "notes": ["Sin barras backend en esta llamada; Genesis usa clasificacion + memoria hasta ejecutar backtest real."],
    }


def _select_recommendation(profile_results: list[dict[str, Any]], classification: dict[str, Any]) -> dict[str, Any]:
    preferred = str(classification.get("recommended_profile") or "")
    for result in profile_results:
        if result["profile"] == preferred and result["metrics"].get("quality_score", score_metrics(result["metrics"])) >= 45 and not result.get("no_trade_decision", {}).get("no_trade"):
            return result
    viable = [item for item in profile_results if not item.get("no_trade_decision", {}).get("no_trade")]
    candidates = viable or profile_results
    return max(candidates, key=lambda item: item["metrics"].get("quality_score", score_metrics(item["metrics"])) - item.get("no_trade_decision", {}).get("no_trade_score", 0) * 0.25)


def _suggested_inputs(classification: dict[str, Any], recommendation: dict[str, Any]) -> dict[str, Any]:
    profile = profile_by_name(recommendation["profile"])
    inputs = dict(profile.default_inputs)
    asset_class = str(classification.get("asset_class") or "")
    preset = profile.default_preset
    if asset_class == INDEX_ETF:
        inputs.update({"tradeMode": "Long Only", "maxTradesPerDay": 1, "useMarketRegimeFilter": True})
    if asset_class == CRYPTO:
        inputs.update(
            {
                "tradeMode": "Long & Short",
                "enableShorts": True,
                "strategyVersion": "Genesis Advantage v10.13 BTC Edge",
                "cryptoV4Mode": True,
                "btcLongTermMode": True,
                "cryptoV3Mode": True,
                "cryptoUseRegimeSwitch": True,
                "cryptoAvoidChop": True,
                "cryptoUseBreakoutRetest": True,
                "cryptoUseVolExpansion": True,
                "cryptoUseTrendContinuation": True,
                "cryptoUseMeanReversionOnlyInRange": True,
                "cryptoUseHTF": True,
                "cryptoNoTradeInChop": True,
                "cryptoAtrMultiplier": 3.0,
                "cryptoTrailATR": 3.8,
                "cryptoMinAdx": 20,
                "cryptoMinVolRel": 1.1,
                "useActiveHedgeOverlay": True,
                "hedgeShortAllowed": True,
                "btcMaxTradesPerDay": 2,
                "btcCooldownBars": 24,
                "btcMinBarsAfterExit": 12,
            }
        )
    inputs.update(
        {
            "preset": preset,
            "assetProfile": asset_class if asset_class != "Unknown" else "Auto",
            "autopilotMode": True,
            "autoProfileMode": True,
            "enableShorts": bool(inputs.get("enableShorts")) if asset_class == CRYPTO else False,
            "safeMode": False,
            "validationMode": False,
            "recommended_timeframe": profile.default_timeframe,
            "recommended_profile": profile.name,
            "minSignalScore": 62 if asset_class == CRYPTO or preset in {"Core Tactical", "Defensive ETF Core"} else 65,
            "useHedgeMode": True,
            "order_policy": "journal_only_no_broker",
        }
    )
    return inputs


def _benchmark_warning(recommendation: dict[str, Any], classification: dict[str, Any]) -> str:
    metrics = recommendation.get("metrics") or {}
    capture = float(metrics.get("benchmark_capture_ratio") or 0)
    if classification.get("asset_class") == INDEX_ETF and recommendation["profile"] != "Defensive ETF Core":
        return "ETF detectado: evitar estrategia tactica generica; usar Defensive ETF Core y comparar contra buy & hold."
    if 0 < capture < 0.25:
        return "Captura de benchmark baja: la estrategia puede estar protegiendo demasiado y dejando escapar tendencia."
    return "Comparar siempre contra buy & hold y forward testing."


def _risk_flags(recommendation: dict[str, Any], classification: dict[str, Any], has_bars: bool) -> list[str]:
    flags = list(classification.get("risk_flags") or [])
    metrics = recommendation.get("metrics") or {}
    flags.extend(metrics.get("quality_flags") or [])
    if not has_bars:
        flags.append("Sin backtest backend en esta llamada; ejecutar paper/forward antes de confiar.")
    if float(metrics.get("profit_factor") or 0) < 1.2:
        flags.append("Perfil fragil: profit factor menor a 1.20.")
    decision = recommendation.get("no_trade_decision") or {}
    if decision.get("no_trade"):
        flags.append("No-trade activo: no forzar operaciones sin edge.")
    elif decision.get("no_trade_score", 0) >= 45:
        flags.append("Edge fragil: esperar confirmacion o cambiar timeframe.")
    return _unique(flags)[:8]


def _reason(classification: dict[str, Any], recommendation: dict[str, Any]) -> str:
    return (
        f"{classification.get('reason')} Perfil elegido: {recommendation['profile']} "
        f"porque su calidad estimada/backtest es {recommendation.get('metrics', {}).get('quality_score', 'n/a')}."
    )


def _is_fragile(result: dict[str, Any]) -> bool:
    metrics = result.get("metrics") or {}
    flags = set(metrics.get("quality_flags") or [])
    decision = result.get("no_trade_decision") or {}
    return "profit_factor_debil" in flags or "win_rate_fragil" in flags or "muestra_insuficiente" in flags or decision.get("edge_status") in {"fragile", "insufficient_sample"}


def _attach_no_trade(
    engine: NoTradeEngine,
    result: dict[str, Any],
    *,
    ticker: str,
    classification: dict[str, Any],
    timeframe: str,
    hedge_score: float,
    memory_failures: int,
) -> dict[str, Any]:
    decision = engine.evaluate(
        ticker=ticker,
        asset_class=str(classification.get("asset_class") or ""),
        timeframe=timeframe,
        profile=str(result.get("profile") or ""),
        metrics=result.get("metrics") or {},
        benchmark=result.get("benchmark") or {},
        hedge_score=hedge_score,
        memory_failures=memory_failures,
    )
    metrics = {**(result.get("metrics") or {}), "no_trade_score": decision["no_trade_score"], "edge_status": decision["edge_status"]}
    return {**result, "metrics": metrics, "no_trade_decision": decision}


def _strategy_health(recommendation: dict[str, Any], decision: dict[str, Any]) -> dict[str, Any]:
    metrics = recommendation.get("metrics") or {}
    return {
        "edge_status": decision.get("edge_status"),
        "action": decision.get("action"),
        "no_trade_score": decision.get("no_trade_score"),
        "profit_factor": metrics.get("profit_factor"),
        "expectancy": metrics.get("expectancy"),
        "max_drawdown": metrics.get("max_drawdown"),
        "benchmark_capture_ratio": metrics.get("benchmark_capture_ratio"),
        "walk_forward_status": (recommendation.get("walk_forward") or {}).get("status"),
        "walk_forward_accepted": bool((recommendation.get("walk_forward") or {}).get("accepted")),
    }


def _memory_failures(memory: MemoryStore, ticker: str) -> int:
    rows = memory.get_recent_events(100)
    text_rows = [str(row.get("payload") or {}).casefold() for row in rows if ticker.casefold() in str(row.get("payload") or {}).casefold()]
    return sum(1 for text in text_rows if "failed_setup" in text or "fragile_setup" in text or "no_edge" in text or "fallo" in text)


def _extract_ticker_fallback(message: str) -> str:
    for token in ("NVDA", "VOO", "SPY", "QQQ", "BTC-USD", "BTC", "BNO", "IAU", "MARA"):
        if token.casefold() in str(message or "").casefold():
            return normalize_ticker(token)
    return ""


def _unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if text and text not in seen:
            out.append(text)
            seen.add(text)
    return out


def _candidate_timeframes(timeframe: str, classification: dict[str, Any]) -> list[str]:
    values = [timeframe]
    recommended = str(classification.get("recommended_timeframe") or "")
    values.extend(part.strip() for part in recommended.split("/") if part.strip())
    asset_class = str(classification.get("asset_class") or "")
    if asset_class == INDEX_ETF:
        values.extend(["4H", "1D"])
    elif asset_class == "Crypto":
        values.extend(["4H", "1D"])
    else:
        values.extend(["1H", "4H"])
    return _unique(values)[:4]

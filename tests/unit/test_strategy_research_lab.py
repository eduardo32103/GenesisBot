from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from services.genesis.agent_router import AgentRouter
from services.genesis.memory_store import MemoryStore
from services.genesis.tool_router import route_message
from services.trading_intelligence.asset_classifier import classify_asset
from services.trading_intelligence.btc_edge_engine import BTCEdgeEngine
from services.trading_intelligence.edge_finder import EdgeFinder
from services.trading_intelligence.no_trade_engine import NoTradeEngine
from services.trading_intelligence.parameter_search import ParameterSearch
from services.trading_intelligence.strategy_metrics import calculate_strategy_metrics
from services.trading_intelligence.strategy_profiles import get_strategy_profiles
from services.trading_intelligence.strategy_research_lab import StrategyResearchLab
from services.trading_intelligence.walk_forward import WalkForwardValidator


class StrategyResearchLabTests(unittest.TestCase):
    def test_asset_classifier_core_classes(self) -> None:
        self.assertEqual(classify_asset("NVDA")["asset_class"], "Mega-cap Growth")
        self.assertEqual(classify_asset("VOO")["asset_class"], "Index ETF")
        self.assertEqual(classify_asset("SPY")["asset_class"], "Index ETF")
        self.assertEqual(classify_asset("BTC")["asset_class"], "Crypto")
        self.assertEqual(classify_asset("BTCUSD")["asset_class"], "Crypto")
        self.assertEqual(classify_asset("GBTC")["asset_class"], "Crypto")
        self.assertEqual(classify_asset("IBIT")["asset_class"], "Crypto")
        self.assertEqual(classify_asset("BNO")["asset_class"], "Commodity")
        self.assertEqual(classify_asset("IAU")["asset_class"], "Gold Defensive")
        self.assertEqual(classify_asset("SLV")["asset_class"], "Gold Defensive")
        self.assertEqual(classify_asset("TSLA")["asset_class"], "High Beta")

    def test_strategy_metrics_contract(self) -> None:
        metrics = calculate_strategy_metrics(
            [
                {"pnl_pct": 2.0},
                {"pnl_pct": -1.0},
                {"pnl_pct": 3.0},
                {"pnl_pct": -0.5},
            ],
            benchmark_return=6.0,
        )

        self.assertEqual(metrics["total_trades"], 4)
        self.assertGreater(metrics["profit_factor"], 1.0)
        self.assertGreater(metrics["win_rate"], 0)
        self.assertIn("benchmark_capture_ratio", metrics)
        self.assertIn("no_trade_score", metrics)

    def test_no_trade_engine_marks_pf_below_one_as_no_edge(self) -> None:
        decision = NoTradeEngine().evaluate(
            ticker="VOO",
            asset_class="Index ETF",
            timeframe="1H",
            profile="Trend Pullback",
            metrics={
                "profit_factor": 0.92,
                "expectancy": -0.15,
                "max_drawdown": 6.0,
                "net_profit": -1.2,
                "win_rate": 38,
                "total_trades": 35,
                "benchmark_capture_ratio": -0.1,
            },
            benchmark={"return": 8.5},
        )

        self.assertTrue(decision["no_trade"])
        self.assertEqual(decision["edge_status"], "no_edge")
        self.assertGreaterEqual(decision["no_trade_score"], 70)
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            store.save_no_edge_decision("VOO", {"no_trade_decision": decision})
            self.assertTrue(store.get_no_edge_decisions("VOO", limit=5))

    def test_research_lab_recommends_profiles_and_saves_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            research = StrategyResearchLab(memory=store).research("VOO", save=True)

            self.assertTrue(research["ok"])
            self.assertEqual(research["asset_class"], "Index ETF")
            self.assertEqual(research["recommended_strategy_profile"], "Defensive ETF Core")
            self.assertIn("profiles_tested", research)
            self.assertIn("suggested_tradingview_inputs", research)
            self.assertIn("no_trade_recommendation", research)
            self.assertIn("strategy_health", research)
            self.assertIn("edge_finder", research)
            self.assertIn("edge_status", research)
            self.assertIn("no_trade_score", research)
            self.assertEqual(research["suggested_tradingview_inputs"]["tradeMode"], "Long Only")
            self.assertTrue(research["suggested_tradingview_inputs"]["noTradeMode"])
            self.assertIn("noTradeScoreInput", research["suggested_tradingview_inputs"])
            self.assertTrue(research["suggested_tradingview_inputs"]["autopilotMode"])
            self.assertFalse(research["suggested_tradingview_inputs"]["enableShorts"])
            self.assertTrue(store.get_backtest_runs("VOO", limit=5))
            self.assertTrue(store.get_asset_strategy_recommendations("VOO", limit=5))
            self.assertTrue(store.get_strategy_profile_results("VOO", limit=5))

    def test_parameter_search_is_bounded_and_walk_forward_reports_oos(self) -> None:
        profile = [item for item in get_strategy_profiles() if item["name"] == "Crypto Momentum V4"][0]
        candidates = ParameterSearch().candidates_for(profile, asset_class="Crypto")

        self.assertLessEqual(len(candidates), 18)
        self.assertIn("atr_stop", candidates[0])
        self.assertIn("donchian_entry", candidates[0])

        trades = [{"pnl_pct": 1.2}, {"pnl_pct": -0.4}, {"pnl_pct": 1.1}, {"pnl_pct": -0.3}] * 4
        validation = WalkForwardValidator().evaluate(trades)

        self.assertEqual(validation["status"], "evaluated")
        self.assertIn("out_of_sample_score", validation)
        self.assertIn("rolling_windows", validation)

    def test_edge_finder_uses_bounded_profiles_and_can_choose_no_trade(self) -> None:
        classification = classify_asset("VOO")
        profiles = [item for item in get_strategy_profiles() if item["name"] == "Defensive ETF Core"]
        bars = _synthetic_trend_bars()

        result = EdgeFinder().find_edge("VOO", classification=classification, profiles=profiles, bars=bars, timeframes=["4H"])

        self.assertIn(result["status"], {"edge_found", "no_edge_found", "no_candidates"})
        self.assertIn("candidate_results", result)
        self.assertIn("timeframes_tested", result)
        self.assertIn("rejected_count", result)
        self.assertIn("no_trade_score", result)

    def test_btc_edge_engine_returns_crypto_momentum_v4_profile(self) -> None:
        payload = BTCEdgeEngine().evaluate("BTCUSD")

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["asset_class"], "Crypto")
        self.assertEqual(payload["strategy_version"], "Genesis Advantage v10.13 BTC Edge")
        self.assertEqual(payload["recommended_preset"], "Crypto Momentum V4")
        self.assertIn(payload["recommended_strategy_profile"], {"Crypto Momentum V4", "BTC Breakout Retest", "BTC Volatility Expansion"})
        self.assertIn(payload["recommended_timeframe"], {"4H", "4H/1D"})
        self.assertIn("btc_regime", payload)
        self.assertIn("btc_edge_score", payload)
        self.assertIn("hedge_mode", payload)
        self.assertTrue(payload["suggested_tradingview_inputs"]["cryptoV4Mode"])
        self.assertTrue(payload["suggested_tradingview_inputs"]["cryptoV3Mode"])
        self.assertTrue(payload["suggested_tradingview_inputs"]["btcLongTermMode"])
        self.assertTrue(payload["suggested_tradingview_inputs"]["useActiveHedgeOverlay"])
        self.assertIn("cryptoAvoidChop", payload["suggested_tradingview_inputs"])
        self.assertFalse(payload["suggested_tradingview_inputs"]["safeMode"])
        self.assertFalse(payload["suggested_tradingview_inputs"]["validationMode"])

    def test_btc_edge_engine_classifies_regime_with_backend_bars(self) -> None:
        payload = BTCEdgeEngine().evaluate("BTCUSD", bars=_synthetic_trend_bars(), hedge_score=62)

        self.assertTrue(payload["ok"])
        self.assertIn(
            payload["btc_regime"],
            {"bull_trend", "bear_trend", "range", "chop", "breakout", "breakdown", "squeeze", "volatility_expansion", "liquidity_sweep", "recovery", "risk_off"},
        )
        self.assertGreaterEqual(payload["btc_edge_score"], 0)
        self.assertLessEqual(payload["btc_edge_score"], 100)
        self.assertTrue(payload["hedge_mode"])
        self.assertTrue(payload["suggested_tradingview_inputs"]["useActiveHedgeOverlay"])

    def test_btc_research_context_includes_btc_edge_context_and_memory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            research = StrategyResearchLab(memory=store).research("BTCUSD", save=True)

            self.assertTrue(research["ok"])
            self.assertEqual(research["asset_class"], "Crypto")
            self.assertEqual(research["recommended_strategy_profile"], "Crypto Momentum V4")
            self.assertEqual(research["recommended_preset"], "Crypto Momentum V4")
            self.assertIn("btc_edge_context", research)
            self.assertEqual(research["suggested_tradingview_inputs"]["preset"], "Crypto Momentum V4")
            self.assertEqual(research["suggested_tradingview_inputs"]["tradeMode"], "Long & Short")
            self.assertTrue(research["suggested_tradingview_inputs"]["cryptoV4Mode"])
            self.assertTrue(research["suggested_tradingview_inputs"]["cryptoV3Mode"])
            self.assertTrue(research["suggested_tradingview_inputs"]["btcLongTermMode"])
            self.assertTrue(research["suggested_tradingview_inputs"]["useActiveHedgeOverlay"])
            self.assertTrue(store.get_backtest_runs("BTCUSD", limit=5))
            self.assertTrue(store.get_btc_edge_results("BTCUSD", limit=5))
            self.assertTrue(store.get_btc_backtest_results("BTCUSD", limit=5))

    def test_strategy_research_chat_intent_and_answer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            self.assertEqual(AgentRouter().route("que estrategia funciona mejor para NVDA").intent, "strategy_research")
            self.assertEqual(AgentRouter().route("por que BTC pierde").intent, "strategy_research")
            self.assertEqual(AgentRouter().route("deberia operar BTC 1H").intent, "strategy_research")
            payload = route_message("por que VOO pierde", memory=store)

            self.assertTrue(payload["ok"])
            self.assertEqual(payload["intent"], "strategy_research")
            self.assertIn("research", payload)
            self.assertIn("Defensive ETF Core", payload["answer"])
            self.assertNotIn("garantiza rentabilidad", payload["answer"].casefold().replace("no garantiza rentabilidad", ""))
            self.assertFalse(payload["research"].get("broker_touched", False))

            btc_payload = route_message("deberia operar BTC 1H", memory=store)
            self.assertTrue(btc_payload["ok"])
            self.assertEqual(btc_payload["intent"], "strategy_research")
            self.assertIn("Crypto Momentum V4", btc_payload["answer"])

def _synthetic_trend_bars() -> list[dict[str, float]]:
    bars: list[dict[str, float]] = []
    price = 100.0
    for idx in range(280):
        pullback = 0.985 if idx % 35 in {0, 1, 2} else 1.0
        price = price * 1.0025 * pullback
        bars.append(
            {
                "open": price * 0.995,
                "high": price * 1.012,
                "low": price * 0.988,
                "close": price,
                "volume": 1_000_000 + idx * 3000,
            }
        )
    return bars


if __name__ == "__main__":
    unittest.main()

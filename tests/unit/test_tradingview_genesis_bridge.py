from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from api.main import create_app
from api.routes.genesis import get_genesis_hedge_plan, get_genesis_portfolio_hedge
from services.genesis.agent_router import AgentRouter
from services.genesis.hedge_engine import HedgeEngine
from services.genesis.memory_store import MemoryStore
from services.genesis.tool_router import route_message
from services.genesis.tradingview_bridge import get_trading_context, receive_tradingview_webhook


class TradingViewGenesisBridgeTests(unittest.TestCase):
    def test_create_app_exposes_tradingview_bridge_endpoints(self) -> None:
        app_config = create_app()

        self.assertEqual(app_config["genesis_trading_context_endpoint"], "/api/genesis/trading-context?ticker={symbol}")
        self.assertEqual(app_config["genesis_hedge_plan_endpoint"], "/api/genesis/hedge-plan?ticker={symbol}")
        self.assertEqual(app_config["genesis_portfolio_hedge_endpoint"], "/api/genesis/portfolio-hedge")
        self.assertEqual(app_config["genesis_tradingview_webhook_endpoint"], "/api/genesis/tradingview-webhook")

    def test_tradingview_signal_questions_route_to_memory(self) -> None:
        self.assertEqual(AgentRouter().route("que senales de TradingView tengo").intent, "memory_query")

    def test_hedge_questions_route_to_hedge_engine(self) -> None:
        self.assertEqual(AgentRouter().route("como cubro NVDA").intent, "hedge_plan")
        self.assertEqual(AgentRouter().route("que cobertura necesita mi cartera").intent, "hedge_plan")

    def test_trading_context_endpoint_contract_score_and_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            store.save_signal_event(
                "NVDA",
                {
                    "event_type": "strategy_signal",
                    "asset_name": "NVIDIA Corporation",
                    "expected_direction": "bullish",
                    "genesis_reading": "Breakout long con volumen; paper only.",
                    "status": "watching",
                },
                "test",
                "alta",
            )
            store.save_news_event("NVDA", {"title": "NVDA demand remains strong", "summary": "bullish catalyst"}, "test", "media")

            payload = get_trading_context("NVDA", memory=store)

            self.assertTrue(payload["ok"])
            self.assertEqual(payload["ticker"], "NVDA")
            self.assertEqual(payload["asset_name"], "NVIDIA Corporation")
            self.assertGreaterEqual(payload["genesis_context_score"], -100)
            self.assertLessEqual(payload["genesis_context_score"], 100)
            self.assertEqual(payload["bias"], "bullish")
            self.assertIn("relevant_news", payload)
            self.assertIn("active_alerts", payload)
            self.assertIn("whale_flow", payload)
            self.assertIn("memory_notes", payload)
            self.assertIn("risk_flags", payload)
            self.assertIn("what_to_watch", payload)
            self.assertIn("hedge_context", payload)
            self.assertIn("hedge_score", payload)
            self.assertIn("hedge_needed", payload)
            self.assertIn("suggested_hedge_type", payload)
            self.assertIn("suggested_hedge_ratio", payload)
            self.assertIn("capital_protection_mode", payload)
            self.assertIn("portfolio_risk", payload)
            self.assertIn("protection_notes", payload)
            self.assertIn("suggested_tradingview_inputs", payload)
            self.assertIn("useHedgeMode", payload["suggested_tradingview_inputs"])
            self.assertIn("asset_class", payload)
            self.assertIn("recommended_strategy_profile", payload)
            self.assertIn("recommended_preset", payload)
            self.assertIn("recommended_timeframe", payload)
            self.assertIn("no_trade_recommendation", payload)
            self.assertIn("no_trade_score", payload)
            self.assertIn("no_trade_decision", payload)
            self.assertIn("edge_status", payload)
            self.assertIn("edge_finder", payload)
            self.assertIn("backtest_summary", payload)
            self.assertIn("benchmark_summary", payload)
            self.assertIn("strategy_health", payload)
            self.assertIn("suggested_core_tactical_mode", payload)
            self.assertIn("suggested_mode", payload)
            self.assertIn("suggested_hedge_impact_mode", payload)
            self.assertIn("suggested_min_signal_score", payload)
            self.assertIn("suggested_trailing_mode", payload)
            self.assertIn("reason", payload)
            self.assertIn("noTradeMode", payload["suggested_tradingview_inputs"])
            self.assertIn("noTradeScoreInput", payload["suggested_tradingview_inputs"])
            self.assertIn("blockIfNoEdge", payload["suggested_tradingview_inputs"])
            self.assertTrue(payload["suggested_tradingview_inputs"]["useGenesisSync"])
            self.assertTrue(payload["suggested_tradingview_inputs"]["autopilotMode"])
            self.assertFalse(payload["suggested_tradingview_inputs"]["enableShorts"])
            self.assertEqual(payload["suggested_tradingview_inputs"]["safeMode"], False)
            self.assertEqual(payload["suggested_tradingview_inputs"]["validationMode"], False)
            self.assertIn("genesisBiasInput", payload["suggested_tradingview_inputs"])
            self.assertIn("genesisNewsScore", payload["suggested_tradingview_inputs"])
            self.assertIn("genesisWhaleScore", payload["suggested_tradingview_inputs"])
            self.assertIn("genesisMacroRiskScore", payload["suggested_tradingview_inputs"])

    def test_btc_trading_context_uses_crypto_momentum_v4(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            payload = get_trading_context("BTCUSD", memory=store)

            self.assertTrue(payload["ok"])
            self.assertEqual(payload["asset_class"], "Crypto")
            self.assertEqual(payload["strategy_version"], "Genesis Advantage v10.13 BTC Edge")
            self.assertEqual(payload["recommended_strategy_profile"], "Crypto Momentum V4")
            self.assertEqual(payload["recommended_preset"], "Crypto Momentum V4")
            self.assertIn(payload["recommended_timeframe"], {"4H", "4H/1D"})
            self.assertEqual(payload["btc_regime"], "pending_bars")
            self.assertIn("btc_edge_score", payload)
            self.assertIn("btc_edge_context", payload["strategy_research"])
            self.assertEqual(payload["suggested_tradingview_inputs"]["preset"], "Crypto Momentum V4")
            self.assertEqual(payload["suggested_tradingview_inputs"]["assetProfile"], "Crypto")
            self.assertTrue(payload["suggested_tradingview_inputs"]["cryptoV4Mode"])
            self.assertTrue(payload["suggested_tradingview_inputs"]["cryptoV3Mode"])
            self.assertTrue(payload["suggested_tradingview_inputs"]["btcLongTermMode"])
            self.assertTrue(payload["suggested_tradingview_inputs"]["cryptoAvoidChop"])
            self.assertTrue(payload["suggested_tradingview_inputs"]["cryptoUseBreakoutRetest"])
            self.assertTrue(payload["suggested_tradingview_inputs"]["cryptoUseVolExpansion"])
            self.assertTrue(payload["suggested_tradingview_inputs"]["useActiveHedgeOverlay"])
            self.assertFalse(payload["suggested_tradingview_inputs"]["safeMode"])
            self.assertFalse(payload["suggested_tradingview_inputs"]["validationMode"])

    def test_hedge_engine_returns_plan_with_valid_score(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            store.save_alert_event("NVDA", "support_break", {"summary": "bearish support_break with outflow"}, "alta")

            plan = HedgeEngine(memory=store).build_hedge_context("NVDA")

            self.assertTrue(plan["ok"])
            self.assertEqual(plan["ticker"], "NVDA")
            self.assertGreaterEqual(plan["hedge_score"], 0)
            self.assertLessEqual(plan["hedge_score"], 100)
            self.assertIn(plan["hedge_type"], {"none", "reduce_exposure", "protective_stop", "inverse_etf", "protective_put", "covered_call", "pair_hedge", "cash_hedge", "volatility_hedge", "crypto_hedge"})
            self.assertFalse(plan["source_status"]["broker_touched"])
            self.assertEqual(plan["source_status"]["order_policy"], "journal_only_no_broker")

    def test_hedge_route_endpoints_return_context(self) -> None:
        plan = get_genesis_hedge_plan("NVDA")
        portfolio = get_genesis_portfolio_hedge()

        self.assertTrue(plan["ok"])
        self.assertEqual(plan["ticker"], "NVDA")
        self.assertTrue(portfolio["ok"])
        self.assertIn("portfolio_risk", portfolio)

    def test_webhook_receives_alert_and_persists_memory_without_orders(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            payload = receive_tradingview_webhook(
                {
                    "source": "tradingview",
                    "strategy": "Genesis Advantage Strategy v1",
                    "strategy_version": "Genesis Advantage v10.13 BTC Edge",
                    "ticker": "NVDA",
                    "recommended_timeframe": "4H/1D",
                    "bot_status": "SIN EDGE - NO OPERAR",
                    "time": "2026-05-15T20:00:00Z",
                    "action": "long_signal",
                    "price": "100",
                    "score": "82",
                    "setup": "Breakout long",
                    "risk": "paper_only",
                    "stop": "95",
                    "target": "110",
                    "regime": "Alcista fuerte",
                    "genesis_context": "45",
                    "no_trade_mode": True,
                    "no_trade_score": 72,
                    "no_trade_block": True,
                    "edge_status": "no_edge",
                    "suggested_mode": "Core Tactical",
                    "core_tactical_mode": True,
                    "core_position_active": True,
                    "core_entry": True,
                    "tactical_entry": False,
                    "hedge_impact_mode": "Balanced",
                    "benchmark_capture_ratio": "0.42",
                    "strategy_vs_hold": "-12.5",
                    "hedge_score": "62",
                    "hedge_overlay": True,
                    "hedge_short_allowed": True,
                    "btc_long_term_mode": True,
                    "crypto_v4_mode": True,
                    "btc_regime": "breakdown",
                    "crypto_v3_mode": True,
                    "entry_quality_score": "78",
                    "no_trade_reason": "watch only",
                    "hedge_needed": True,
                    "hedge_type": "protective_stop",
                    "hedge_ratio": "0.3",
                    "capital_protection_mode": True,
                    "protect_open_profit": True,
                    "notes": "paper alert",
                },
                memory=store,
                now=datetime(2026, 5, 15, 20, 0, tzinfo=timezone.utc),
            )

            self.assertTrue(payload["ok"])
            self.assertFalse(payload["order_executed"])
            self.assertFalse(payload["broker_touched"])
            self.assertEqual(payload["execution_policy"], "journal_only_no_broker")
            self.assertIn("strategy_signals", payload["collections_saved"])
            self.assertIn("tradingview_alerts", payload["collections_saved"])
            self.assertIn("strategy_outcomes", payload["collections_saved"])
            self.assertIn("backtest_notes", payload["collections_saved"])
            self.assertIn("trade_journal", payload["collections_saved"])
            self.assertIn("benchmark_comparison", payload["collections_saved"])
            self.assertIn("hedge_events", payload["collections_saved"])
            self.assertIn("hedge_recommendations", payload["collections_saved"])
            self.assertIn("protected_trades", payload["collections_saved"])
            self.assertIn("no_edge_decisions", payload["collections_saved"])

            recent = store.get_recent_events(5, "tradingview_alert")
            signals = store.get_signal_events("NVDA", limit=5)
            outcomes = store.get_outcome_tracking("NVDA", limit=5)
            journal = store.get_asset_memory("NVDA", limit=5)
            notes = store.get_hypotheses("NVDA", limit=5)
            hedge_events = store.get_hedge_events("NVDA", limit=5)
            no_edge = store.get_no_edge_decisions("NVDA", limit=5)
            btc_edge = store.get_btc_edge_results("NVDA", limit=5)
            btc_backtests = store.get_btc_backtest_results("NVDA", limit=5)
            benchmark_events = store.get_recent_events(5, "benchmark_comparison")

            self.assertEqual(recent[0]["payload"]["ticker"], "NVDA")
            self.assertEqual(signals[0]["payload"]["event_type"], "strategy_signal")
            self.assertEqual(outcomes[0]["payload"]["event_type"], "strategy_outcome")
            self.assertIn("actual_outcome_1h", outcomes[0]["payload"])
            self.assertIn("actual_outcome_24h", outcomes[0]["payload"])
            self.assertIn("actual_outcome_7d", outcomes[0]["payload"])
            self.assertEqual(journal[0]["payload"]["event_type"], "trade_journal")
            self.assertEqual(notes[0]["payload"]["event_type"], "backtest_note")
            self.assertEqual(hedge_events[0]["payload"]["event_type"], "hedge_event")
            self.assertEqual(signals[0]["payload"]["strategy_version"], "Genesis Advantage v10.13 BTC Edge")
            self.assertEqual(signals[0]["payload"]["bot_status"], "SIN EDGE - NO OPERAR")
            self.assertEqual(signals[0]["payload"]["recommended_timeframe"], "4H/1D")
            self.assertEqual(signals[0]["payload"]["hedge_score"], 62.0)
            self.assertEqual(signals[0]["payload"]["btc_regime"], "breakdown")
            self.assertTrue(signals[0]["payload"]["btc_long_term_mode"])
            self.assertTrue(signals[0]["payload"]["crypto_v4_mode"])
            self.assertTrue(signals[0]["payload"]["crypto_v3_mode"])
            self.assertEqual(signals[0]["payload"]["entry_quality_score"], 78.0)
            self.assertTrue(signals[0]["payload"]["hedge_overlay"])
            self.assertTrue(signals[0]["payload"]["core_tactical_mode"])
            self.assertTrue(signals[0]["payload"]["core_entry"])
            self.assertEqual(signals[0]["payload"]["suggested_mode"], "Core Tactical")
            self.assertEqual(signals[0]["payload"]["no_trade_score"], 72.0)
            self.assertEqual(signals[0]["payload"]["edge_status"], "no_edge")
            self.assertEqual(no_edge[0]["payload"]["edge_status"], "no_edge")
            self.assertEqual(btc_edge[0]["payload"]["strategy_version"], "Genesis Advantage v10.13 BTC Edge")
            self.assertEqual(btc_backtests[0]["payload"]["event_type"], "btc_backtest_result")
            self.assertEqual(benchmark_events[0]["payload"]["benchmark_capture_ratio"], 0.42)

    def test_genesis_chat_answers_hedge_without_broker_or_guarantee(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            payload = route_message("como cubro NVDA", memory=store)

            self.assertTrue(payload["ok"])
            self.assertEqual(payload["intent"], "hedge_plan")
            self.assertFalse(payload["hedge"]["source_status"]["broker_touched"])
            self.assertEqual(payload["hedge"]["source_status"]["order_policy"], "journal_only_no_broker")
            self.assertNotIn("nunca pierde", payload["answer"].casefold())
            self.assertIn("reduce riesgo", payload["answer"].casefold())

    def test_webhook_does_not_store_or_echo_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MemoryStore(database_url="", sqlite_path=Path(tmp) / "memory.sqlite3")
            payload = receive_tradingview_webhook(
                {
                    "ticker": "MSFT",
                    "action": "watch_only",
                    "score": 61,
                    "FMP_API_KEY": "should-not-persist",
                    "OPENAI_API_KEY": "should-not-persist",
                    "notes": "contains fmp_api_key=should-not-persist",
                },
                memory=store,
            )

            encoded = json.dumps(payload, sort_keys=True)
            self.assertNotIn("should-not-persist", encoded)
            self.assertNotIn("FMP_API_KEY", encoded)
            self.assertNotIn("OPENAI_API_KEY", encoded)
            self.assertFalse(payload["order_executed"])

    def test_pine_script_contract(self) -> None:
        root = Path(__file__).resolve().parents[2]
        script = (root / "tradingview" / "genesis_advantage_strategy_v1.pine").read_text(encoding="utf-8")
        docs = (root / "docs" / "TRADINGVIEW_GENESIS_STRATEGY.md").read_text(encoding="utf-8")

        self.assertIn("//@version=6", script)
        self.assertIn("strategy(", script)
        self.assertIn("Genesis Advantage Strategy v1", script)
        self.assertIn("freePlanMode", script)
        self.assertIn("validationMode", script)
        self.assertIn("safeMode", script)
        self.assertIn("validationPulseBars", script)
        self.assertIn("debugMode", script)
        self.assertIn("paperQualityMode", script)
        self.assertIn("preset", script)
        self.assertIn('input.string("Auto"', script)
        self.assertIn("Trend Runner", script)
        self.assertIn("Core Tactical", script)
        self.assertIn("autopilotMode", script)
        self.assertIn("enableShorts", script)
        self.assertIn("autoProfileMode", script)
        self.assertIn("assetProfile", script)
        self.assertIn("noTradeMode", script)
        self.assertIn("noTradeScoreInput", script)
        self.assertIn("blockIfNoEdge", script)
        self.assertIn("No Trade / Watch Only", script)
        self.assertIn("Defensive ETF Core", script)
        self.assertIn("Crypto Momentum", script)
        self.assertIn("strategyVersion", script)
        self.assertIn("v10.13", script)
        self.assertIn("Crypto Momentum V4", script)
        self.assertIn("cryptoV3Mode", script)
        self.assertIn("cryptoV4Mode", script)
        self.assertIn("BTC Crypto Momentum V4", script)
        self.assertIn("btcLongTermMode", script)
        self.assertIn("btcRegime", script)
        self.assertIn("btcChop", script)
        self.assertIn("btcVolExpansion", script)
        self.assertIn("btcBreakout", script)
        self.assertIn("btcBreakdown", script)
        self.assertIn("entryQualityScore", script)
        self.assertIn("useActiveHedgeOverlay", script)
        self.assertIn("hedgeShortAllowed", script)
        self.assertIn("btcMaxTradesPerDay", script)
        self.assertIn("btcCooldownBars", script)
        self.assertIn("cryptoAvoidChop", script)
        self.assertIn("cryptoUseBreakoutRetest", script)
        self.assertIn("cryptoUseVolExpansion", script)
        self.assertIn("cryptoAtrMultiplier", script)
        self.assertIn("cryptoTrailATR", script)
        self.assertIn("VALIDACION: no usar para evaluar rentabilidad", script)
        self.assertIn("validation_not_edge", script)
        self.assertIn("Commodity Regime", script)
        self.assertIn("Gold Defensive", script)
        self.assertIn("ETF core funciona mejor en 4H/1D; 1H puede sobreoperar.", script)
        self.assertIn("trendRunnerMode", script)
        self.assertIn("coreTacticalMode", script)
        self.assertIn("corePositionActive", script)
        self.assertIn("coreEntryCondition", script)
        self.assertIn("coreExitCondition", script)
        self.assertIn("tacticalEntry", script)
        self.assertIn("hedgeImpactMode", script)
        self.assertIn("benchmarkCaptureRatio", script)
        self.assertIn("strategyVsHold", script)
        self.assertIn("coreTrailMode", script)
        self.assertIn("suggested_mode", script)
        self.assertIn("avoidShortsInBullTrend", script)
        self.assertIn("avoidLongsInBearTrend", script)
        self.assertIn("strategyAlive", script)
        self.assertIn("Bot Status", script)
        self.assertIn("Long Raw / Final", script)
        self.assertIn("Short Raw / Final", script)
        self.assertIn("longConditionRaw", script)
        self.assertIn("shortConditionRaw", script)
        self.assertIn("longRaw", script)
        self.assertIn("shortRaw", script)
        self.assertIn("qualityGateLong", script)
        self.assertIn("qualityGateShort", script)
        self.assertIn("chopFilterEnabled", script)
        self.assertIn("useHTFConfirmation", script)
        self.assertIn("cooldownBars", script)
        self.assertIn("maxTradesPerDay", script)
        self.assertIn("useBreakEvenStop", script)
        self.assertIn("useRunnerExit", script)
        self.assertIn("runnerTrailATR", script)
        self.assertIn("benchmark awareness", script)
        self.assertIn("useHedgeMode", script)
        self.assertIn("hedgeScoreInput", script)
        self.assertIn("avoidTradeIfHedgeScoreAbove", script)
        self.assertIn("reduceSizeIfHedgeScoreAbove", script)
        self.assertIn("capitalProtectionMode", script)
        self.assertIn("protectOpenProfit", script)
        self.assertIn("Hedge Needed", script)
        self.assertIn("hedge_score", script)
        self.assertIn("no_trade_score", script)
        self.assertIn("edge_status", script)
        self.assertIn("btc_regime", script)
        self.assertIn("btc_long_term_mode", script)
        self.assertIn("crypto_v4_mode", script)
        self.assertIn("bot_status", script)
        self.assertIn("crypto_v3_mode", script)
        self.assertIn("hedge_overlay", script)
        self.assertIn("overextensionPenalty", script)
        self.assertIn("htfScore", script)
        self.assertIn("longCondition =", script)
        self.assertIn("shortCondition =", script)
        self.assertIn("strategy.entry(", script)
        self.assertIn("strategy.exit(", script)
        self.assertIn("buildAlertJson", script)
        self.assertIn("plotshape(", script)
        self.assertIn("table.new", script)
        self.assertIn("alertcondition", script)
        self.assertIn("alert_message", script)
        self.assertIn("broker_touched", script)
        self.assertIn("journal_only_no_broker", script)
        self.assertIn("blockReason", script)
        self.assertIn("Modo plan gratis / barras cargadas", script)
        self.assertIn("Modo validacion: generar senales", script)
        self.assertIn("Genesis Context Score", script)
        self.assertIn("marketRegimeScore", script)
        self.assertIn("finalSignalScore", script)
        self.assertIn("strategy.risk.max_intraday_loss", script)
        self.assertIn("strategy.risk.max_drawdown", script)
        self.assertIn("request.security", script)
        self.assertIn("Uso sin pagar Deep Backtesting", docs)
        self.assertIn("Bot apagado / sin operaciones", docs)
        self.assertIn("Como mejorar porcentaje de aciertos sin sobreoptimizar", docs)
        self.assertIn("Trend Runner Mode", docs)
        self.assertIn("Capital Protection / Hedge Mode", docs)
        self.assertIn("Core + Tactical Mode", docs)
        self.assertIn("Strategy Research Lab", docs)
        self.assertIn("Auto Profile Mode", docs)
        self.assertIn("No-Trade Mode", docs)
        self.assertIn("BTC Edge Engine", docs)
        self.assertIn("BTC Crypto Momentum V4", docs)
        self.assertIn("BTC Long Term Edge Mode", docs)
        self.assertIn("Active Hedge Overlay", docs)
        self.assertIn("Si BTC 1H tiene PF menor a 1 o expectancy negativa", docs)
        self.assertIn("Si un activo/timeframe tiene PF menor a 1 o expectancy negativa", docs)
        self.assertIn("Genesis Brain como contexto", docs)
        self.assertIn("/api/genesis/hedge-plan", docs)
        self.assertIn("can't parse argument number", docs)
        self.assertIn("strategy.order.alert_message", script)
        self.assertIn("strategy.order.alert_message", docs)
        self.assertNotIn("str.format(", script)
        self.assertNotIn("str.format(\"{\\\"source\\\"", script)
        self.assertNotIn("str.format('{\"source\"", script)
        self.assertNotIn("message=longAlertMessage", script)
        self.assertNotIn("message=shortAlertMessage", script)
        self.assertNotIn("FMP_API_KEY", script)
        self.assertNotIn("OPENAI_API_KEY", script)


if __name__ == "__main__":
    unittest.main()

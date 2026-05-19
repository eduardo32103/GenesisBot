from __future__ import annotations

import unicodedata
from typing import Any

from services.dashboard.get_opportunity_radar import get_opportunity_radar_snapshot
from services.genesis.agent_router import AgentRouter
from services.genesis.alerts_agent import get_alerts_agent
from services.genesis.hedge_engine import HedgeEngine
from services.genesis.llm_orchestrator import get_llm_orchestrator
from services.genesis.market_format import format_signed_money, format_signed_percent
from services.genesis.memory_store import MemoryStore
from services.genesis.news_macro_agent import get_news_macro_agent
from services.genesis.performance_tracker import build_genesis_performance_report
from services.genesis.portfolio_agent import get_portfolio_agent
from services.genesis.price_agent import get_price_agent
from services.genesis.response_composer import get_response_composer
from services.genesis.technical_agent import get_technical_agent
from services.genesis.time_tool import detect_date_request, detect_time_request, get_date_answer, get_time_answer
from services.genesis.ticker_parser import normalize_ticker
from services.genesis.tracking_agent import get_tracking_agent
from services.genesis.weather_agent import get_weather_agent
from services.genesis.weather_tool import detect_weather_request
from services.mt5.mt5_bridge import (
    mt5_adaptive_recommendations,
    mt5_adaptive_state,
    mt5_decision,
    mt5_health,
    mt5_memory_summary,
    mt5_performance,
)
from services.genesis.whale_agent import get_whale_agent
from services.trading_intelligence.strategy_research_lab import StrategyResearchLab


def route_message(
    message: str,
    context: str = "general",
    ticker: str = "",
    panel_context: Any | None = None,
    memory: MemoryStore | None = None,
    conversation_id: str = "default",
) -> dict[str, Any]:
    store = memory or MemoryStore()
    clean = str(message or "").strip()
    clean_conversation_id = str(conversation_id or "default").strip()[:120] or "default"
    route = AgentRouter().route(clean, context=panel_context if isinstance(panel_context, dict) else None, ticker=ticker)
    if clean:
        store.save_message(clean_conversation_id, "user", clean, {"context": context, "intent": route.intent})
        store.save_recent_topic(route.intent, {"message": clean, "tickers": route.tickers})
    tickers = route.tickers
    explicit_ticker = normalize_ticker(route.primary_ticker)
    price_agent = get_price_agent()
    composer = get_response_composer()

    if route.intent != "mt5_bridge" and _should_route_mt5_learning(clean):
        mt5_symbol = _mt5_metrics_symbol(clean, explicit_ticker)
        memory_summary = mt5_memory_summary(symbol=mt5_symbol, memory=store)
        state = mt5_adaptive_state(symbol=mt5_symbol, memory=store)
        recommendations = mt5_adaptive_recommendations(symbol=mt5_symbol, memory=store)
        answer = _mt5_learning_answer(memory_summary, state, recommendations)
        store.save_event(
            "mt5_learning_query",
            {"message": clean, "symbol": mt5_symbol, "broker_touched": False, "order_executed": False},
            "mt5_bridge",
            "media",
        )
        return _payload(
            "mt5_bridge",
            answer,
            [mt5_symbol] if mt5_symbol else [],
            extra={"mt5": memory_summary, "adaptive_state": state, "recommendations": recommendations, "kind": "mt5_learning"},
            memory=store,
            prompt=clean,
            conversation_id=clean_conversation_id,
        )

    if route.intent != "mt5_bridge" and _should_route_mt5_forward_metrics(clean):
        mt5_symbol = _mt5_metrics_symbol(clean, explicit_ticker)
        performance = mt5_performance(symbol=mt5_symbol, memory=store)
        answer = _mt5_performance_answer(performance)
        store.save_event(
            "mt5_forward_test_query",
            {"message": clean, "symbol": mt5_symbol, "summary_auto": performance.get("summary_auto"), "broker_touched": False},
            "mt5_bridge",
            "media",
        )
        return _payload(
            "mt5_bridge",
            answer,
            [mt5_symbol] if mt5_symbol else [],
            extra={"mt5": performance, "performance": performance, "kind": "mt5_forward_test"},
            memory=store,
            prompt=clean,
            conversation_id=clean_conversation_id,
        )

    if route.intent == "greeting":
        answer = composer.greeting()
        store.save_event("greeting", {"message": clean}, "genesis", "alta")
        return _payload("greeting", answer, tickers, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "time" or (route.intent not in {"strategy_research", "hedge_plan"} and detect_time_request(clean)):
        time_payload = get_time_answer()
        store.save_event("time_request", {"message": clean, "timezone": time_payload["timezone"]}, "time", "alta")
        return _payload("time", time_payload["answer"], [], extra={"time": time_payload}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "date" or (route.intent not in {"strategy_research", "hedge_plan"} and detect_date_request(clean)):
        date_payload = get_date_answer()
        store.save_event("date_request", {"message": clean, "timezone": date_payload["timezone"], "date": date_payload["date"]}, "time", "alta")
        return _payload("date", date_payload["answer"], [], extra={"date": date_payload}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "weather" or detect_weather_request(clean):
        weather = get_weather_agent().answer(clean)
        store.save_event("weather_request", {"message": clean, "city": weather.get("city"), "source": weather.get("source")}, "weather", "media")
        return _payload("weather", weather["answer"], [], extra={"weather": weather}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "daily_briefing":
        briefing = get_news_macro_agent().daily_briefing(clean)
        store.save_event("daily_briefing", {"summary": briefing["answer"]}, "macro", "media")
        structured = composer.market_briefing(briefing)
        return _payload("daily_briefing", briefing["answer"], [], extra={"briefing": briefing, "structured": structured, "kind": structured["kind"]}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "market_overview":
        overview = get_news_macro_agent().market_overview(clean)
        store.save_event("market_overview", {"summary": overview["answer"]}, "macro", "media")
        structured = composer.market_briefing(overview)
        return _payload("market_overview", overview["answer"], [], extra={"overview": overview, "structured": structured, "kind": structured["kind"]}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "macro_news":
        overview = get_news_macro_agent().market_overview(clean)
        store.save_event("news_brief", {"summary": overview["answer"], "news_count": len(overview.get("news") or [])}, "macro", "media")
        _remember_news_events(store, overview)
        structured = composer.news_brief(overview)
        return _payload(
            "macro_news",
            overview["answer"],
            tickers,
            extra={"overview": overview, "briefing": overview, "structured": structured, "kind": structured["kind"]},
            memory=store,
            prompt=clean,
            conversation_id=clean_conversation_id,
        )

    if route.intent == "portfolio_summary":
        briefing = get_portfolio_agent().summary()
        store.save_event("portfolio_briefing", {"summary": briefing["answer"]}, "portfolio", "media")
        return _payload("portfolio_summary", briefing["answer"], tickers, extra={"portfolio": briefing}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "tracking_summary":
        tracking = get_tracking_agent().summary()
        store.save_event("tracking_summary", {"count": len(tracking.get("items", []))}, "tracking", "media")
        return _payload("tracking_summary", tracking["answer"], tickers, extra={"tracking": tracking}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "performance_review":
        report = build_genesis_performance_report(clean, memory=store)
        structured = composer.performance_review(report)
        return _payload(
            "performance_review",
            report["answer"],
            [report["ticker"]] if report.get("ticker") else [],
            extra={"performance": report, "structured": structured, "kind": structured["kind"]},
            memory=store,
            prompt=clean,
            conversation_id=clean_conversation_id,
        )

    if route.intent == "mt5_bridge":
        if _should_route_mt5_learning(clean):
            mt5_symbol = _mt5_metrics_symbol(clean, explicit_ticker)
            memory_summary = mt5_memory_summary(symbol=mt5_symbol, memory=store)
            state = mt5_adaptive_state(symbol=mt5_symbol, memory=store)
            recommendations = mt5_adaptive_recommendations(symbol=mt5_symbol, memory=store)
            answer = _mt5_learning_answer(memory_summary, state, recommendations)
            store.save_event(
                "mt5_learning_query",
                {"message": clean, "symbol": mt5_symbol, "broker_touched": False, "order_executed": False},
                "mt5_bridge",
                "media",
            )
            return _payload(
                "mt5_bridge",
                answer,
                [mt5_symbol] if mt5_symbol else [],
                extra={"mt5": memory_summary, "adaptive_state": state, "recommendations": recommendations, "kind": "mt5_learning"},
                memory=store,
                prompt=clean,
                conversation_id=clean_conversation_id,
            )
        if _mentions_mt5_forward_metrics(clean):
            mt5_symbol = _mt5_metrics_symbol(clean, explicit_ticker)
            performance = mt5_performance(symbol=mt5_symbol, memory=store)
            answer = _mt5_performance_answer(performance)
            store.save_event(
                "mt5_forward_test_query",
                {"message": clean, "symbol": mt5_symbol, "summary_auto": performance.get("summary_auto"), "broker_touched": False},
                "mt5_bridge",
                "media",
            )
            return _payload(
                "mt5_bridge",
                answer,
                [mt5_symbol] if mt5_symbol else [],
                extra={"mt5": performance, "performance": performance, "kind": "mt5_forward_test"},
                memory=store,
                prompt=clean,
                conversation_id=clean_conversation_id,
            )
        if explicit_ticker:
            decision = mt5_decision(explicit_ticker, memory=store)
            structured = composer.mt5_decision_card(decision)
            answer = _mt5_decision_answer(decision)
            store.save_event(
                "mt5_chat_query",
                {
                    "message": clean,
                    "symbol": decision.get("symbol"),
                    "decision": decision.get("decision"),
                    "order_policy": decision.get("order_policy"),
                    "broker_touched": False,
                },
                "mt5_bridge",
                "media",
            )
            return _payload(
                "mt5_bridge",
                answer,
                [explicit_ticker],
                extra={"mt5": decision, "structured": structured, "kind": structured["kind"]},
                memory=store,
                prompt=clean,
                conversation_id=clean_conversation_id,
            )
        health = mt5_health(memory=store)
        structured = composer.mt5_decision_card({"symbol": "MT5", "decision": "STATUS", **health})
        answer = _mt5_health_answer(health)
        store.save_event("mt5_chat_query", {"message": clean, **health}, "mt5_bridge", "media")
        return _payload(
            "mt5_bridge",
            answer,
            [],
            extra={"mt5": health, "structured": structured, "kind": structured["kind"]},
            memory=store,
            prompt=clean,
            conversation_id=clean_conversation_id,
        )

    if route.intent == "hedge_plan":
        hedge_engine = HedgeEngine(memory=store)
        plan = hedge_engine.build_hedge_context(explicit_ticker, portfolio_mode=not bool(explicit_ticker))
        structured = composer.hedge_plan(plan)
        answer = _hedge_plan_answer(plan)
        store.save_event(
            "hedge_recommendation",
            {
                "ticker": explicit_ticker,
                "hedge_score": plan.get("hedge_score"),
                "hedge_type": plan.get("hedge_type"),
                "hedge_ratio": plan.get("suggested_hedge_ratio"),
                "reason": plan.get("reason") or plan.get("genesis_reading"),
                "order_policy": "journal_only_no_broker",
                "broker_touched": False,
            },
            "hedge_engine",
            "media",
        )
        return _payload(
            "hedge_plan",
            answer,
            [explicit_ticker] if explicit_ticker else [],
            extra={"hedge": plan, "structured": structured, "kind": structured["kind"]},
            memory=store,
            prompt=clean,
            conversation_id=clean_conversation_id,
        )

    if route.intent == "strategy_research":
        lab = StrategyResearchLab(memory=store)
        research_payload = lab.answer(clean, explicit_ticker)
        research = research_payload["research"]
        structured = composer.strategy_research_summary(research)
        store.save_event(
            "strategy_research_query",
            {
                "message": clean,
                "ticker": research.get("ticker"),
                "recommended_profile": research.get("recommended_strategy_profile"),
                "recommended_preset": research.get("recommended_preset"),
                "order_policy": "journal_only_no_broker",
                "broker_touched": False,
            },
            "strategy_research_lab",
            "media",
        )
        return _payload(
            "strategy_research",
            research_payload["answer"],
            [research["ticker"]] if research.get("ticker") else [],
            extra={"research": research, "structured": structured, "kind": structured["kind"]},
            memory=store,
            prompt=clean,
            conversation_id=clean_conversation_id,
        )

    if route.intent == "memory_query":
        memory_summary = store.get_asset_learning_summary(explicit_ticker) if explicit_ticker else store.get_memory_summary(clean)
        answer = _asset_memory_answer(memory_summary) if explicit_ticker else _memory_answer(memory_summary)
        store.save_event("memory_query", {"message": clean}, "memory", "media")
        structured = _memory_digest_structured(memory_summary, answer)
        return _payload(
            "memory_query",
            answer,
            [explicit_ticker] if explicit_ticker else [],
            extra={"memory_summary": memory_summary, "structured": structured, "kind": structured["kind"]},
            memory=store,
            prompt=clean,
            conversation_id=clean_conversation_id,
        )

    if route.intent == "whale_activity":
        learned = get_whale_agent().activity(explicit_ticker or None, memory=store)
        structured = composer.whale_flow(learned)
        return _payload(
            "whale_activity",
            learned["answer"],
            tickers,
            extra={"whales": learned, "structured": structured, "kind": structured["kind"]},
            memory=store,
            prompt=clean,
            conversation_id=clean_conversation_id,
        )

    if route.intent == "alerts":
        alerts = get_alerts_agent().summary()
        store.save_event("alerts_summary", {"count": len(alerts.get("items", [])), "answer": alerts.get("answer")}, "alerts", "media")
        _remember_alert_events(store, alerts)
        structured = composer.alerts_digest(alerts)
        return _payload(
            "alerts",
            alerts["answer"],
            [],
            extra={"alerts": alerts, "structured": structured, "kind": structured["kind"]},
            memory=store,
            prompt=clean,
            conversation_id=clean_conversation_id,
        )

    if route.intent == "opportunities":
        radar = get_opportunity_radar_snapshot(force_refresh=False)
        items = radar.get("items") if isinstance(radar.get("items"), list) else []
        mode = _opportunity_mode_from_prompt(clean)
        answer = _opportunity_radar_answer(items, mode)
        structured = _opportunity_radar_structured(radar, items, mode, answer)
        store.save_event(
            "opportunity_radar",
            {
                "mode": mode["mode"],
                "count": len(items),
                "top_ticker": (items[0] or {}).get("ticker") if items else "",
                "source_status": radar.get("source_status"),
            },
            "opportunity_agent",
            "media",
        )
        for item in items[:5]:
            ticker_item = normalize_ticker(item.get("ticker"))
            if ticker_item:
                store.save_signal_event(
                    ticker_item,
                    {
                        "ticker": ticker_item,
                        "event_type": "opportunity_radar",
                        "price_at_decision": item.get("price"),
                        "expected_direction": item.get("expected_direction") or item.get("direction") or "watching",
                        "expected_impact": item.get("decision_label_es") or item.get("decision") or "vigilar",
                        "confidence": item.get("confidence") or item.get("opportunity_score"),
                        "genesis_reading": item.get("genesis_reading_es") or item.get("summary_es") or answer,
                        "status": "watching",
                    },
                    "opportunity_agent",
                    item.get("confidence") or "media",
                )
        return _payload(
            "opportunities",
            answer,
            [],
            extra={
                "opportunities": items,
                "radar": radar,
                "structured": structured,
                "kind": structured["kind"],
                "source_status": radar.get("source_status") or {},
            },
            memory=store,
            prompt=clean,
            conversation_id=clean_conversation_id,
        )

    if route.intent == "comparison":
        quotes = [price_agent.quote(item) for item in tickers[:2]]
        store.save_event("comparison", {"tickers": tickers[:2], "quotes": [_safe_quote_memory(item) for item in quotes]}, "price_truth", "media")
        for item in tickers[:2]:
            store.track_entity(item, "asset", {"reason": "comparison"})
        return _payload("comparison", _comparison_answer(quotes), tickers[:2], extra={"quotes": quotes}, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "chart_request":
        chart = route.chart or {"is_chart": True, "ticker": "", "range": "1Y"}
        if not chart["ticker"]:
            return _payload("chart_request", "Que activo quieres revisar?", tickers, extra={"chart": chart}, memory=store, prompt=clean, conversation_id=clean_conversation_id)
        quote = price_agent.quote(chart["ticker"])
        store.save_event("chart_request", {"ticker": chart["ticker"], "range": chart["range"], "quote": _safe_quote_memory(quote)}, "chart", "alta" if quote.get("current_price") else "baja")
        store.track_entity(chart["ticker"], "asset", {"reason": "chart_request", "range": chart["range"]})
        store.save_learned_context(f"asset_interest:{chart['ticker']}", {"ticker": chart["ticker"], "last_intent": "chart_request"}, "genesis", "media")
        technical = get_technical_agent().for_ticker(chart["ticker"], chart["range"])
        answer = _chart_answer(chart["ticker"], quote, chart.get("overlays") or [])
        structured = composer.asset_analysis(chart["ticker"], quote=quote, technical=technical)
        _remember_asset_analysis(store, chart["ticker"], quote, technical, structured, route.intent)
        extra = {"chart": chart, "quote": quote, "technical": technical, "structured": structured, "kind": structured["kind"]}
        return _payload("chart_request", answer, [chart["ticker"], *[item for item in tickers if item != chart["ticker"]]], extra=extra, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    if route.intent == "trade_decision" and explicit_ticker:
        quote = price_agent.quote(explicit_ticker)
        technical = get_technical_agent().for_ticker(explicit_ticker, "1Y")
        memory_summary = store.get_asset_learning_summary(explicit_ticker, limit=8)
        store.save_event(
            "trade_decision_request",
            {"ticker": explicit_ticker, "quote": _safe_quote_memory(quote), "memory_counts": memory_summary.get("counts")},
            "genesis",
            "alta" if quote.get("current_price") else "baja",
        )
        store.track_entity(explicit_ticker, "asset", {"reason": "trade_decision"})
        store.save_learned_context(
            f"asset_interest:{explicit_ticker}",
            {"ticker": explicit_ticker, "last_intent": route.intent, "last_question": clean[:240]},
            "genesis",
            "media",
        )
        decision = _trade_decision(explicit_ticker, quote, technical, memory_summary, clean)
        structured = composer.asset_analysis(explicit_ticker, quote=quote, technical=technical)
        structured["decision"] = decision
        structured["thesis"] = decision["reason_es"]
        structured["scenario"] = {
            "probable": decision["entry_condition_es"],
            "invalidacion": decision["invalidation_es"],
        }
        structured.setdefault("sections", [])
        structured["sections"] = [
            {"title": "Veredicto", "bullets": [decision["label_es"], decision["reason_es"]]},
            {"title": "Entrada condicional", "bullets": [decision["entry_condition_es"]]},
            {"title": "Invalidacion", "bullets": [decision["invalidation_es"]]},
            {"title": "Que vigilar", "bullets": decision["what_to_watch_es"]},
        ]
        _remember_asset_analysis(store, explicit_ticker, quote, technical, structured, route.intent)
        _remember_trade_decision(store, explicit_ticker, quote, technical, decision, memory_summary)
        extra = {
            "quote": quote,
            "technical": technical,
            "decision": decision,
            "memory_summary": memory_summary,
            "structured": structured,
            "kind": structured["kind"],
        }
        return _payload(
            "trade_decision",
            _trade_decision_answer(explicit_ticker, quote, decision),
            tickers or [explicit_ticker],
            extra=extra,
            memory=store,
            prompt=clean,
            conversation_id=clean_conversation_id,
        )

    if route.intent in {"ticker_analysis", "technical_indicators"} and explicit_ticker:
        quote = price_agent.quote(explicit_ticker)
        technical = get_technical_agent().for_ticker(explicit_ticker, "1Y")
        store.save_event("ticker_analysis", {"ticker": explicit_ticker, "quote": _safe_quote_memory(quote), "technical_requested": bool(technical)}, "price_truth", "alta" if quote.get("current_price") else "baja")
        store.track_entity(explicit_ticker, "asset", {"reason": route.intent})
        store.save_learned_context(f"asset_interest:{explicit_ticker}", {"ticker": explicit_ticker, "last_intent": route.intent}, "genesis", "media")
        structured = composer.asset_analysis(explicit_ticker, quote=quote, technical=technical)
        _remember_asset_analysis(store, explicit_ticker, quote, technical, structured, route.intent)
        extra = {"quote": quote, "technical": technical, "structured": structured, "kind": structured["kind"]}
        return _payload(route.intent, _ticker_answer(explicit_ticker, quote, technical), tickers or [explicit_ticker], extra=extra, memory=store, prompt=clean, conversation_id=clean_conversation_id)

    answer = _general_assistant_answer(clean, composer)
    if _is_personal_support_prompt(clean):
        store.save_learned_context(
            "user_context:personal_support",
            {"last_topic": "relationship_support", "last_message": clean[:360]},
            "conversation",
            "media",
        )
        store.save_event("personal_support", {"message": clean, "mode": "general_assistant"}, "genesis", "media")
    else:
        store.save_event("general_question", {"message": clean}, "genesis", "media")
    structured = _general_assistant_structured(clean, answer)
    return _payload(
        "general",
        answer,
        [],
        extra={"structured": structured, "kind": structured["kind"]},
        memory=store,
        prompt=clean,
        conversation_id=clean_conversation_id,
    )


def _general_assistant_answer(message: str, composer: Any) -> str:
    clean = str(message or "").strip()
    if _is_personal_support_prompt(clean):
        return (
            "Claro. Esto es una pregunta cotidiana, no un ticker. Te ayudo como Genesis humano: primero baja la temperatura, no intentes ganar la discusion.\n"
            "Mensaje sugerido: 'Oye, me importa como te sientes. Quiero entender que paso y arreglarlo contigo, no pelear. Si hice algo que te dolio, quiero escucharlo bien.'\n"
            "Hazlo corto, calmado y sin justificarte de entrada. Pregunta que necesita de ti ahora: espacio, disculpa concreta o hablarlo en persona.\n"
            "Si me cuentas que paso, te ayudo a escribir un mensaje exacto con el tono correcto."
        )
    if clean:
        return (
            "Te sigo. Puedo razonar esa pregunta como asistente general o convertirla en analisis financiero si me das un activo, noticia, alerta o cartera.\n"
            "Si es algo personal, te respondo natural. Si es mercado, uso FMP/backend y separo datos confirmados de inferencias.\n"
            "Dime el contexto y te lo ordeno en lectura clara, siguiente paso y riesgos."
        )
    return composer.general()


def _hedge_plan_answer(plan: dict[str, Any]) -> str:
    ticker = str(plan.get("ticker") or "cartera").upper()
    score = plan.get("hedge_score")
    risk = plan.get("risk_level") or "low"
    hedge_type = plan.get("hedge_type") or ("portfolio_hedge" if plan.get("portfolio_mode") else "none")
    ratio = plan.get("suggested_hedge_ratio") or plan.get("suggested_reduction_pct") or 0
    reason = plan.get("reason") or plan.get("genesis_reading") or "Sin deterioro fuerte confirmado."
    watch = plan.get("what_to_watch") if isinstance(plan.get("what_to_watch"), list) else []
    first_watch = watch[0] if watch else "precio, volumen, soporte, SPY/QQQ, VIX y DXY."
    return (
        f"{ticker}: hedge score {score}/100, riesgo {risk}. "
        f"Sugerencia paper: {hedge_type} con ratio aproximado {ratio}. "
        f"Razon: {reason} "
        "Esto no es una orden real ni una garantia; la cobertura reduce riesgo, pero no elimina perdidas. "
        f"Vigila: {first_watch}"
    )


def _mt5_decision_answer(decision: dict[str, Any]) -> str:
    symbol = str(decision.get("symbol") or "MT5")
    action = str(decision.get("decision") or "WAIT")
    reason = str(decision.get("reason") or "Genesis mantiene modo seguro.")
    policy = str(decision.get("order_policy") or "journal_only_no_broker")
    flags = decision.get("risk_flags") if isinstance(decision.get("risk_flags"), list) else []
    first_flag = flags[0] if flags else "sin bloqueo adicional confirmado"
    return (
        f"{symbol}: decision MT5 = {action}. Razon: {reason}. "
        f"Politica: {policy}; order_executed=false y broker_touched=false. "
        f"Bloqueo/riesgo principal: {first_flag}. Primero demo, journal y backtest."
    )


def _mentions_mt5_forward_metrics(message: str) -> bool:
    text = _fold_prompt(message)
    return any(
        token in text
        for token in (
            "forward test",
            "shadow",
            "automatico",
            "automático",
            "auto trades",
            "auto win",
            "auto profit",
            "ignora pruebas manuales",
            "efectividad",
            "win rate",
            "profit factor",
            "porcentaje",
            "senales gano",
            "senales perdio",
            "señales gano",
            "señales perdio",
            "como va el test",
            "desempeno mt5",
            "desempeño mt5",
        )
    )


def _should_route_mt5_forward_metrics(message: str) -> bool:
    text = _fold_prompt(message)
    return any(
        token in text
        for token in (
            "forward test",
            "shadow",
            "automatico",
            "auto trades",
            "auto win",
            "auto profit",
            "ignora pruebas manuales",
            "pruebas manuales",
            "efectividad",
            "win rate",
            "profit factor",
            "como va el test",
            "desempeno mt5",
            "por que no esta operando",
            "por qué no está operando",
            "no-trade",
            "no trade",
            "exploration",
            "exploracion",
            "exploración",
            "replay",
            "desempeÃ±o mt5",
            "por que no esta operando",
            "por qué no está operando",
            "no-trade",
            "no trade",
            "exploration",
            "exploracion",
            "exploración",
            "replay",
        )
    )


def _should_route_mt5_learning(message: str) -> bool:
    text = _fold_prompt(message)
    return "mt5" in text and any(
        token in text
        for token in (
            "aprendio",
            "aprendizaje",
            "memoria",
            "learning",
            "adaptive",
            "adaptativo",
            "recomendaciones",
            "recomienda",
            "racha",
            "streak",
            "estado adaptativo",
            "que aprendio",
            "que aprendiste",
        )
    )


def _mt5_metrics_symbol(message: str, explicit_ticker: str) -> str:
    text = _fold_prompt(message)
    if "btcusd" in text or str(explicit_ticker or "").upper() in {"BTCUSD", "BTCUSDT", "BTC-USD"}:
        return "BTCUSD"
    if "btc" in text or str(explicit_ticker or "").upper() == "BTC":
        return "BTCUSD"
    return str(explicit_ticker or "").upper().strip()


def _mt5_performance_answer(performance: dict[str, Any]) -> str:
    summary = performance.get("summary_auto") if isinstance(performance.get("summary_auto"), dict) else performance.get("summary") if isinstance(performance.get("summary"), dict) else {}
    exploration = performance.get("summary_exploration") if isinstance(performance.get("summary_exploration"), dict) else {}
    replay = performance.get("summary_replay") if isinstance(performance.get("summary_replay"), dict) else {}
    manual = performance.get("summary_manual") if isinstance(performance.get("summary_manual"), dict) else {}
    warning = performance.get("auto_sample_warning") or summary.get("sample_warning") or ""
    symbol = performance.get("symbol") or "MT5"
    return (
        f"MT5 Forward Test automatico {symbol}: auto win rate {summary.get('win_rate', 0)}%, "
        f"auto profit factor {summary.get('profit_factor', 0)}, expectancy {summary.get('expectancy', 0)}R, "
        f"P/L simulado auto {summary.get('net_pnl', 0)}R y drawdown {summary.get('max_drawdown', 0)}R. "
        f"Auto trades {summary.get('shadow_trades', 0)}: "
        f"{summary.get('wins', 0)} ganadoras, "
        f"{summary.get('losses', 0)} perdedoras y {summary.get('open', 0)} abiertas. "
        f"Exploration paper separado: {exploration.get('shadow_trades', 0)} trades, PF {exploration.get('profit_factor', 0)}. "
        f"Replay separado: {replay.get('replay_trades', 0)} trades, PF {replay.get('profit_factor', 0)}. "
        f"Pruebas manuales separadas: {manual.get('shadow_trades', 0)}. "
        f"{warning + ' ' if warning else ''}"
        "Todo sigue journal-only: order_executed=false, broker_touched=false."
    )


def _mt5_learning_answer(summary: dict[str, Any], state: dict[str, Any], recommendations: dict[str, Any]) -> str:
    symbol = summary.get("symbol") or state.get("symbol") or "MT5"
    recs = recommendations.get("recommendations") if isinstance(recommendations.get("recommendations"), list) else []
    main_rec = recs[0] if recs else {}
    return (
        f"Memoria adaptativa MT5 {symbol}: Genesis tiene {summary.get('total_memories', 0)} memorias "
        f"y {summary.get('lessons_count', 0)} lecciones cerradas. "
        f"Estado actual: {state.get('bot_state', 'normal')}, racha ganadora {state.get('current_win_streak', 0)}, "
        f"racha perdedora {state.get('current_loss_streak', 0)}, PF rolling {state.get('rolling_profit_factor', 0)} "
        f"y expectancy {state.get('rolling_expectancy', 0)}R. "
        f"Recomendacion: {main_rec.get('recommendation') or state.get('recommendation_summary') or 'seguir midiendo en paper'}. "
        "No aplica cambios automaticos: order_executed=false, broker_touched=false."
    )


def _mt5_health_answer(health: dict[str, Any]) -> str:
    return (
        f"MT5 bridge: {health.get('status')}. "
        f"enabled={health.get('mt5_enabled')}, demo_only={health.get('demo_only')}, "
        f"live_trading_enabled={health.get('live_trading_enabled')}, kill_switch={health.get('kill_switch')}. "
        "En esta fase no se ejecuta broker real; solo demo/journal/backtest."
    )


def _general_assistant_structured(message: str, answer: str) -> dict[str, Any]:
    personal = _is_personal_support_prompt(message)
    if personal:
        title = "Modo humano"
        mode = "Vida diaria"
        confidence = 0.82
        steps = [
            "Bajar tension antes de explicar.",
            "Validar como se siente la otra persona.",
            "Hacer una pregunta concreta y escuchar.",
            "Responder sin defenderte de inmediato.",
        ]
        watch = [
            "No mandar parrafos largos si esta molesta.",
            "No convertir una disculpa en debate.",
            "Elegir llamada o persona si el texto escala.",
        ]
    else:
        title = "Genesis"
        mode = "Asistente completo"
        confidence = 0.72
        steps = [
            "Detectar si la pregunta es vida diaria, memoria, mercado o activo.",
            "Usar datos verificados solo cuando pidas cifras financieras.",
            "Guardar contexto util sin secretos.",
        ]
        watch = [
            "Si pides precio, Genesis valida FMP/backend.",
            "Si pides memoria, usa conversaciones y eventos guardados.",
        ]
    return {
        "kind": "general_assistant",
        "title": title,
        "mode": mode,
        "summary": answer,
        "confidence": confidence,
        "sections": [
            {"title": "Lectura rapida", "bullets": [answer.splitlines()[0] if answer else "Estoy listo."]},
            {"title": "Siguiente paso", "bullets": steps},
            {"title": "Que cuidar", "bullets": watch},
        ],
    }


def _is_personal_support_prompt(message: str) -> bool:
    text = f" {_fold_prompt(message)} "
    return any(
        token in text
        for token in (
            " mi novia ",
            " mi novio ",
            " mi esposa ",
            " mi esposo ",
            " mi pareja ",
            " relacion ",
            " enojada ",
            " enojado ",
            " molesta ",
            " molesto ",
            " triste ",
            " ansioso ",
            " ansiosa ",
            " necesito consejo ",
            " dame consejo ",
            " problema personal ",
            " que le digo ",
            " como le digo ",
            " disculparme ",
            " pedir perdon ",
        )
    )


def _fold_prompt(value: object) -> str:
    normalized = unicodedata.normalize("NFD", str(value or "").casefold())
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn")


def _opportunity_mode_from_prompt(message: str) -> dict[str, str]:
    text = f" {_fold_prompt(message)} "
    if any(token in text for token in (" buena validacion ", " con buena validacion ", " entrada validada ", " entradas validas ", " validacion ")):
        return {
            "mode": "validation",
            "title": "Validador de entradas",
            "tag": "validando",
            "empty": "No valido ninguna entrada limpia ahora. Espero precio vivo, volumen, nivel e invalidacion antes de elevar conviccion.",
        }
    if any(
        token in text
        for token in (
            " comprar con cautela ",
            " compra con cautela ",
            " que hay para comprar ",
            " que puedo comprar ",
            " que podemos comprar ",
            " que podria comprar ",
            " que hay bueno para comprar ",
            " que compro ",
            " que comprar ",
            " que deberia comprar ",
            " ideas de compra ",
            " ideas para comprar ",
            " oportunidades de compra ",
            " donde hay compra ",
            " donde hay entrada ",
            " comprar hoy ",
            " compra hoy ",
            " que acciones compro ",
            " que activo compro ",
        )
    ):
        return {
            "mode": "cautious_buy",
            "title": "Compra con cautela",
            "tag": "cautela",
            "empty": "No hay compra cautelosa de calidad ahora. No fuerzo entradas: espero precio vivo, volumen y soporte de catalizador.",
        }
    return {
        "mode": "hunter",
        "title": "Cazador de buenos precios",
        "tag": "cazando",
        "empty": "No hay presa limpia ahora. Mantengo el radar abierto buscando descuento, ruptura con volumen y asimetria real.",
    }


def _opportunity_radar_answer(items: list[dict[str, Any]], mode: dict[str, str]) -> str:
    if not items:
        return f"{mode['title']}: {mode['empty']}"
    top = items[0] or {}
    ticker = top.get("ticker") or "Mercado"
    label = top.get("decision_label_es") or top.get("decision") or "vigilar"
    score = top.get("opportunity_score") or top.get("score") or "medio"
    price = top.get("price")
    level = top.get("entry_level") or top.get("resistance") or top.get("support")
    price_text = f" en {format_signed_money(price)}" if isinstance(price, (int, float)) else ""
    level_text = f" Validar nivel {format_signed_money(level)}." if isinstance(level, (int, float)) else " Validar nivel, volumen y cierre antes de actuar."
    return (
        f"{mode['title']}: {ticker} lidera el radar con {label} y score {score}{price_text}. "
        f"No es orden real; es idea para validar con precio, volumen relativo y riesgo.{level_text}"
    )


def _opportunity_radar_structured(
    radar: dict[str, Any],
    items: list[dict[str, Any]],
    mode: dict[str, str],
    answer: str,
) -> dict[str, Any]:
    total_volume = sum(float(item.get("dollar_volume") or 0) for item in items if isinstance(item.get("dollar_volume"), (int, float)))
    summary = radar.get("summary") if isinstance(radar.get("summary"), dict) else {}
    return {
        "kind": "opportunity_radar",
        "title": mode["title"],
        "summary": answer,
        "tag": mode["tag"],
        "metrics": {
            "mode": mode["mode"],
            "total": len(items),
            "actionables": len([item for item in items if str(item.get("decision") or "").lower() in {"buy_cautiously", "watch_breakout", "opportunity"}]),
            "volume": total_volume or summary.get("total_dollar_volume") or "Vigilando",
            "top_ticker": summary.get("top_ticker") or ((items[0] or {}).get("ticker") if items else ""),
            "top_score": summary.get("top_score") or ((items[0] or {}).get("opportunity_score") if items else None),
        },
        "opportunities": items[:5],
        "sections": [
            {
                "title": "Regla Genesis",
                "bullets": [
                    "Solo elevo ideas con precio vivo, volumen y nivel de invalidacion.",
                    "No es compra real ni broker; es radar para decidir mejor.",
                ],
            }
        ],
    }


def _payload(
    intent: str,
    answer: str,
    tickers: list[str],
    *,
    extra: dict[str, Any] | None = None,
    memory: MemoryStore,
    prompt: str = "",
    conversation_id: str = "default",
) -> dict[str, Any]:
    response_type = _response_type_for_intent(intent)
    memory_context = memory.get_memory_summary(answer)
    llm_result = get_llm_orchestrator().compose(
        prompt or answer,
        {
            "intent": intent,
            "response_type": response_type,
            "tickers": tickers,
            "deterministic_answer": answer,
            "data": extra or {},
            "memory": memory_context,
            "source_policy": "verified_backend_only",
        },
        answer,
    )
    answer = llm_result["answer"]
    memory.save_message(conversation_id, "assistant", answer, {"intent": intent, "tickers": tickers})
    payload = {
        "ok": True,
        "status": "genesis_intelligence_ready",
        "intent": intent,
        "response_type": response_type,
        "answer": answer,
        "tickers": tickers,
        "memory": {
            "backend": memory.backend,
            "recent_events": memory_context["recent_events"][:5],
            "recent_messages": memory_context["recent_messages"][-5:],
            "tracked_entities": memory_context["tracked_entities"][:5],
            "durable_on_railway": memory.backend == "postgres",
        },
        "llm": {"used": llm_result["used_llm"], "reason": llm_result["reason"]},
        "source_policy": "Los precios salen de FMP, snapshot validado o referencia paper. Genesis no inventa precios.",
    }
    if extra:
        payload.update(extra)
    return payload


def _response_type_for_intent(intent: str) -> str:
    return {
        "daily_briefing": "market_summary",
        "market_overview": "market_summary",
        "ticker_analysis": "asset_analysis",
        "trade_decision": "asset_analysis",
        "technical_indicators": "asset_analysis",
        "chart_request": "chart_analysis",
        "comparison": "comparison",
        "weather": "weather",
        "alerts": "alerts_digest",
        "whale_activity": "whale_flow",
        "opportunities": "opportunity_radar",
        "macro_news": "news_brief",
        "portfolio_summary": "general_assistant",
        "tracking_summary": "general_assistant",
        "performance_review": "performance_review",
        "hedge_plan": "hedge_plan",
        "mt5_bridge": "mt5_decision",
        "image_chart_analysis": "chart_analysis",
    }.get(intent, "general_assistant")


def _chart_answer(ticker: str, quote: dict[str, Any], overlays: list[str] | None = None) -> str:
    if not quote.get("current_price"):
        return (
            f"{ticker}: no tengo precio confirmado para ese activo en la fuente activa. "
            "No doy precio ni entrada operativa sin confirmacion; puedo revisar velas, retornos o contexto si FMP devuelve OHLC suficiente."
        )
    change = format_signed_money(quote.get("daily_change"))
    pct = format_signed_percent(quote.get("daily_change_pct"))
    overlay_text = f" Incluyo indicadores solicitados: {', '.join(overlays)}." if overlays else ""
    return (
        f"{ticker}: precio confirmado {quote.get('formatted_price')} ({change}, {pct}). "
        "Cargo velas japonesas con retornos por temporalidad. La lectura usa datos confirmados, no precios inventados."
        f"{overlay_text}"
    )


def _ticker_answer(ticker: str, quote: dict[str, Any], technical: dict[str, Any] | None = None) -> str:
    if not quote.get("current_price"):
        return (
            f"{ticker}: no tengo precio confirmado en FMP o snapshot validado. "
            "Lectura: no conviene tomar decision con dato incompleto. Siguiente paso: reconfirmar fuente, revisar chart OHLC y esperar precio directo."
        )
    answer = (
        f"{ticker}: {quote.get('formatted_price')} confirmado por {quote.get('source_label')}. "
        f"Cambio diario {format_signed_money(quote.get('daily_change'))} / {format_signed_percent(quote.get('daily_change_pct'))}. "
        "Veredicto: vigilar con contexto; entrada solo con confirmacion de precio, volumen y riesgo."
    )
    if technical and technical.get("ok"):
        indicators = technical.get("indicators") or {}
        answer += (
            f" Indicadores pedidos: RSI {indicators.get('rsi')}, "
            f"MACD {indicators.get('macd', {}).get('line')}, "
            f"soporte {indicators.get('support')}, resistencia {indicators.get('resistance')}, "
            f"golden pocket {indicators.get('golden_pocket')}."
        )
    return answer


def _trade_decision_answer(ticker: str, quote: dict[str, Any], decision: dict[str, Any]) -> str:
    if not quote.get("current_price"):
        return (
            f"{ticker}: veredicto {decision['label_es']}. "
            f"{decision['reason_es']} {decision['entry_condition_es']}"
        )
    return (
        f"{ticker}: {decision['label_es']}. "
        f"Precio {quote.get('formatted_price')}; {decision['reason_es']} "
        f"Entrada: {decision['entry_condition_es']} Invalidacion: {decision['invalidation_es']}"
    )


def _trade_decision(
    ticker: str,
    quote: dict[str, Any],
    technical: dict[str, Any] | None,
    memory_summary: dict[str, Any],
    prompt: str,
) -> dict[str, Any]:
    indicators = (technical or {}).get("indicators") if isinstance((technical or {}).get("indicators"), dict) else {}
    price = _number(quote.get("current_price"))
    pct = _number(quote.get("daily_change_pct"))
    rsi = _number(indicators.get("rsi"))
    rel_volume = _number(indicators.get("relative_volume"))
    support = _number(indicators.get("support"))
    resistance = _number(indicators.get("resistance"))
    volume = _number(indicators.get("volume") or quote.get("volume"))
    direction = _expected_direction_from_quote(quote, indicators)
    wants_exit = any(token in _fold_prompt(prompt) for token in ("vender", "vendo", "salirme", "tomar ganancia"))
    counts = memory_summary.get("counts") if isinstance(memory_summary.get("counts"), dict) else {}
    memory_hint = (
        f"Memoria: {counts.get('decisions', 0)} decisiones, "
        f"{counts.get('signals', 0) + counts.get('alerts', 0)} senales y "
        f"{counts.get('news', 0)} noticias previas guardadas para {ticker}."
    )

    if not price:
        action = "wait_source"
        label = "Esperar fuente"
        tone = "neutral"
        confidence = 0.25
        reason = "No hay precio actual confirmado; Genesis no recomienda operar con datos incompletos."
        entry = "Reintentar cuando FMP/backend confirme precio, volumen, soporte y resistencia."
        invalidation = "La lectura no es valida hasta tener precio vivo y grafica reciente."
        risk = "Riesgo principal: actuar con un dato que podria estar vencido o incompleto."
    elif wants_exit:
        action = "manage_risk"
        label = "Gestionar salida con niveles"
        tone = "watching"
        confidence = 0.64
        reason = (
            f"Precio confirmado {quote.get('formatted_price')}; la decision de salida depende de soporte "
            f"{_fmt_level(support)} y resistencia {_fmt_level(resistance)}."
        )
        entry = "Reducir solo si pierde soporte con volumen o si tu tesis original ya se invalido."
        invalidation = f"Recuperar/defender soporte {_fmt_level(support)} con volumen mejora la lectura."
        risk = "Riesgo principal: vender por ruido si el nivel clave sigue intacto."
    elif direction == "bullish" and (rsi is None or rsi < 72) and (rel_volume is None or rel_volume >= 0.9):
        action = "buy_cautious"
        label = "Comprar con cautela"
        tone = "bullish"
        confidence = 0.72
        reason = (
            f"{ticker} trae sesgo comprador: {format_signed_percent(pct)} diario, "
            f"RSI {_fmt_level(rsi)} y volumen {_fmt_volume(volume)}."
        )
        entry = f"Entrada parcial solo si respeta soporte {_fmt_level(support)} y confirma cierre arriba de {_fmt_level(resistance)}."
        invalidation = f"Perder soporte {_fmt_level(support)} o caer con volumen invalida la entrada."
        risk = "Riesgo principal: comprar tarde si el movimiento ya esta extendido."
    elif direction == "bullish" and rsi is not None and rsi >= 72:
        action = "wait_pullback"
        label = "Esperar retroceso"
        tone = "watching"
        confidence = 0.68
        reason = f"Hay fuerza, pero RSI {_fmt_level(rsi)} sugiere extension; Genesis prefiere mejor entrada."
        entry = f"Esperar retroceso hacia soporte {_fmt_level(support)} o nueva ruptura con volumen relativo mayor a 1x."
        invalidation = f"Si rompe {_fmt_level(resistance)} con volumen alto, se vuelve continuacion vigilable."
        risk = "Riesgo principal: perseguir precio en zona extendida."
    elif direction == "bearish":
        action = "avoid_or_wait"
        label = "No comprar todavia"
        tone = "bearish"
        confidence = 0.70
        reason = (
            f"La lectura esta presionada: {format_signed_percent(pct)} diario, "
            f"soporte {_fmt_level(support)} y resistencia {_fmt_level(resistance)}."
        )
        entry = f"Esperar recuperacion de {_fmt_level(resistance)} con volumen antes de pensar entrada."
        invalidation = f"Perder {_fmt_level(support)} confirma riesgo y mantiene la idea en espera."
        risk = "Riesgo principal: entrar contra tendencia sin confirmacion."
    else:
        action = "watch_confirmation"
        label = "Vigilar confirmacion"
        tone = "neutral"
        confidence = 0.58
        reason = (
            f"{ticker} no tiene senal suficiente: precio {quote.get('formatted_price')}, "
            f"volumen {_fmt_volume(volume)} y direccion {direction}."
        )
        entry = f"Esperar ruptura de {_fmt_level(resistance)} o rebote claro en {_fmt_level(support)} con volumen."
        invalidation = "Si el volumen no confirma, la lectura se queda en vigilancia."
        risk = "Riesgo principal: operar una zona lateral como si fuera tendencia."

    watch = [
        f"Precio contra soporte {_fmt_level(support)} y resistencia {_fmt_level(resistance)}.",
        f"Volumen {_fmt_volume(volume)} y volumen relativo {_fmt_level(rel_volume)}.",
        "Noticias, alertas y flujo institucional que cambien la tesis.",
    ]
    if memory_hint:
        watch.append(memory_hint)
    return {
        "action": action,
        "label_es": label,
        "tone": tone,
        "reason_es": reason,
        "entry_condition_es": entry,
        "invalidation_es": invalidation,
        "risk_es": risk,
        "what_to_watch_es": watch,
        "expected_direction": direction,
        "confidence": confidence,
        "source": "genesis_trade_decision",
        "memory_hint_es": memory_hint,
    }


def _comparison_answer(quotes: list[dict[str, Any]]) -> str:
    parts = []
    for quote in quotes:
        ticker = quote.get("ticker") or "Activo"
        if not quote.get("current_price"):
            parts.append(f"{ticker}: sin precio confirmado")
        else:
            parts.append(f"{ticker}: {quote.get('formatted_price')} ({format_signed_percent(quote.get('daily_change_pct'))})")
    return "Comparacion con precio confirmado: " + " | ".join(parts) + ". No uso precios inventados."


def _memory_answer(memory_summary: dict[str, Any]) -> str:
    entities = [item.get("ticker") for item in memory_summary.get("tracked_entities", []) if item.get("ticker")]
    topics = [item.get("topic") for item in memory_summary.get("recent_topics", []) if item.get("topic")]
    if not entities and not topics:
        return "Todavía tengo poca memoria útil. A partir de tus análisis, gráficas y cartera voy guardando activos, temas y preferencias sin guardar secretos."
    entity_text = ", ".join(entities[:5]) if entities else "sin activos recurrentes todavía"
    topic_text = ", ".join(topics[:4]) if topics else "sin temas recientes claros"
    return f"Recuerdo como contexto reciente: activos {entity_text}. Temas: {topic_text}. Uso esta memoria como apoyo, no como fuente de precios."


def _asset_memory_answer(memory_summary: dict[str, Any]) -> str:
    ticker = memory_summary.get("ticker") or "el activo"
    lines = [str(line).strip() for line in memory_summary.get("summary_lines", []) if str(line or "").strip()]
    counts = memory_summary.get("counts") if isinstance(memory_summary.get("counts"), dict) else {}
    if not lines:
        return f"Todavía tengo poca memoria útil de {ticker}. Desde ahora guardo tesis, noticias, alertas, ballenas y resultados para comparar después."
    context = (
        f"Memoria de {ticker}: {counts.get('decisions', 0)} decisiones, {counts.get('signals', 0) + counts.get('alerts', 0)} señales, "
        f"{counts.get('news', 0)} noticias y {counts.get('whales', 0)} lecturas de flujo guardadas."
    )
    return " ".join([context, *lines[:4], "Uso esto como contexto histórico; los precios actuales siguen viniendo de FMP/backend."])


def _memory_digest_structured(memory_summary: dict[str, Any], answer: str) -> dict[str, Any]:
    ticker = memory_summary.get("ticker") or ""
    counts = memory_summary.get("counts") if isinstance(memory_summary.get("counts"), dict) else {}
    lines = [str(line).strip() for line in memory_summary.get("summary_lines", []) if str(line or "").strip()]
    return {
        "kind": "memory_digest",
        "ticker": ticker,
        "title": f"Memoria de {ticker}" if ticker else "Memoria Genesis",
        "summary": answer,
        "metrics": {
            "decisions": counts.get("decisions") or 0,
            "signals": (counts.get("signals") or 0) + (counts.get("alerts") or 0),
            "news": counts.get("news") or 0,
            "whales": counts.get("whales") or 0,
            "outcomes": counts.get("outcomes") or 0,
        },
        "sections": [
            {"title": "Aprendizaje", "bullets": lines[:4] or ["Aún falta historial para detectar patrón confiable."]},
            {"title": "Qué vigilar", "bullets": ["Comparar tesis previas contra precio 1h/24h/7d.", "Separar inferencias de datos confirmados antes de operar."]},
        ],
    }


def _remember_asset_analysis(
    store: MemoryStore,
    ticker: str,
    quote: dict[str, Any],
    technical: dict[str, Any] | None,
    structured: dict[str, Any],
    intent: str,
) -> None:
    normalized = normalize_ticker(ticker)
    if not normalized:
        return
    indicators = (technical or {}).get("indicators") if isinstance((technical or {}).get("indicators"), dict) else {}
    scenario = structured.get("scenario") if isinstance(structured.get("scenario"), dict) else {}
    price = quote.get("current_price")
    confidence = structured.get("confidence") or ("alta" if price else "baja")
    expected_direction = _expected_direction_from_quote(quote, indicators)
    base = {
        "ticker": normalized,
        "asset_name": quote.get("name") or structured.get("title") or normalized,
        "event_type": "asset_analysis",
        "intent": intent,
        "current_price": price,
        "price_at_decision": price,
        "daily_change_pct": quote.get("daily_change_pct"),
        "support": indicators.get("support"),
        "resistance": indicators.get("resistance"),
        "rsi": indicators.get("rsi"),
        "macd": indicators.get("macd"),
        "volume": indicators.get("volume") or quote.get("volume"),
        "relative_volume": indicators.get("relative_volume"),
        "expected_direction": expected_direction,
        "expected_impact": "vigilar" if expected_direction == "neutral" else expected_direction,
        "genesis_reading": structured.get("thesis") or _ticker_answer(normalized, quote, technical),
        "status": "watching",
        "source": quote.get("source") or "price_truth",
        "confidence": confidence,
    }
    store.save_asset_memory(normalized, base, "asset_analysis", confidence)
    store.save_decision_note(
        normalized,
        _verdict_from_direction(expected_direction, price),
        {
            **base,
            "event_type": "decision_note",
            "verdict": _verdict_from_direction(expected_direction, price),
            "reason": structured.get("thesis") or "Genesis guardo lectura con precio, niveles y volumen disponibles.",
            "invalidation": scenario.get("invalidacion") or scenario.get("invalidation") or "Perder soporte o invalidar volumen.",
        },
        "genesis",
        confidence,
    )
    store.save_hypothesis(
        normalized,
        {
            **base,
            "event_type": "hypothesis",
            "hypothesis": scenario.get("probable") or "Confirmar precio y volumen antes de elevar conviccion.",
            "actual_outcome_1h": None,
            "actual_outcome_24h": None,
            "actual_outcome_7d": None,
        },
        "genesis",
        confidence,
    )
    store.save_outcome_tracking(
        normalized,
        {
            **base,
            "event_type": "outcome_tracking",
            "decision_timestamp": None,
            "status": "open" if price else "waiting_for_source",
            "review_windows": ["1h", "24h", "7d"],
            "actual_outcome_1h": None,
            "actual_outcome_24h": None,
            "actual_outcome_7d": None,
        },
        "genesis",
        confidence,
    )


def _remember_trade_decision(
    store: MemoryStore,
    ticker: str,
    quote: dict[str, Any],
    technical: dict[str, Any] | None,
    decision: dict[str, Any],
    memory_summary: dict[str, Any],
) -> None:
    normalized = normalize_ticker(ticker)
    if not normalized:
        return
    indicators = (technical or {}).get("indicators") if isinstance((technical or {}).get("indicators"), dict) else {}
    payload = {
        "ticker": normalized,
        "event_type": "trade_decision",
        "verdict": decision.get("label_es"),
        "action": decision.get("action"),
        "reason": decision.get("reason_es"),
        "price_at_decision": quote.get("current_price"),
        "support": indicators.get("support"),
        "resistance": indicators.get("resistance"),
        "expected_direction": decision.get("expected_direction"),
        "expected_impact": decision.get("label_es"),
        "confidence": decision.get("confidence"),
        "invalidation": decision.get("invalidation_es"),
        "memory_counts": memory_summary.get("counts"),
        "status": "open" if quote.get("current_price") else "waiting_for_source",
    }
    store.save_decision_note(
        normalized,
        str(decision.get("label_es") or "vigilar"),
        {
            **payload,
            "event_type": "decision_note",
            "reason": decision.get("reason_es"),
            "entry_condition": decision.get("entry_condition_es"),
            "risk": decision.get("risk_es"),
        },
        "genesis_trade_decision",
        decision.get("confidence") or "media",
    )
    store.save_signal_event(normalized, payload, "genesis_trade_decision", decision.get("confidence") or "media")
    store.save_hypothesis(
        normalized,
        {
            **payload,
            "hypothesis": decision.get("entry_condition_es"),
            "actual_outcome_1h": None,
            "actual_outcome_24h": None,
            "actual_outcome_7d": None,
        },
        "genesis_trade_decision",
        decision.get("confidence") or "media",
    )
    store.save_outcome_tracking(
        normalized,
        {
            **payload,
            "event_type": "outcome_tracking",
            "review_windows": ["1h", "24h", "7d"],
            "actual_outcome_1h": None,
            "actual_outcome_24h": None,
            "actual_outcome_7d": None,
        },
        "genesis_trade_decision",
        decision.get("confidence") or "media",
    )


def _remember_news_events(store: MemoryStore, overview: dict[str, Any]) -> None:
    for item in (overview.get("news") if isinstance(overview.get("news"), list) else [])[:20]:
        tickers = _news_tickers(item) or ["MARKET"]
        for ticker in tickers[:5]:
            store.save_news_event(
                ticker,
                {
                    "id": item.get("id"),
                    "event_type": "news_event",
                    "ticker": ticker,
                    "title": item.get("title_es") or item.get("title"),
                    "title_es": item.get("title_es") or item.get("title"),
                    "original_title": item.get("original_title") or item.get("title"),
                    "summary": item.get("summary_es") or item.get("summary") or item.get("genesis_takeaway"),
                    "source": item.get("source") or item.get("provider") or "news",
                    "published_at": item.get("published_at") or item.get("date"),
                    "expected_impact": item.get("impact") or "neutral",
                    "expected_direction": item.get("impact") or "neutral",
                    "confidence": item.get("confidence") or "media",
                    "genesis_reading": item.get("genesis_takeaway_es") or item.get("genesis_takeaway") or item.get("why_it_matters_es") or item.get("summary"),
                    "url": item.get("url"),
                },
                str(item.get("source") or item.get("provider") or "news")[:80],
                item.get("confidence") or "media",
            )


def _remember_alert_events(store: MemoryStore, alerts: dict[str, Any]) -> None:
    for item in (alerts.get("items") if isinstance(alerts.get("items"), list) else [])[:30]:
        ticker = normalize_ticker(item.get("ticker") or "")
        if not ticker:
            continue
        payload = {
            **item,
            "event_type": "signal_event",
            "ticker": ticker,
            "expected_direction": item.get("direction") or item.get("impact") or "neutral",
            "expected_impact": item.get("impact") or item.get("severity") or "watching",
            "genesis_reading": item.get("genesis_reading_es") or item.get("genesis_reading") or item.get("summary_es") or item.get("summary"),
            "status": item.get("status") or "watching",
        }
        store.save_signal_event(ticker, payload, item.get("source") or "alerts", item.get("confidence") or "media")
        store.save_alert_event(ticker, item.get("type") or item.get("alert_type") or "alert", payload, item.get("confidence") or "media")


def _news_tickers(item: dict[str, Any]) -> list[str]:
    raw = item.get("tickers_affected") or item.get("tickers") or item.get("symbols") or item.get("affected_assets") or []
    if isinstance(raw, str):
        raw = [raw]
    return [normalize_ticker(value) for value in raw if normalize_ticker(value)]


def _expected_direction_from_quote(quote: dict[str, Any], indicators: dict[str, Any]) -> str:
    pct = quote.get("daily_change_pct")
    try:
        value = float(pct)
    except (TypeError, ValueError):
        value = 0.0
    if value > 0.35:
        return "bullish"
    if value < -0.35:
        return "bearish"
    trend = str(indicators.get("trend") or "").casefold()
    if "alcista" in trend:
        return "bullish"
    if "bajista" in trend or "presion" in trend:
        return "bearish"
    return "neutral"


def _number(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _fmt_level(value: Any) -> str:
    number = _number(value)
    if number is None:
        return "pendiente"
    if abs(number) >= 100:
        return f"${number:,.2f}"
    return f"{number:,.2f}"


def _fmt_volume(value: Any) -> str:
    number = _number(value)
    if number is None:
        return "pendiente"
    if number >= 1_000_000_000:
        return f"{number / 1_000_000_000:.1f}B"
    if number >= 1_000_000:
        return f"{number / 1_000_000:.1f}M"
    if number >= 1_000:
        return f"{number / 1_000:.1f}K"
    return f"{number:,.0f}"


def _verdict_from_direction(direction: str, price: Any) -> str:
    if not price:
        return "esperar fuente"
    if direction == "bullish":
        return "vigilar continuacion"
    if direction == "bearish":
        return "vigilar riesgo"
    return "vigilar confirmación"


def _safe_quote_memory(quote: dict[str, Any]) -> dict[str, Any]:
    return {
        "ticker": quote.get("ticker"),
        "current_price": quote.get("current_price"),
        "previous_close": quote.get("previous_close"),
        "daily_change_pct": quote.get("daily_change_pct"),
        "source": quote.get("source"),
        "is_live": quote.get("is_live"),
        "sanity": quote.get("sanity"),
    }

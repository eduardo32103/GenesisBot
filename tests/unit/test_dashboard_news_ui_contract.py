from __future__ import annotations

import unittest
from pathlib import Path


class DashboardNewsUiContractTests(unittest.TestCase):
    def test_news_cards_use_stable_id_map_and_detail_sheet(self) -> None:
        script = Path("app/dashboard/app.js").read_text(encoding="utf-8")

        self.assertIn("newsItemsById", script)
        self.assertIn("alertItemsById", script)
        self.assertIn("whaleItemsById", script)
        self.assertIn("function indexNewsItems", script)
        self.assertIn("function indexAlertItems", script)
        self.assertIn("function indexWhaleItems", script)
        self.assertIn("data-news-id", script)
        self.assertIn("data-alert-id", script)
        self.assertIn("data-whale-id", script)
        self.assertIn("function openNewsDetail", script)
        self.assertIn("function openAlertDetail", script)
        self.assertIn("function openWhaleDetail", script)
        self.assertIn("const newsUrl = options.force", script)
        self.assertIn("data-news-refresh", script)
        self.assertIn("loadNews({ force: true })", script)
        self.assertIn("function normalizeNewsItemForUi", script)
        self.assertIn("function newsDisplayTitle", script)
        self.assertIn("function filteredNewsItems", script)
        self.assertIn("data-news-filter", script)
        self.assertIn("Importantes / influyentes", script)
        self.assertIn("Ultimas noticias", script)
        self.assertIn("news-sheet", Path("app/dashboard/index.html").read_text(encoding="utf-8"))
        self.assertNotIn("No encontre esa noticia en la lectura actual", script)
        self.assertNotIn("[data-alert-open]", script)
        self.assertNotIn("[data-whale-open]", script)

    def test_news_feed_does_not_promote_internal_placeholders_as_real_news(self) -> None:
        script = Path("app/dashboard/app.js").read_text(encoding="utf-8")

        self.assertIn("function isInternalNewsPlaceholder", script)
        self.assertNotIn("contexto pendiente de catalizador", script)
        self.assertNotIn("...alertItems.slice", script)
        self.assertNotIn("...whaleItems.slice", script)

    def test_search_bars_are_collapsed_by_default(self) -> None:
        script = Path("app/dashboard/app.js").read_text(encoding="utf-8")

        self.assertIn("searchOpen", script)
        self.assertIn('tracking: false', script)
        self.assertIn('portfolio: false', script)
        self.assertIn('whales: false', script)
        self.assertIn('data-toggle-search="tracking"', script)
        self.assertIn('data-toggle-search="portfolio"', script)
        self.assertIn('data-toggle-search="whales"', script)

    def test_alerts_have_opportunity_scanner_and_visual_strategy_contract(self) -> None:
        script = Path("app/dashboard/app.js").read_text(encoding="utf-8")

        self.assertIn("OPPORTUNITY_TICKERS", script)
        self.assertIn("function loadOpportunityQuotes", script)
        self.assertIn("function mergeAlertRowsWithOpportunities", script)
        self.assertIn("function localStrategyForOpportunity", script)
        self.assertIn("function flowVolumeVisualMarkup", script)
        self.assertIn("function strategyChecklistMarkup", script)
        self.assertIn("is_opportunity: true", script)
        self.assertIn("Oportunidad ·", script)
        self.assertNotIn("senal de actividad", script)
        self.assertNotIn("Que significa:", script)

    def test_genesis_whale_questions_cannot_render_as_fake_tickers(self) -> None:
        script = Path("app/dashboard/app.js").read_text(encoding="utf-8")

        self.assertIn("function isWhaleQuestion", script)
        self.assertIn("function isNewsQuestion", script)
        self.assertIn("function correctGenesisIntentPayload", script)
        self.assertIn("function forcedWhalePayloadFromState", script)
        self.assertIn("function forcedNewsPayloadFromState", script)
        self.assertIn("correctGenesisIntentPayload(payload, question)", script)
        self.assertIn("isWhaleQuestion(question) || isWhalePayload(payload)", script)
        self.assertIn("isNewsQuestion(question) || isNewsPayload(payload)", script)
        self.assertIn('"ESTA"', script)
        self.assertIn('"BALLENAS"', script)
        self.assertIn('"NOTICIAS"', script)

    def test_live_refresh_indicator_contract_exists(self) -> None:
        script = Path("app/dashboard/app.js").read_text(encoding="utf-8")
        styles = Path("app/dashboard/styles.css").read_text(encoding="utf-8")

        self.assertIn("function setLiveRefreshIndicator", script)
        self.assertIn("function liveRefreshBadgeMarkup", script)
        self.assertIn("is-live-refreshing", styles)
        self.assertIn("market-pulse-render.is-refreshing", styles)

    def test_brent_internal_ticker_is_humanized_in_news_ui(self) -> None:
        script = Path("app/dashboard/app.js").read_text(encoding="utf-8")

        self.assertIn("function humanizeInternalTickerText", script)
        self.assertIn("function displayAssetLabel", script)
        self.assertIn("Brent Crude Oil", script)
        self.assertIn("humanizeInternalTickerText(cleanCopy", script)
        self.assertIn("displayAssetLabel(asset)", script)

    def test_bottom_nav_scrolls_active_screen_to_top(self) -> None:
        script = Path("app/dashboard/app.js").read_text(encoding="utf-8")

        self.assertIn("function scrollActiveScreenToTop", script)
        self.assertIn("function scrollScreenElementToTop", script)
        self.assertIn("scrollActiveScreenToTop(screen)", script)
        self.assertIn('document.getElementById("genesis-thread")', script)
        self.assertIn('button.addEventListener("click", () => setActiveScreen', script)

    def test_genesis_voice_chat_contract_exists(self) -> None:
        script = Path("app/dashboard/app.js").read_text(encoding="utf-8")
        styles = Path("app/dashboard/styles.css").read_text(encoding="utf-8")

        self.assertIn("data-voice-toggle", script)
        self.assertIn("function toggleGenesisVoiceInput", script)
        self.assertIn("SpeechRecognition", script)
        self.assertIn("webkitSpeechRecognition", script)
        self.assertIn("speechSynthesis", script)
        self.assertIn("SpeechSynthesisUtterance", script)
        self.assertIn("function speakGenesisReply", script)
        self.assertIn("function pushGenesisAssistantMessage", script)
        self.assertIn("chat-voice-status", styles)
        self.assertIn("voice-pulse", styles)


if __name__ == "__main__":
    unittest.main()

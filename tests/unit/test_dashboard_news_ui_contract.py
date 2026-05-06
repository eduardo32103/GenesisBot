from __future__ import annotations

import unittest
from pathlib import Path


class DashboardNewsUiContractTests(unittest.TestCase):
    def test_news_cards_use_stable_id_map_and_detail_sheet(self) -> None:
        script = Path("app/dashboard/app.js").read_text(encoding="utf-8")

        self.assertIn("newsItemsById", script)
        self.assertIn("function indexNewsItems", script)
        self.assertIn("data-news-id", script)
        self.assertIn("function openNewsDetail", script)
        self.assertIn("news-sheet", Path("app/dashboard/index.html").read_text(encoding="utf-8"))
        self.assertNotIn("No encontre esa noticia en la lectura actual", script)

    def test_search_bars_are_collapsed_by_default(self) -> None:
        script = Path("app/dashboard/app.js").read_text(encoding="utf-8")

        self.assertIn("searchOpen", script)
        self.assertIn('tracking: false', script)
        self.assertIn('portfolio: false', script)
        self.assertIn('whales: false', script)
        self.assertIn('data-toggle-search="tracking"', script)
        self.assertIn('data-toggle-search="portfolio"', script)
        self.assertIn('data-toggle-search="whales"', script)


if __name__ == "__main__":
    unittest.main()

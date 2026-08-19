import unittest

from tests.context import yfinance as yf


class TestSearch(unittest.TestCase):
    def test_invalid_query(self):
        search = yf.Search(query="XYZXYZ")

        self.assertEqual(len(search.quotes), 0)
        self.assertEqual(len(search.news), 0)
        self.assertEqual(len(search.lists), 0)
        self.assertEqual(len(search.nav), 0)
        self.assertEqual(len(search.research), 0)

    def test_empty_query(self):
        search = yf.Search(query="")

        self.assertEqual(len(search.quotes), 0)
        self.assertEqual(len(search.news), 0)

    def test_fuzzy_query(self):
        search = yf.Search(query="Appel", enable_fuzzy_query=True)

        # Check if the fuzzy search retrieves relevant results despite the typo
        self.assertGreater(len(search.quotes), 0)
        self.assertIn("AAPL", search.quotes[0]['symbol'])

    def test_quotes(self):
        search = yf.Search(query="AAPL", max_results=5)

        self.assertEqual(len(search.quotes), 5)
        self.assertIn("AAPL", search.quotes[0]['symbol'])

    def test_news(self):
        search = yf.Search(query="AAPL", news_count=3)

        self.assertEqual(len(search.news), 3)

    def test_research_reports(self):
        search = yf.Search(query="AAPL", include_research=True)
        self.assertEqual(len(search.research), 3)

import unittest

import polars as pl

from tests.context import session_gbl
from tests.context import yfinance as yf


class TestLookup(unittest.TestCase):
    def setUp(self):
        self.query = "A"  # Generic query to make sure all lookup types are returned
        self.lookup = yf.Lookup(query=self.query, session=session_gbl)

    def test_get_all(self):
        result = self.lookup.get_all(count=5)

        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.height, 5)

    def test_get_stock(self):
        result = self.lookup.get_stock(count=5)

        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.height, 5)

    def test_get_mutualfund(self):
        result = self.lookup.get_mutualfund(count=5)

        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.height, 5)

    def test_get_etf(self):
        result = self.lookup.get_etf(count=5)

        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.height, 5)

    def test_get_index(self):
        result = self.lookup.get_index(count=5)

        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.height, 5)

    def test_get_future(self):
        result = self.lookup.get_future(count=5)

        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.height, 5)

    def test_get_currency(self):
        result = self.lookup.get_currency(count=5)

        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.height, 5)

    def test_get_cryptocurrency(self):
        result = self.lookup.get_cryptocurrency(count=5)

        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.height, 5)

    def test_large_all(self):
        result = self.lookup.get_all(count=1000)

        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.height, 1000)


if __name__ == "__main__":
    unittest.main()

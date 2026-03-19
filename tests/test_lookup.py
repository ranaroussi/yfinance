"""Lookup integration tests."""

import unittest

import pandas as pd

from tests.context import SESSION_GBL, yfinance as yf


class TestLookup(unittest.TestCase):
    """Verify lookup result accessors."""

    def setUp(self):
        """Create a lookup instance for each test."""
        self.query = "A"  # Generic query to make sure all lookup types are returned
        self.lookup = yf.Lookup(query=self.query, session=SESSION_GBL)

    def test_get_all(self):
        """Return mixed lookup results."""
        result = self.lookup.get_all(count=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_get_stock(self):
        """Return stock lookup results."""
        result = self.lookup.get_stock(count=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_get_mutualfund(self):
        """Return mutual fund lookup results."""
        result = self.lookup.get_mutualfund(count=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_get_etf(self):
        """Return ETF lookup results."""
        result = self.lookup.get_etf(count=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_get_index(self):
        """Return index lookup results."""
        result = self.lookup.get_index(count=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_get_future(self):
        """Return futures lookup results."""
        result = self.lookup.get_future(count=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_get_currency(self):
        """Return currency lookup results."""
        result = self.lookup.get_currency(count=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_get_cryptocurrency(self):
        """Return cryptocurrency lookup results."""
        result = self.lookup.get_cryptocurrency(count=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_large_all(self):
        """Return a large mixed lookup result set."""
        result = self.lookup.get_all(count=1000)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1000)


if __name__ == "__main__":
    unittest.main()

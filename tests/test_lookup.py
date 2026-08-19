import unittest

import pandas as pd

from tests.context import yfinance as yf, session_gbl
from yfinance.exceptions import YFDataException


class TestLookup(unittest.TestCase):
    def setUp(self):
        self.query = "A"  # Generic query to make sure all lookup types are returned
        self.lookup = yf.Lookup(query=self.query, session=session_gbl)

    def test_get_all(self):
        result = self.lookup.get_all(count=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_get_stock(self):
        result = self.lookup.get_stock(count=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_get_mutualfund(self):
        result = self.lookup.get_mutualfund(count=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_get_etf(self):
        result = self.lookup.get_etf(count=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_get_index(self):
        result = self.lookup.get_index(count=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_get_future(self):
        result = self.lookup.get_future(count=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_get_currency(self):
        result = self.lookup.get_currency(count=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_get_cryptocurrency(self):
        result = self.lookup.get_cryptocurrency(count=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_large_all(self):
        result = self.lookup.get_all(count=1000)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1000)


class TestLookupErrorHandling(unittest.TestCase):
    """Offline tests for Lookup error handling (no network access)."""

    def test_error_response_raises_yfdataexception(self):
        # When Yahoo returns a 'finance.error' payload, Lookup should raise
        # YFDataException with the query in the message, not crash with an
        # AttributeError from referencing a non-existent 'self.ticker'.
        query = "INVALID-QUERY"
        lookup = yf.Lookup(query=query)

        class _FakeResponse:
            text = '{"finance": {"error": {"code": "Bad Request"}}}'

            @staticmethod
            def json():
                return {"finance": {"result": None,
                                    "error": {"code": "Bad Request",
                                              "description": "Invalid input"}}}

        class _FakeData:
            @staticmethod
            def get(*args, **kwargs):
                return _FakeResponse()

        # Bypass the network layer entirely.
        lookup._data = _FakeData()

        with self.assertRaises(YFDataException) as cm:
            lookup.get_all(count=5)
        self.assertIn(query, str(cm.exception))


if __name__ == "__main__":
    unittest.main()

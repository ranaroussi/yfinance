import json
import unittest
from unittest import mock

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


class TestLookupCache(unittest.TestCase):
    """Lookup cache tests using mocked responses, without network requests."""

    def setUp(self):
        self.lookup = yf.Lookup("AAPL")
        self.lookup._data = mock.Mock()

        self.bad_response = mock.Mock(text="<html>Temporary error</html>")
        self.bad_response.json.side_effect = json.JSONDecodeError(
            "Expecting value", self.bad_response.text, 0)

        self.good_response = mock.Mock(text="{}")
        self.good_response.json.return_value = {
            "finance": {
                "error": None,
                "result": [{"documents": [{"symbol": "AAPL", "shortName": "Apple Inc."}]}],
            }
        }

        hide_exceptions = mock.patch.dict(yf.config.debug.data, {"hide_exceptions": True})
        hide_exceptions.start()
        self.addCleanup(hide_exceptions.stop)

    def test_lookup_recovers_after_invalid_json(self):
        self.lookup._data.get.side_effect = [self.bad_response, self.good_response]

        self.assertTrue(self.lookup.get_stock(count=5).empty)
        result = self.lookup.get_stock(count=5)
        self.assertEqual(result.index.tolist(), ["AAPL"])
        self.assertEqual(result.loc["AAPL", "shortName"], "Apple Inc.")

        # Once a valid response arrives, subsequent calls should use the cache.
        pd.testing.assert_frame_equal(self.lookup.get_stock(count=5), result)
        self.assertEqual(self.lookup._data.get.call_count, 2)

    def test_repeated_invalid_json_is_not_cached(self):
        self.lookup._data.get.return_value = self.bad_response

        self.assertTrue(self.lookup.get_stock(count=5).empty)
        self.assertTrue(self.lookup.get_stock(count=5).empty)
        self.assertEqual(self.lookup._data.get.call_count, 2)

    def test_valid_empty_results_are_cached(self):
        self.good_response.json.return_value["finance"]["result"][0]["documents"] = []
        self.lookup._data.get.return_value = self.good_response

        self.assertTrue(self.lookup.get_stock(count=5).empty)
        self.assertTrue(self.lookup.get_stock(count=5).empty)
        self.assertEqual(self.lookup._data.get.call_count, 1)

    def test_invalid_json_still_raises_when_exceptions_are_visible(self):
        self.lookup._data.get.side_effect = [self.bad_response, self.good_response]

        with mock.patch.dict(yf.config.debug.data, {"hide_exceptions": False}):
            with self.assertRaises(json.JSONDecodeError):
                self.lookup.get_stock(count=5)

        self.assertEqual(self.lookup.get_stock(count=5).index.tolist(), ["AAPL"])
        self.assertEqual(self.lookup._data.get.call_count, 2)


if __name__ == "__main__":
    unittest.main()

"""Lookup integration tests."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from tests.context import SESSION_GBL, yfinance as yf
from yfinance.exceptions import YFDataException


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

    @patch("yfinance.lookup.time.sleep", return_value=None)
    @patch("yfinance.data.YfData.get")
    def test_retries_transient_internal_server_error(self, mock_get, _mock_sleep):
        """Retry transient Yahoo lookup failures encoded in the JSON payload."""
        error_response = MagicMock()
        error_response.json.return_value = {
            "finance": {
                "error": {
                    "code": "Internal Server Error",
                    "description": "Server caught an exception",
                }
            }
        }
        success_response = MagicMock()
        success_response.json.return_value = {
            "finance": {
                "result": [
                    {
                        "documents": [
                            {"symbol": "A", "name": "Alpha"},
                        ]
                    }
                ]
            }
        }
        mock_get.side_effect = [error_response, success_response]

        lookup = yf.Lookup(query=self.query, session=SESSION_GBL)
        result = lookup.get_all(count=1)

        self.assertEqual(mock_get.call_count, 2)
        self.assertListEqual(result.index.tolist(), ["A"])

    @patch("yfinance.lookup.time.sleep", return_value=None)
    @patch("yfinance.data.YfData.get")
    def test_raises_after_retry_budget_exhausted(self, mock_get, _mock_sleep):
        """Raise once transient lookup retries are exhausted."""
        error_response = MagicMock()
        error_response.json.return_value = {
            "finance": {
                "error": {
                    "code": "Internal Server Error",
                    "description": "Server caught an exception",
                }
            }
        }
        mock_get.side_effect = [error_response, error_response, error_response]

        lookup = yf.Lookup(query=self.query, session=SESSION_GBL)

        with self.assertRaises(YFDataException):
            lookup.get_all(count=1)

        self.assertEqual(mock_get.call_count, 3)


if __name__ == "__main__":
    unittest.main()

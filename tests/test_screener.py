"""Integration tests for screener queries."""

import unittest
from typing import Any
from unittest.mock import MagicMock, patch

from yfinance.screener.query import EquityQuery
from yfinance.screener.client import screen


class TestScreener(unittest.TestCase):
    """Validate screener API request and response behavior."""

    @classmethod
    def setUpClass(cls):
        """Create shared query fixtures for screener tests."""
        operand: list[Any] = ['eodprice', 3]
        cls.query = EquityQuery('gt', operand)
        cls.predefined = 'aggressive_small_caps'

    @patch('yfinance.screener.client.YfData.post')
    def test_set_large_size_in_body(self, _mock_post):
        """Reject request bodies with unsupported `size` values."""
        with self.assertRaises(ValueError):
            screen(self.query, size=251)

    @patch('yfinance.data.YfData.post')
    def test_fetch_query(self, mock_post):
        """Return first result when screening with a custom query."""
        mock_response = MagicMock()
        mock_response.json.return_value = {'finance': {'result': [{'key': 'value'}]}}
        mock_post.return_value = mock_response

        response = screen(self.query)
        self.assertEqual(response, {'key': 'value'})

    @patch('yfinance.data.YfData.get')
    def test_fetch_predefined(self, mock_get):
        """Return first result when screening with a predefined key."""
        mock_response = MagicMock()
        mock_response.json.return_value = {'finance': {'result': [{'key': 'value'}]}}
        mock_get.return_value = mock_response

        response = screen(self.predefined)
        self.assertEqual(response, {'key': 'value'})

if __name__ == '__main__':
    unittest.main()

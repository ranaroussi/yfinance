import json
import unittest
from unittest.mock import patch, MagicMock
from yfinance.screener.screener import screen
from yfinance.screener.query import (
    EquityQuery,
    IndexQuery,
    FutureQuery,
    CryptoQuery,
    CurrencyQuery,
)


class TestScreener(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.query = EquityQuery('gt',['eodprice',3])
        self.predefined = 'aggressive_small_caps'

    @patch('yfinance.screener.screener.YfData.post')
    def test_set_large_size_in_body(self, mock_post):
        with self.assertRaises(ValueError):
            screen(self.query, size=251)

    @patch('yfinance.data.YfData.post')
    def test_fetch_query(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {'finance': {'result': [{'key': 'value'}]}}
        mock_post.return_value = mock_response

        response = screen(self.query)
        self.assertEqual(response, {'key': 'value'})

    @patch('yfinance.data.YfData.get')
    def test_fetch_predefined(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {'finance': {'result': [{'key': 'value'}]}}
        mock_get.return_value = mock_response

        response = screen(self.predefined)
        self.assertEqual(response, {'key': 'value'})

    @patch('yfinance.data.YfData.post')
    def test_extra_quotetypes_set_correct_quote_type(self, mock_post):
        """Each non-EQUITY/FUND/ETF query class must dispatch to the matching
        Yahoo quoteType. We assert by inspecting the JSON body that screen()
        POSTs."""
        mock_response = MagicMock()
        mock_response.json.return_value = {'finance': {'result': [{'key': 'value'}]}}
        mock_post.return_value = mock_response

        cases = [
            (IndexQuery('gt', ['intradayprice', 0]), 'INDEX'),
            (FutureQuery('gt', ['intradayprice', 0]), 'FUTURE'),
            (CryptoQuery('gt', ['intradaymarketcap', 1]), 'CRYPTOCURRENCY'),
            (CurrencyQuery('gt', ['intradayprice', 0]), 'CURRENCY'),
        ]
        for q, expected in cases:
            mock_post.reset_mock()
            screen(q)
            self.assertEqual(mock_post.call_count, 1)
            sent_body = json.loads(mock_post.call_args.kwargs['data'])
            self.assertEqual(sent_body['quoteType'], expected,
                f'{type(q).__name__} should dispatch quoteType={expected!r}')

    def test_currency_rejects_unknown_field(self):
        """The empirical field schema must reject fields not in the verified
        intersection — e.g. CURRENCY has no marketcap."""
        with self.assertRaises(ValueError):
            CurrencyQuery('gt', ['intradaymarketcap', 1])


if __name__ == '__main__':
    unittest.main()
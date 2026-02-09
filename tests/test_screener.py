import unittest
from unittest.mock import patch, MagicMock
from yfinance.screener.screener import screen
from yfinance.screener.query import EquityQuery, ETFQuery


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

    def test_etf_query_basic(self):
        # Test constructing an ETF query
        q = ETFQuery('gt', ['fundTotalAssets', 1000000])
        self.assertEqual(q.operator, 'GT')
        self.assertEqual(q.operands[0], 'fundTotalAssets')
        self.assertEqual(q.operands[1], 1000000)

        # Test validation failure
        with self.assertRaises(ValueError):
             ETFQuery('eq', ['invalid_field', 'value'])

    @patch('yfinance.data.YfData.post')
    def test_etf_screener_integration(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {'finance': {'result': [{'key': 'value'}]}}
        mock_post.return_value = mock_response

        q = ETFQuery('and', [
            ETFQuery('lt', ["annualReportExpenseRatio", 0.005]), 
            ETFQuery('gt', ["fundTotalAssets", 1000000000])
        ])
        
        response = screen(q)
        self.assertEqual(response, {'key': 'value'})
        
        # Verify call args to ensure quoteType is ETF
        args, kwargs = mock_post.call_args
        # kwargs['data'] is a json string, I should check if it contains "quoteType":"ETF"
        self.assertIn('"quoteType":"ETF"', kwargs['data'])

if __name__ == '__main__':
    unittest.main()

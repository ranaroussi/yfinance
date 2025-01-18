import unittest
from unittest.mock import patch, MagicMock
from yfinance.screener.screener import screen
from yfinance.screener.query import EquityQuery


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

if __name__ == '__main__':
    unittest.main()
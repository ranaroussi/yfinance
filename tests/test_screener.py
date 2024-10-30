import unittest
from unittest.mock import patch, MagicMock
from yfinance.const import PREDEFINED_SCREENER_BODY_MAP
from yfinance.screener.screener import Screener
from yfinance.screener.screener_query import EquityQuery


class TestScreener(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.screener = Screener()
        self.query = EquityQuery('gt',['eodprice',3])

    def test_set_default_body(self):
        self.screener.set_default_body(self.query)

        self.assertEqual(self.screener.body['offset'], 0)
        self.assertEqual(self.screener.body['size'], 100)
        self.assertEqual(self.screener.body['sortField'], 'ticker')
        self.assertEqual(self.screener.body['sortType'], 'desc')
        self.assertEqual(self.screener.body['quoteType'], 'equity')
        self.assertEqual(self.screener.body['query'], self.query.to_dict())
        self.assertEqual(self.screener.body['userId'], '')
        self.assertEqual(self.screener.body['userIdType'], 'guid')

    def test_set_predefined_body(self):
        k = 'most_actives'
        self.screener.set_predefined_body(k)
        self.assertEqual(self.screener.body, PREDEFINED_SCREENER_BODY_MAP[k])

    def test_set_predefined_body_invalid_key(self):
        with self.assertRaises(ValueError):
            self.screener.set_predefined_body('invalid_key')

    def test_set_body(self):
        body = {
            "offset": 0,
            "size": 100,
            "sortField": "ticker",
            "sortType": "desc",
            "quoteType": "equity",
            "query": self.query.to_dict(),
            "userId": "",
            "userIdType": "guid"
        }
        self.screener.set_body(body)

        self.assertEqual(self.screener.body, body)

    def test_set_body_missing_keys(self):
        body = {
            "offset": 0,
            "size": 100,
            "sortField": "ticker",
            "sortType": "desc",
            "quoteType": "equity"
        }
        with self.assertRaises(ValueError):
            self.screener.set_body(body)

    def test_set_body_extra_keys(self):
        body = {
            "offset": 0,
            "size": 100,
            "sortField": "ticker",
            "sortType": "desc",
            "quoteType": "equity",
            "query": self.query.to_dict(),
            "userId": "",
            "userIdType": "guid",
            "extraKey": "extraValue"
        }
        with self.assertRaises(ValueError):
            self.screener.set_body(body)

    def test_patch_body(self):
        initial_body = {
            "offset": 0,
            "size": 100,
            "sortField": "ticker",
            "sortType": "desc",
            "quoteType": "equity",
            "query": self.query.to_dict(),
            "userId": "",
            "userIdType": "guid"
        }
        self.screener.set_body(initial_body)
        patch_values = {"size": 50}
        self.screener.patch_body(patch_values)

        self.assertEqual(self.screener.body['size'], 50)
        self.assertEqual(self.screener.body['query'], self.query.to_dict())

    def test_patch_body_extra_keys(self):
        initial_body = {
            "offset": 0,
            "size": 100,
            "sortField": "ticker",
            "sortType": "desc",
            "quoteType": "equity",
            "query": self.query.to_dict(),
            "userId": "",
            "userIdType": "guid"
        }
        self.screener.set_body(initial_body)
        patch_values = {"extraKey": "extraValue"}
        with self.assertRaises(ValueError):
            self.screener.patch_body(patch_values)

    @patch('yfinance.screener.screener.YfData.post')
    def test_fetch(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {'finance': {'result': [{}]}}
        mock_post.return_value = mock_response

        self.screener.set_default_body(self.query)
        response = self.screener._fetch()

        self.assertEqual(response, {'finance': {'result': [{}]}})

    @patch('yfinance.screener.screener.YfData.post')
    def test_fetch_and_parse(self, mock_post):
        mock_response = MagicMock()
        mock_response.json.return_value = {'finance': {'result': [{'key': 'value'}]}}
        mock_post.return_value = mock_response

        self.screener.set_default_body(self.query)
        self.screener._fetch_and_parse()
        self.assertEqual(self.screener.response, {'key': 'value'})

if __name__ == '__main__':
    unittest.main()
import json
import unittest
from unittest.mock import patch, MagicMock
from yfinance.const import EQUITY_SCREENER_FIELDS
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

    @patch('yfinance.data.YfData.post')
    def test_predefined_with_offset_sends_count_as_size(self, mock_post):
        screen(self.predefined, offset=50, count=100)
        body = json.loads(mock_post.call_args.kwargs['data'])
        self.assertEqual(body['size'], 100)

    def test_no_screener_field_is_two_fields_concatenated(self):
        # A missing comma between two adjacent string literals silently
        # concatenates them into one invalid field (this is how netepsbasic and
        # netepsdiluted got merged). Guard the whole class instead of that one
        # instance: every screener field is `metric` or `metric.period`, so a
        # field carrying more than one '.' is two fields stuck together.
        for category, fields in EQUITY_SCREENER_FIELDS.items():
            for field in fields:
                self.assertLessEqual(
                    field.count('.'), 1,
                    f"{category} field {field!r} has multiple '.' segments — "
                    f"likely two fields concatenated by a missing comma",
                )



class TestNumpyScreenerOperands(unittest.TestCase):
    @patch('yfinance.data.YfData.post')
    def test_numpy_comparisons_serialize_through_screen(self, mock_post):
        import numpy as np

        mock_post.return_value.json.return_value = {
            'finance': {'result': [{'key': 'value'}]}
        }
        for scalar in [np.int32(3), np.int64(3), np.uint64(3), np.float32(3.5)]:
            for operator in ['GT', 'LT', 'GTE', 'LTE', 'EQ', 'BTWN', 'IS-IN']:
                with self.subTest(scalar=type(scalar).__name__, operator=operator):
                    operands = ['eodprice', scalar]
                    if operator == 'BTWN':
                        operands.append(np.float32(10))
                    leaf = EquityQuery(operator, operands)
                    query = EquityQuery('AND', [leaf, EquityQuery('GT', ['eodvolume', 1])])
                    self.assertEqual(screen(query), {'key': 'value'})
                    body = json.loads(mock_post.call_args.kwargs['data'])
                    serialized = body['query']['operands'][0]
                    if operator == 'IS-IN':
                        serialized = serialized['operands'][0]
                    self.assertEqual(serialized['operands'][1], float(scalar))
                    self.assertIs(leaf.operands[1], scalar)

    def test_large_numpy_integer_keeps_exact_value(self):
        import numpy as np

        value = np.uint64(2**63 + 1)
        query = EquityQuery('GT', ['intradaymarketcap', value])
        payload = json.loads(json.dumps(query.to_dict()))
        self.assertEqual(payload['operands'][1], 2**63 + 1)


if __name__ == '__main__':
    unittest.main()

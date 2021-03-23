'''
Module for testings earnings property
'''

import unittest
import yfinance as yf

from unittest import mock
from pathlib import Path

from mock import get_mocked_get_json

# Mock based on https://stackoverflow.com/a/28507806/3558475:
data_path = Path(__file__).parent/'data'

url_map0 ={
  'https://finance.yahoo.com/quote/GOOG/financials': 'goog_financials.json'
}

class TestEarnings(unittest.TestCase):
  '''
  Class for testings earnings property
  '''
  @mock.patch('yfinance.utils.get_json',
    side_effect=get_mocked_get_json(url_map0)
  )
  def test_mock(self,mock_get_json):
    goog = yf.Ticker('GOOG')

    earnings = goog.earnings

    earning_2017 = earnings['Earnings'].iloc[0]
    self.assertEqual(earning_2017,12662000000)

    self.assertEqual(len(mock_get_json.call_args_list), 2)

class TestDataValues(unittest.TestCase):
    goog = yf.Ticker('GOOG')

    def test_incomplete_data(self):
        pass

    @mock.patch('yfinance.utils.get_json',
    side_effect=get_mocked_get_json(url_map0))
    def test_no_quarterly_data_annual(self):
        # Test to ensure yearly earnings is still working after removal of a quarter
        earnings_2020 = earnings['Earnings'].iloc[3]
        self.assertEqual(earnings_2020,40269000000)

    @mock.patch('yfinance.utils.get_json',
    side_effect=get_mocked_get_json(url_map0))
    def test_no_quarterly_data_quarter(self):
        # Test to ensure quarterly earnings is still working after removal of a quarter
        earnings_2020_quarterly = goog.quarterly_earnings
        self.assertNotIn(6836000000, earnings_2020_quarter)

    def test_missing_quarter(self):
        pass
      
if __name__ == '__main__':
  unittest.main()

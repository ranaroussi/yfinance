'''
Module for testings earnings property
'''

import unittest
import yfinance as yf

from unittest import mock
from pathlib import Path

from mock import get_mocked_get_json
from math import isnan

# Mock based on https://stackoverflow.com/a/28507806/3558475:
data_path = Path(__file__).parent/'data'



class TestEarnings(unittest.TestCase):
    '''
    Class for testings earnings property
    '''
    url_map0 ={
        'https://finance.yahoo.com/quote/GOOG/financials': 'goog_financials.json'
    }

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
    '''
    Class for testing Missing or Incomplete Data values.
    Removed: quarterly earnings 1Q2020, yearly earnings 2019
    '''
    url_map_broken_goog ={
        'https://finance.yahoo.com/quote/GOOG/financials': 'broken_goog_financials.json'
    }
    url_map_missing_fields ={
        'https://finance.yahoo.com/quote/GOOG/financials': 'goog_removed.json'
    }

    @mock.patch('yfinance.utils.get_json',
    side_effect=get_mocked_get_json(url_map_broken_goog)
    )
    def test_no_yearly_data(self, mock_get_json):
        goog = yf.Ticker('GOOG')

        earnings = goog.earnings

        self.assertNotIn(34343000000, earnings) # This is the year we deleted
        self.assertEqual(12662000000, earnings['Earnings'].iloc[0])
        self.assertEqual(30736000000, earnings['Earnings'].iloc[1])
        self.assertEqual(40269000000, earnings['Earnings'].iloc[2])
        self.assertEqual(3, earnings["Earnings"].size) #only have 3 years in total

    @mock.patch('yfinance.utils.get_json',
    side_effect=get_mocked_get_json(url_map_broken_goog)
    )
    def test_no_quarterly_data(self, mock_get_json):
        goog = yf.Ticker("GOOG")
        earnings = goog.quarterly_earnings["Earnings"]

        self.assertNotIn(6836000000, earnings)
        self.assertEqual(6959000000, earnings.iloc[0])
        self.assertEqual(11247000000, earnings.iloc[1])
        self.assertEqual(15227000000, earnings.iloc[2])
        self.assertTrue(earnings.size == 3)

    # should we make these a different class?
    # These are test for when the "earnings" field is missing in the pandas object
    @mock.patch('yfinance.utils.get_json',
    side_effect=get_mocked_get_json(url_map_missing_fields)
    )
    def test_annual_earnings_field_missing(self, mock_get_json):
        goog = yf.Ticker("GOOG")
        earnings = goog.earnings
        self.assertTrue(isnan(earnings['Earnings'].iloc[0]))

    @mock.patch('yfinance.utils.get_json',
    side_effect=get_mocked_get_json(url_map_missing_fields)
    )
    def test_quarterly_earnings_field_missing(self, mock_get_json):
        goog = yf.Ticker("GOOG")
        earnings = goog.quarterly_earnings["Earnings"]
        self.assertTrue(isnan(earnings.iloc[0]))

      
if __name__ == '__main__':
  unittest.main()

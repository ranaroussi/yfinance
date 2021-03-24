import utils
import unittest
import datetime
import pandas as pd
import numpy as np
from pandas.testing import *
#sample datas and its expected outputs
data1={'meta': {'currency': 'USD', 'symbol': 'TSM', 'exchangeName': 'NYQ', 'instrumentType': 'EQUITY', 'firstTradeDate': 876403800, 'regularMarketTime': 1616529601, 'gmtoffset': -14400, 'timezone': 'EDT', 'exchangeTimezoneName': 'America/New_York', 'regularMarketPrice': 114.89, 'chartPreviousClose': 77.92, 'priceHint': 2, 'currentTradingPeriod': {'pre': {'timezone': 'EDT', 'start': 1616572800, 'end': 1616592600, 'gmtoffset': -14400}, 'regular': {'timezone': 'EDT', 'start': 1616592600, 'end': 1616616000, 'gmtoffset': -14400}, 'post': {'timezone': 'EDT', 'start': 1616616000, 'end': 1616630400, 'gmtoffset': -14400}}, 'dataGranularity': '1d', 'range': '6mo', 'validRanges': ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']},  'events': {'dividends': {'1615987800': {'amount': 0.448, 'date': 1615987800}, '1608215400': {'amount': 0.442, 'date': 1608215400}}}}
data2={'meta': {'currency': 'USD', 'symbol': 'TSM', 'exchangeName': 'NYQ', 'instrumentType': 'EQUITY', 'firstTradeDate': 876403800, 'regularMarketTime': 1616529601, 'gmtoffset': -14400, 'timezone': 'EDT', 'exchangeTimezoneName': 'America/New_York', 'regularMarketPrice': 114.89, 'chartPreviousClose': 77.92, 'priceHint': 2, 'currentTradingPeriod': {'pre': {'timezone': 'EDT', 'start': 1616572800, 'end': 1616592600, 'gmtoffset': -14400}, 'regular': {'timezone': 'EDT', 'start': 1616592600, 'end': 1616616000, 'gmtoffset': -14400}, 'post': {'timezone': 'EDT', 'start': 1616616000, 'end': 1616630400, 'gmtoffset': -14400}}, 'dataGranularity': '1d', 'range': '6mo', 'validRanges': ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']}}
output1 = pd.DataFrame(columns=["Dividends"])
output1=pd.DataFrame(data=[{'amount': 0.448, 'date': 1615987800},{'amount': 0.442, 'date': 1608215400}])
output1.set_index("date",inplace=True)
output1.index = pd.to_datetime(output1.index, unit="s")
output1.sort_index(inplace=True)
#dividends.index = dividends.index.tz_localize(tz)
output1.columns=["Dividends"]
class Test_parse_action(unittest.TestCase):
    """
    test parse_actions function
    """
    
    def test_dividend(self):
        """
        Test if it can correctly return the dividends in the correct order and format(parsed correctly)
        """
        #case1: when Data that contains two different dividends and contains no splits.
        result=utils.parse_actions(data1)
        self.assertNotEqual(result, None)#check if the returned result is None
        self.assertFalse(result[0].empty)#check if the returned dividents is not empty
        self.assertTrue(result[1].empty)#check if the returned splits returned is empty
        assert_frame_equal(result[0],output1)#check if the data frame returned match with expected output

        #case2: when data has no dividends and splits events
        result2=utils.parse_actions(data2)
        self.assertTrue(result2[0].empty)
        self.assertTrue(result2[1].empty)
    
if __name__ == '__main__':
    unittest.main()
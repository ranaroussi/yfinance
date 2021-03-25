import utils
import unittest
import datetime
import pandas as pd
import numpy as np
from pandas.testing import *
#sample datas and its expected outputs
data1={'meta': {'currency': 'USD', 'symbol': 'TSM', 'exchangeName': 'NYQ', 'instrumentType': 'EQUITY', 'firstTradeDate': 876403800, 'regularMarketTime': 1616529601, 'gmtoffset': -14400, 'timezone': 'EDT', 'exchangeTimezoneName': 'America/New_York', 'regularMarketPrice': 114.89, 'chartPreviousClose': 77.92, 'priceHint': 2, 'currentTradingPeriod': {'pre': {'timezone': 'EDT', 'start': 1616572800, 'end': 1616592600, 'gmtoffset': -14400}, 'regular': {'timezone': 'EDT', 'start': 1616592600, 'end': 1616616000, 'gmtoffset': -14400}, 'post': {'timezone': 'EDT', 'start': 1616616000, 'end': 1616630400, 'gmtoffset': -14400}}, 'dataGranularity': '1d', 'range': '6mo', 'validRanges': ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']},  'events': {'dividends': {'1615987800': {'amount': 0.448, 'date': 1615987800}, '1608215400': {'amount': 0.442, 'date': 1608215400}}}}
data2={'meta': {'currency': 'USD', 'symbol': 'TSM', 'exchangeName': 'NYQ', 'instrumentType': 'EQUITY', 'firstTradeDate': 876403800, 'regularMarketTime': 1616529601, 'gmtoffset': -14400, 'timezone': 'EDT', 'exchangeTimezoneName': 'America/New_York', 'regularMarketPrice': 114.89, 'chartPreviousClose': 77.92, 'priceHint': 2, 'currentTradingPeriod': {'pre': {'timezone': 'EDT', 'start': 1616572800, 'end': 1616592600, 'gmtoffset': -14400}, 'regular': {'timezone': 'EDT', 'start': 1616592600, 'end': 1616616000, 'gmtoffset': -14400}, 'post': {'timezone': 'EDT', 'start': 1616616000, 'end': 1616630400, 'gmtoffset': -14400}}, 'dataGranularity': '1d', 'range': '6mo', 'validRanges': ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']}}
data3={'meta': {'currency': 'EUR', 'symbol': 'UH7.F', 'exchangeName': 'FRA', 'instrumentType': 'EQUITY', 'firstTradeDate': 1140678000, 'regularMarketTime': 1584084000, 'gmtoffset': 3600, 'timezone': 'CET', 'exchangeTimezoneName': 'Europe/Berlin', 'regularMarketPrice': 0.0005, 'chartPreviousClose': 0.0005, 'priceHint': 4, 'currentTradingPeriod': {'pre': {'timezone': 'CET', 'end': 1616569200, 'start': 1616569200, 'gmtoffset': 3600}, 'regular': {'timezone': 'CET', 'end': 1616619600, 'start': 1616569200, 'gmtoffset': 3600}, 'post': {'timezone': 'CET', 'end': 1616619600, 'start': 1616619600, 'gmtoffset': 3600}}, 'dataGranularity': '1d', 'range': '6mo', 'validRanges': ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']}, 'events': {'splits': {'1616482800': {'date': 1616482800, 'numerator': 1, 'denominator': 50, 'splitRatio': '1:50'}}}}
data4={'meta': {'currency': 'USD', 'symbol': 'AAPL', 'exchangeName': 'NMS', 'instrumentType': 'EQUITY', 'firstTradeDate': 345479400, 'regularMarketTime': 1616616002, 'gmtoffset': -14400, 'timezone': 'EDT', 'exchangeTimezoneName': 'America/New_York', 'regularMarketPrice': 120.09, 'chartPreviousClose': 108.22, 'priceHint': 2, 'currentTradingPeriod': {'pre': {'timezone': 'EDT', 'start': 1616659200, 'end': 1616679000, 'gmtoffset': -14400}, 'regular': {'timezone': 'EDT', 'start': 1616679000, 'end': 1616702400, 'gmtoffset': -14400}, 'post': {'timezone': 'EDT', 'start': 1616702400, 'end': 1616716800, 'gmtoffset': -14400}}, 'dataGranularity': '1d', 'range': '6mo', 'validRanges': ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']}, 'events': {'dividends': {'1604673000': {'amount': 0.205, 'date': 1604673000}, '1612535400': {'amount': 0.205, 'date': 1612535400}}}}
data5=''
data6={'meta': {'currency': 'ABC', 'symbol': 'AAPL', 'exchangeName': 'NMS', 'instrumentType': 'EQUITY', 'firstTradeDate': 345479400, 'regularMarketTime': 1616616002, 'gmtoffset': -14400, 'timezone': 'EDT', 'exchangeTimezoneName': 'America/New_York', 'regularMarketPrice': 120.09, 'chartPreviousClose': 108.22, 'priceHint': 2, 'currentTradingPeriod': {'pre': {'timezone': 'EDT', 'start': 1616659200, 'end': 1616679000, 'gmtoffset': -14400}, 'regular': {'timezone': 'EDT', 'start': 1616679000, 'end': 1616702400, 'gmtoffset': -14400}, 'post': {'timezone': 'EDT', 'start': 1616702400, 'end': 1616716800, 'gmtoffset': -14400}}, 'dataGranularity': '1d', 'range': '6mo', 'validRanges': ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']}, 'events': {'dividends': {'1604673000': {'amount': 0.205, 'date': 1604673000}, '1612535400': {'amount': 0.205, 'date': -9999999}}}}
output1=pd.DataFrame(columns=["Dividends"])
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
        output1=pd.DataFrame(columns=["Dividends"])
        output1=pd.DataFrame(data=[{'amount': 0.448, 'date': 1615987800},{'amount': 0.442, 'date': 1608215400}])
        output1.set_index("date",inplace=True)
        output1.index = pd.to_datetime(output1.index, unit="s")
        output1.sort_index(inplace=True)
        #dividends.index = dividends.index.tz_localize(tz)
        output1.columns=["Dividends"]
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
    
    def test_split(self):
        
        #mock dataframe containing a stock info with splits
        output = pd.DataFrame(data=[{'date': 1616482800, 'numerator': 1, 'denominator': 50, 'splitRatio': '1:50'}])
        output.set_index("date",inplace=True)
        output.index = pd.to_datetime(output.index, unit="s")
        output.sort_index(inplace=True)
        output["Stock Splits"] = output["numerator"] / \
                output["denominator"]
        output = output["Stock Splits"]

        #case3: data has only split event
        result=utils.parse_actions(data3) #call parse action function with the above data
        self.assertNotEqual(result, None) #Check if the result is None
        self.assertFalse(result[1].empty) #Check if the result contains splits
        self.assertTrue(result[0].empty) #Check if the result does not contain dividends
        assert_series_equal(result[1], output) #Check if the result matches the mock data

    def test_dateTime(self):
        """
        Test if date and time are in correct format
        """

        output = pd.DataFrame(columns=["Dividends"])
        output = pd.DataFrame(data=[{'amount': 0.205, 'date': 1604673000}, {'amount': 0.205, 'date': 1612535400}])
        output.set_index("date",inplace=True)
        output.index = pd.to_datetime(output.index, unit="s")
        dividends, splits = utils.parse_actions(data4)

        self.assertTrue(splits.empty)
        self.assertFalse(dividends.empty)
        self.assertEqual(dividends.index[0], output.index[0])


    def test_emptyInput(self):
        """
        Test if data can handle wrong input argument
        """

        #output = pd.DataFrame(columns=["Dividends"])
        #output = pd.DataFrame(data=[{'amount': 0.205, 'date': 1604673000}, {'amount': 0.205, 'date': 1612535400}])
        output = pd.DataFrame(columns=["Dividends"])
        output = pd.DataFrame(data=[''])
        output.index = pd.to_datetime(output.index, unit="s")

        result = utils.parse_actions(data5)
        self.assertTrue(result[0].empty)
        self.assertTrue(result[1].empty)


    def test_wrongData(self):
        """
        Test wring data
        """
        
        result = utils.parse_actions(data6)
        print(result)


if __name__ == '__main__':
    unittest.main()
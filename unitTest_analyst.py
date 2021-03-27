import yfinance as yf
from yfinance.base import TickerBase
from yfinance import utils
import unittest
import pandas as _pd
import datetime

symbols = ['MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']
tickers = [yf.Ticker(symbol) for symbol in symbols]


class TestMethods(unittest.TestCase):

    def test_happyPath_shouldReturnCorrectData(self):
        ''' 
        Test case: Test if analyst_recommendations() outputs self._recommendations, in which 
            index.name should = "Date"
        Test condition: if scrape URL is correct, ticker is MSFT, and all conditions are met
        Return type: 
            analyst_recommendations(data).index.name = "Date"

        >>> yf.Ticker('MSFT').analyst_recommendations(utils.get_json("{}/{}".format('https://finance.yahoo.com/quote', 'MSFT'), None)).index.name
        'Date'
        '''
        print("TESTING test_happyPath_shouldReturnCorrectData")
        for symbol in symbols:
            # setup
            tickerbase = TickerBase(symbol)
            scrape_url = 'https://finance.yahoo.com/quote'
            ticker_url = "{}/{}".format(scrape_url, symbol)

            data = utils.get_json(ticker_url, None)

            # call method to be tested
            output = tickerbase.analyst_recommendations(data)
            # print(output)

            # test
            if output is not None:
                self.assertEqual(output.index.name, "Date")
            else:
                self.assertEqual(output, None)

    def test_incorrectInputData_shouldReturnNone(self):
        ''' 
        Test case: Test if analyst_recommendations() outputs None when data provided is incorrect
        Test condition: if data provided for analyst_recommendations() is incorrect
        Return type: 
            analyst_recommendations(True) -> None
            analyst_recommendations(False) -> None
            analyst_recommendations(None) -> None
            analyst_recommendations('wrong data format') -> None
            analyst_recommendations([1,2,3]) -> None
            analyst_recommendations(1) -> None

        >>> yf.Ticker('MSFT').analyst_recommendations(True) is None
        True
        >>> yf.Ticker('MSFT').analyst_recommendations(False) is None
        True
        >>> yf.Ticker('MSFT').analyst_recommendations(None) is None
        True
        >>> yf.Ticker('MSFT').analyst_recommendations('wrong data format') is None
        True
        >>> yf.Ticker('MSFT').analyst_recommendations([1,2,3]) is None
        True
        >>> yf.Ticker('MSFT').analyst_recommendations(1) is None
        True
        '''
        print("TESTING test_incorrectInputData_shouldReturnNone")
        for symbol in symbols:
            # setup
            tickerbase = TickerBase(symbol)

            # test
            self.assertIsNone(tickerbase.analyst_recommendations(True))
            self.assertIsNone(tickerbase.analyst_recommendations(False))
            self.assertIsNone(tickerbase.analyst_recommendations(None))
            self.assertIsNone(tickerbase.analyst_recommendations("123"))
            self.assertIsNone(tickerbase.analyst_recommendations([1, 2, 3]))
            self.assertIsNone(tickerbase.analyst_recommendations(1))

    def test_camel2title_should_correctly_camel_titles(self):
        ''' 
        Test case: Test if the titles of the output of analyst_recommendations(data) 
            is correctly converted from camel case to title case
        Test condition: if the data input is correct, all conditions met
        Return type: 
            analyst_recommendations(data).columns.to_numpy() = ['Firm', 'To Grade', 'From Grade', 'Action']

        >>> yf.Ticker('MSFT').analyst_recommendations(utils.get_json("{}/{}".format('https://finance.yahoo.com/quote', 'MSFT'), None)).columns.to_numpy()
        array(['Firm', 'To Grade', 'From Grade', 'Action'], dtype=object)
        '''
        print("TESTING test_camel2title_should_correctly_camel_titles")
        scrape_url = 'https://finance.yahoo.com/quote'
        ticker_url = "{}/{}".format(scrape_url, 'MSFT')
        data = utils.get_json(ticker_url, None)
        res = TickerBase.analyst_recommendations(self, data)
        titles = res.columns
        titles_arr = titles.to_numpy()
        alist = ['firm', 'toGrade', 'fromGrade', 'action']
        expected_res = utils.camel2title(alist)
        self.assertTrue((titles_arr == expected_res).all())

    def test_if_sorted(self):
        '''
        Test Case: To check the index ordering of the data is the ascending order 
        Test Condition: Check the the previous index has a lower datetime than the current index datetime
        Return type: TickerBase.analyst_recommendations(self, data).index[i] > TickerBase.analyst_recommendations(self, data).index[i-1]
            
        >>> 
            
        '''

        print("TESTING test_if_sorted")
        ticker_url = "{}/{}".format('https://finance.yahoo.com/quote', "MSFT")
        data = utils.get_json(ticker_url, None)
        for i in range(1, len(TickerBase.analyst_recommendations(self, data).index)):
            self.assertTrue(TickerBase.analyst_recommendations(self, data).index[i] >
                            TickerBase.analyst_recommendations(self, data).index[i - 1])

    def test_date_time_format(self):
        '''
        Test Case: To check the index used in the dataframe work is stored as a datetime format 
        Test Condition: Check if every index value in the dataframe is in datetime timestamp
        Return type: TickerBase.analyst_recommendations(self, data).index
            
        >>> type(yf.Ticker('MSFT').analyst_recommendations(utils.get_json("{}/{}".format('https://finance.yahoo.com/quote', "MSFT"), None)).index)
            <class 'pandas.core.indexes.datetimes.DatetimeIndex'>
        >>> type(yf.Ticker('MSFT').analyst_recommendations(utils.get_json("{}/{}".format('https://finance.yahoo.com/quote', "MSFT"), None)).index[0])
            <class 'pandas._libs.tslibs.timestamps.Timestamp'>
            
        '''

        print("TESTING test_date_time_format")
        ticker_url = "{}/{}".format('https://finance.yahoo.com/quote', "MSFT")
        data = utils.get_json(ticker_url, None)
        for index in TickerBase.analyst_recommendations(self, data).index:
            self.assertTrue(isinstance(index, datetime.datetime))

    def test_if_dataframe(self):
        '''
        Test Case: To check the output data of analyst recommendation is stored as a panda dataframe 
        Test Condition: If the returned variable instance is a dataframe
        Return type: isinstance(TickerBase.analyst_recommendations(self, data), _pd.core.frame.DataFrame)
            
        >>> type(yf.Ticker('MSFT').analyst_recommendations(utils.get_json("{}/{}".format('https://finance.yahoo.com/quote', "MSFT"), None)))
            <class 'pandas.core.frame.DataFrame'>
        '''

        print("TESTING test_if_dataframe")
        ticker_url = "{}/{}".format('https://finance.yahoo.com/quote', "MSFT")
        data = utils.get_json(ticker_url, None)
        output = TickerBase.analyst_recommendations(self, data)
        self.assertTrue(isinstance(output, _pd.core.frame.DataFrame))


if __name__ == "__main__":
    unittest.main()

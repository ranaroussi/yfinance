import yfinance as yf
from yfinance.base import TickerBase
from yfinance import utils
import unittest 
import doctest
import pandas as _pd

symbols = ['MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']
tickers = [yf.Ticker(symbol) for symbol in symbols]

class TestMethods(unittest.TestCase):
    
    def test_happyPath_shouldReturnCorrectData(self):
        ''' 
        Test case: if scrape URL is correct, ticker is MSFT, and all conditions are met
        Return type: analyst_recommendations(data) should return self._recommendations, in which 
            index.name should = "Date"
        '''

        for symbol in symbols: 
            # setup
            tickerbase = TickerBase(symbol)
            scrape_url = 'https://finance.yahoo.com/quote'
            ticker_url = "{}/{}".format(scrape_url, symbol)

            data = utils.get_json(ticker_url, None)

            # call method to be tested 
            output = tickerbase.analyst_recommendations(data)
            print(output)

            # test 
            if output is not None:
                self.assertEqual(output.index.name, "Date")

    def test_incorrectInputData_shouldReturnNone(self):
        for symbol in symbols: 
            # setup
            tickerbase = TickerBase(symbol)

            # test 
            self.assertIsNone(tickerbase.analyst_recommendations(True))
            self.assertIsNone(tickerbase.analyst_recommendations(False))
            self.assertIsNone(tickerbase.analyst_recommendations(None))
            self.assertIsNone(tickerbase.analyst_recommendations("wrong data format"))
            self.assertIsNone(tickerbase.analyst_recommendations(1))
    
    # def test_exception(self):
    #     for symbol in symbols: 
    #         # setup
    #         tickerbase = TickerBase(symbol)
    #         scrape_url = 'https://finance.yahoo.com/quote'
    #         ticker_url = "{}/{}".format(scrape_url, symbol)

    #         data = utils.get_json(ticker_url, None)

    #         # call method to be tested 
    #         output = tickerbase.analyst_recommendations(data)

    #         rec = _pd.DataFrame(data['upgradeDowngradeHistory']['history'])
    #         rec['earningsDate'] = _pd.to_datetime(rec['epochGradeDate'], unit='s')
    #         rec.set_index('earningsDate', inplace=True)
    #         print(rec)

            # data = rec[['Firm', 'To Grade', 'From Grade', 'Action']].sort_index()
            # print(output)

            # test 
            # self.assertIsNone(output)

    # def test_deletion(self):
    #     for symbol in symbols: 
    #         # setup
    #         tickerbase = TickerBase(symbol)
    #         scrape_url = 'https://finance.yahoo.com/quote'
    #         ticker_url = "{}/{}".format(scrape_url, symbol)

    #         data = utils.get_json(ticker_url, None)

    #         # call method to be tested 
    #         if 'earningsDate' in data.index:
    #             del data['earningsDate'] 
    #         output = tickerbase.analyst_recommendations(data)
    #         # print(output)

    #         # test 
    #         self.assertIsNone(output)


if __name__ == "__main__":
    unittest.main()
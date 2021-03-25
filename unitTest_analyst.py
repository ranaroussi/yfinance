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
            # print(output)

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

    def test_camel2title_should_correctly_camel_titles(self):
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
        ticker_url = "{}/{}".format('https://finance.yahoo.com/quote', "MSFT")
        data = utils.get_json(ticker_url, None)
        for i in range(1, len(TickerBase.analyst_recommendations(self, data).index)):
            self.assertTrue(TickerBase.analyst_recommendations(self, data).index[i] >
                            TickerBase.analyst_recommendations(self, data).index[i - 1])

    def test_date_time_format(self):
        ticker_url = "{}/{}".format('https://finance.yahoo.com/quote', "MSFT")
        data = utils.get_json(ticker_url, None)
        for i in range(1, len(TickerBase.analyst_recommendations(self, data).index)):
            test_variable = TickerBase.analyst_recommendations(self, data).index[i]
            self.assertTrue(isinstance(test_variable, datetime.datetime))

    def test_if_dataframe(self):
        ticker_url = "{}/{}".format('https://finance.yahoo.com/quote', "MSFT")
        data = utils.get_json(ticker_url, None)
        output = TickerBase.analyst_recommendations(self, data)
        self.assertTrue(isinstance(output, _pd.core.frame.DataFrame))

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

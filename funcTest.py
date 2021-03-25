import yfinance as yf
import unittest
import doctest
from yfinance.base import TickerBase
from yfinance import utils
import datetime

symbols = ['MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']
tickers = [yf.Ticker(symbol) for symbol in symbols]


class TestMethods(unittest.TestCase):
    def test_xx(self):
        pass

    def test_camel2title_should_correctly_camel_titles(self):
        scrape_url = 'https://finance.yahoo.com/quote'
        ticker_url = "{}/{}".format(scrape_url, 'MSFT')
        data = utils.get_json(ticker_url, None)
        res = TickerBase.analyst_recommendations(self, data)
        titles = res.columns
        titles_arr = titles.to_numpy()
        alist = ['firm', 'toGrade', 'fromGrade', 'action']
        expected_res = utils.camel2title(alist)
        print((titles_arr == expected_res).all())
        self.assertTrue((titles_arr == expected_res).all())

if __name__ == "__main__":
    unittest.main()

import yfinance as yf
import unittest 
import doctest

symbols = ['MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']
tickers = [yf.Ticker(symbol) for symbol in symbols]

class TestMethods(unittest.TestCase):
    def test_xx(self): 
        pass

    def test_recommendations(self):
        for ticker in tickers:
            print(ticker.recommendations)
    


if __name__ == "__main__":
    unittest.main()
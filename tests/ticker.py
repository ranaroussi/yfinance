"""
Tests for Ticker

To run all tests in suite from commandline:
   python -m unittest tests.ticker

Specific test class:
   python -m unittest tests.ticker.TestTicker

"""
import pandas as pd
from requests import HTTPError

from .context import yfinance as yf

import unittest
import requests_cache

# Set this to see the exact requests that are made during tests
DEBUG_LOG_REQUESTS = False

if DEBUG_LOG_REQUESTS:
    import logging

    logging.basicConfig(level=logging.DEBUG)


class TestTicker(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = requests_cache.CachedSession(backend='memory')

    @classmethod
    def tearDownClass(cls):
        if cls.session is not None:
            cls.session.close()

    def test_getTz(self):
        tkrs = ["IMP.JO", "BHG.JO", "SSW.JO", "BP.L", "INTC"]
        for tkr in tkrs:
            # First step: remove ticker from tz-cache
            yf.utils.get_tz_cache().store(tkr, None)

            # Test:
            dat = yf.Ticker(tkr, session=self.session)
            tz = dat._get_ticker_tz(debug_mode=False, proxy=None, timeout=None)

            self.assertIsNotNone(tz)

    def test_badTicker(self):
        # Check yfinance doesn't die when ticker delisted

        tkr = "AM2Z.TA"
        dat = yf.Ticker(tkr, session=self.session)
        dat.history(period="1wk")
        dat.history(start="2022-01-01")
        dat.history(start="2022-01-01", end="2022-03-01")
        yf.download([tkr], period="1wk")
        dat.isin
        dat.major_holders
        dat.institutional_holders
        dat.mutualfund_holders
        dat.dividends
        dat.splits
        dat.actions
        dat.shares
        dat.info
        dat.calendar
        dat.recommendations
        dat.earnings
        dat.quarterly_earnings
        dat.income_stmt
        dat.quarterly_income_stmt
        dat.balance_sheet
        dat.quarterly_balance_sheet
        dat.cashflow
        dat.quarterly_cashflow
        dat.recommendations_summary
        dat.analyst_price_target
        dat.revenue_forecasts
        dat.sustainability
        dat.options
        dat.news
        dat.earnings_trend
        dat.earnings_dates
        dat.earnings_forecasts

    def test_goodTicker(self):
        # that yfinance works when full api is called on same instance of ticker

        tkr = "IBM"
        dat = yf.Ticker(tkr, session=self.session)

        dat.isin
        dat.major_holders
        dat.institutional_holders
        dat.mutualfund_holders
        dat.dividends
        dat.splits
        dat.actions
        dat.shares
        dat.info
        dat.calendar
        dat.recommendations
        dat.earnings
        dat.quarterly_earnings
        dat.income_stmt
        dat.quarterly_income_stmt
        dat.balance_sheet
        dat.quarterly_balance_sheet
        dat.cashflow
        dat.quarterly_cashflow
        dat.recommendations_summary
        dat.analyst_price_target
        dat.revenue_forecasts
        dat.sustainability
        dat.options
        dat.news
        dat.earnings_trend
        dat.earnings_dates
        dat.earnings_forecasts

        dat.history(period="1wk")
        dat.history(start="2022-01-01")
        dat.history(start="2022-01-01", end="2022-03-01")
        yf.download([tkr], period="1wk")


class TestTickerHistory(unittest.TestCase):
    def setUp(self):
        # use a ticker that has dividends
        self.ticker = yf.Ticker("IBM")

    def tearDown(self):
        self.ticker = None

    def test_dividends(self):
        data = self.ticker.dividends
        self.assertIsInstance(data, pd.Series, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

    def test_splits(self):
        data = self.ticker.splits
        self.assertIsInstance(data, pd.Series, "data has wrong type")
        # self.assertFalse(data.empty, "data is empty")

    def test_actions(self):
        data = self.ticker.actions
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

    def test_metadata(self):
        data = self.ticker.metadata
        self.assertIsInstance(data, dict, "data has wrong type")
        self.assertTrue(data, "data is empty")
        self.assertIn("currency", data, "metadata seems to be missing data")

    def test_metadata_should_fail_if_bad_ticker(self):
        ticker = yf.Ticker("DOES_NOT_EXIST")
        self.assertRaises(HTTPError, lambda _: ticker.metadata, "Did not get 404")


class TestTickerEarnings(unittest.TestCase):

    def setUp(self):
        self.ticker = yf.Ticker("GOOGL")

    def tearDown(self):
        self.ticker = None

    def test_earnings(self):
        data = self.ticker.earnings
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.earnings
        self.assertIs(data, data_cached, "data not cached")

    def test_quarterly_earnings(self):
        data = self.ticker.quarterly_earnings
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.quarterly_earnings
        self.assertIs(data, data_cached, "data not cached")

    def test_earnings_forecasts(self):
        data = self.ticker.earnings_forecasts
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.earnings_forecasts
        self.assertIs(data, data_cached, "data not cached")

    def test_earnings_dates(self):
        data = self.ticker.earnings_dates
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.earnings_dates
        self.assertIs(data, data_cached, "data not cached")

    def test_earnings_trend(self):
        data = self.ticker.earnings_trend
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.earnings_trend
        self.assertIs(data, data_cached, "data not cached")

    def test_earnings_dates_with_limit(self):
        # use ticker with lots of historic earnings
        ticker = yf.Ticker("IBM")
        limit = 110
        data = ticker.get_earnings_dates(limit=limit)
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        self.assertEqual(len(data), limit, "Wrong number or rows")

        data_cached = ticker.get_earnings_dates(limit=limit)
        self.assertIs(data, data_cached, "data not cached")


class TestTickerHolders(unittest.TestCase):

    def setUp(self):
        self.ticker = yf.Ticker("GOOGL")

    def tearDown(self):
        self.ticker = None

    def test_major_holders(self):
        data = self.ticker.major_holders
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.major_holders
        self.assertIs(data, data_cached, "data not cached")

    def test_institutional_holders(self):
        data = self.ticker.institutional_holders
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.institutional_holders
        self.assertIs(data, data_cached, "data not cached")

    def test_mutualfund_holders(self):
        data = self.ticker.mutualfund_holders
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.mutualfund_holders
        self.assertIs(data, data_cached, "data not cached")


class TestTickerMiscFinancials(unittest.TestCase):

    def setUp(self):
        self.ticker = yf.Ticker("GOOGL")

    def tearDown(self):
        self.ticker = None

    def test_income_statement(self):
        expected_row = "TotalRevenue"
        data = self.ticker.income_stmt
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        self.assertIn(expected_row, data.index, "Did not find expected row in index")

        data_cached = self.ticker.income_stmt
        self.assertIs(data, data_cached, "data not cached")

    def test_quarterly_income_statement(self):
        expected_row = "TotalRevenue"
        data = self.ticker.quarterly_income_stmt
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        self.assertIn(expected_row, data.index, "Did not find expected row in index")

        data_cached = self.ticker.quarterly_income_stmt
        self.assertIs(data, data_cached, "data not cached")

    def test_balance_sheet(self):
        expected_row = "TotalAssets"
        data = self.ticker.balance_sheet
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        self.assertIn(expected_row, data.index, "Did not find expected row in index")

        data_cached = self.ticker.balance_sheet
        self.assertIs(data, data_cached, "data not cached")

    def test_quarterly_balance_sheet(self):
        expected_row = "TotalAssets"
        data = self.ticker.quarterly_balance_sheet
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        self.assertIn(expected_row, data.index, "Did not find expected row in index")

        data_cached = self.ticker.quarterly_balance_sheet
        self.assertIs(data, data_cached, "data not cached")

    def test_cashflow(self):
        expected_row = "OperatingCashFlow"
        data = self.ticker.cashflow
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        self.assertIn(expected_row, data.index, "Did not find expected row in index")

        data_cached = self.ticker.cashflow
        self.assertIs(data, data_cached, "data not cached")

    def test_quarterly_cashflow(self):
        expected_row = "OperatingCashFlow"
        data = self.ticker.quarterly_cashflow
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        self.assertIn(expected_row, data.index, "Did not find expected row in index")

        data_cached = self.ticker.quarterly_cashflow
        self.assertIs(data, data_cached, "data not cached")

    def test_sustainability(self):
        data = self.ticker.sustainability
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.sustainability
        self.assertIs(data, data_cached, "data not cached")

    def test_recommendations(self):
        data = self.ticker.recommendations
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.recommendations
        self.assertIs(data, data_cached, "data not cached")

    def test_recommendations_summary(self):
        data = self.ticker.recommendations_summary
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.recommendations_summary
        self.assertIs(data, data_cached, "data not cached")

    def test_analyst_price_target(self):
        data = self.ticker.analyst_price_target
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.analyst_price_target
        self.assertIs(data, data_cached, "data not cached")

    def test_revenue_forecasts(self):
        data = self.ticker.revenue_forecasts
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.revenue_forecasts
        self.assertIs(data, data_cached, "data not cached")

    def test_calendar(self):
        data = self.ticker.calendar
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.calendar
        self.assertIs(data, data_cached, "data not cached")

    def test_isin(self):
        data = self.ticker.isin
        self.assertIsInstance(data, str, "data has wrong type")
        self.assertEqual("ARDEUT116159", data, "data is empty")

        data_cached = self.ticker.isin
        self.assertIs(data, data_cached, "data not cached")

    def test_options(self):
        data = self.ticker.options
        self.assertIsInstance(data, tuple, "data has wrong type")
        self.assertTrue(len(data) > 1, "data is empty")

    def test_shares(self):
        data = self.ticker.shares
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

    def test_info(self):
        data = self.ticker.info
        self.assertIsInstance(data, dict, "data has wrong type")
        self.assertIn("symbol", data.keys(), "Did not find expected key in info dict")
        self.assertEqual("GOOGL", data["symbol"], "Wrong symbol value in info dict")

    def test_bad_freq_value_raises_exception(self):
        self.assertRaises(ValueError, lambda: self.ticker.get_cashflow(freq="badarg"))


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestTicker('Test ticker'))
    suite.addTest(TestTickerEarnings('Test earnings'))
    suite.addTest(TestTickerHolders('Test holders'))
    suite.addTest(TestTickerHistory('Test Ticker history'))
    suite.addTest(TestTickerMiscFinancials('Test misc financials'))
    return suite


if __name__ == '__main__':
    unittest.main()

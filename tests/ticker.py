"""
Tests for Ticker

To run all tests in suite from commandline:
   python -m unittest tests.ticker

Specific test class:
   python -m unittest tests.ticker.TestTicker

"""
import pandas as pd
import numpy as np

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

        cls.proxy = None

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
            tz = dat._get_ticker_tz(proxy=None, timeout=None, debug_mode=False, raise_errors=False)

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
        dat.get_shares_full()
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
        dat.get_shares_full()
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

    def test_goodTicker_withProxy(self):
        # that yfinance works when full api is called on same instance of ticker

        tkr = "IBM"
        dat = yf.Ticker(tkr, session=self.session)

        dat._fetch_ticker_tz(proxy=self.proxy, timeout=5, debug_mode=False, raise_errors=False)
        dat._get_ticker_tz(proxy=self.proxy, timeout=5, debug_mode=False, raise_errors=False)
        dat.history(period="1wk", proxy=self.proxy)

        v = dat.stats(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertTrue(len(v) > 0)

        v = dat.get_recommendations(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_calendar(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_major_holders(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_institutional_holders(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_mutualfund_holders(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_info(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertTrue(len(v) > 0)

        v = dat.get_sustainability(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_recommendations_summary(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_analyst_price_target(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_rev_forecast(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_earnings_forecast(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_trend_details(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_earnings_trend(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_earnings(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_income_stmt(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_incomestmt(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_financials(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_balance_sheet(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_balancesheet(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_cash_flow(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_cashflow(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_shares(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_shares_full(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        v = dat.get_isin(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertTrue(v != "")

        v = dat.get_news(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertTrue(len(v) > 0)

        v = dat.get_earnings_dates(proxy=self.proxy)
        self.assertIsNotNone(v)
        self.assertFalse(v.empty)

        # TODO: enable after merge
        # dat.get_history_metadata(proxy=self.proxy)
        # self.assertIsNotNone(v)
        # self.assertTrue(len(v) > 0)


class TestTickerHistory(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = requests_cache.CachedSession(backend='memory')

    @classmethod
    def tearDownClass(cls):
        if cls.session is not None:
            cls.session.close()

    def setUp(self):
        # use a ticker that has dividends
        self.ticker = yf.Ticker("IBM", session=self.session)

    def tearDown(self):
        self.ticker = None

    def test_history(self):
        with self.assertRaises(RuntimeError):
            self.ticker.history_metadata
        data = self.ticker.history("1y")
        self.assertIn("IBM", self.ticker.history_metadata.values(), "metadata missing")
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

    def test_no_expensive_calls_introduced(self):
        """
        Make sure calling history to get price data has not introduced more calls to yahoo than absolutely necessary.
        As doing other type of scraping calls than "query2.finance.yahoo.com/v8/finance/chart" to yahoo website
        will quickly trigger spam-block when doing bulk download of history data.
        """
        session = requests_cache.CachedSession(backend='memory')
        ticker = yf.Ticker("GOOGL", session=session)
        ticker.history("1y")
        actual_urls_called = tuple([r.url for r in session.cache.filter()])
        session.close()
        expected_urls = (
            'https://query2.finance.yahoo.com/v8/finance/chart/GOOGL?range=1y&interval=1d&includePrePost=False&events=div%2Csplits%2CcapitalGains',
        )
        self.assertEqual(expected_urls, actual_urls_called, "Different than expected url used to fetch history.")

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


class TestTickerEarnings(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = requests_cache.CachedSession(backend='memory')

    @classmethod
    def tearDownClass(cls):
        if cls.session is not None:
            cls.session.close()

    def setUp(self):
        self.ticker = yf.Ticker("GOOGL", session=self.session)

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
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = requests_cache.CachedSession(backend='memory')

    @classmethod
    def tearDownClass(cls):
        if cls.session is not None:
            cls.session.close()

    def setUp(self):
        self.ticker = yf.Ticker("GOOGL", session=self.session)

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
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = requests_cache.CachedSession(backend='memory')

    @classmethod
    def tearDownClass(cls):
        if cls.session is not None:
            cls.session.close()

    def setUp(self):
        self.ticker = yf.Ticker("GOOGL", session=self.session)
        
        # For ticker 'BSE.AX' (and others), Yahoo not returning 
        # full quarterly financials (usually cash-flow) with all entries, 
        # instead returns a smaller version in different data store.
        self.ticker_old_fmt = yf.Ticker("BSE.AX", session=self.session)

    def tearDown(self):
        self.ticker = None

    def test_income_statement(self):
        expected_keys = ["Total Revenue", "Basic EPS"]
        expected_periods_days = 365

        # Test contents of table
        data = self.ticker.get_income_stmt(pretty=True)
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.index, "Did not find expected row in index")
        period = abs((data.columns[0]-data.columns[1]).days)
        self.assertLess(abs(period-expected_periods_days), 20, "Not returning annual financials")

        # Test property defaults
        data2 = self.ticker.income_stmt
        self.assertTrue(data.equals(data2), "property not defaulting to 'pretty=True'")

        # Test pretty=False
        expected_keys = [k.replace(' ', '') for k in expected_keys]
        data = self.ticker.get_income_stmt(pretty=False)
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.index, "Did not find expected row in index")

        # Test to_dict
        data = self.ticker.get_income_stmt(as_dict=True)
        self.assertIsInstance(data, dict, "data has wrong type")


    def test_quarterly_income_statement(self):
        expected_keys = ["Total Revenue", "Basic EPS"]
        expected_periods_days = 365//4

        # Test contents of table
        data = self.ticker.get_income_stmt(pretty=True, freq="quarterly")
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.index, "Did not find expected row in index")
        period = abs((data.columns[0]-data.columns[1]).days)
        self.assertLess(abs(period-expected_periods_days), 20, "Not returning quarterly financials")

        # Test property defaults
        data2 = self.ticker.quarterly_income_stmt
        self.assertTrue(data.equals(data2), "property not defaulting to 'pretty=True'")

        # Test pretty=False
        expected_keys = [k.replace(' ', '') for k in expected_keys]
        data = self.ticker.get_income_stmt(pretty=False, freq="quarterly")
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.index, "Did not find expected row in index")

        # Test to_dict
        data = self.ticker.get_income_stmt(as_dict=True)
        self.assertIsInstance(data, dict, "data has wrong type")

    def test_quarterly_income_statement_old_fmt(self):
        expected_row = "TotalRevenue"
        data = self.ticker_old_fmt.get_income_stmt(freq="quarterly", legacy=True)
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        self.assertIn(expected_row, data.index, "Did not find expected row in index")

        data_cached = self.ticker_old_fmt.get_income_stmt(freq="quarterly", legacy=True)
        self.assertIs(data, data_cached, "data not cached")

    def test_balance_sheet(self):
        expected_keys = ["Total Assets", "Net PPE"]
        expected_periods_days = 365

        # Test contents of table
        data = self.ticker.get_balance_sheet(pretty=True)
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.index, "Did not find expected row in index")
        period = abs((data.columns[0]-data.columns[1]).days)
        self.assertLess(abs(period-expected_periods_days), 20, "Not returning annual financials")

        # Test property defaults
        data2 = self.ticker.balance_sheet
        self.assertTrue(data.equals(data2), "property not defaulting to 'pretty=True'")

        # Test pretty=False
        expected_keys = [k.replace(' ', '') for k in expected_keys]
        data = self.ticker.get_balance_sheet(pretty=False)
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.index, "Did not find expected row in index")

        # Test to_dict
        data = self.ticker.get_balance_sheet(as_dict=True)
        self.assertIsInstance(data, dict, "data has wrong type")

    def test_quarterly_balance_sheet(self):
        expected_keys = ["Total Assets", "Net PPE"]
        expected_periods_days = 365//4

        # Test contents of table
        data = self.ticker.get_balance_sheet(pretty=True, freq="quarterly")
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.index, "Did not find expected row in index")
        period = abs((data.columns[0]-data.columns[1]).days)
        self.assertLess(abs(period-expected_periods_days), 20, "Not returning quarterly financials")

        # Test property defaults
        data2 = self.ticker.quarterly_balance_sheet
        self.assertTrue(data.equals(data2), "property not defaulting to 'pretty=True'")

        # Test pretty=False
        expected_keys = [k.replace(' ', '') for k in expected_keys]
        data = self.ticker.get_balance_sheet(pretty=False, freq="quarterly")
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.index, "Did not find expected row in index")

        # Test to_dict
        data = self.ticker.get_balance_sheet(as_dict=True, freq="quarterly")
        self.assertIsInstance(data, dict, "data has wrong type")

    def test_quarterly_balance_sheet_old_fmt(self):
        expected_row = "TotalAssets"
        data = self.ticker_old_fmt.get_balance_sheet(freq="quarterly", legacy=True)
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        self.assertIn(expected_row, data.index, "Did not find expected row in index")

        data_cached = self.ticker_old_fmt.get_balance_sheet(freq="quarterly", legacy=True)
        self.assertIs(data, data_cached, "data not cached")

    def test_cash_flow(self):
        expected_keys = ["Operating Cash Flow", "Net PPE Purchase And Sale"]
        expected_periods_days = 365

        # Test contents of table
        data = self.ticker.get_cashflow(pretty=True)
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.index, "Did not find expected row in index")
        period = abs((data.columns[0]-data.columns[1]).days)
        self.assertLess(abs(period-expected_periods_days), 20, "Not returning annual financials")

        # Test property defaults
        data2 = self.ticker.cashflow
        self.assertTrue(data.equals(data2), "property not defaulting to 'pretty=True'")

        # Test pretty=False
        expected_keys = [k.replace(' ', '') for k in expected_keys]
        data = self.ticker.get_cashflow(pretty=False)
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.index, "Did not find expected row in index")

        # Test to_dict
        data = self.ticker.get_cashflow(as_dict=True)
        self.assertIsInstance(data, dict, "data has wrong type")

    def test_quarterly_cash_flow(self):
        expected_keys = ["Operating Cash Flow", "Net PPE Purchase And Sale"]
        expected_periods_days = 365//4

        # Test contents of table
        data = self.ticker.get_cashflow(pretty=True, freq="quarterly")
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.index, "Did not find expected row in index")
        period = abs((data.columns[0]-data.columns[1]).days)
        self.assertLess(abs(period-expected_periods_days), 20, "Not returning quarterly financials")

        # Test property defaults
        data2 = self.ticker.quarterly_cashflow
        self.assertTrue(data.equals(data2), "property not defaulting to 'pretty=True'")

        # Test pretty=False
        expected_keys = [k.replace(' ', '') for k in expected_keys]
        data = self.ticker.get_cashflow(pretty=False, freq="quarterly")
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.index, "Did not find expected row in index")

        # Test to_dict
        data = self.ticker.get_cashflow(as_dict=True)
        self.assertIsInstance(data, dict, "data has wrong type")

    def test_quarterly_cashflow_old_fmt(self):
        expected_row = "NetIncome"
        data = self.ticker_old_fmt.get_cashflow(legacy=True, freq="quarterly")
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        self.assertIn(expected_row, data.index, "Did not find expected row in index")

        data_cached = self.ticker_old_fmt.get_cashflow(legacy=True, freq="quarterly")
        self.assertIs(data, data_cached, "data not cached")

    def test_income_alt_names(self):
        i1 = self.ticker.income_stmt
        i2 = self.ticker.incomestmt
        self.assertTrue(i1.equals(i2))
        i3 = self.ticker.financials
        self.assertTrue(i1.equals(i3))

        i1 = self.ticker.get_income_stmt()
        i2 = self.ticker.get_incomestmt()
        self.assertTrue(i1.equals(i2))
        i3 = self.ticker.get_financials()
        self.assertTrue(i1.equals(i3))

        i1 = self.ticker.quarterly_income_stmt
        i2 = self.ticker.quarterly_incomestmt
        self.assertTrue(i1.equals(i2))
        i3 = self.ticker.quarterly_financials
        self.assertTrue(i1.equals(i3))

        i1 = self.ticker.get_income_stmt(freq="quarterly")
        i2 = self.ticker.get_incomestmt(freq="quarterly")
        self.assertTrue(i1.equals(i2))
        i3 = self.ticker.get_financials(freq="quarterly")
        self.assertTrue(i1.equals(i3))

    def test_balance_sheet_alt_names(self):
        i1 = self.ticker.balance_sheet
        i2 = self.ticker.balancesheet
        self.assertTrue(i1.equals(i2))

        i1 = self.ticker.get_balance_sheet()
        i2 = self.ticker.get_balancesheet()
        self.assertTrue(i1.equals(i2))

        i1 = self.ticker.quarterly_balance_sheet
        i2 = self.ticker.quarterly_balancesheet
        self.assertTrue(i1.equals(i2))

        i1 = self.ticker.get_balance_sheet(freq="quarterly")
        i2 = self.ticker.get_balancesheet(freq="quarterly")
        self.assertTrue(i1.equals(i2))

    def test_cash_flow_alt_names(self):
        i1 = self.ticker.cash_flow
        i2 = self.ticker.cashflow
        self.assertTrue(i1.equals(i2))

        i1 = self.ticker.get_cash_flow()
        i2 = self.ticker.get_cashflow()
        self.assertTrue(i1.equals(i2))

        i1 = self.ticker.quarterly_cash_flow
        i2 = self.ticker.quarterly_cashflow
        self.assertTrue(i1.equals(i2))

        i1 = self.ticker.get_cash_flow(freq="quarterly")
        i2 = self.ticker.get_cashflow(freq="quarterly")
        self.assertTrue(i1.equals(i2))

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

    def test_shares_full(self):
        data = self.ticker.get_shares_full()
        self.assertIsInstance(data, pd.Series, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

    def test_bad_freq_value_raises_exception(self):
        self.assertRaises(ValueError, lambda: self.ticker.get_cashflow(freq="badarg"))


class TestTickerInfo(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = requests_cache.CachedSession(backend='memory')

    @classmethod
    def tearDownClass(cls):
        if cls.session is not None:
            cls.session.close()

    def setUp(self):
        self.symbols = []
        self.symbols += ["ESLT.TA", "BP.L", "GOOGL"]
        self.symbols.append("QCSTIX")  # good for testing, doesn't trade
        self.symbols += ["BTC-USD", "IWO", "VFINX", "^GSPC"]
        self.symbols += ["SOKE.IS", "ADS.DE"]  # detected bugs
        self.tickers = [yf.Ticker(s, session=self.session) for s in self.symbols]

    def tearDown(self):
        self.ticker = None

    def test_info(self):
        data = self.tickers[0].info
        self.assertIsInstance(data, dict, "data has wrong type")
        self.assertIn("symbol", data.keys(), "Did not find expected key in info dict")
        self.assertEqual(self.symbols[0], data["symbol"], "Wrong symbol value in info dict")

    def test_fast_info(self):
        yf.scrapers.quote.PRUNE_INFO = False

        fast_info_keys = set()
        for ticker in self.tickers:
            fast_info_keys.update(set(ticker.fast_info.keys()))
        fast_info_keys = sorted(list(fast_info_keys))

        key_rename_map = {}
        key_rename_map["currency"] = "currency"
        key_rename_map["quote_type"] = "quoteType"
        key_rename_map["timezone"] = "exchangeTimezoneName"

        key_rename_map["last_price"] = ["currentPrice", "regularMarketPrice"]
        key_rename_map["open"] = ["open", "regularMarketOpen"]
        key_rename_map["day_high"] = ["dayHigh", "regularMarketDayHigh"]
        key_rename_map["day_low"] = ["dayLow", "regularMarketDayLow"]
        key_rename_map["previous_close"] = ["previousClose"]
        key_rename_map["regular_market_previous_close"] = ["regularMarketPreviousClose"]

        key_rename_map["fifty_day_average"] = "fiftyDayAverage"
        key_rename_map["two_hundred_day_average"] = "twoHundredDayAverage"
        key_rename_map["year_change"] = ["52WeekChange", "fiftyTwoWeekChange"]
        key_rename_map["year_high"] = "fiftyTwoWeekHigh"
        key_rename_map["year_low"] = "fiftyTwoWeekLow"

        key_rename_map["last_volume"] = ["volume", "regularMarketVolume"]
        key_rename_map["ten_day_average_volume"] = ["averageVolume10days", "averageDailyVolume10Day"]
        key_rename_map["three_month_average_volume"] = "averageVolume"

        key_rename_map["market_cap"] = "marketCap"
        key_rename_map["shares"] = "sharesOutstanding"

        for k in list(key_rename_map.keys()):
            if '_' in k:
                key_rename_map[yf.utils.snake_case_2_camelCase(k)] = key_rename_map[k]

        # Note: share count items in info[] are bad. Sometimes the float > outstanding!
        # So often fast_info["shares"] does not match. 
        # Why isn't fast_info["shares"] wrong? Because using it to calculate market cap always correct.
        bad_keys = {"shares"}

        # Loose tolerance for averages, no idea why don't match info[]. Is info wrong?
        custom_tolerances = {}
        custom_tolerances["year_change"] = 1.0
        # custom_tolerances["ten_day_average_volume"] = 1e-3
        custom_tolerances["ten_day_average_volume"] = 1e-1
        # custom_tolerances["three_month_average_volume"] = 1e-2
        custom_tolerances["three_month_average_volume"] = 5e-1
        custom_tolerances["fifty_day_average"] = 1e-2
        custom_tolerances["two_hundred_day_average"] = 1e-2
        for k in list(custom_tolerances.keys()):
            if '_' in k:
                custom_tolerances[yf.utils.snake_case_2_camelCase(k)] = custom_tolerances[k]

        for k in fast_info_keys:
            if k in key_rename_map:
                k2 = key_rename_map[k]
            else:
                k2 = k

            if not isinstance(k2, list):
                k2 = [k2]

            for m in k2:
                for ticker in self.tickers:
                    if not m in ticker.info:
                        # print(f"symbol={ticker.ticker}: fast_info key '{k}' mapped to info key '{m}' but not present in info")
                        continue

                    if k in bad_keys:
                        continue

                    if k in custom_tolerances:
                        rtol = custom_tolerances[k]
                    else:
                        rtol = 5e-3
                        # rtol = 1e-4

                    correct = ticker.info[m]
                    test = ticker.fast_info[k]
                    # print(f"Testing: symbol={ticker.ticker} m={m} k={k}: test={test} vs correct={correct}")
                    if k in ["market_cap","marketCap"] and ticker.fast_info["currency"] in ["GBp", "ILA"]:
                        # Adjust for currency to match Yahoo:
                        test *= 0.01
                    try:
                        if correct is None:
                            self.assertTrue(test is None or (not np.isnan(test)), f"{k}: {test} must be None or real value because correct={correct}")
                        elif isinstance(test, float) or isinstance(correct, int):
                            self.assertTrue(np.isclose(test, correct, rtol=rtol), f"{ticker.ticker} {k}: {test} != {correct}")
                        else:
                            self.assertEqual(test, correct, f"{k}: {test} != {correct}")
                    except:
                        if k in ["regularMarketPreviousClose"] and ticker.ticker in ["ADS.DE"]:
                            # Yahoo is wrong, is returning post-market close not regular
                            continue
                        else:
                            raise



def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestTicker('Test ticker'))
    suite.addTest(TestTickerEarnings('Test earnings'))
    suite.addTest(TestTickerHolders('Test holders'))
    suite.addTest(TestTickerHistory('Test Ticker history'))
    suite.addTest(TestTickerMiscFinancials('Test misc financials'))
    suite.addTest(TestTickerInfo('Test info & fast_info'))
    return suite


if __name__ == '__main__':
    unittest.main()

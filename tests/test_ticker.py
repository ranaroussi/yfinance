"""
Tests for Ticker

To run all tests in suite from commandline:
   python -m unittest tests.ticker

Specific test class:
   python -m unittest tests.ticker.TestTicker

"""
from datetime import datetime, timedelta

import pandas as pd

from tests.context import yfinance as yf
from tests.context import session_gbl
from yfinance.exceptions import YFPricesMissingError, YFInvalidPeriodError, YFNotImplementedError, YFTickerMissingError, YFTzMissingError, YFDataException


import unittest
# import requests_cache
from typing import Union, Any, get_args, _GenericAlias
# from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

ticker_attributes = (
    ("major_holders", pd.DataFrame),
    ("institutional_holders", pd.DataFrame),
    ("mutualfund_holders", pd.DataFrame),
    ("insider_transactions", pd.DataFrame),
    ("insider_purchases", pd.DataFrame),
    ("insider_roster_holders", pd.DataFrame),
    ("splits", pd.Series),
    ("actions", pd.DataFrame),
    ("shares", pd.DataFrame),
    ("info", dict),
    ("calendar", dict),
    ("recommendations", Union[pd.DataFrame, dict]),
    ("recommendations_summary", Union[pd.DataFrame, dict]),
    ("upgrades_downgrades", Union[pd.DataFrame, dict]),
    ("ttm_cashflow", pd.DataFrame),
    ("quarterly_cashflow", pd.DataFrame),
    ("cashflow", pd.DataFrame),
    ("quarterly_balance_sheet", pd.DataFrame),
    ("balance_sheet", pd.DataFrame),
    ("ttm_income_stmt", pd.DataFrame),
    ("quarterly_income_stmt", pd.DataFrame),
    ("income_stmt", pd.DataFrame),
    ("analyst_price_targets", dict),
    ("earnings_estimate", pd.DataFrame),
    ("revenue_estimate", pd.DataFrame),
    ("earnings_history", pd.DataFrame),
    ("eps_trend", pd.DataFrame),
    ("eps_revisions", pd.DataFrame),
    ("growth_estimates", pd.DataFrame),
    ("sustainability", pd.DataFrame),
    ("options", tuple),
    ("news", Any),
    ("earnings_dates", pd.DataFrame),
)

def assert_attribute_type(testClass: unittest.TestCase, instance, attribute_name, expected_type):
    try:
        attribute = getattr(instance, attribute_name)
        if attribute is not None and expected_type is not Any:
            err_msg = f'{attribute_name} type is {type(attribute)} not {expected_type}'
            if isinstance(expected_type, _GenericAlias) and expected_type.__origin__ is Union:
                allowed_types = get_args(expected_type)
                testClass.assertTrue(isinstance(attribute, allowed_types), err_msg)
            else:
                testClass.assertEqual(type(attribute), expected_type, err_msg)
    except Exception:
        testClass.assertRaises(
            YFNotImplementedError, lambda: getattr(instance, attribute_name)
        )

class TestTicker(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = session_gbl

    @classmethod
    def tearDownClass(cls):
        if cls.session is not None:
            cls.session.close()

    def test_getTz(self):
        tkrs = ["IMP.JO", "BHG.JO", "SSW.JO", "BP.L", "INTC"]
        for tkr in tkrs:
            # First step: remove ticker from tz-cache
            yf.cache.get_tz_cache().store(tkr, None)

            # Test:
            dat = yf.Ticker(tkr, session=self.session)
            tz = dat._get_ticker_tz(timeout=5)

            self.assertIsNotNone(tz)

    def test_badTicker(self):
        # Check yfinance doesn't die when ticker delisted

        tkr = "DJI"  # typo of "^DJI"
        dat = yf.Ticker(tkr, session=self.session)

        dat.history(period="5d")
        dat.history(start="2022-01-01")
        dat.history(start="2022-01-01", end="2022-03-01")
        yf.download([tkr], period="5d", threads=False, ignore_tz=False)
        yf.download([tkr], period="5d", threads=True, ignore_tz=False)
        yf.download([tkr], period="5d", threads=False, ignore_tz=True)
        yf.download([tkr], period="5d", threads=True, ignore_tz=True)

        for k in dat.fast_info:
            dat.fast_info[k]

        for attribute_name, attribute_type in ticker_attributes:
            assert_attribute_type(self, dat, attribute_name, attribute_type)

        assert isinstance(dat.dividends, pd.Series)
        assert dat.dividends.empty
        assert isinstance(dat.splits, pd.Series)
        assert dat.splits.empty
        assert isinstance(dat.capital_gains, pd.Series)
        assert dat.capital_gains.empty
        with self.assertRaises(YFNotImplementedError):
            assert isinstance(dat.shares, pd.DataFrame)
            assert dat.shares.empty
        assert isinstance(dat.actions, pd.DataFrame)
        assert dat.actions.empty

    def test_invalid_period(self):
        tkr = 'VALE'
        dat = yf.Ticker(tkr, session=self.session)
        with self.assertRaises(YFInvalidPeriodError):
            dat.history(period="2wks", interval="1d", raise_errors=True)
        with self.assertRaises(YFInvalidPeriodError):
            dat.history(period="2mos", interval="1d", raise_errors=True)

    def test_valid_custom_periods(self):
        valid_periods = [
            # Yahoo provided periods
            ("1d", "1m"), ("5d", "15m"), ("1mo", "1d"), ("3mo", "1wk"),
            ("6mo", "1d"), ("1y", "1mo"), ("5y", "1wk"), ("max", "1mo"),

            # Custom periods
            ("2d", "30m"), ("10mo", "1d"), ("1y", "1d"), ("3y", "1d"),
            ("2wk", "15m"), ("6mo", "5d"), ("10y", "1wk")
        ]

        tkr = "AAPL"
        dat = yf.Ticker(tkr, session=self.session)

        for period, interval in valid_periods:
            with self.subTest(period=period, interval=interval):
                df = dat.history(period=period, interval=interval, raise_errors=True)
                self.assertIsInstance(df, pd.DataFrame)
                self.assertFalse(df.empty, f"No data returned for period={period}, interval={interval}")
                self.assertIn("Close", df.columns, f"'Close' column missing for period={period}, interval={interval}")

                # Validate date range
                now = datetime.now()
                if period != "max":  # Difficult to assert for "max", therefore we skip
                    if period.endswith("d"):
                        days = int(period[:-1])
                        expected_start = now - timedelta(days=days)
                    elif period.endswith("mo"):
                        months = int(period[:-2])
                        expected_start = now - timedelta(days=30 * months)
                    elif period.endswith("y"):
                        years = int(period[:-1])
                        expected_start = now - timedelta(days=365 * years)
                    elif period.endswith("wk"):
                        weeks = int(period[:-2])
                        expected_start = now - timedelta(weeks=weeks)
                    else:
                        continue

                    actual_start = df.index[0].to_pydatetime().replace(tzinfo=None)
                    expected_start = expected_start.replace(hour=0, minute=0, second=0, microsecond=0)

                    # leeway added because of weekends
                    self.assertGreaterEqual(actual_start, expected_start - timedelta(days=10),
                                            f"Start date {actual_start} out of range for period={period}")
                    self.assertLessEqual(df.index[-1].to_pydatetime().replace(tzinfo=None), now,
                                         f"End date {df.index[-1]} out of range for period={period}")

    def test_prices_missing(self):
        # this test will need to be updated every time someone wants to run a test
        # hard to find a ticker that matches this error other than options
        # META call option, 2024 April 26th @ strike of 180000
        tkr = 'META240426C00180000'
        dat = yf.Ticker(tkr, session=self.session)
        with self.assertRaises(YFPricesMissingError):
            dat.history(period="5d", interval="1m", raise_errors=True)

    def test_ticker_missing(self):
        tkr = 'ATVI'
        dat = yf.Ticker(tkr, session=self.session)
        # A missing ticker can trigger either a niche error or the generalized error
        with self.assertRaises((YFTickerMissingError, YFTzMissingError, YFPricesMissingError)):
            dat.history(period="3mo", interval="1d", raise_errors=True)

    def test_goodTicker(self):
        # that yfinance works when full api is called on same instance of ticker

        tkrs = ["IBM"]
        tkrs.append("QCSTIX")  # weird ticker, no price history but has previous close
        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)

            dat.history(period="5d")
            dat.history(start="2022-01-01")
            dat.history(start="2022-01-01", end="2022-03-01")
            yf.download([tkr], period="5d", threads=False, ignore_tz=False)
            yf.download([tkr], period="5d", threads=True, ignore_tz=False)
            yf.download([tkr], period="5d", threads=False, ignore_tz=True)
            yf.download([tkr], period="5d", threads=True, ignore_tz=True)

            for k in dat.fast_info:
                dat.fast_info[k]

            for attribute_name, attribute_type in ticker_attributes:
                assert_attribute_type(self, dat, attribute_name, attribute_type)

    def test_goodTicker_withProxy(self):
        tkr = "IBM"
        dat = yf.Ticker(tkr, session=self.session)

        dat._fetch_ticker_tz(timeout=5)
        dat._get_ticker_tz(timeout=5)
        dat.history(period="5d")

        for attribute_name, attribute_type in ticker_attributes:
            assert_attribute_type(self, dat, attribute_name, attribute_type)


class TestTickerHistory(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = session_gbl

    @classmethod
    def tearDownClass(cls):
        if cls.session is not None:
            cls.session.close()

    def setUp(self):
        # use a ticker that has dividends
        self.symbol = "IBM"
        self.ticker = yf.Ticker(self.symbol, session=self.session)

        self.symbols = ["AMZN", "MSFT", "NVDA"]

    def tearDown(self):
        self.ticker = None

    def test_history(self):
        md = self.ticker.history_metadata
        self.assertIn("IBM", md.values(), "metadata missing")
        data = self.ticker.history("1y")
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

    def test_download(self):
        tomorrow = pd.Timestamp.now().date() + pd.Timedelta(days=1)  # helps with caching
        for t in [False, True]:
            for i in [False, True]:
                for m in [False, True]:
                    for n in [1, 'all']:
                        symbols = self.symbols[0] if n == 1 else self.symbols
                        data = yf.download(symbols, end=tomorrow, session=self.session, 
                                           threads=t, ignore_tz=i, multi_level_index=m)
                        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
                        self.assertFalse(data.empty, "data is empty")
                        if i:
                            self.assertIsNone(data.index.tz)
                        else:
                            self.assertIsNotNone(data.index.tz)
                        if (not m) and n == 1:
                            self.assertFalse(isinstance(data.columns, pd.MultiIndex))
                        else:
                            self.assertIsInstance(data.columns, pd.MultiIndex)

    # Hopefully one day we find an equivalent "requests_cache" that works with "curl_cffi"
    # def test_no_expensive_calls_introduced(self):
    #     """
    #     Make sure calling history to get price data has not introduced more calls to yahoo than absolutely necessary.
    #     As doing other type of scraping calls than "query2.finance.yahoo.com/v8/finance/chart" to yahoo website
    #     will quickly trigger spam-block when doing bulk download of history data.
    #     """
    #     symbol = "GOOGL"
    #     period = "1y"
    #     with requests_cache.CachedSession(backend="memory") as session:
    #         ticker = yf.Ticker(symbol, session=session)
    #         ticker.history(period=period)
    #         actual_urls_called = [r.url for r in session.cache.filter()]

    #     # Remove 'crumb' argument
    #     for i in range(len(actual_urls_called)):
    #         u = actual_urls_called[i]
    #         parsed_url = urlparse(u)
    #         query_params = parse_qs(parsed_url.query)
    #         query_params.pop('crumb', None)
    #         query_params.pop('cookie', None)
    #         u = urlunparse(parsed_url._replace(query=urlencode(query_params, doseq=True)))
    #         actual_urls_called[i] = u
    #     actual_urls_called = tuple(actual_urls_called)

    #     expected_urls = [
    #         f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1d",  # ticker's tz
    #         f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?events=div%2Csplits%2CcapitalGains&includePrePost=False&interval=1d&range={period}"
    #     ]
    #     for url in actual_urls_called:
    #         self.assertTrue(url in expected_urls, f"Unexpected URL called: {url}")

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

    def test_chained_history_calls(self):
        _ = self.ticker.history(period="2d")
        data = self.ticker.dividends
        self.assertIsInstance(data, pd.Series, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")


class TestTickerEarnings(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = session_gbl

    @classmethod
    def tearDownClass(cls):
        if cls.session is not None:
            cls.session.close()

    def setUp(self):
        self.ticker = yf.Ticker("GOOGL", session=self.session)

    def tearDown(self):
        self.ticker = None

    def test_earnings_dates(self):
        data = self.ticker.earnings_dates
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

    def test_earnings_dates_with_limit(self):
        # use ticker with lots of historic earnings
        ticker = yf.Ticker("IBM")
        limit = 100
        data = ticker.get_earnings_dates(limit=limit)
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        self.assertEqual(len(data), limit, "Wrong number or rows")

        data_cached = ticker.get_earnings_dates(limit=limit)
        self.assertIs(data, data_cached, "data not cached")

    # Below will fail because not ported to Yahoo API

    # def test_earnings_forecasts(self):
    #     data = self.ticker.earnings_forecasts
    #     self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
    #     self.assertFalse(data.empty, "data is empty")

    #     data_cached = self.ticker.earnings_forecasts
    #     self.assertIs(data, data_cached, "data not cached")

    #     data_cached = self.ticker.earnings_dates
    #     self.assertIs(data, data_cached, "data not cached")

    # def test_earnings_trend(self):
    #     data = self.ticker.earnings_trend
    #     self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
    #     self.assertFalse(data.empty, "data is empty")

    #     data_cached = self.ticker.earnings_trend
    #     self.assertIs(data, data_cached, "data not cached")


class TestTickerHolders(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = session_gbl

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

    def test_insider_transactions(self):
        data = self.ticker.insider_transactions
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.insider_transactions
        self.assertIs(data, data_cached, "data not cached")

    def test_insider_purchases(self):
        data = self.ticker.insider_purchases
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.insider_purchases
        self.assertIs(data, data_cached, "data not cached")

    def test_insider_roster_holders(self):
        data = self.ticker.insider_roster_holders
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.insider_roster_holders
        self.assertIs(data, data_cached, "data not cached")


class TestTickerMiscFinancials(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = session_gbl

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

    def test_shares_full(self):
        data = self.ticker.get_shares_full()
        self.assertIsInstance(data, pd.Series, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

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

    def test_ttm_income_statement(self):
        expected_keys = ["Total Revenue", "Pretax Income", "Normalized EBITDA"]

        # Test contents of table
        data = self.ticker.get_income_stmt(pretty=True, freq='trailing')
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.index, "Did not find expected row in index")
        # Trailing 12 months there must be exactly one column
        self.assertEqual(len(data.columns), 1, "Only one column should be returned on TTM income statement")

        # Test property defaults
        data2 = self.ticker.ttm_income_stmt
        self.assertTrue(data.equals(data2), "property not defaulting to 'pretty=True'")

        # Test pretty=False
        expected_keys = [k.replace(' ', '') for k in expected_keys]
        data = self.ticker.get_income_stmt(pretty=False, freq='trailing')
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.index, "Did not find expected row in index")

        # Test to_dict
        data = self.ticker.get_income_stmt(as_dict=True, freq='trailing')
        self.assertIsInstance(data, dict, "data has wrong type")

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

    def test_ttm_cash_flow(self):
        expected_keys = ["Operating Cash Flow", "Net PPE Purchase And Sale"]

        # Test contents of table
        data = self.ticker.get_cashflow(pretty=True, freq='trailing')
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.index, "Did not find expected row in index")
        # Trailing 12 months there must be exactly one column
        self.assertEqual(len(data.columns), 1, "Only one column should be returned on TTM cash flow")

        # Test property defaults
        data2 = self.ticker.ttm_cashflow
        self.assertTrue(data.equals(data2), "property not defaulting to 'pretty=True'")

        # Test pretty=False
        expected_keys = [k.replace(' ', '') for k in expected_keys]
        data = self.ticker.get_cashflow(pretty=False, freq='trailing')
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.index, "Did not find expected row in index")

        # Test to_dict
        data = self.ticker.get_cashflow(as_dict=True, freq='trailing')
        self.assertIsInstance(data, dict, "data has wrong type")

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

        i1 = self.ticker.ttm_income_stmt
        i2 = self.ticker.ttm_incomestmt
        self.assertTrue(i1.equals(i2))
        i3 = self.ticker.ttm_financials
        self.assertTrue(i1.equals(i3))

        i1 = self.ticker.get_income_stmt(freq="trailing")
        i2 = self.ticker.get_incomestmt(freq="trailing")
        self.assertTrue(i1.equals(i2))
        i3 = self.ticker.get_financials(freq="trailing")
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

        i1 = self.ticker.ttm_cash_flow
        i2 = self.ticker.ttm_cashflow
        self.assertTrue(i1.equals(i2))

        i1 = self.ticker.get_cash_flow(freq="trailing")
        i2 = self.ticker.get_cashflow(freq="trailing")
        self.assertTrue(i1.equals(i2))

    def test_bad_freq_value_raises_exception(self):
        self.assertRaises(ValueError, lambda: self.ticker.get_cashflow(freq="badarg"))

    def test_calendar(self):
        data = self.ticker.calendar
        self.assertIsInstance(data, dict, "data has wrong type")
        self.assertTrue(len(data) > 0, "data is empty")
        self.assertIn("Earnings Date", data.keys(), "data missing expected key")
        self.assertIn("Earnings Average", data.keys(), "data missing expected key")
        self.assertIn("Earnings Low", data.keys(), "data missing expected key")
        self.assertIn("Earnings High", data.keys(), "data missing expected key")
        self.assertIn("Revenue Average", data.keys(), "data missing expected key")
        self.assertIn("Revenue Low", data.keys(), "data missing expected key")
        self.assertIn("Revenue High", data.keys(), "data missing expected key")
        # dividend date is not available for tested ticker GOOGL
        if self.ticker.ticker != "GOOGL":
            self.assertIn("Dividend Date", data.keys(), "data missing expected key")
        # ex-dividend date is not always available
        data_cached = self.ticker.calendar
        self.assertIs(data, data_cached, "data not cached")


    def test_sustainability(self):
        data = self.ticker.sustainability
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.sustainability
        self.assertIs(data, data_cached, "data not cached")

    # def test_shares(self):
    #     data = self.ticker.shares
    #     self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
    #     self.assertFalse(data.empty, "data is empty")


class TestTickerAnalysts(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = session_gbl

    @classmethod
    def tearDownClass(cls):
        if cls.session is not None:
            cls.session.close()

    def setUp(self):
        self.ticker = yf.Ticker("GOOGL", session=self.session)
        self.ticker_no_analysts = yf.Ticker("^GSPC", session=self.session)

    def tearDown(self):
        self.ticker = None
        self.ticker_no_analysts = None

    def test_recommendations(self):
        data = self.ticker.recommendations
        data_summary = self.ticker.recommendations_summary
        self.assertTrue(data.equals(data_summary))
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.recommendations
        self.assertIs(data, data_cached, "data not cached")

    def test_recommendations_summary(self):  # currently alias for recommendations
        data = self.ticker.recommendations_summary
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.recommendations_summary
        self.assertIs(data, data_cached, "data not cached")

    def test_upgrades_downgrades(self):
        data = self.ticker.upgrades_downgrades
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")
        self.assertIsInstance(data.index, pd.DatetimeIndex, "data has wrong index type")

        data_cached = self.ticker.upgrades_downgrades
        self.assertIs(data, data_cached, "data not cached")

    def test_analyst_price_targets(self):
        data = self.ticker.analyst_price_targets
        self.assertIsInstance(data, dict, "data has wrong type")

        data_cached = self.ticker.analyst_price_targets
        self.assertIs(data, data_cached, "data not cached")

    def test_earnings_estimate(self):
        data = self.ticker.earnings_estimate
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.earnings_estimate
        self.assertIs(data, data_cached, "data not cached")

    def test_revenue_estimate(self):
        data = self.ticker.revenue_estimate
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.revenue_estimate
        self.assertIs(data, data_cached, "data not cached")

    def test_earnings_history(self):
        data = self.ticker.earnings_history
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        self.assertIsInstance(data.index, pd.DatetimeIndex, "data has wrong index type")

        data_cached = self.ticker.earnings_history
        self.assertIs(data, data_cached, "data not cached")

    def test_eps_trend(self):
        data = self.ticker.eps_trend
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.eps_trend
        self.assertIs(data, data_cached, "data not cached")

    def test_growth_estimates(self):
        data = self.ticker.growth_estimates
        self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        data_cached = self.ticker.growth_estimates
        self.assertIs(data, data_cached, "data not cached")

    def test_no_analysts(self):
        attributes = [
            'recommendations',
            'upgrades_downgrades',
            'earnings_estimate',
            'revenue_estimate',
            'earnings_history',
            'eps_trend',
            'growth_estimates',
        ]

        for attribute in attributes:
            try:
                data = getattr(self.ticker_no_analysts, attribute)
                self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
                self.assertTrue(data.empty, "data is not empty")
            except Exception as e:
                self.fail(f"Exception raised for attribute '{attribute}': {e}")



class TestTickerInfo(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = session_gbl

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
        self.symbols += ["EXTO", "NEPT" ] # Issues 2343 and 2363
        self.tickers = [yf.Ticker(s, session=self.session) for s in self.symbols]

    def tearDown(self):
        self.ticker = None

    def test_fast_info(self):
        f = yf.Ticker("AAPL", session=self.session).fast_info
        for k in f:
            self.assertIsNotNone(f[k])

    def test_info(self):
        data = self.tickers[0].info
        self.assertIsInstance(data, dict, "data has wrong type")
        expected_keys = ['industry', 'currentPrice', 'exchange', 'floatShares', 'companyOfficers', 'bid']
        for k in expected_keys:
            self.assertIn("symbol", data.keys(), f"Did not find expected key '{k}' in info dict")
        self.assertEqual(self.symbols[0], data["symbol"], "Wrong symbol value in info dict")

    def test_complementary_info(self):
        # This test is to check that we can successfully retrieve the trailing PEG ratio

        # We don't expect this one to have a trailing PEG ratio
        data1 = self.tickers[0].info
        self.assertIsNone(data1['trailingPegRatio'])

        # This one should have a trailing PEG ratio
        data2 = self.tickers[2].info
        self.assertIsInstance(data2['trailingPegRatio'], float)

    def test_isin_info(self):
        isin_list = {"ES0137650018": True,
                     "does_not_exist": True,  # Nonexistent but doesn't raise an error
                     "INF209K01EN2": True,
                     "INX846K01K35": False,    # Nonexistent and raises an error
                     "INF846K01K35": True
                     }
        for isin in isin_list:
            if not isin_list[isin]:
                with self.assertRaises(ValueError) as context:
                    ticker = yf.Ticker(isin)
                self.assertIn(str(context.exception), [ f"Invalid ISIN number: {isin}", "Empty tickername" ])
            else:
                ticker = yf.Ticker(isin)
            ticker.info
            
    def test_empty_info(self):
        # Test issue 2343 (Empty result _fetch)
        data = self.tickers[10].info
        self.assertCountEqual(['quoteType', 'symbol', 'underlyingSymbol', 'uuid', 'maxAge', 'trailingPegRatio'], data.keys())
        self.assertIn("trailingPegRatio", data.keys(), "Did not find expected key 'trailingPegRatio' in info dict")

        # Test issue 2363 (Empty QuoteResponse)
        data = self.tickers[11].info
        expected_keys = ['maxAge', 'priceHint', 'previousClose', 'open', 'dayLow', 'dayHigh', 'regularMarketPreviousClose',
                         'regularMarketOpen', 'regularMarketDayLow', 'regularMarketDayHigh', 'volume', 'regularMarketVolume',
                         'bid', 'ask', 'bidSize', 'askSize', 'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 'currency', 'tradeable',
                         'exchange', 'quoteType', 'symbol', 'underlyingSymbol', 'shortName', 'timeZoneFullName', 'timeZoneShortName',
                         'uuid', 'gmtOffSetMilliseconds', 'trailingPegRatio']
        self.assertCountEqual(expected_keys, data.keys())

    # def test_fast_info_matches_info(self):
    #     fast_info_keys = set()
    #     for ticker in self.tickers:
    #         fast_info_keys.update(set(ticker.fast_info.keys()))
    #     fast_info_keys = sorted(list(fast_info_keys))

    #     key_rename_map = {}
    #     key_rename_map["currency"] = "currency"
    #     key_rename_map["quote_type"] = "quoteType"
    #     key_rename_map["timezone"] = "exchangeTimezoneName"

    #     key_rename_map["last_price"] = ["currentPrice", "regularMarketPrice"]
    #     key_rename_map["open"] = ["open", "regularMarketOpen"]
    #     key_rename_map["day_high"] = ["dayHigh", "regularMarketDayHigh"]
    #     key_rename_map["day_low"] = ["dayLow", "regularMarketDayLow"]
    #     key_rename_map["previous_close"] = ["previousClose"]
    #     key_rename_map["regular_market_previous_close"] = ["regularMarketPreviousClose"]

    #     key_rename_map["fifty_day_average"] = "fiftyDayAverage"
    #     key_rename_map["two_hundred_day_average"] = "twoHundredDayAverage"
    #     key_rename_map["year_change"] = ["52WeekChange", "fiftyTwoWeekChange"]
    #     key_rename_map["year_high"] = "fiftyTwoWeekHigh"
    #     key_rename_map["year_low"] = "fiftyTwoWeekLow"

    #     key_rename_map["last_volume"] = ["volume", "regularMarketVolume"]
    #     key_rename_map["ten_day_average_volume"] = ["averageVolume10days", "averageDailyVolume10Day"]
    #     key_rename_map["three_month_average_volume"] = "averageVolume"

    #     key_rename_map["market_cap"] = "marketCap"
    #     key_rename_map["shares"] = "sharesOutstanding"

    #     for k in list(key_rename_map.keys()):
    #         if '_' in k:
    #             key_rename_map[yf.utils.snake_case_2_camelCase(k)] = key_rename_map[k]

    #     # Note: share count items in info[] are bad. Sometimes the float > outstanding!
    #     # So often fast_info["shares"] does not match.
    #     # Why isn't fast_info["shares"] wrong? Because using it to calculate market cap always correct.
    #     bad_keys = {"shares"}

    #     # Loose tolerance for averages, no idea why don't match info[]. Is info wrong?
    #     custom_tolerances = {}
    #     custom_tolerances["year_change"] = 1.0
    #     # custom_tolerances["ten_day_average_volume"] = 1e-3
    #     custom_tolerances["ten_day_average_volume"] = 1e-1
    #     # custom_tolerances["three_month_average_volume"] = 1e-2
    #     custom_tolerances["three_month_average_volume"] = 5e-1
    #     custom_tolerances["fifty_day_average"] = 1e-2
    #     custom_tolerances["two_hundred_day_average"] = 1e-2
    #     for k in list(custom_tolerances.keys()):
    #         if '_' in k:
    #             custom_tolerances[yf.utils.snake_case_2_camelCase(k)] = custom_tolerances[k]

    #     for k in fast_info_keys:
    #         if k in key_rename_map:
    #             k2 = key_rename_map[k]
    #         else:
    #             k2 = k

    #         if not isinstance(k2, list):
    #             k2 = [k2]

    #         for m in k2:
    #             for ticker in self.tickers:
    #                 if not m in ticker.info:
    #                     # print(f"symbol={ticker.ticker}: fast_info key '{k}' mapped to info key '{m}' but not present in info")
    #                     continue

    #                 if k in bad_keys:
    #                     continue

    #                 if k in custom_tolerances:
    #                     rtol = custom_tolerances[k]
    #                 else:
    #                     rtol = 5e-3
    #                     # rtol = 1e-4

    #                 correct = ticker.info[m]
    #                 test = ticker.fast_info[k]
    #                 # print(f"Testing: symbol={ticker.ticker} m={m} k={k}: test={test} vs correct={correct}")
    #                 if k in ["market_cap","marketCap"] and ticker.fast_info["currency"] in ["GBp", "ILA"]:
    #                     # Adjust for currency to match Yahoo:
    #                     test *= 0.01
    #                 try:
    #                     if correct is None:
    #                         self.assertTrue(test is None or (not np.isnan(test)), f"{k}: {test} must be None or real value because correct={correct}")
    #                     elif isinstance(test, float) or isinstance(correct, int):
    #                         self.assertTrue(np.isclose(test, correct, rtol=rtol), f"{ticker.ticker} {k}: {test} != {correct}")
    #                     else:
    #                         self.assertEqual(test, correct, f"{k}: {test} != {correct}")
    #                 except:
    #                     if k in ["regularMarketPreviousClose"] and ticker.ticker in ["ADS.DE"]:
    #                         # Yahoo is wrong, is returning post-market close not regular
    #                         continue
    #                     else:
    #                         raise

class TestTickerFundsData(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = session_gbl

    @classmethod
    def tearDownClass(cls):
        if cls.session is not None:
            cls.session.close()

    def setUp(self):
        self.test_tickers = [yf.Ticker("SPY", session=self.session),    # equity etf
                            yf.Ticker("JNK", session=self.session),     # bonds etf
                            yf.Ticker("VTSAX", session=self.session)]   # mutual fund

    def tearDown(self):
        self.ticker = None

    def test_fetch_and_parse(self):
        try:
            for ticker in self.test_tickers:
                ticker.funds_data._fetch_and_parse()

        except Exception as e:
            self.fail(f"_fetch_and_parse raised an exception unexpectedly: {e}")

        with self.assertRaises(YFDataException):
            ticker = yf.Ticker("AAPL", session=self.session) # stock, not funds
            ticker.funds_data._fetch_and_parse()
            self.fail("_fetch_and_parse should have failed when calling for non-funds data")

    def test_description(self):
        for ticker in self.test_tickers:
            description = ticker.funds_data.description
            self.assertIsInstance(description, str)
            self.assertTrue(len(description) > 0)

    def test_fund_overview(self):
        for ticker in self.test_tickers:
            fund_overview = ticker.funds_data.fund_overview
            self.assertIsInstance(fund_overview, dict)

    def test_fund_operations(self):
        for ticker in self.test_tickers:
            fund_operations = ticker.funds_data.fund_operations
            self.assertIsInstance(fund_operations, pd.DataFrame)

    def test_asset_classes(self):
        for ticker in self.test_tickers:
            asset_classes = ticker.funds_data.asset_classes
            self.assertIsInstance(asset_classes, dict)

    def test_top_holdings(self):
        for ticker in self.test_tickers:
            top_holdings = ticker.funds_data.top_holdings
            self.assertIsInstance(top_holdings, pd.DataFrame)

    def test_equity_holdings(self):
        for ticker in self.test_tickers:
            equity_holdings = ticker.funds_data.equity_holdings
            self.assertIsInstance(equity_holdings, pd.DataFrame)

    def test_bond_holdings(self):
        for ticker in self.test_tickers:
            bond_holdings = ticker.funds_data.bond_holdings
            self.assertIsInstance(bond_holdings, pd.DataFrame)

    def test_bond_ratings(self):
        for ticker in self.test_tickers:
            bond_ratings = ticker.funds_data.bond_ratings
            self.assertIsInstance(bond_ratings, dict)

    def test_sector_weightings(self):
        for ticker in self.test_tickers:
            sector_weightings = ticker.funds_data.sector_weightings
            self.assertIsInstance(sector_weightings, dict)

def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestTicker('Test ticker'))
    suite.addTest(TestTickerEarnings('Test earnings'))
    suite.addTest(TestTickerHolders('Test holders'))
    suite.addTest(TestTickerHistory('Test Ticker history'))
    suite.addTest(TestTickerMiscFinancials('Test misc financials'))
    suite.addTest(TestTickerInfo('Test info & fast_info'))
    suite.addTest(TestTickerFundsData('Test Funds Data'))
    return suite


if __name__ == '__main__':
    unittest.main()

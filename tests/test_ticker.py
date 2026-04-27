"""
Tests for Ticker

To run all tests in suite from commandline:
   python -m unittest tests.ticker

Specific test class:
   python -m unittest tests.ticker.TestTicker

"""

import unittest
from datetime import datetime, timedelta
from typing import Any, Union, _GenericAlias, get_args

# import requests_cache
from unittest.mock import MagicMock, patch

import polars as pl

from tests.context import session_gbl
from tests.context import yfinance as yf
from yfinance.config import YfConfig
from yfinance.exceptions import (
    YFDataException,
    YFInvalidPeriodError,
    YFNotImplementedError,
    YFPricesMissingError,
    YFTickerMissingError,
    YFTzMissingError,
)

# from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

ticker_attributes = (
    ("major_holders", pl.DataFrame),
    ("institutional_holders", pl.DataFrame),
    ("mutualfund_holders", pl.DataFrame),
    ("insider_transactions", pl.DataFrame),
    ("insider_purchases", pl.DataFrame),
    ("insider_roster_holders", pl.DataFrame),
    ("splits", pl.DataFrame),
    ("actions", pl.DataFrame),
    ("shares", pl.DataFrame),
    ("info", dict),
    ("calendar", dict),
    ("recommendations", Union[pl.DataFrame, dict]),
    ("recommendations_summary", Union[pl.DataFrame, dict]),
    ("upgrades_downgrades", Union[pl.DataFrame, dict]),
    ("ttm_cashflow", pl.DataFrame),
    ("quarterly_cashflow", pl.DataFrame),
    ("cashflow", pl.DataFrame),
    ("quarterly_balance_sheet", pl.DataFrame),
    ("balance_sheet", pl.DataFrame),
    ("ttm_income_stmt", pl.DataFrame),
    ("quarterly_income_stmt", pl.DataFrame),
    ("income_stmt", pl.DataFrame),
    ("analyst_price_targets", dict),
    ("earnings_estimate", pl.DataFrame),
    ("revenue_estimate", pl.DataFrame),
    ("earnings_history", pl.DataFrame),
    ("eps_trend", pl.DataFrame),
    ("eps_revisions", pl.DataFrame),
    ("growth_estimates", pl.DataFrame),
    ("sustainability", pl.DataFrame),
    ("options", tuple),
    ("news", Any),
    ("earnings_dates", pl.DataFrame),
)


def assert_attribute_type(
    testClass: unittest.TestCase, instance, attribute_name, expected_type
):
    try:
        attribute = getattr(instance, attribute_name)
        if attribute is not None and expected_type is not Any:
            err_msg = f"{attribute_name} type is {type(attribute)} not {expected_type}"
            if (
                isinstance(expected_type, _GenericAlias)
                and expected_type.__origin__ is Union
            ):
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

        assert isinstance(dat.dividends, pl.DataFrame)
        assert dat.dividends.is_empty()
        assert isinstance(dat.splits, pl.DataFrame)
        assert dat.splits.is_empty()
        assert isinstance(dat.capital_gains, pl.DataFrame)
        assert dat.capital_gains.is_empty()
        with self.assertRaises(YFNotImplementedError):
            assert isinstance(dat.shares, pl.DataFrame)
            assert dat.shares.is_empty()
        assert isinstance(dat.actions, pl.DataFrame)
        assert dat.actions.is_empty()

    def test_invalid_period(self):
        tkr = "VALE"
        dat = yf.Ticker(tkr, session=self.session)
        YfConfig.debug.hide_exceptions = False
        with self.assertRaises(YFInvalidPeriodError):
            dat.history(period="2wks", interval="1d")
        with self.assertRaises(YFInvalidPeriodError):
            dat.history(period="2mos", interval="1d")

    def test_valid_custom_periods(self):
        valid_periods = [
            # Yahoo provided periods
            ("1d", "1m"),
            ("5d", "15m"),
            ("1mo", "1d"),
            ("3mo", "1wk"),
            ("6mo", "1d"),
            ("1y", "1mo"),
            ("5y", "1wk"),
            ("max", "1mo"),
            # Custom periods
            ("2d", "30m"),
            ("10mo", "1d"),
            ("1y", "1d"),
            ("3y", "1d"),
            ("2wk", "15m"),
            ("6mo", "5d"),
            ("10y", "1wk"),
        ]

        tkr = "AAPL"
        dat = yf.Ticker(tkr, session=self.session)

        YfConfig.debug.hide_exceptions = False

        # Determine which date column to expect (intraday vs daily)
        _intraday_intervals = {"1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"}

        for period, interval in valid_periods:
            with self.subTest(period=period, interval=interval):
                df = dat.history(period=period, interval=interval)
                self.assertIsInstance(df, pl.DataFrame)
                self.assertFalse(
                    df.is_empty(),
                    f"No data returned for period={period}, interval={interval}",
                )
                self.assertIn(
                    "Close",
                    df.columns,
                    f"'Close' column missing for period={period}, interval={interval}",
                )

                date_col = "Datetime" if interval in _intraday_intervals else "Date"
                self.assertIn(
                    date_col,
                    df.columns,
                    f"'{date_col}' column missing for period={period}, interval={interval}",
                )

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

                    first_val = df[date_col][0]
                    last_val = df[date_col][-1]
                    # Convert polars datetime/date to Python datetime for comparison
                    if hasattr(first_val, "replace"):
                        actual_start = (
                            first_val.replace(tzinfo=None)
                            if hasattr(first_val, "tzinfo")
                            else datetime(
                                first_val.year, first_val.month, first_val.day
                            )
                        )
                    else:
                        actual_start = datetime(
                            first_val.year, first_val.month, first_val.day
                        )
                    if hasattr(last_val, "replace"):
                        actual_end = (
                            last_val.replace(tzinfo=None)
                            if hasattr(last_val, "tzinfo")
                            else datetime(last_val.year, last_val.month, last_val.day)
                        )
                    else:
                        actual_end = datetime(
                            last_val.year, last_val.month, last_val.day
                        )

                    expected_start = expected_start.replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )

                    # leeway added because of weekends
                    self.assertGreaterEqual(
                        actual_start,
                        expected_start - timedelta(days=10),
                        f"Start date {actual_start} out of range for period={period}",
                    )
                    self.assertLessEqual(
                        actual_end,
                        now,
                        f"End date {actual_end} out of range for period={period}",
                    )

    # # 2025-12-11: test failing and no time to find new tkr
    # def test_prices_missing(self):
    #     # this test will need to be updated every time someone wants to run a test
    #     # hard to find a ticker that matches this error other than options
    #     # META call option, 2024 April 26th @ strike of 180000

    #     tkr = 'META240426C00180000'
    #     dat = yf.Ticker(tkr, session=self.session)
    #     YfConfig.debug.hide_exceptions = False
    #     with self.assertRaises(YFPricesMissingError):
    #         dat.history(period="5d", interval="1m")

    def test_ticker_missing(self):
        tkr = "ATVI"
        dat = yf.Ticker(tkr, session=self.session)
        # A missing ticker can trigger either a niche error or the generalized error
        with self.assertRaises(
            (YFTickerMissingError, YFTzMissingError, YFPricesMissingError)
        ):
            YfConfig.debug.hide_exceptions = False
            dat.history(period="3mo", interval="1d")

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

    def test_ticker_with_symbol_mic(self):
        equities = [
            ("OR", "XPAR"),  # L'Oréal on Euronext Paris
            ("AAPL", "XNYS"),  # Apple on NYSE
            ("GOOGL", "XNAS"),  # Alphabet on NASDAQ
            ("BMW", "XETR"),  # BMW on XETRA
        ]
        for eq in equities:
            # No exception = pass
            yf.Ticker(eq)
            yf.Ticker((eq[0], eq[1].lower()))

    def test_ticker_with_symbol_mic_invalid(self):
        with self.assertRaises(ValueError) as cm:
            yf.Ticker(("ABC", "XXXX"))
        self.assertIn("Unknown MIC code: 'XXXX'", str(cm.exception))


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
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")

    def test_download(self):
        from datetime import date, timedelta

        tomorrow = date.today() + timedelta(days=1)  # helps with caching
        for t in [False, True]:
            for i in [False, True]:
                for n in [1, "all"]:
                    symbols = self.symbols[0] if n == 1 else self.symbols
                    data = yf.download(
                        symbols,
                        end=tomorrow,
                        session=self.session,
                        threads=t,
                        ignore_tz=i,
                    )
                    self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
                    self.assertFalse(data.is_empty(), "data is empty")
                    # In polars output, date/datetime column holds timezone info
                    date_col = "Datetime" if "Datetime" in data.columns else "Date"
                    self.assertIn(date_col, data.columns)
                    if i:
                        # ignore_tz=True: no timezone on datetime column
                        self.assertIsNone(data[date_col].dtype.time_zone)
                    else:
                        self.assertIsNotNone(data[date_col].dtype.time_zone)
                    # Multi-ticker download has a "Ticker" column
                    if n != 1:
                        self.assertIn("Ticker", data.columns)

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
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        self.assertIn("Dividends", data.columns)

    def test_splits(self):
        data = self.ticker.splits
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        # self.assertFalse(data.is_empty(), "data is empty")

    def test_actions(self):
        data = self.ticker.actions
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")

    def test_chained_history_calls(self):
        _ = self.ticker.history(period="2d")
        data = self.ticker.dividends
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        self.assertIn("Dividends", data.columns)


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
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")

    def test_earnings_dates_with_limit(self):
        # use ticker with lots of historic earnings
        ticker = yf.Ticker("IBM")
        limit = 100
        data = ticker.get_earnings_dates(limit=limit)
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        self.assertEqual(data.height, limit, "Wrong number or rows")

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
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")

        data_cached = self.ticker.major_holders
        self.assertIs(data, data_cached, "data not cached")

    def test_institutional_holders(self):
        data = self.ticker.institutional_holders
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")

        data_cached = self.ticker.institutional_holders
        self.assertIs(data, data_cached, "data not cached")

    def test_mutualfund_holders(self):
        data = self.ticker.mutualfund_holders
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")

        data_cached = self.ticker.mutualfund_holders
        self.assertIs(data, data_cached, "data not cached")

    def test_insider_transactions(self):
        data = self.ticker.insider_transactions
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")

        data_cached = self.ticker.insider_transactions
        self.assertIs(data, data_cached, "data not cached")

    def test_insider_purchases(self):
        data = self.ticker.insider_purchases
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")

        data_cached = self.ticker.insider_purchases
        self.assertIs(data, data_cached, "data not cached")

    def test_insider_roster_holders(self):
        data = self.ticker.insider_roster_holders
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")

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
        self.assertEqual("CA02080M1005", data, "data is empty")

        data_cached = self.ticker.isin
        self.assertIs(data, data_cached, "data not cached")

    def test_options(self):
        data = self.ticker.options
        self.assertIsInstance(data, tuple, "data has wrong type")
        self.assertTrue(len(data) > 1, "data is empty")

    def test_shares_full(self):
        data = self.ticker.get_shares_full()
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")

    def test_income_statement(self):
        expected_keys = ["Total Revenue", "Basic EPS"]
        expected_periods_days = 365

        # Test contents of table
        data = self.ticker.get_income_stmt(pretty=True)
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.columns, "Did not find expected column")

        # Test property defaults
        data2 = self.ticker.income_stmt
        self.assertTrue(data.equals(data2), "property not defaulting to 'pretty=True'")

        # Test pretty=False
        expected_keys_raw = [k.replace(" ", "") for k in expected_keys]
        data = self.ticker.get_income_stmt(pretty=False)
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        for k in expected_keys_raw:
            self.assertIn(k, data.columns, "Did not find expected column")

        # Test to_dict
        data = self.ticker.get_income_stmt(as_dict=True)
        self.assertIsInstance(data, dict, "data has wrong type")

    def test_quarterly_income_statement(self):
        expected_keys = ["Total Revenue", "Basic EPS"]

        # Test contents of table
        data = self.ticker.get_income_stmt(pretty=True, freq="quarterly")
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.columns, "Did not find expected column")

        # Test property defaults
        data2 = self.ticker.quarterly_income_stmt
        self.assertTrue(data.equals(data2), "property not defaulting to 'pretty=True'")

        # Test pretty=False
        expected_keys_raw = [k.replace(" ", "") for k in expected_keys]
        data = self.ticker.get_income_stmt(pretty=False, freq="quarterly")
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        for k in expected_keys_raw:
            self.assertIn(k, data.columns, "Did not find expected column")

        # Test to_dict
        data = self.ticker.get_income_stmt(as_dict=True)
        self.assertIsInstance(data, dict, "data has wrong type")

    def test_ttm_income_statement(self):
        expected_keys = ["Total Revenue", "Pretax Income", "Normalized EBITDA"]

        # Test contents of table
        data = self.ticker.get_income_stmt(pretty=True, freq="trailing")
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.columns, "Did not find expected column")

        # Test property defaults
        data2 = self.ticker.ttm_income_stmt
        self.assertTrue(data.equals(data2), "property not defaulting to 'pretty=True'")

        # Test pretty=False
        expected_keys_raw = [k.replace(" ", "") for k in expected_keys]
        data = self.ticker.get_income_stmt(pretty=False, freq="trailing")
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        for k in expected_keys_raw:
            self.assertIn(k, data.columns, "Did not find expected column")

        # Test to_dict
        data = self.ticker.get_income_stmt(as_dict=True, freq="trailing")
        self.assertIsInstance(data, dict, "data has wrong type")

    def test_balance_sheet(self):
        expected_keys = ["Total Assets", "Net PPE"]

        # Test contents of table
        data = self.ticker.get_balance_sheet(pretty=True)
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.columns, "Did not find expected column")

        # Test property defaults
        data2 = self.ticker.balance_sheet
        self.assertTrue(data.equals(data2), "property not defaulting to 'pretty=True'")

        # Test pretty=False
        expected_keys_raw = [k.replace(" ", "") for k in expected_keys]
        data = self.ticker.get_balance_sheet(pretty=False)
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        for k in expected_keys_raw:
            self.assertIn(k, data.columns, "Did not find expected column")

        # Test to_dict
        data = self.ticker.get_balance_sheet(as_dict=True)
        self.assertIsInstance(data, dict, "data has wrong type")

    def test_quarterly_balance_sheet(self):
        expected_keys = ["Total Assets", "Net PPE"]

        # Test contents of table
        data = self.ticker.get_balance_sheet(pretty=True, freq="quarterly")
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.columns, "Did not find expected column")

        # Test property defaults
        data2 = self.ticker.quarterly_balance_sheet
        self.assertTrue(data.equals(data2), "property not defaulting to 'pretty=True'")

        # Test pretty=False
        expected_keys_raw = [k.replace(" ", "") for k in expected_keys]
        data = self.ticker.get_balance_sheet(pretty=False, freq="quarterly")
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        for k in expected_keys_raw:
            self.assertIn(k, data.columns, "Did not find expected column")

        # Test to_dict
        data = self.ticker.get_balance_sheet(as_dict=True, freq="quarterly")
        self.assertIsInstance(data, dict, "data has wrong type")

    def test_cash_flow(self):
        expected_keys = ["Operating Cash Flow", "Net PPE Purchase And Sale"]

        # Test contents of table
        data = self.ticker.get_cashflow(pretty=True)
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.columns, "Did not find expected column")

        # Test property defaults
        data2 = self.ticker.cashflow
        self.assertTrue(data.equals(data2), "property not defaulting to 'pretty=True'")

        # Test pretty=False
        expected_keys_raw = [k.replace(" ", "") for k in expected_keys]
        data = self.ticker.get_cashflow(pretty=False)
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        for k in expected_keys_raw:
            self.assertIn(k, data.columns, "Did not find expected column")

        # Test to_dict
        data = self.ticker.get_cashflow(as_dict=True)
        self.assertIsInstance(data, dict, "data has wrong type")

    def test_quarterly_cash_flow(self):
        expected_keys = ["Operating Cash Flow", "Net PPE Purchase And Sale"]

        # Test contents of table
        data = self.ticker.get_cashflow(pretty=True, freq="quarterly")
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.columns, "Did not find expected column")

        # Test property defaults
        data2 = self.ticker.quarterly_cashflow
        self.assertTrue(data.equals(data2), "property not defaulting to 'pretty=True'")

        # Test pretty=False
        expected_keys_raw = [k.replace(" ", "") for k in expected_keys]
        data = self.ticker.get_cashflow(pretty=False, freq="quarterly")
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        for k in expected_keys_raw:
            self.assertIn(k, data.columns, "Did not find expected column")

        # Test to_dict
        data = self.ticker.get_cashflow(as_dict=True)
        self.assertIsInstance(data, dict, "data has wrong type")

    def test_ttm_cash_flow(self):
        expected_keys = ["Operating Cash Flow", "Net PPE Purchase And Sale"]

        # Test contents of table
        data = self.ticker.get_cashflow(pretty=True, freq="trailing")
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        for k in expected_keys:
            self.assertIn(k, data.columns, "Did not find expected column")

        # Test property defaults
        data2 = self.ticker.ttm_cashflow
        self.assertTrue(data.equals(data2), "property not defaulting to 'pretty=True'")

        # Test pretty=False
        expected_keys_raw = [k.replace(" ", "") for k in expected_keys]
        data = self.ticker.get_cashflow(pretty=False, freq="trailing")
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        for k in expected_keys_raw:
            self.assertIn(k, data.columns, "Did not find expected column")

        # Test to_dict
        data = self.ticker.get_cashflow(as_dict=True, freq="trailing")
        self.assertIsInstance(data, dict, "data has wrong type")

    def test_income_alt_names(self):
        i1 = self.ticker.income_stmt
        i2 = self.ticker.incomestmt
        self.assertTrue(i1.frame_equal(i2))
        i3 = self.ticker.financials
        self.assertTrue(i1.frame_equal(i3))

        i1 = self.ticker.get_income_stmt()
        i2 = self.ticker.get_incomestmt()
        self.assertTrue(i1.frame_equal(i2))
        i3 = self.ticker.get_financials()
        self.assertTrue(i1.frame_equal(i3))

        i1 = self.ticker.quarterly_income_stmt
        i2 = self.ticker.quarterly_incomestmt
        self.assertTrue(i1.frame_equal(i2))
        i3 = self.ticker.quarterly_financials
        self.assertTrue(i1.frame_equal(i3))

        i1 = self.ticker.get_income_stmt(freq="quarterly")
        i2 = self.ticker.get_incomestmt(freq="quarterly")
        self.assertTrue(i1.frame_equal(i2))
        i3 = self.ticker.get_financials(freq="quarterly")
        self.assertTrue(i1.frame_equal(i3))

        i1 = self.ticker.ttm_income_stmt
        i2 = self.ticker.ttm_incomestmt
        self.assertTrue(i1.frame_equal(i2))
        i3 = self.ticker.ttm_financials
        self.assertTrue(i1.frame_equal(i3))

        i1 = self.ticker.get_income_stmt(freq="trailing")
        i2 = self.ticker.get_incomestmt(freq="trailing")
        self.assertTrue(i1.frame_equal(i2))
        i3 = self.ticker.get_financials(freq="trailing")
        self.assertTrue(i1.frame_equal(i3))

    def test_balance_sheet_alt_names(self):
        i1 = self.ticker.balance_sheet
        i2 = self.ticker.balancesheet
        self.assertTrue(i1.frame_equal(i2))

        i1 = self.ticker.get_balance_sheet()
        i2 = self.ticker.get_balancesheet()
        self.assertTrue(i1.frame_equal(i2))

        i1 = self.ticker.quarterly_balance_sheet
        i2 = self.ticker.quarterly_balancesheet
        self.assertTrue(i1.frame_equal(i2))

        i1 = self.ticker.get_balance_sheet(freq="quarterly")
        i2 = self.ticker.get_balancesheet(freq="quarterly")
        self.assertTrue(i1.frame_equal(i2))

    def test_cash_flow_alt_names(self):
        i1 = self.ticker.cash_flow
        i2 = self.ticker.cashflow
        self.assertTrue(i1.frame_equal(i2))

        i1 = self.ticker.get_cash_flow()
        i2 = self.ticker.get_cashflow()
        self.assertTrue(i1.frame_equal(i2))

        i1 = self.ticker.quarterly_cash_flow
        i2 = self.ticker.quarterly_cashflow
        self.assertTrue(i1.frame_equal(i2))

        i1 = self.ticker.get_cash_flow(freq="quarterly")
        i2 = self.ticker.get_cashflow(freq="quarterly")
        self.assertTrue(i1.frame_equal(i2))

        i1 = self.ticker.ttm_cash_flow
        i2 = self.ticker.ttm_cashflow
        self.assertTrue(i1.frame_equal(i2))

        i1 = self.ticker.get_cash_flow(freq="trailing")
        i2 = self.ticker.get_cashflow(freq="trailing")
        self.assertTrue(i1.frame_equal(i2))

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

    # # sustainability stopped working
    # def test_sustainability(self):
    #     data = self.ticker.sustainability
    #     self.assertIsInstance(data, pd.DataFrame, "data has wrong type")
    #     self.assertFalse(data.empty, "data is empty")

    #     data_cached = self.ticker.sustainability
    #     self.assertIs(data, data_cached, "data not cached")

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
        self.assertTrue(data.frame_equal(data_summary))
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")

        data_cached = self.ticker.recommendations
        self.assertIs(data, data_cached, "data not cached")

    def test_recommendations_summary(self):  # currently alias for recommendations
        data = self.ticker.recommendations_summary
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")

        data_cached = self.ticker.recommendations_summary
        self.assertIs(data, data_cached, "data not cached")

    def test_upgrades_downgrades(self):
        data = self.ticker.upgrades_downgrades
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        # In polars output, date is a column rather than an index
        date_col = (
            "GradeDate"
            if "GradeDate" in data.columns
            else ("Date" if "Date" in data.columns else data.columns[0])
        )
        self.assertIn(date_col, data.columns, "data missing date column")

        data_cached = self.ticker.upgrades_downgrades
        self.assertIs(data, data_cached, "data not cached")

    def test_analyst_price_targets(self):
        data = self.ticker.analyst_price_targets
        self.assertIsInstance(data, dict, "data has wrong type")

        data_cached = self.ticker.analyst_price_targets
        self.assertIs(data, data_cached, "data not cached")

    def test_earnings_estimate(self):
        data = self.ticker.earnings_estimate
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")

        data_cached = self.ticker.earnings_estimate
        self.assertIs(data, data_cached, "data not cached")

    def test_revenue_estimate(self):
        data = self.ticker.revenue_estimate
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")

        data_cached = self.ticker.revenue_estimate
        self.assertIs(data, data_cached, "data not cached")

    def test_earnings_history(self):
        data = self.ticker.earnings_history
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")
        # In polars output, date is a column
        date_col = "Date" if "Date" in data.columns else data.columns[0]
        self.assertIn(date_col, data.columns, "data missing date column")

        data_cached = self.ticker.earnings_history
        self.assertIs(data, data_cached, "data not cached")

    def test_eps_trend(self):
        data = self.ticker.eps_trend
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")

        data_cached = self.ticker.eps_trend
        self.assertIs(data, data_cached, "data not cached")

    def test_growth_estimates(self):
        data = self.ticker.growth_estimates
        self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
        self.assertFalse(data.is_empty(), "data is empty")

        data_cached = self.ticker.growth_estimates
        self.assertIs(data, data_cached, "data not cached")

    def test_no_analysts(self):
        attributes = [
            "recommendations",
            "upgrades_downgrades",
            "earnings_estimate",
            "revenue_estimate",
            "earnings_history",
            "eps_trend",
            "growth_estimates",
        ]

        for attribute in attributes:
            try:
                data = getattr(self.ticker_no_analysts, attribute)
                self.assertIsInstance(data, pl.DataFrame, "data has wrong type")
                self.assertTrue(data.is_empty(), "data is not empty")
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
        self.symbols += ["EXTO"]  # Issues 2343
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
        expected_keys = [
            "industry",
            "currentPrice",
            "exchange",
            "floatShares",
            "companyOfficers",
            "bid",
        ]
        for k in expected_keys:
            self.assertIn(
                "symbol", data.keys(), f"Did not find expected key '{k}' in info dict"
            )
        self.assertEqual(
            self.symbols[0], data["symbol"], "Wrong symbol value in info dict"
        )

    def test_complementary_info(self):
        # This test is to check that we can successfully retrieve the trailing PEG ratio

        # We don't expect this one to have a trailing PEG ratio
        data1 = self.tickers[0].info
        self.assertIsNone(data1["trailingPegRatio"])

        # This one should have a trailing PEG ratio
        data2 = self.tickers[2].info
        self.assertIsInstance(data2["trailingPegRatio"], float)

    def test_isin_info(self):
        isin_list = {
            "ES0137650018": True,
            "does_not_exist": True,  # Nonexistent but doesn't raise an error
            "INF209K01EN2": True,
            "INX846K01K35": False,  # Nonexistent and raises an error
            "INF846K01K35": True,
        }
        for isin in isin_list:
            if not isin_list[isin]:
                with self.assertRaises(ValueError) as context:
                    ticker = yf.Ticker(isin)
                self.assertIn(
                    str(context.exception),
                    [f"Invalid ISIN number: {isin}", "Empty tickername"],
                )
            else:
                ticker = yf.Ticker(isin)
            ticker.info

    def test_empty_info(self):
        # Test issue 2343 (Empty result _fetch)
        data = self.tickers[10].info
        self.assertCountEqual(
            [
                "quoteType",
                "symbol",
                "underlyingSymbol",
                "uuid",
                "maxAge",
                "trailingPegRatio",
            ],
            data.keys(),
        )
        self.assertIn(
            "trailingPegRatio",
            data.keys(),
            "Did not find expected key 'trailingPegRatio' in info dict",
        )

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
        self.test_tickers = [
            yf.Ticker("SPY", session=self.session),  # equity etf
            yf.Ticker("JNK", session=self.session),  # bonds etf
            yf.Ticker("VTSAX", session=self.session),
        ]  # mutual fund

    def tearDown(self):
        self.ticker = None

    def test_fetch_and_parse(self):
        try:
            for ticker in self.test_tickers:
                ticker.funds_data._fetch_and_parse()

        except Exception as e:
            self.fail(f"_fetch_and_parse raised an exception unexpectedly: {e}")

        with self.assertRaises(YFDataException):
            ticker = yf.Ticker("AAPL", session=self.session)  # stock, not funds
            ticker.funds_data._fetch_and_parse()
            self.fail(
                "_fetch_and_parse should have failed when calling for non-funds data"
            )

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
            self.assertIsInstance(fund_operations, pl.DataFrame)

    def test_asset_classes(self):
        for ticker in self.test_tickers:
            asset_classes = ticker.funds_data.asset_classes
            self.assertIsInstance(asset_classes, dict)

    def test_top_holdings(self):
        for ticker in self.test_tickers:
            top_holdings = ticker.funds_data.top_holdings
            self.assertIsInstance(top_holdings, pl.DataFrame)

    def test_equity_holdings(self):
        for ticker in self.test_tickers:
            equity_holdings = ticker.funds_data.equity_holdings
            self.assertIsInstance(equity_holdings, pl.DataFrame)

    def test_bond_holdings(self):
        for ticker in self.test_tickers:
            bond_holdings = ticker.funds_data.bond_holdings
            self.assertIsInstance(bond_holdings, pl.DataFrame)

    def test_bond_ratings(self):
        for ticker in self.test_tickers:
            bond_ratings = ticker.funds_data.bond_ratings
            self.assertIsInstance(bond_ratings, dict)

    def test_sector_weightings(self):
        for ticker in self.test_tickers:
            sector_weightings = ticker.funds_data.sector_weightings
            self.assertIsInstance(sector_weightings, dict)


class TestTickerValuationMeasures(unittest.TestCase):
    _MOCK_HTML = """<html><body>
    <table>
        <tr><td></td><td>Current</td><td>12/31/2025</td><td>9/30/2025</td></tr>
        <tr><td>Market Cap</td><td>3.76T</td><td>4.00T</td><td>3.76T</td></tr>
        <tr><td>Enterprise Value</td><td>3.78T</td><td>4.04T</td><td>3.81T</td></tr>
        <tr><td>Trailing P/E</td><td>32.39</td><td>36.44</td><td>38.64</td></tr>
        <tr><td>Forward P/E</td><td>29.76</td><td>32.79</td><td>31.65</td></tr>
        <tr><td>PEG Ratio (5yr expected)</td><td>2.27</td><td>2.75</td><td>2.44</td></tr>
        <tr><td>Price/Sales</td><td>8.77</td><td>9.80</td><td>9.41</td></tr>
        <tr><td>Price/Book</td><td>42.60</td><td>54.21</td><td>57.14</td></tr>
        <tr><td>Enterprise Value/Revenue</td><td>8.68</td><td>9.71</td><td>9.32</td></tr>
        <tr><td>Enterprise Value/EBITDA</td><td>24.73</td><td>27.92</td><td>26.87</td></tr>
    </table>
    </body></html>"""

    def _make_ticker_with_mock(self, html):
        mock_response = MagicMock()
        mock_response.text = html
        with patch("yfinance.data.YfData.cache_get", return_value=mock_response):
            dat = yf.Ticker("AAPL")
            data = dat.valuation_measures
        return data

    def test_valuation_measures(self):
        data = self._make_ticker_with_mock(self._MOCK_HTML)
        self.assertIsInstance(data, pl.DataFrame)
        # Metrics are rows; columns are the date periods plus a label column
        self.assertIn("Current", data.columns)
        self.assertIn("12/31/2025", data.columns)
        self.assertIn("9/30/2025", data.columns)
        # Check metric label column contains expected entries
        label_col = data.columns[0]
        labels = data[label_col].to_list()
        self.assertIn("Market Cap", labels)
        self.assertIn("Trailing P/E", labels)
        self.assertIn("Enterprise Value/EBITDA", labels)
        # Check a value
        mc_row = data.filter(pl.col(label_col) == "Market Cap")
        self.assertEqual(mc_row["Current"][0], "3.76T")
        fpe_row = data.filter(pl.col(label_col) == "Forward P/E")
        self.assertEqual(fpe_row["12/31/2025"][0], "32.79")

    def test_valuation_measures_no_table(self):
        data = self._make_ticker_with_mock(
            "<html><body><p>No tables here</p></body></html>"
        )
        self.assertIsInstance(data, pl.DataFrame)
        self.assertTrue(data.is_empty())

    def test_valuation_measures_fetch_error(self):
        with patch(
            "yfinance.data.YfData.cache_get", side_effect=Exception("network error")
        ):
            dat = yf.Ticker("AAPL")
            data = dat.valuation_measures
        self.assertIsInstance(data, pl.DataFrame)
        self.assertTrue(data.is_empty())


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestTicker("Test ticker"))
    suite.addTest(TestTickerEarnings("Test earnings"))
    suite.addTest(TestTickerHolders("Test holders"))
    suite.addTest(TestTickerHistory("Test Ticker history"))
    suite.addTest(TestTickerMiscFinancials("Test misc financials"))
    suite.addTest(TestTickerInfo("Test info & fast_info"))
    suite.addTest(TestTickerFundsData("Test Funds Data"))
    suite.addTest(TestTickerValuationMeasures("Test valuation measures"))
    return suite


if __name__ == "__main__":
    unittest.main()

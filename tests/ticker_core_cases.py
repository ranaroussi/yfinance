"""Core ticker case modules."""

from tests.ticker_support import (
    SessionTickerTestCase,
    YFInvalidPeriodError,
    YFPricesMissingError,
    YFTickerMissingError,
    YFTzMissingError,
    YF_CONFIG,
    assert_attribute_type,
    as_dataframe,
    call_private,
    datetime,
    pd,
    ticker_attributes,
    timedelta,
    yf,
)


class TestTickerCore(SessionTickerTestCase):
    """Validate core ticker construction and top-level accessors."""

    def tearDown(self):
        """Restore raise_on_error to default after each test."""
        YF_CONFIG.debug.raise_on_error = False

    def test_get_tz(self):
        """Fetch and cache timezone data for several tickers."""
        for ticker_symbol in ["IMP.JO", "BHG.JO", "SSW.JO", "BP.L", "INTC"]:
            timezone_cache = yf.cache.get_tz_cache()
            assert timezone_cache is not None
            timezone_cache.store(ticker_symbol, None)

            ticker = yf.Ticker(ticker_symbol, session=self.session)
            timezone = call_private(ticker, "_get_ticker_tz", timeout=5)
            self.assertIsNotNone(timezone)

    def test_bad_ticker(self):
        """Handle a delisted or invalid ticker without crashing."""
        ticker_symbol = "DJI"
        ticker = yf.Ticker(ticker_symbol, session=self.session)

        ticker.history(period="5d")
        ticker.history(start="2022-01-01")
        ticker.history(start="2022-01-01", end="2022-03-01")
        for threads in [False, True]:
            for ignore_tz in [False, True]:
                yf.download([ticker_symbol], period="5d", threads=threads, ignore_tz=ignore_tz)

        for key in ticker.fast_info:
            _ = ticker.fast_info[key]

        for attribute_name, attribute_type in ticker_attributes:
            assert_attribute_type(self, ticker, attribute_name, attribute_type)

        self.assertIsInstance(ticker.dividends, pd.Series)
        self.assertTrue(ticker.dividends.empty)
        self.assertIsInstance(ticker.splits, pd.Series)
        self.assertTrue(ticker.splits.empty)
        self.assertIsInstance(ticker.capital_gains, pd.Series)
        self.assertTrue(ticker.capital_gains.empty)
        with self.assertRaises(Exception):
            _ = ticker.shares
        self.assertIsInstance(ticker.actions, pd.DataFrame)
        self.assertTrue(ticker.actions.empty)

    def test_invalid_period(self):
        """Reject invalid custom period aliases."""
        ticker = yf.Ticker('VALE', session=self.session)
        YF_CONFIG.debug.raise_on_error = True
        for invalid_period in ["2wks", "2mos"]:
            with self.assertRaises(YFInvalidPeriodError):
                ticker.history(period=invalid_period, interval="1d")

    def test_valid_custom_periods(self):
        """Accept supported Yahoo and custom periods."""
        valid_periods = [
            ("1d", "1m"),
            ("5d", "15m"),
            ("1mo", "1d"),
            ("3mo", "1wk"),
            ("6mo", "1d"),
            ("1y", "1mo"),
            ("5y", "1wk"),
            ("max", "1mo"),
            ("2d", "30m"),
            ("10mo", "1d"),
            ("1y", "1d"),
            ("3y", "1d"),
            ("2wk", "15m"),
            ("6mo", "5d"),
            ("10y", "1wk"),
        ]
        ticker = yf.Ticker("AAPL", session=self.session)
        YF_CONFIG.debug.raise_on_error = True

        for period, interval in valid_periods:
            with self.subTest(period=period, interval=interval):
                frame = ticker.history(period=period, interval=interval)
                self.assertIsInstance(frame, pd.DataFrame)
                self.assertFalse(frame.empty)
                self.assertIn("Close", frame.columns)
                if period == "max":
                    continue
                now = datetime.now()
                if period.endswith("d"):
                    expected_start = now - timedelta(days=int(period[:-1]))
                elif period.endswith("mo"):
                    expected_start = now - timedelta(days=30 * int(period[:-2]))
                elif period.endswith("y"):
                    expected_start = now - timedelta(days=365 * int(period[:-1]))
                else:
                    expected_start = now - timedelta(weeks=int(period[:-2]))
                actual_start = datetime.fromisoformat(str(frame.index[0])).replace(tzinfo=None)
                actual_end = datetime.fromisoformat(str(frame.index[-1])).replace(tzinfo=None)
                expected_start = expected_start.replace(hour=0, minute=0, second=0, microsecond=0)
                self.assertGreaterEqual(actual_start, expected_start - timedelta(days=10))
                self.assertLessEqual(actual_end, now)

    def test_ticker_missing(self):
        """Raise a ticker-missing style error for unavailable historical data."""
        ticker = yf.Ticker('ATVI', session=self.session)
        with self.assertRaises((YFTickerMissingError, YFTzMissingError, YFPricesMissingError)):
            YF_CONFIG.debug.raise_on_error = True
            ticker.history(period="3mo", interval="1d")

    def test_good_ticker(self):
        """Exercise the full ticker API for supported tickers."""
        for ticker_symbol in ["IBM", "QCSTIX"]:
            ticker = yf.Ticker(ticker_symbol, session=self.session)
            ticker.history(period="5d")
            ticker.history(start="2022-01-01")
            ticker.history(start="2022-01-01", end="2022-03-01")
            for threads in [False, True]:
                for ignore_tz in [False, True]:
                    yf.download([ticker_symbol], period="5d", threads=threads, ignore_tz=ignore_tz)
            for key in ticker.fast_info:
                _ = ticker.fast_info[key]
            for attribute_name, attribute_type in ticker_attributes:
                assert_attribute_type(self, ticker, attribute_name, attribute_type)

    def test_good_ticker_with_proxy(self):
        """Exercise the same ticker through the timezone bootstrap path."""
        ticker = yf.Ticker("IBM", session=self.session)
        call_private(ticker, "_fetch_ticker_tz", timeout=5)
        call_private(ticker, "_get_ticker_tz", timeout=5)
        ticker.history(period="5d")
        for attribute_name, attribute_type in ticker_attributes:
            assert_attribute_type(self, ticker, attribute_name, attribute_type)

    def test_ticker_with_symbol_mic(self):
        """Accept tickers provided as `(symbol, mic)` tuples."""
        equities = [
            ("OR", "XPAR"),
            ("AAPL", "XNYS"),
            ("GOOGL", "XNAS"),
            ("BMW", "XETR"),
        ]
        for equity in equities:
            yf.Ticker(equity)
            yf.Ticker((equity[0], equity[1].lower()))

    def test_ticker_with_symbol_mic_invalid(self):
        """Reject unknown MIC codes."""
        with self.assertRaises(ValueError) as raised:
            yf.Ticker(('ABC', 'XXXX'))
        self.assertIn("Unknown MIC code: 'XXXX'", str(raised.exception))


class TestTickerHistoryCases(SessionTickerTestCase):
    """Validate historical market data helpers."""

    def setUp(self):
        """Create the shared ticker used by the history tests."""
        self.ticker = yf.Ticker("IBM", session=self.session)
        self.symbols = ["AMZN", "MSFT", "NVDA"]

    def test_history(self):
        """Return metadata and a non-empty history dataframe."""
        metadata = self.ticker.history_metadata
        self.assertIn("IBM", metadata.values())
        frame = self.ticker.history("1y")
        self.assertIsInstance(frame, pd.DataFrame)
        self.assertFalse(frame.empty)

    def test_download(self):
        """Download single and multi-ticker history across option combinations."""
        tomorrow = datetime.now().date() + timedelta(days=1)
        for threads in [False, True]:
            for ignore_tz in [False, True]:
                for multi_level_index in [False, True]:
                    for count in [1, 'all']:
                        symbols = self.symbols[0] if count == 1 else self.symbols
                        frame = yf.download(
                            symbols,
                            end=tomorrow,
                            session=self.session,
                            threads=threads,
                            ignore_tz=ignore_tz,
                            multi_level_index=multi_level_index,
                        )
                        frame = as_dataframe(frame)
                        self.assertFalse(frame.empty)
                        index = pd.DatetimeIndex(frame.index)
                        if ignore_tz:
                            self.assertIsNone(index.tz)
                        else:
                            self.assertIsNotNone(index.tz)
                        if (not multi_level_index) and count == 1:
                            self.assertFalse(isinstance(frame.columns, pd.MultiIndex))
                        else:
                            self.assertIsInstance(frame.columns, pd.MultiIndex)

    def test_actions_views(self):
        """Return dividends, splits, and actions in the expected formats."""
        self.assertIsInstance(self.ticker.dividends, pd.Series)
        self.assertFalse(self.ticker.dividends.empty)
        self.assertIsInstance(self.ticker.splits, pd.Series)
        self.assertIsInstance(self.ticker.actions, pd.DataFrame)
        self.assertFalse(self.ticker.actions.empty)

    def test_chained_history_calls(self):
        """Allow follow-up corporate action access after history calls."""
        _ = self.ticker.history(period="2d")
        dividends = self.ticker.dividends
        self.assertIsInstance(dividends, pd.Series)
        self.assertFalse(dividends.empty)

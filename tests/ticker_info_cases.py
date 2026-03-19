"""Analyst, info, and funds ticker case modules."""

from tests.ticker_support import (
    SessionTickerTestCase,
    YFDataException,
    as_dataframe,
    as_non_none,
    call_private,
    pd,
    yf,
)


class TestTickerInfoCases(SessionTickerTestCase):
    """Validate analyst tables, info payloads, and funds data."""

    def test_analyst_tables(self):
        """Return cached analyst dataframes and dict payloads."""
        ticker = yf.Ticker("GOOGL", session=self.session)
        recommendation_columns = ["recommendations", "recommendations_summary"]
        frames = [as_dataframe(getattr(ticker, name)) for name in recommendation_columns]
        self.assertTrue(frames[0].equals(frames[1]))
        self.assertFalse(frames[0].empty)

        for attribute_name in [
            "upgrades_downgrades",
            "earnings_estimate",
            "revenue_estimate",
            "earnings_history",
            "eps_trend",
            "growth_estimates",
        ]:
            with self.subTest(attribute=attribute_name):
                frame = as_dataframe(getattr(ticker, attribute_name))
                self.assertFalse(frame.empty)
                self.assertIs(frame, getattr(ticker, attribute_name))

        upgrades = as_dataframe(ticker.upgrades_downgrades)
        self.assertIsInstance(upgrades.index, pd.DatetimeIndex)
        history = as_dataframe(ticker.earnings_history)
        self.assertIsInstance(history.index, pd.DatetimeIndex)
        self.assertIsInstance(ticker.analyst_price_targets, dict)

    def test_no_analysts(self):
        """Return empty dataframes when analyst coverage is unavailable."""
        ticker = yf.Ticker("^GSPC", session=self.session)
        for attribute_name in [
            "recommendations",
            "upgrades_downgrades",
            "earnings_estimate",
            "revenue_estimate",
            "earnings_history",
            "eps_trend",
            "growth_estimates",
        ]:
            with self.subTest(attribute=attribute_name):
                frame = as_dataframe(getattr(ticker, attribute_name))
                self.assertTrue(frame.empty)

    def test_info_payloads(self):
        """Return fast info, main info, complementary info, and empty info cases."""
        symbols = [
            "ESLT.TA",
            "BP.L",
            "GOOGL",
            "QCSTIX",
            "BTC-USD",
            "IWO",
            "VFINX",
            "^GSPC",
            "SOKE.IS",
            "ADS.DE",
            "EXTO",
        ]
        tickers = [yf.Ticker(symbol, session=self.session) for symbol in symbols]

        fast_info = yf.Ticker("AAPL", session=self.session).fast_info
        for key, value in fast_info.items():
            self.assertIsNotNone(value, key)

        info = tickers[0].info
        self.assertIsInstance(info, dict)
        self.assertEqual(symbols[0], info["symbol"])
        for key in [
            'industry',
            'currentPrice',
            'exchange',
            'floatShares',
            'companyOfficers',
            'bid',
        ]:
            self.assertIn(key, info.keys())

        self.assertIsNone(tickers[0].info['trailingPegRatio'])
        self.assertIsInstance(tickers[2].info['trailingPegRatio'], float)

        empty_info = tickers[10].info
        self.assertCountEqual(
            ['quoteType', 'symbol', 'underlyingSymbol', 'uuid', 'maxAge', 'trailingPegRatio'],
            empty_info.keys(),
        )

    def test_isin_info(self):
        """Support known-good ISIN lookups and reject invalid ones."""
        isin_expectations = {
            "ES0137650018": True,
            "does_not_exist": True,
            "INF209K01EN2": True,
            "INX846K01K35": False,
            "INF846K01K35": True,
        }
        for isin, is_valid in isin_expectations.items():
            with self.subTest(isin=isin):
                if not is_valid:
                    with self.assertRaises(ValueError) as raised:
                        yf.Ticker(isin)
                    self.assertIn(
                        str(raised.exception),
                        [f"Invalid ISIN number: {isin}", "Empty tickername"],
                    )
                    continue
                ticker = yf.Ticker(isin)
                _ = ticker.info

    def test_funds_data(self):
        """Return the expected funds data payloads and reject non-fund tickers."""
        test_tickers = [
            yf.Ticker("SPY", session=self.session),
            yf.Ticker("JNK", session=self.session),
            yf.Ticker("VTSAX", session=self.session),
        ]
        for ticker in test_tickers:
            with self.subTest(ticker=ticker.ticker):
                funds_data = as_non_none(ticker.funds_data)
                call_private(funds_data, "_fetch_and_parse")
                self.assertIsInstance(funds_data.description, str)
                self.assertTrue(len(funds_data.description) > 0)
                self.assertIsInstance(funds_data.fund_overview, dict)
                self.assertIsInstance(funds_data.fund_operations, pd.DataFrame)
                self.assertIsInstance(funds_data.asset_classes, dict)
                self.assertIsInstance(funds_data.top_holdings, pd.DataFrame)
                self.assertIsInstance(funds_data.equity_holdings, pd.DataFrame)
                self.assertIsInstance(funds_data.bond_holdings, pd.DataFrame)
                self.assertIsInstance(funds_data.bond_ratings, dict)
                self.assertIsInstance(funds_data.sector_weightings, dict)

        with self.assertRaises(YFDataException):
            funds_data = as_non_none(yf.Ticker("AAPL", session=self.session).funds_data)
            call_private(funds_data, "_fetch_and_parse")

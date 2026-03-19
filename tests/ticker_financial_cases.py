"""Financial and holder ticker case modules."""

from tests.ticker_support import (
    SessionTickerTestCase,
    as_dataframe,
    as_non_none,
    pd,
    yf,
)

class TestTickerFinancialCases(SessionTickerTestCase):
    """Validate financial statements, holders, earnings, and calendar data."""

    def _assert_cached_dataframe(self, value, *, allow_empty: bool = False):
        frame = as_dataframe(value)
        if not allow_empty:
            self.assertFalse(frame.empty)
        return frame

    def _assert_statement_family(
        self,
        ticker,
        getter_name: str,
        property_name: str,
        expected_keys: list[str],
    ):
        getter = getattr(ticker, getter_name)
        property_value = getattr(ticker, property_name)
        annual = self._assert_cached_dataframe(getter(pretty=True))
        quarterly = self._assert_cached_dataframe(getter(pretty=True, freq="quarterly"))
        trailing = self._assert_cached_dataframe(getter(pretty=True, freq='trailing'))
        self.assertTrue(annual.equals(property_value))
        self.assertTrue(len(trailing.columns) == 1)
        for frame in [annual, quarterly, trailing]:
            for key in expected_keys:
                self.assertIn(key, frame.index)
        self.assertIsInstance(getter(as_dict=True), dict)

    def test_earnings_dates(self):
        """Return earnings dates and support explicit limits with caching."""
        ticker = yf.Ticker("GOOGL", session=self.session)
        frame = self._assert_cached_dataframe(ticker.earnings_dates)
        self.assertFalse(frame.empty)

        ibm = yf.Ticker("IBM", session=self.session)
        limited = self._assert_cached_dataframe(ibm.get_earnings_dates(limit=100))
        self.assertEqual(len(limited), 100)
        self.assertIs(limited, ibm.get_earnings_dates(limit=100))

    def test_holder_tables(self):
        """Return cached holder dataframes for all holder accessors."""
        ticker = yf.Ticker("GOOGL", session=self.session)
        for attribute_name in [
            "major_holders",
            "institutional_holders",
            "mutualfund_holders",
            "insider_transactions",
            "insider_purchases",
            "insider_roster_holders",
        ]:
            with self.subTest(attribute=attribute_name):
                frame = self._assert_cached_dataframe(getattr(ticker, attribute_name))
                self.assertIs(frame, getattr(ticker, attribute_name))

    def test_misc_financial_metadata(self):
        """Return basic string, tuple, and series metadata for financial helpers."""
        ticker = yf.Ticker("GOOGL", session=self.session)
        self.assertEqual(ticker.isin, "CA02080M1005")
        self.assertTrue(len(ticker.options) > 1)
        shares = as_non_none(ticker.get_shares_full())
        self.assertIsInstance(shares, pd.Series)
        self.assertFalse(shares.empty)

    def test_financial_statements(self):
        """Return annual, quarterly, and trailing statement families."""
        ticker = yf.Ticker("GOOGL", session=self.session)
        self._assert_statement_family(
            ticker,
            "get_income_stmt",
            "income_stmt",
            ["Total Revenue", "Basic EPS"],
        )
        self._assert_statement_family(
            ticker,
            "get_balance_sheet",
            "balance_sheet",
            ["Total Assets", "Net PPE"],
        )
        self._assert_statement_family(
            ticker,
            "get_cashflow",
            "cashflow",
            ["Operating Cash Flow", "Net PPE Purchase And Sale"],
        )

    def test_financial_alt_names(self):
        """Keep alternate statement accessors aligned."""
        ticker = yf.Ticker("GOOGL", session=self.session)
        self.assertTrue(
            as_dataframe(ticker.income_stmt).equals(as_dataframe(ticker.incomestmt))
        )
        self.assertTrue(
            as_dataframe(ticker.income_stmt).equals(as_dataframe(ticker.financials))
        )
        self.assertTrue(
            as_dataframe(ticker.balance_sheet).equals(as_dataframe(ticker.balancesheet))
        )
        self.assertTrue(as_dataframe(ticker.cash_flow).equals(as_dataframe(ticker.cashflow)))
        self.assertTrue(
            as_dataframe(ticker.get_income_stmt(freq="quarterly")).equals(
                as_dataframe(ticker.get_incomestmt(freq="quarterly"))
            )
        )
        self.assertTrue(
            as_dataframe(ticker.get_cash_flow(freq="trailing")).equals(
                as_dataframe(ticker.get_cashflow(freq="trailing"))
            )
        )

    def test_bad_frequency_raises(self):
        """Reject unsupported frequency arguments."""
        ticker = yf.Ticker("GOOGL", session=self.session)
        with self.assertRaises(ValueError):
            ticker.get_cashflow(freq="badarg")

    def test_calendar(self):
        """Return a populated earnings calendar dictionary."""
        ticker = yf.Ticker("GOOGL", session=self.session)
        calendar = ticker.calendar
        self.assertIsInstance(calendar, dict)
        self.assertTrue(len(calendar) > 0)
        for key in [
            "Earnings Date",
            "Earnings Average",
            "Earnings Low",
            "Earnings High",
            "Revenue Average",
            "Revenue Low",
            "Revenue High",
        ]:
            self.assertIn(key, calendar.keys())
        self.assertIs(calendar, ticker.calendar)

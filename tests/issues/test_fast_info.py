"""Fast-info and extra ticker regressions split from the main close-candidates file."""

import datetime as dt
import unittest

import pandas as pd
import yfinance.client as yf
from tests.close_candidates_support import SessionTickerTestCase, call_private
from yfinance.scrapers.quote import FastInfo


class TestFastInfoIssues(unittest.TestCase):
    """Fast-info regression tests collected from reported issues."""

    def test_fast_info_regular_market_previous_close_handles_missing_current_trading_period(self):
        """fast_info.get() should not raise when chart metadata omits currentTradingPeriod."""
        today = pd.Timestamp.now("UTC").normalize()
        prices = pd.DataFrame(
            {"Close": [220.0, 225.5]},
            index=pd.DatetimeIndex([today - pd.Timedelta(days=2), today - pd.Timedelta(days=1)]),
        )

        class FakeTicker:
            """Minimal ticker stub with chart metadata missing currentTradingPeriod."""

            def history(self, **kwargs):
                """Return cached prices after validating the expected call shape."""
                expected = (kwargs["period"], kwargs["auto_adjust"], kwargs["keepna"])
                if expected != ("1y", False, True):
                    raise AssertionError(f"Unexpected history kwargs: {kwargs}")
                return prices

            def get_history_metadata(self):
                """Return chart metadata without currentTradingPeriod."""
                return {"exchangeTimezoneName": "America/New_York"}

            def get_info(self):
                """Return an empty info payload for the fallback path."""
                return {}

        fast_info = FastInfo(FakeTicker())

        cached_prices = call_private(fast_info, "_get_1y_prices")
        previous_close = fast_info.get("regularMarketPreviousClose")

        pd.testing.assert_frame_equal(cached_prices, prices)
        self.assertIsNone(getattr(fast_info, "_cache")["today_open"])
        self.assertIsNone(getattr(fast_info, "_cache")["today_close"])
        self.assertIsNone(getattr(fast_info, "_cache")["today_midnight"])
        self.assertEqual(previous_close, 220.0)


class TestSessionTickerIssueExtras(SessionTickerTestCase):
    """Remaining session-backed regression tests collected from reported issues."""

    def test_lse_etf_info_exposes_current_price(self):
        """MOAT.L should return a usable currentPrice instead of missing the key."""
        ticker = yf.Ticker("MOAT.L", session=self.session)
        info = ticker.info

        self.assertEqual(info.get("symbol"), "MOAT.L")
        self.assertIn("currentPrice", info)
        self.assertEqual(info["currentPrice"], info.get("regularMarketPrice"))
        self.assertIsInstance(info["currentPrice"], float)

    def test_fast_info_missing_metadata_returns_none_instead_of_keyerror(self):
        """Missing history metadata should degrade to None rather than raising."""

        class FakeTicker:
            """Minimal ticker stub for unavailable-quote fast_info paths."""

            def history(self, **kwargs):
                """Return an empty price history for unavailable quotes."""
                _ = kwargs
                return pd.DataFrame(
                    columns=["Open", "High", "Low", "Close", "Adj Close", "Volume"]
                )

            def get_history_metadata(self):
                """Return empty metadata for unavailable quotes."""
                return {}

            def get_info(self):
                """Return an empty info payload for unavailable quotes."""
                return {}

            def get_shares_full(self, start=None):
                """Return no share-count data for unavailable quotes."""
                _ = start

        fast_info = FastInfo(FakeTicker())

        for key in fast_info:
            with self.subTest(key=key):
                self.assertIsNone(fast_info.get(key))

    def test_aapl_dividend_dates_2022(self):
        """Dividend dates should match between history and dividends properties."""
        ticker = yf.Ticker("AAPL", session=self.session)
        start = dt.date(2022, 1, 1)
        end = dt.date(2023, 1, 1)

        history = ticker.history(start=start, end=end, interval="1d", actions=True)
        history_dividends = [
            pd.Timestamp(value).date()
            for value in history[history["Dividends"] != 0].index.tolist()
        ]
        property_dividends = [
            pd.Timestamp(value).date()
            for value in ticker.dividends.loc[str(start):str(end)].index.tolist()
        ]

        expected = [
            dt.date(2022, 2, 4),
            dt.date(2022, 5, 6),
            dt.date(2022, 8, 5),
            dt.date(2022, 11, 4),
        ]

        self.assertEqual(history_dividends, expected)
        self.assertEqual(property_dividends, expected)

"""Issue-specific history regression tests."""

import pandas as pd

import yfinance.client as yf
from tests.close_candidates_support import (
    SessionTickerTestCase,
    require_dataframe,
    require_datetime_index,
)


class TestSessionTickerHistoryIssueScenarios(SessionTickerTestCase):
    """Session-backed history regressions collected from reported issues."""

    def test_reported_asianpaint_single_day_history_returns_data(self):
        """The exact reported symbol/date should return a non-empty daily history frame."""
        frame = yf.Ticker("ASIANPAINT.NS", session=self.session).history(
            start="2022-05-04",
            end="2022-05-05",
            interval="1d",
        )

        self.assertIsInstance(frame, pd.DataFrame)
        self.assertFalse(frame.empty)
        self.assertEqual(len(frame), 1)
        self.assertEqual(str(frame.index[0].date()), "2022-05-04")

    def test_history_period_with_end_date_returns_data_for_reported_symbols(self):
        """Period-plus-end requests should return non-empty data on the reported symbols."""
        for symbol in ["AAPL", "^FTSE"]:
            with self.subTest(symbol=symbol):
                frame = yf.Ticker(symbol, session=self.session).history(
                    period="5y",
                    end="2022-11-11",
                )

                self.assertIsInstance(frame, pd.DataFrame)
                self.assertFalse(frame.empty)
                self.assertLessEqual(frame.index[-1].date().isoformat(), "2022-11-10")

    def test_weekly_histories_align_for_reported_symbol_pair(self):
        """GDX and QQQ should now return the same weekly index for the reported window."""
        df1 = yf.Ticker("GDX", session=self.session).history(
            start="2014-12-29",
            end="2020-11-29",
            interval="1wk",
            auto_adjust=False,
        )
        df2 = yf.Ticker("QQQ", session=self.session).history(
            start="2014-12-29",
            end="2020-11-29",
            interval="1wk",
            auto_adjust=False,
        )

        self.assertFalse(df1.empty)
        self.assertFalse(df2.empty)
        self.assertEqual(len(df1), len(df2))
        self.assertTrue(df1.index.equals(df2.index))

    def test_reported_monthly_and_quarterly_history_stay_populated_after_2022(self):
        """
        The reported AAPL max-range 1mo/3mo paths should keep valid OHLC rows
        in recent periods.
        """
        cases = [
            ("download", "1mo"),
            ("download", "3mo"),
            ("history", "1mo"),
            ("history", "3mo"),
        ]

        for source, interval in cases:
            with self.subTest(source=source, interval=interval):
                if source == "download":
                    frame = yf.download(
                        "AAPL",
                        period="max",
                        interval=interval,
                        actions=True,
                        progress=False,
                        threads=False,
                        session=self.session,
                    )
                    frame = require_dataframe(frame, "yf.download() returned None")
                    if isinstance(frame.columns, pd.MultiIndex):
                        frame = frame.xs("AAPL", axis=1, level=1)
                else:
                    frame = yf.Ticker("AAPL", session=self.session).history(
                        period="max",
                        interval=interval,
                        actions=True,
                    )
                frame_index = require_datetime_index(frame.index)

                self.assertIsInstance(frame, pd.DataFrame)
                self.assertFalse(frame.empty)
                self.assertTrue({"Open", "High", "Low", "Close"}.issubset(frame.columns))
                self.assertFalse(frame["Open"].tail(5).isna().any())
                self.assertFalse(frame["Close"].tail(5).isna().any())
                self.assertGreaterEqual(frame_index[-1].date().isoformat(), "2025-12-01")

    def test_history_and_download_match_for_default_and_unadjusted_paths(self):
        """The reported AAPL history/download mismatch should no longer reproduce."""
        start = "2023-01-03"
        end = "2023-02-01"

        default_history = yf.Ticker("AAPL", session=self.session).history(
            start=start,
            end=end,
            actions=False,
        )
        default_download = yf.download(
            "AAPL",
            start=start,
            end=end,
            actions=False,
            progress=False,
            threads=False,
            session=self.session,
        )
        default_download = require_dataframe(default_download, "yf.download() returned None")
        if isinstance(default_download.columns, pd.MultiIndex):
            default_download = default_download.xs("AAPL", axis=1, level=1)

        self.assertNotIn("Adj Close", default_history.columns)
        self.assertNotIn("Adj Close", default_download.columns)
        default_history_close = default_history["Close"].copy()
        default_download_close = default_download["Close"].copy()
        default_history_close.index = pd.Index([item.date() for item in default_history.index])
        default_download_index = require_datetime_index(default_download.index)
        default_download_close.index = pd.Index([item.date() for item in default_download_index])
        pd.testing.assert_series_equal(default_history_close, default_download_close)

        raw_history = yf.Ticker("AAPL", session=self.session).history(
            start=start,
            end=end,
            auto_adjust=False,
            actions=False,
        )
        raw_download = yf.download(
            "AAPL",
            start=start,
            end=end,
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=False,
            session=self.session,
        )
        raw_download = require_dataframe(raw_download, "yf.download() returned None")
        if isinstance(raw_download.columns, pd.MultiIndex):
            raw_download = raw_download.xs("AAPL", axis=1, level=1)

        self.assertTrue({"Close", "Adj Close"}.issubset(raw_history.columns))
        self.assertTrue({"Close", "Adj Close"}.issubset(raw_download.columns))
        raw_download_index = require_datetime_index(raw_download.index)
        for column in ["Close", "Adj Close"]:
            history_series = raw_history[column].copy()
            download_series = raw_download[column].copy()
            history_series.index = pd.Index([item.date() for item in raw_history.index])
            download_series.index = pd.Index([item.date() for item in raw_download_index])
            pd.testing.assert_series_equal(history_series, download_series)

    def test_start_end_window_with_empty_non_trading_range_stays_empty(self):
        """
        The reported CRSR non-trading window should return an empty frame
        rather than the prior trading day.
        """
        start = "2022-12-31"
        end = "2023-01-01"

        history_frame = yf.Ticker("CRSR", session=self.session).history(
            start=start,
            end=end,
            interval="1d",
        )
        download_frame = yf.download(
            "CRSR",
            start=start,
            end=end,
            interval="1d",
            progress=False,
            threads=False,
            session=self.session,
        )
        download_frame = require_dataframe(download_frame, "yf.download() returned None")

        self.assertIsInstance(history_frame, pd.DataFrame)
        self.assertTrue(history_frame.empty)
        self.assertEqual(list(history_frame.index), [])

        self.assertIsInstance(download_frame, pd.DataFrame)
        self.assertTrue(download_frame.empty)
        self.assertEqual(list(download_frame.index), [])

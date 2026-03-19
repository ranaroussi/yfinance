"""Issue-specific verification tests for upstream close candidates."""

import datetime as dt
import unittest
import warnings
from unittest.mock import patch

import pandas as pd
from curl_cffi import requests

from tests.ticker_support import SessionTickerTestCase, call_private, yf
from yfinance.data import YfData
from yfinance.scrapers.history.price_repair import _prepare_adjusted_price_data


class TestIssue2688(unittest.TestCase):
    """Verify repair logic handles read-only numpy arrays."""

    def test_price_repair_copies_read_only_price_data(self):
        price_columns = ["Open", "High", "Low", "Close"]
        frame = pd.DataFrame(
            {
                "Open": [100.0, 120.0],
                "High": [110.0, 125.0],
                "Low": [95.0, 115.0],
                "Close": [105.0, 120.0],
                "Adj Close": [52.5, 60.0],
            }
        )
        workings = frame[price_columns].copy()

        original_to_numpy = pd.DataFrame.to_numpy

        def patched_to_numpy(dataframe, *args, **kwargs):
            result = original_to_numpy(dataframe, *args, **kwargs)
            if list(getattr(dataframe, "columns", [])) == price_columns:
                result.setflags(write=False)
            return result

        with patch("pandas.core.frame.DataFrame.to_numpy", new=patched_to_numpy):
            adjusted, zeros = _prepare_adjusted_price_data(frame, workings, price_columns)

        self.assertFalse(zeros.any())
        self.assertAlmostEqual(adjusted[0, 0], 50.0)
        self.assertAlmostEqual(adjusted[1, 3], 60.0)


class TestIssue1924(unittest.TestCase):
    """Verify cookie fetch can survive fc.yahoo.com DNS blocking."""

    def setUp(self):
        self.data = YfData()
        setattr(self.data, "_cookie", None)
        setattr(self.data, "_crumb", None)
        setattr(self.data, "_cookie_strategy", "basic")

    def test_get_cookie_basic_handles_dns_error(self):
        with (
            patch.object(self.data, "_load_cookie_curl_cffi", return_value=False),
            patch.object(
                getattr(self.data, "_session"),
                "get",
                side_effect=requests.exceptions.DNSError("fc.yahoo.com blocked"),
            ),
        ):
            self.assertFalse(call_private(self.data, "_get_cookie_basic", timeout=1))

    def test_cookie_flow_falls_back_to_csrf_after_basic_failure(self):
        with (
            patch.object(self.data, "_get_cookie_and_crumb_basic", return_value=None),
            patch.object(self.data, "_get_crumb_csrf", return_value="csrf-crumb"),
        ):
            crumb, strategy = call_private(self.data, "_get_cookie_and_crumb", timeout=1)

        self.assertEqual(crumb, "csrf-crumb")
        self.assertEqual(strategy, "csrf")


class TestIssue1801(SessionTickerTestCase):
    """Verify download no longer emits utcfromtimestamp deprecation warnings."""

    def test_download_does_not_emit_utcfromtimestamp_warning(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            data = yf.download(
                ["AAPL"],
                start="2021-01-01",
                end="2021-02-01",
                progress=False,
                threads=False,
                session=self.session,
            )

        self.assertFalse(data.empty)
        matching = [
            warning
            for warning in caught
            if "utcfromtimestamp" in str(warning.message)
        ]
        self.assertEqual(matching, [])


class TestIssue1951(SessionTickerTestCase):
    """Verify unavailable quotes no longer crash fast_info access."""

    def test_fast_info_handles_unavailable_quote(self):
        ticker = yf.Ticker("DJI", session=self.session)
        fast_info = ticker.fast_info

        for key in fast_info:
            _ = fast_info.get(key)


class TestIssue930(SessionTickerTestCase):
    """Verify AAPL dividend events are still present in history output."""

    def test_aapl_dividend_dates_2022(self):
        ticker = yf.Ticker("AAPL", session=self.session)
        start = dt.date(2022, 1, 1)
        end = dt.date(2023, 1, 1)

        history = ticker.history(start=start, end=end, interval="1d", actions=True)
        history_dividends = history[history["Dividends"] != 0].index.date.tolist()
        property_dividends = ticker.dividends.loc[str(start):str(end)].index.date.tolist()

        expected = [
            dt.date(2022, 2, 4),
            dt.date(2022, 5, 6),
            dt.date(2022, 8, 5),
            dt.date(2022, 11, 4),
        ]

        self.assertEqual(history_dividends, expected)
        self.assertEqual(property_dividends, expected)
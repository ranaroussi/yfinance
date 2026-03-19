"""Tests for utility helpers."""

from datetime import datetime
import unittest
from unittest import TestSuite

import pandas as pd

from yfinance.utils import _parse_user_dt, is_valid_period_format
from yfinance.utils_price import _dts_in_same_interval


class TestPandas(unittest.TestCase):
    """Validate timezone handling in pandas conversions used by yfinance."""

    date_strings = ["2024-08-07 09:05:00+02:00", "2024-08-07 09:05:00-04:00"]

    @unittest.expectedFailure
    def test_mixed_timezones_to_datetime_fails(self):
        """Document pandas behavior when utc=True is omitted."""
        series = pd.Series(self.date_strings)
        series = series.map(pd.Timestamp)
        converted = pd.to_datetime(series)
        first = pd.Timestamp(converted.iloc[0])
        self.assertIsNotNone(first.tz)

    def test_mixed_timezones_to_datetime(self):
        """Preserve timezone-aware values when normalizing to UTC."""
        series = pd.Series(self.date_strings)
        series = series.map(pd.Timestamp)
        converted = pd.to_datetime(series, utc=True)
        first = pd.Timestamp(converted.iloc[0])
        self.assertIsNotNone(first.tz)
        for i, dt_value in enumerate(converted):
            ts = pd.Timestamp(series.iloc[i])
            self.assertEqual(dt_value.isoformat(), ts.tz_convert(tz="UTC").isoformat())


class TestUtils(unittest.TestCase):
    """Validate utility functions for interval parsing."""

    def test_is_valid_period_format_valid(self):
        """Accept valid period suffixes."""
        self.assertTrue(is_valid_period_format("1d"))
        self.assertTrue(is_valid_period_format("5wk"))
        self.assertTrue(is_valid_period_format("12mo"))
        self.assertTrue(is_valid_period_format("2y"))

    def test_is_valid_period_format_invalid(self):
        """Reject invalid period formats."""
        self.assertFalse(is_valid_period_format("1m"))  # Incorrect suffix
        self.assertFalse(is_valid_period_format("2wks"))  # Incorrect suffix
        self.assertFalse(is_valid_period_format("10"))  # Missing suffix
        self.assertFalse(is_valid_period_format("abc"))  # Invalid string
        self.assertFalse(is_valid_period_format(""))  # Empty string

    def test_is_valid_period_format_edge_cases(self):
        """Handle None and boundary values for period format validation."""
        self.assertFalse(is_valid_period_format(None))  # None input
        self.assertFalse(is_valid_period_format("0d"))  # Zero is invalid
        self.assertTrue(is_valid_period_format("999mo"))  # Large number valid


class TestDateIntervalCheck(unittest.TestCase):
    """Validate interval comparisons for timestamps and datetimes."""

    def test_same_day(self):
        """Treat timestamps on the same date as the same daily interval."""
        dt1 = pd.Timestamp("2024-10-15 10:00:00")
        dt2 = pd.Timestamp("2024-10-15 14:30:00")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1d"))

    def test_different_days(self):
        """Treat different dates as different daily intervals."""
        dt1 = pd.Timestamp("2024-10-15 10:00:00")
        dt2 = pd.Timestamp("2024-10-16 09:00:00")
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1d"))

    def test_same_week_mid_week(self):
        """Treat dates within the same ISO week as the same weekly interval."""
        dt1 = pd.Timestamp("2024-10-16")
        dt2 = pd.Timestamp("2024-10-18")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1wk"))

    def test_different_weeks(self):
        """Treat dates in different ISO weeks as different weekly intervals."""
        dt1 = pd.Timestamp("2024-10-14")
        dt2 = pd.Timestamp("2024-10-21")
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1wk"))

    def test_week_year_boundary(self):
        """Handle weekly intervals across year boundaries."""
        dt1 = pd.Timestamp("2024-12-30")
        dt2 = pd.Timestamp("2025-01-03")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1wk"))

    def test_same_month(self):
        """Treat dates in the same month as the same monthly interval."""
        dt1 = pd.Timestamp("2024-10-01")
        dt2 = pd.Timestamp("2024-10-31")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1mo"))

    def test_different_months(self):
        """Treat dates in different months as different monthly intervals."""
        dt1 = pd.Timestamp("2024-10-31")
        dt2 = pd.Timestamp("2024-11-01")
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1mo"))

    def test_month_year_boundary(self):
        """Treat cross-year monthly intervals correctly."""
        dt1 = pd.Timestamp("2024-12-15")
        dt2 = pd.Timestamp("2025-01-15")
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1mo"))

    def test_standard_quarters(self):
        """Validate quarter alignment when quarter starts in January."""
        q1_start = datetime(2023, 1, 1)
        self.assertTrue(_dts_in_same_interval(q1_start, datetime(2023, 1, 15), "3mo"))
        self.assertTrue(_dts_in_same_interval(q1_start, datetime(2023, 3, 31), "3mo"))
        self.assertFalse(_dts_in_same_interval(q1_start, datetime(2023, 4, 1), "3mo"))
        self.assertFalse(
            _dts_in_same_interval(q1_start, datetime(2022, 1, 15), "3mo")
        )  # Previous year
        self.assertFalse(
            _dts_in_same_interval(q1_start, datetime(2024, 1, 15), "3mo")
        )  # Next year

        q2_start = datetime(2023, 4, 1)
        self.assertTrue(_dts_in_same_interval(q2_start, datetime(2023, 5, 15), "3mo"))
        self.assertTrue(_dts_in_same_interval(q2_start, datetime(2023, 6, 30), "3mo"))
        self.assertFalse(_dts_in_same_interval(q2_start, datetime(2023, 7, 1), "3mo"))

    def test_nonstandard_quarters(self):
        """Validate quarter alignment when quarter starts in February."""
        q1_start = datetime(2023, 2, 1)
        self.assertTrue(_dts_in_same_interval(q1_start, datetime(2023, 3, 1), "3mo"))
        self.assertTrue(_dts_in_same_interval(q1_start, datetime(2023, 4, 25), "3mo"))
        self.assertFalse(
            _dts_in_same_interval(q1_start, datetime(2023, 1, 25), "3mo")
        )  # Before quarter start
        self.assertFalse(
            _dts_in_same_interval(q1_start, datetime(2023, 6, 1), "3mo")
        )  # Start of next quarter
        self.assertFalse(
            _dts_in_same_interval(q1_start, datetime(2023, 9, 1), "3mo")
        )  # Start of Q3

        q2_start = datetime(2023, 5, 1)
        self.assertTrue(_dts_in_same_interval(q2_start, datetime(2023, 6, 1), "3mo"))
        self.assertTrue(_dts_in_same_interval(q2_start, datetime(2023, 7, 25), "3mo"))
        self.assertFalse(_dts_in_same_interval(q2_start, datetime(2023, 8, 1), "3mo"))

    def test_cross_year_quarters(self):
        """Validate quarter logic for ranges crossing a year boundary."""
        q4_start = datetime(2023, 11, 1)

        self.assertTrue(_dts_in_same_interval(q4_start, datetime(2023, 11, 15), "3mo"))
        self.assertTrue(_dts_in_same_interval(q4_start, datetime(2024, 1, 15), "3mo"))
        self.assertTrue(_dts_in_same_interval(q4_start, datetime(2024, 1, 25), "3mo"))

        self.assertFalse(
            _dts_in_same_interval(q4_start, datetime(2024, 2, 1), "3mo")
        )  # Start of next quarter
        self.assertFalse(
            _dts_in_same_interval(q4_start, datetime(2023, 10, 14), "3mo")
        )  # Before quarter start

    def test_hourly_interval(self):
        """Validate same-hour and next-hour interval behavior."""
        dt1 = pd.Timestamp("2024-10-15 14:00:00")
        dt2 = pd.Timestamp("2024-10-15 14:59:59")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1h"))

        dt3 = pd.Timestamp("2024-10-15 15:00:00")
        self.assertFalse(_dts_in_same_interval(dt1, dt3, "1h"))

    def test_custom_intervals(self):
        """Validate custom multi-hour interval behavior."""
        dt1 = pd.Timestamp("2024-10-15 10:00:00")
        dt2 = pd.Timestamp("2024-10-15 13:59:59")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "4h"))

        dt3 = pd.Timestamp("2024-10-15 14:00:00")
        self.assertFalse(_dts_in_same_interval(dt1, dt3, "4h"))

    def test_minute_intervals(self):
        """Validate minute interval grouping behavior."""
        dt1 = pd.Timestamp("2024-10-15 10:30:00")
        dt2 = pd.Timestamp("2024-10-15 10:30:45")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1min"))

        dt3 = pd.Timestamp("2024-10-15 10:31:00")
        self.assertFalse(_dts_in_same_interval(dt1, dt3, "1min"))

    def test_parse_user_dt(self):
        """Parse user-provided datetime values into exchange timezone timestamps."""
        exchange_tz = "US/Eastern"
        dtstr = "2024-01-04"
        epoch = 1704344400
        expected = pd.Timestamp(dtstr, tz=exchange_tz)

        self.assertEqual(_parse_user_dt(epoch, exchange_tz), expected)
        self.assertEqual(_parse_user_dt(dtstr, exchange_tz), expected)
        self.assertEqual(
            _parse_user_dt(datetime(year=2024, month=1, day=4).date(), exchange_tz),
            expected,
        )
        self.assertEqual(
            _parse_user_dt(datetime(year=2024, month=1, day=4), exchange_tz),
            expected,
        )
        with self.assertRaises(ValueError):
            self.assertEqual(_parse_user_dt(float(epoch), exchange_tz), expected)


def suite():
    """Build the test suite for this module."""
    ts: TestSuite = unittest.TestSuite()
    ts.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(TestPandas))
    ts.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(TestUtils))
    ts.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(TestDateIntervalCheck))
    return ts


if __name__ == "__main__":
    unittest.main()

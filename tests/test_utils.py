"""
Tests for utils

To run all tests in suite from commandline:
   python -m unittest tests.utils

Specific test class:
   python -m unittest tests.utils.TestTicker

"""

import unittest
from datetime import datetime, timedelta, timezone
from unittest import TestSuite
from zoneinfo import ZoneInfo

from yfinance.utils import _dts_in_same_interval, _parse_user_dt, is_valid_period_format


class TestUtils(unittest.TestCase):
    def test_is_valid_period_format_valid(self):
        self.assertTrue(is_valid_period_format("1d"))
        self.assertTrue(is_valid_period_format("5wk"))
        self.assertTrue(is_valid_period_format("12mo"))
        self.assertTrue(is_valid_period_format("2y"))

    def test_is_valid_period_format_invalid(self):
        self.assertFalse(is_valid_period_format("1m"))  # Incorrect suffix
        self.assertFalse(is_valid_period_format("2wks"))  # Incorrect suffix
        self.assertFalse(is_valid_period_format("10"))  # Missing suffix
        self.assertFalse(is_valid_period_format("abc"))  # Invalid string
        self.assertFalse(is_valid_period_format(""))  # Empty string

    def test_is_valid_period_format_edge_cases(self):
        self.assertFalse(is_valid_period_format(None))  # None input
        self.assertFalse(is_valid_period_format("0d"))  # Zero is invalid
        self.assertTrue(is_valid_period_format("999mo"))  # Large number valid


class TestDateIntervalCheck(unittest.TestCase):
    def test_same_day(self):
        dt1 = datetime(2024, 10, 15, 10, 0, 0)
        dt2 = datetime(2024, 10, 15, 14, 30, 0)
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1d"))

    def test_different_days(self):
        dt1 = datetime(2024, 10, 15, 10, 0, 0)
        dt2 = datetime(2024, 10, 16, 9, 0, 0)
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1d"))

    def test_same_week_mid_week(self):
        # Wednesday and Friday in same week
        dt1 = datetime(2024, 10, 16)  # Wednesday
        dt2 = datetime(2024, 10, 18)  # Friday
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1wk"))

    def test_different_weeks(self):
        dt1 = datetime(2024, 10, 14)  # Monday week 42
        dt2 = datetime(2024, 10, 21)  # Monday week 43
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1wk"))

    def test_week_year_boundary(self):
        # Week 52 of 2024 spans into 2025
        dt1 = datetime(2024, 12, 30)  # Monday in week 1 (ISO calendar)
        dt2 = datetime(2025, 1, 3)  # Friday in week 1 (ISO calendar)
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1wk"))

    def test_same_month(self):
        dt1 = datetime(2024, 10, 1)
        dt2 = datetime(2024, 10, 31)
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1mo"))

    def test_different_months(self):
        dt1 = datetime(2024, 10, 31)
        dt2 = datetime(2024, 11, 1)
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1mo"))

    def test_month_year_boundary(self):
        dt1 = datetime(2024, 12, 15)
        dt2 = datetime(2025, 1, 15)
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1mo"))

    def test_standard_quarters(self):
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
        q1_start = datetime(2023, 2, 1)
        # Same quarter
        self.assertTrue(_dts_in_same_interval(q1_start, datetime(2023, 3, 1), "3mo"))
        self.assertTrue(_dts_in_same_interval(q1_start, datetime(2023, 4, 25), "3mo"))
        # Different quarters
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
        q4_start = datetime(2023, 11, 1)

        # Same quarter, different year
        self.assertTrue(_dts_in_same_interval(q4_start, datetime(2023, 11, 15), "3mo"))
        self.assertTrue(_dts_in_same_interval(q4_start, datetime(2024, 1, 15), "3mo"))
        self.assertTrue(_dts_in_same_interval(q4_start, datetime(2024, 1, 25), "3mo"))

        # Different quarters
        self.assertFalse(
            _dts_in_same_interval(q4_start, datetime(2024, 2, 1), "3mo")
        )  # Start of next quarter
        self.assertFalse(
            _dts_in_same_interval(q4_start, datetime(2023, 10, 14), "3mo")
        )  # Before quarter start

    def test_hourly_interval(self):
        dt1 = datetime(2024, 10, 15, 14, 0, 0)
        dt2 = datetime(2024, 10, 15, 14, 59, 59)
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1h"))

        dt3 = datetime(2024, 10, 15, 15, 0, 0)
        self.assertFalse(_dts_in_same_interval(dt1, dt3, "1h"))

    def test_custom_intervals(self):
        # Test 4 hour interval
        dt1 = datetime(2024, 10, 15, 10, 0, 0)
        dt2 = datetime(2024, 10, 15, 13, 59, 59)
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "4h"))

        dt3 = datetime(2024, 10, 15, 14, 0, 0)
        self.assertFalse(_dts_in_same_interval(dt1, dt3, "4h"))

    def test_minute_intervals(self):
        dt1 = datetime(2024, 10, 15, 10, 30, 0)
        dt2 = datetime(2024, 10, 15, 10, 30, 45)
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1m"))

        dt3 = datetime(2024, 10, 15, 10, 31, 0)
        self.assertFalse(_dts_in_same_interval(dt1, dt3, "1m"))

    def test_parse_user_dt(self):
        exchange_tz = "US/Eastern"
        dtstr = "2024-01-04"
        # _parse_user_dt now returns a stdlib datetime with ZoneInfo
        tz_obj = ZoneInfo(exchange_tz)
        expected = datetime(2024, 1, 4, 0, 0, 0, tzinfo=tz_obj)
        epoch = int(expected.timestamp())  # 1704344400

        result_from_epoch = _parse_user_dt(epoch, exchange_tz)
        result_from_str = _parse_user_dt(dtstr, exchange_tz)
        result_from_date = _parse_user_dt(
            datetime(year=2024, month=1, day=4).date(), exchange_tz
        )
        result_from_dt = _parse_user_dt(
            datetime(year=2024, month=1, day=4), exchange_tz
        )

        # Compare as UTC timestamps to avoid ZoneInfo vs pytz tzinfo equality issues
        self.assertAlmostEqual(
            result_from_epoch.timestamp(), expected.timestamp(), places=0
        )
        self.assertAlmostEqual(
            result_from_str.timestamp(), expected.timestamp(), places=0
        )
        self.assertAlmostEqual(
            result_from_date.timestamp(), expected.timestamp(), places=0
        )
        self.assertAlmostEqual(
            result_from_dt.timestamp(), expected.timestamp(), places=0
        )

        with self.assertRaises(ValueError):
            _parse_user_dt(float(epoch), exchange_tz)


if __name__ == "__main__":
    unittest.main()


def suite():
    ts: TestSuite = unittest.TestSuite()
    ts.addTest(TestUtils("Test utils"))
    return ts


if __name__ == "__main__":
    unittest.main()

"""
Tests for utils

To run all tests in suite from commandline:
   python -m unittest tests.utils

Specific test class:
   python -m unittest tests.utils.TestTicker

"""
from datetime import datetime
from unittest import TestSuite

import pandas as pd

import unittest

from yfinance.utils import is_valid_period_format, _dts_in_same_interval


class TestPandas(unittest.TestCase):
    date_strings = ["2024-08-07 09:05:00+02:00", "2024-08-07 09:05:00-04:00"]

    @unittest.expectedFailure
    def test_mixed_timezones_to_datetime_fails(self):
        series = pd.Series(self.date_strings)
        series = series.map(pd.Timestamp)
        converted = pd.to_datetime(series)
        self.assertIsNotNone(converted[0].tz)

    def test_mixed_timezones_to_datetime(self):
        series = pd.Series(self.date_strings)
        series = series.map(pd.Timestamp)
        converted = pd.to_datetime(series, utc=True)
        self.assertIsNotNone(converted[0].tz)
        i = 0
        for dt in converted:
            dt: datetime
            ts: pd.Timestamp = series[i]
            self.assertEqual(dt.isoformat(), ts.tz_convert(tz="UTC").isoformat())
            i += 1


class TestUtils(unittest.TestCase):
    def test_is_valid_period_format_valid(self):
        self.assertTrue(is_valid_period_format("1d"))
        self.assertTrue(is_valid_period_format("5wk"))
        self.assertTrue(is_valid_period_format("12mo"))
        self.assertTrue(is_valid_period_format("2y"))

    def test_is_valid_period_format_invalid(self):
        self.assertFalse(is_valid_period_format("1m"))    # Incorrect suffix
        self.assertFalse(is_valid_period_format("2wks"))  # Incorrect suffix
        self.assertFalse(is_valid_period_format("10"))    # Missing suffix
        self.assertFalse(is_valid_period_format("abc"))   # Invalid string
        self.assertFalse(is_valid_period_format(""))      # Empty string

    def test_is_valid_period_format_edge_cases(self):
        self.assertFalse(is_valid_period_format(None))    # None input
        self.assertFalse(is_valid_period_format("0d"))    # Zero is invalid
        self.assertTrue(is_valid_period_format("999mo"))  # Large number valid


class TestDateIntervalCheck(unittest.TestCase):
    def test_same_day(self):
        dt1 = pd.Timestamp("2024-10-15 10:00:00")
        dt2 = pd.Timestamp("2024-10-15 14:30:00")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1d"))
        
    def test_different_days(self):
        dt1 = pd.Timestamp("2024-10-15 10:00:00")
        dt2 = pd.Timestamp("2024-10-16 09:00:00")
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1d"))
    
    def test_same_week_mid_week(self):
        # Wednesday and Friday in same week
        dt1 = pd.Timestamp("2024-10-16")  # Wednesday
        dt2 = pd.Timestamp("2024-10-18")  # Friday
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1wk"))
    
    def test_different_weeks(self):
        dt1 = pd.Timestamp("2024-10-14")  # Monday week 42
        dt2 = pd.Timestamp("2024-10-21")  # Monday week 43
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1wk"))
    
    def test_week_year_boundary(self):
        # Week 52 of 2024 spans into 2025
        dt1 = pd.Timestamp("2024-12-30")  # Monday in week 1 (ISO calendar)
        dt2 = pd.Timestamp("2025-01-03")  # Friday in week 1 (ISO calendar)
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1wk"))
    
    def test_same_month(self):
        dt1 = pd.Timestamp("2024-10-01")
        dt2 = pd.Timestamp("2024-10-31")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1mo"))
    
    def test_different_months(self):
        dt1 = pd.Timestamp("2024-10-31")
        dt2 = pd.Timestamp("2024-11-01")
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1mo"))
    
    def test_month_year_boundary(self):
        dt1 = pd.Timestamp("2024-12-15")
        dt2 = pd.Timestamp("2025-01-15")
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1mo"))

    def test_standard_quarters(self):
        q1_start = datetime(2023, 1, 1)
        self.assertTrue(_dts_in_same_interval(q1_start, datetime(2023, 1, 15), '3mo'))
        self.assertTrue(_dts_in_same_interval(q1_start, datetime(2023, 3, 31), '3mo'))
        self.assertFalse(_dts_in_same_interval(q1_start, datetime(2023, 4, 1), '3mo'))
        self.assertFalse(_dts_in_same_interval(q1_start, datetime(2022, 1, 15), '3mo'))  # Previous year
        self.assertFalse(_dts_in_same_interval(q1_start, datetime(2024, 1, 15), '3mo'))  # Next year
        
        q2_start = datetime(2023, 4, 1)
        self.assertTrue(_dts_in_same_interval(q2_start, datetime(2023, 5, 15), '3mo'))
        self.assertTrue(_dts_in_same_interval(q2_start, datetime(2023, 6, 30), '3mo'))
        self.assertFalse(_dts_in_same_interval(q2_start, datetime(2023, 7, 1), '3mo'))
    
    def test_nonstandard_quarters(self):
        q1_start = datetime(2023, 2, 1)
        # Same quarter
        self.assertTrue(_dts_in_same_interval(q1_start, datetime(2023, 3, 1), '3mo'))
        self.assertTrue(_dts_in_same_interval(q1_start, datetime(2023, 4, 25), '3mo'))
        # Different quarters
        self.assertFalse(_dts_in_same_interval(q1_start, datetime(2023, 1, 25), '3mo'))  # Before quarter start
        self.assertFalse(_dts_in_same_interval(q1_start, datetime(2023, 6, 1), '3mo'))  # Start of next quarter
        self.assertFalse(_dts_in_same_interval(q1_start, datetime(2023, 9, 1), '3mo'))  # Start of Q3
        
        q2_start = datetime(2023, 5, 1)
        self.assertTrue(_dts_in_same_interval(q2_start, datetime(2023, 6, 1), '3mo'))
        self.assertTrue(_dts_in_same_interval(q2_start, datetime(2023, 7, 25), '3mo'))
        self.assertFalse(_dts_in_same_interval(q2_start, datetime(2023, 8, 1), '3mo'))
    
    def test_cross_year_quarters(self):
        q4_start = datetime(2023, 11, 1)
        
        # Same quarter, different year
        self.assertTrue(_dts_in_same_interval(q4_start, datetime(2023, 11, 15), '3mo'))
        self.assertTrue(_dts_in_same_interval(q4_start, datetime(2024, 1, 15), '3mo'))
        self.assertTrue(_dts_in_same_interval(q4_start, datetime(2024, 1, 25), '3mo'))
        
        # Different quarters
        self.assertFalse(_dts_in_same_interval(q4_start, datetime(2024, 2, 1), '3mo'))  # Start of next quarter
        self.assertFalse(_dts_in_same_interval(q4_start, datetime(2023, 10, 14), '3mo'))  # Before quarter start
    
    def test_hourly_interval(self):
        dt1 = pd.Timestamp("2024-10-15 14:00:00")
        dt2 = pd.Timestamp("2024-10-15 14:59:59")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1h"))
        
        dt3 = pd.Timestamp("2024-10-15 15:00:00")
        self.assertFalse(_dts_in_same_interval(dt1, dt3, "1h"))
    
    def test_custom_intervals(self):
        # Test 4 hour interval
        dt1 = pd.Timestamp("2024-10-15 10:00:00")
        dt2 = pd.Timestamp("2024-10-15 13:59:59")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "4h"))
        
        dt3 = pd.Timestamp("2024-10-15 14:00:00")
        self.assertFalse(_dts_in_same_interval(dt1, dt3, "4h"))
        
    def test_minute_intervals(self):
        dt1 = pd.Timestamp("2024-10-15 10:30:00")
        dt2 = pd.Timestamp("2024-10-15 10:30:45")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1min"))
        
        dt3 = pd.Timestamp("2024-10-15 10:31:00")
        self.assertFalse(_dts_in_same_interval(dt1, dt3, "1min"))

if __name__ == "__main__":
    unittest.main()


def suite():
    ts: TestSuite = unittest.TestSuite()
    ts.addTest(TestPandas("Test pandas"))
    ts.addTest(TestUtils("Test utils"))
    return ts


if __name__ == '__main__':
    unittest.main()

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

from yfinance.utils import is_valid_period_format


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


def suite():
    ts: TestSuite = unittest.TestSuite()
    ts.addTest(TestPandas("Test pandas"))
    ts.addTest(TestUtils("Test utils"))
    return ts


if __name__ == '__main__':
    unittest.main()

"""Unix epoch zero is a supplied calendar bound, not an omitted date."""

import unittest

from yfinance.calendars import Calendars


class TestCalendarEpochZero(unittest.TestCase):
    def test_epoch_zero_start(self):
        calendar = Calendars(start=0, end=86400)
        self.assertEqual(calendar._start, "1970-01-01")
        self.assertEqual(calendar._end, "1970-01-02")

    def test_epoch_zero_end(self):
        calendar = Calendars(start=-86400, end=0)
        self.assertEqual(calendar._start, "1969-12-31")
        self.assertEqual(calendar._end, "1970-01-01")

    def test_empty_dates_still_default(self):
        calendar = Calendars(start="", end="")
        self.assertNotEqual(calendar._start, "")
        self.assertNotEqual(calendar._end, "")

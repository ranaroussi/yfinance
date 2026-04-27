import unittest
from datetime import datetime, timedelta, timezone

import polars as pl

from tests.context import session_gbl
from tests.context import yfinance as yf


class TestCalendars(unittest.TestCase):
    def setUp(self):
        self.calendars = yf.Calendars(session=session_gbl)

    def test_get_earnings_calendar(self):
        result = self.calendars.get_earnings_calendar(limit=1)

        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.height, 1)

    def test_get_earnings_calendar_init_params(self):
        result = self.calendars.get_earnings_calendar(limit=5)
        self.assertGreater(result.height, 0)

        start = datetime.now(tz=timezone.utc) - timedelta(days=7)
        result = yf.Calendars(start=start).get_earnings_calendar(limit=5)
        self.assertGreater(result.height, 0)

    def test_get_ipo_info_calendar(self):
        result = self.calendars.get_ipo_info_calendar(limit=5)

        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.height, 5)

    def test_get_economic_events_calendar(self):
        result = self.calendars.get_economic_events_calendar(limit=5)

        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.height, 5)

    def test_get_splits_calendar(self):
        result = self.calendars.get_splits_calendar(limit=5)

        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.height, 5)


if __name__ == "__main__":
    unittest.main()

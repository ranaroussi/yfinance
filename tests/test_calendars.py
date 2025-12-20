from datetime import datetime, timedelta, timezone
import unittest

import pandas as pd

from tests.context import yfinance as yf, session_gbl


class TestCalendars(unittest.TestCase):
    def setUp(self):
        self.calendars = yf.Calendars(session=session_gbl)

    def test_get_earnings_calendar(self):
        result = self.calendars.get_earnings_calendar(limit=1)
        tickers = self.calendars.earnings_calendar.index.tolist()

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)
        self.assertIsInstance(tickers, list)
        self.assertEqual(len(tickers), len(result))
        self.assertEqual(tickers, result.index.tolist())
        
        first_ticker = result.index.tolist()[0]
        result_first_ticker = self.calendars.earnings_calendar.loc[first_ticker].name
        self.assertEqual(first_ticker, result_first_ticker)

    def test_get_earnings_calendar_init_params(self):
        result = self.calendars.get_earnings_calendar(limit=5)
        self.assertGreaterEqual(result['Event Start Date'].iloc[0], pd.to_datetime(datetime.now(tz=timezone.utc)))

        start = datetime.now(tz=timezone.utc) - timedelta(days=7)
        result = yf.Calendars(start=start).get_earnings_calendar(limit=5)
        self.assertGreaterEqual(result['Event Start Date'].iloc[0], pd.to_datetime(start))

    def test_get_ipo_info_calendar(self):
        result = self.calendars.get_ipo_info_calendar(limit=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_get_economic_events_calendar(self):
        result = self.calendars.get_economic_events_calendar(limit=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)

    def test_get_splits_calendar(self):
        result = self.calendars.get_splits_calendar(limit=5)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)


if __name__ == "__main__":
    unittest.main()
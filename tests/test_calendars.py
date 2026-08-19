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
        self.assertGreaterEqual(result['Event Start Date'].iloc[0].date(), start.date())

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


class TestCalendarsResponseHandling(unittest.TestCase):
    """Offline tests for parsing of malformed or empty calendar responses."""

    def setUp(self):
        self.calendars = yf.Calendars(session=session_gbl)

    def test_create_df_undecodable_response(self):
        # _get_data falls back to {} when response.json() raises, so parsing
        # that payload has to degrade to an empty frame rather than KeyError.
        df = self.calendars._create_df({})

        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)

    def test_create_df_no_results(self):
        for payload in (
            {"finance": {"result": []}},
            {"finance": {"result": [{"documents": []}]}},
            {"finance": None},
        ):
            with self.subTest(payload=payload):
                df = self.calendars._create_df(payload)

                self.assertIsInstance(df, pd.DataFrame)
                self.assertTrue(df.empty)

    def test_create_df_parses_document(self):
        payload = {"finance": {"result": [{"documents": [{
            "columns": [
                {"label": "Symbol", "type": "STRING"},
                {"label": "Event Start Date", "type": "DATE"},
                {"label": "Event Start Date", "type": "STRING"},
            ],
            "rows": [["AAPL", "2026-10-29", "After Market Close"]],
        }]}]}}

        df = self.calendars._create_df(payload)

        # The second "Event Start Date" column is renamed to "Timing".
        self.assertEqual(list(df.columns), ["Symbol", "Event Start Date", "Timing"])
        self.assertEqual(df.iloc[0]["Symbol"], "AAPL")
        self.assertEqual(df.iloc[0]["Timing"], "After Market Close")


if __name__ == "__main__":
    unittest.main()
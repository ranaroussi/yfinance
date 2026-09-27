from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import MagicMock, patch

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


class TestCalendarsZeroValues(unittest.TestCase):
    @staticmethod
    def _response(columns, rows):
        payload = {"finance": {"result": [{"documents": [{"columns": [{"label": label, "type": typ} for label, typ in columns],
                                                          "rows": rows}]}],
                               "error": None}}
        return MagicMock(json=MagicMock(return_value=payload))

    def test_economic_events_keep_zero_values(self):
        # Rows as Yahoo returned them. Economic values arrive as strings, not-yet-released ones as null.
        columns = [("Event", "STRING"), ("Country Code", "STRING"), ("Event Time", "DATE"), ("For", "STRING"),
                   ("Actual", "STRING"), ("Market Expectation", "STRING"), ("Prior to This", "STRING"), ("Revised from", "STRING")]
        rows = [["SNB Policy Rate", "CH", "2026-09-24T07:30:00.000Z", "Q3", "0", None, "0", None],
                ["IGAE Econ Activity MM", "MX", "2026-09-24T12:00:00.000Z", "Jul", "0.8", None, "-0.1", "0"],
                ["Durable Goods, R MM", "US", "2026-10-02T14:00:00.000Z", "Aug", None, None, "0", None]]
        with patch("yfinance.data.YfData.post", return_value=self._response(columns, rows)):
            df = yf.Calendars(start="2026-09-19", end="2026-10-03").get_economic_events_calendar(limit=3)

        self.assertEqual(df.loc["SNB Policy Rate", "Actual"], 0.0)
        self.assertEqual(df.loc["SNB Policy Rate", "Last"], 0.0)
        self.assertEqual(df.loc["IGAE Econ Activity MM", "Revised"], 0.0)
        self.assertEqual(df.loc["Durable Goods, R MM", "Last"], 0.0)
        # Missing values stay NaN
        self.assertTrue(pd.isna(df.loc["Durable Goods, R MM", "Actual"]))
        self.assertTrue(pd.isna(df.loc["SNB Policy Rate", "Expected"]))

    def test_earnings_surprise_placeholder_still_nan(self):
        # Yahoo sends Surprise 0.0 when there is no reported EPS yet.
        columns = [("Symbol", "STRING"), ("Company Name", "STRING"), ("Market Cap (Intraday)", "NUMBER"),
                   ("Event Name", "STRING"), ("Event Start Date", "DATE"), ("Event Start Date", "STRING"),
                   ("EPS Estimate", "NUMBER"), ("Reported EPS", "NUMBER"), ("Surprise (%)", "NUMBER")]
        rows = [["EBF", "Ennis, Inc.", 0.0, "Q2 2027 Earnings Announcement", "2026-09-21T12:30:00.000Z", "BMO", 0.39, None, 0.0]]
        with patch("yfinance.data.YfData.post", return_value=self._response(columns, rows)):
            df = yf.Calendars(start="2026-09-21", end="2026-09-26").get_earnings_calendar(filter_most_active=False, limit=1)

        self.assertEqual(df.loc["EBF", "EPS Estimate"], 0.39)
        self.assertTrue(pd.isna(df.loc["EBF", "Surprise(%)"]))


if __name__ == "__main__":
    unittest.main()
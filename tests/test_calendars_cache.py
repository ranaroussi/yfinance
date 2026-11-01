"""Offline checks for calendar requests and their local cache."""

import unittest
from unittest.mock import Mock, patch

from yfinance.calendars import Calendars


def _response(symbol, payable_on="2026-09-21"):
    return Mock(json=Mock(return_value={"finance": {"result": [{"documents": [{
        "columns": [{"label": "Symbol", "type": "STRING"},
                    {"label": "Payable On", "type": "DATE"}],
        "rows": [[symbol, payable_on]],
    }]}]}}))


def _earnings_response():
    return Mock(json=Mock(return_value={"finance": {"result": [{"documents": [{
        "columns": [{"label": "Symbol", "type": "STRING"},
                    {"label": "Event Start Date", "type": "DATE"},
                    {"label": "EPS Estimate", "type": "NUMBER"},
                    {"label": "Reported EPS", "type": "NUMBER"},
                    {"label": "Surprise (%)", "type": "NUMBER"}],
        "rows": [["ACME", "2026-09-21", 1.0, 1.0, 0.0]],
    }]}]}}))


class TestCalendarCache(unittest.TestCase):
    def setUp(self):
        self.calendars = Calendars(start="2026-09-20", end="2026-09-27")

    def test_changed_date_retries_after_network_failure(self):
        with patch.object(self.calendars._data, "post", side_effect=[
            _response("OLD"), TimeoutError("offline timeout"), _response("NEW")
        ]) as post:
            old = self.calendars.get_splits_calendar()
            self.assertIs(self.calendars.get_splits_calendar(), old)
            self.assertEqual(post.call_count, 1)

            with self.assertRaisesRegex(TimeoutError, "offline timeout"):
                self.calendars.get_splits_calendar(start="2026-10-01", end="2026-10-07")
            self.assertIs(self.calendars.get_splits_calendar(), old)
            self.assertEqual(post.call_count, 2)

            new = self.calendars.get_splits_calendar(start="2026-10-01", end="2026-10-07")
            self.assertEqual(new.index.tolist(), ["NEW"])
            self.assertEqual(post.call_count, 3)
            self.assertIs(self.calendars.get_splits_calendar(start="2026-10-01", end="2026-10-07"), new)
            self.assertEqual(post.call_count, 3)

    def test_cleanup_failure_preserves_previous_cache(self):
        with patch.object(self.calendars._data, "post", side_effect=[
            _response("OLD"), _response("BROKEN", "not-a-date"), _response("NEW")
        ]) as post:
            old = self.calendars.get_splits_calendar()
            with self.assertRaises(ValueError):
                self.calendars.get_splits_calendar(start="2026-10-01", end="2026-10-07")
            self.assertIs(self.calendars.get_splits_calendar(), old)
            self.assertEqual(post.call_count, 2)
            new = self.calendars.get_splits_calendar(start="2026-10-01", end="2026-10-07")
            self.assertEqual(new.index.tolist(), ["NEW"])
            self.assertEqual(post.call_count, 3)

    def test_force_refresh_and_failed_force_keep_last_good_data(self):
        with patch.object(self.calendars._data, "post", side_effect=[
            _response("OLD"), _response("REFRESHED"), TimeoutError("offline timeout"), _response("NEW")
        ]) as post:
            self.calendars.get_splits_calendar()
            refreshed = self.calendars.get_splits_calendar(force=True)
            self.assertEqual(refreshed.index.tolist(), ["REFRESHED"])
            with self.assertRaisesRegex(TimeoutError, "offline timeout"):
                self.calendars.get_splits_calendar(force=True)
            self.assertIs(self.calendars.get_splits_calendar(), refreshed)
            self.assertEqual(post.call_count, 3)
            self.assertEqual(self.calendars.get_splits_calendar(force=True).index.tolist(), ["NEW"])
            self.assertEqual(post.call_count, 4)

    def test_earnings_market_cap_and_force_refresh_most_active_filter(self):
        quotes = [
            {"quotes": [{"symbol": "SMALL", "marketCap": 20_000_000},
                        {"symbol": "BIG", "marketCap": 200_000_000}]},
            {"quotes": [{"symbol": "SMALL", "marketCap": 20_000_000},
                        {"symbol": "BIG", "marketCap": 200_000_000}]},
            {"quotes": [{"symbol": "NEW", "marketCap": 200_000_000}]},
        ]
        with patch("yfinance.calendars.screen", side_effect=quotes) as screen, \
                patch.object(self.calendars._data, "post", return_value=_earnings_response()) as post:
            self.calendars.get_earnings_calendar(market_cap=100_000_000)
            self.calendars.get_earnings_calendar(market_cap=10_000_000)
            self.calendars.get_earnings_calendar(market_cap=10_000_000, force=True)
            self.calendars.get_earnings_calendar(market_cap=10_000_000)

        ticker_filters = [call.kwargs["body"]["query"]["operands"][-1]["operands"]
                          for call in post.call_args_list]
        self.assertEqual([[operand["operands"][1] for operand in group] for group in ticker_filters],
                         [["BIG"], ["SMALL", "BIG"], ["NEW"]])
        self.assertEqual(screen.call_count, 3)
        self.assertEqual(post.call_count, 3)


if __name__ == "__main__":
    unittest.main()

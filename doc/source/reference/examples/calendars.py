"""Calendars example."""

from datetime import datetime, timedelta

import yfinance as yf


def main():
    """Fetch several calendar views and a filtered earnings slice."""
    default_calendar = yf.Calendars()
    tomorrow = datetime.now() + timedelta(days=1)
    calendar = yf.Calendars(end=tomorrow)

    today = datetime.now()
    filtered_calendar = yf.Calendars(
        today,
        today + timedelta(days=4 if today.weekday() == 4 else 2),
    )
    earnings_df = filtered_calendar.get_earnings_calendar(limit=100)

    return {
        "default_calendar": default_calendar,
        "earnings_calendar": calendar.earnings_calendar,
        "ipo_info_calendar": calendar.ipo_info_calendar,
        "splits_calendar": calendar.splits_calendar,
        "economic_events_calendar": calendar.economic_events_calendar,
        "earnings_query": calendar.get_earnings_calendar(),
        "ipo_query": calendar.get_ipo_info_calendar(),
        "splits_query": calendar.get_splits_calendar(),
        "economic_events_query": calendar.get_economic_events_calendar(),
        "active_earnings_query": calendar.get_earnings_calendar(
            market_cap=100_000_000,
            filter_most_active=True,
        ),
        "unreported_df": earnings_df[earnings_df["Reported EPS"].isnull()],
    }

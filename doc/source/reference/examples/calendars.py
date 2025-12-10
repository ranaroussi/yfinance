import yfinance as yf
from datetime import datetime, timedelta

# Default init (today + 7 days)
calendar = yf.Calendars()

# Today's events: calendar of 1 day
tomorrow = datetime.now() + timedelta(days=1)
calendar = yf.Calendars(end=tomorrow)

# Default calendar queries - accessing the properties will fetch the data from YF
calendar.earnings_calendar
calendar.ipo_info_calendar
calendar.splits_calendar
calendar.economic_events_calendar

# Manual queries
calendar.get_earnings_calendar()
calendar.get_ipo_info_calendar()
calendar.get_splits_calendar()
calendar.get_economic_events_calendar()

# Earnings calendar custom filters
calendar.get_earnings_calendar(
    market_cap=100_000_000,  # filter out small-cap 
    filter_most_active=True,  # show only actively traded. Uses: `screen(query="MOST_ACTIVES")`
)

# Example of real use case:
# Get inminent unreported earnings events
today = datetime.now()
is_friday = today.weekday() == 4
day_after_tomorrow = today + timedelta(days=4 if is_friday else 2)

calendar = yf.Calendars(today, day_after_tomorrow)
df = calendar.get_earnings_calendar(limit=100)

unreported_df = df[df["Reported EPS"].isnull()]

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
import yfinance as yf


@pytest.fixture
def calendars():
    return yf.Calendars()


def test_get_earnings_calendar(calendars):
    result = calendars.get_earnings_calendar(limit=1)
    tickers = calendars.earnings_calendar.index.tolist()

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert isinstance(tickers, list)
    assert len(tickers) == len(result)
    assert tickers == result.index.tolist()

    first_ticker = result.index.tolist()[0]
    result_first_ticker = calendars.earnings_calendar.loc[first_ticker].name
    assert first_ticker == result_first_ticker


def test_get_earnings_calendar_init_params(calendars):
    result = calendars.get_earnings_calendar(limit=5)
    assert result["Event Start Date"].iloc[0] >= pd.to_datetime(datetime.now(tz=timezone.utc))

    start = datetime.now(tz=timezone.utc) - timedelta(days=7)
    result = yf.Calendars(start=start).get_earnings_calendar(limit=5)
    assert result["Event Start Date"].iloc[0] >= pd.to_datetime(start)


def test_get_ipo_info_calendar(calendars):
    result = calendars.get_ipo_info_calendar(limit=5)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 5


def test_get_economic_events_calendar(calendars):
    result = calendars.get_economic_events_calendar(limit=5)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 5


def test_get_splits_calendar(calendars):
    result = calendars.get_splits_calendar(limit=5)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 5

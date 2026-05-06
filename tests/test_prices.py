import datetime as _dt
import socket

import numpy as _np
import pandas as _pd
import pytz as _tz
import pytest

import yfinance as yf
from yfinance.data import _is_transient_error
from yfinance.exceptions import YFPricesMissingError


def test_daily_index():
    tkrs = ["BHP.AX", "IMP.JO", "BP.L", "PNL.L", "INTC"]
    intervals = ["1d", "1wk", "1mo"]
    for tkr in tkrs:
        dat = yf.Ticker(tkr)
        for interval in intervals:
            df = dat.history(period="5y", interval=interval)
            assert (_dt.time(0) == df.index.time).all(), \
                f"Non-zero times in {tkr} {interval}: {df.index[df.index.time != _dt.time(0)]}"


def test_download_multi_large_interval():
    tkrs = ["BHP.AX", "IMP.JO", "BP.L", "PNL.L", "INTC"]
    intervals = ["1d", "1wk", "1mo"]
    for interval in intervals:
        df = yf.download(tkrs, period="5y", interval=interval, progress=False)
        assert (_dt.time(0) == df.index.time).all(), \
            f"Non-zero times for interval={interval}"
        df_tkrs = df.columns.get_level_values("Ticker").unique().tolist()
        assert sorted(tkrs) == sorted(df_tkrs)


def test_download_multi_small_interval():
    use_tkrs = ["AAPL", "0Q3.DE", "ATVI"]
    df = yf.download(use_tkrs, period="1d", interval="5m", auto_adjust=True, progress=False)
    assert df.index.tz == _dt.timezone.utc


def test_download_with_invalid_ticker():
    invalid_tkrs = ["AAPL", "ATVI"]
    valid_tkrs = ["AAPL", "INTC"]

    start_d = _dt.date.today() - _dt.timedelta(days=30)
    data_invalid_sym = yf.download(invalid_tkrs, start=start_d, auto_adjust=True, progress=False)
    data_valid_sym = yf.download(valid_tkrs, start=start_d, auto_adjust=True, progress=False)
    dt_compare = data_valid_sym.index[0]
    assert data_invalid_sym['Close']['AAPL'][dt_compare] == data_valid_sym['Close']['AAPL'][dt_compare]


def test_duplicatingHourly():
    tkrs = ["IMP.JO", "BHG.JO", "SSW.JO", "BP.L", "INTC"]
    for tkr in tkrs:
        dat = yf.Ticker(tkr)
        tz = dat._get_ticker_tz(timeout=None)

        dt_utc = _pd.Timestamp.now('UTC')
        dt = dt_utc.astimezone(_tz.timezone(tz))
        start_d = dt.date() - _dt.timedelta(days=7)
        df = dat.history(start=start_d, interval="1h")

        dt0 = df.index[-2]
        dt1 = df.index[-1]
        assert dt0.hour != dt1.hour, f"Duplicate hour for ticker {tkr}"


def test_duplicatingDaily():
    tkrs = ["IMP.JO", "BHG.JO", "SSW.JO", "BP.L", "INTC"]
    test_run = False
    for tkr in tkrs:
        dat = yf.Ticker(tkr)
        tz = dat._get_ticker_tz(timeout=None)

        dt_utc = _pd.Timestamp.now('UTC')
        dt = dt_utc.astimezone(_tz.timezone(tz))
        if dt.time() < _dt.time(17, 0):
            continue
        test_run = True

        df = dat.history(start=dt.date() - _dt.timedelta(days=7), interval="1d")
        dt0 = df.index[-2]
        dt1 = df.index[-1]
        assert dt0 != dt1, f"Duplicate daily bar for ticker {tkr}"

    if not test_run:
        pytest.skip("Only expected to fail just after market close")


def test_duplicatingWeekly():
    tkrs = ['MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']
    test_run = False
    for tkr in tkrs:
        dat = yf.Ticker(tkr)
        tz = dat._get_ticker_tz(timeout=None)

        dt = _tz.timezone(tz).localize(_dt.datetime.now())
        if dt.date().weekday() not in [1, 2, 3, 4]:
            continue
        test_run = True

        df = dat.history(start=dt.date() - _dt.timedelta(days=7), interval="1wk")
        dt0 = df.index[-2]
        dt1 = df.index[-1]
        assert dt0.week != dt1.week, \
            f"Ticker={tkr}: last two rows within same week"

    if not test_run:
        pytest.skip("Not possible to fail Monday/weekend")


def test_pricesEventsMerge():
    tkr = 'INTC'
    start_d = _dt.date(2022, 1, 1)
    end_d = _dt.date(2023, 1, 1)
    df = yf.Ticker(tkr).history(interval='1d', start=start_d, end=end_d)
    div = 1.0
    future_div_dt = df.index[-1] + _dt.timedelta(days=1)
    if future_div_dt.weekday() in [5, 6]:
        future_div_dt += _dt.timedelta(days=1) * (7 - future_div_dt.weekday())
    divs = _pd.DataFrame(data={"Dividends": [div]}, index=[future_div_dt])
    df2 = yf.utils.safe_merge_dfs(df.drop(['Dividends', 'Stock Splits'], axis=1), divs, '1d')
    assert future_div_dt in df2.index
    assert "Dividends" in df2.columns
    assert df2['Dividends'].iloc[-1] == div


def test_pricesEventsMerge_bug():
    interval = '30m'
    df_index = []
    d = 13
    for h in range(0, 16):
        for m in [0, 30]:
            df_index.append(_dt.datetime(2023, 9, d, h, m))
    df_index.append(_dt.datetime(2023, 9, d, 16))
    df = _pd.DataFrame(index=df_index)
    df.index = _pd.to_datetime(df.index)
    df['Close'] = 1.0

    div = 1.0
    future_div_dt = _dt.datetime(2023, 9, 14, 10)
    divs = _pd.DataFrame(data={"Dividends": [div]}, index=[future_div_dt])

    yf.utils.safe_merge_dfs(df, divs, interval)
    # No exception = pass


def test_intraDayWithEvents():
    tkrs = ["BHP.AX", "IMP.JO", "BP.L", "PNL.L", "INTC"]
    test_run = False
    for tkr in tkrs:
        start_d = _dt.date.today() - _dt.timedelta(days=59)
        df_daily = yf.Ticker(tkr).history(start=start_d, end=None, interval="1d", actions=True)
        df_daily_divs = df_daily["Dividends"][df_daily["Dividends"] != 0]
        if df_daily_divs.shape[0] == 0:
            continue

        start_d = df_daily_divs.index[0].date()
        end_d = df_daily_divs.index[-1].date() + _dt.timedelta(days=1)
        df_intraday = yf.Ticker(tkr).history(start=start_d, end=end_d, interval="15m", actions=True)
        assert (df_intraday["Dividends"] != 0.0).any()

        df_intraday_divs = df_intraday["Dividends"][df_intraday["Dividends"] != 0]
        df_intraday_divs.index = df_intraday_divs.index.floor('D')
        assert df_daily_divs.index.equals(df_intraday_divs.index)
        test_run = True

    if not test_run:
        pytest.skip("No tickers had a dividend in last 60 days")


def test_intraDayWithEvents_tase():
    tase_tkrs = ["ICL.TA", "ESLT.TA", "ONE.TA", "MGDL.TA"]
    test_run = False
    for tkr in tase_tkrs:
        start_d = _dt.date.today() - _dt.timedelta(days=59)
        df_daily = yf.Ticker(tkr).history(start=start_d, end=None, interval="1d", actions=True)
        df_daily_divs = df_daily["Dividends"][df_daily["Dividends"] != 0]
        if df_daily_divs.shape[0] == 0:
            continue

        start_d = df_daily_divs.index[0].date()
        end_d = df_daily_divs.index[-1].date() + _dt.timedelta(days=1)
        df_intraday = yf.Ticker(tkr).history(start=start_d, end=end_d, interval="15m", actions=True)
        assert (df_intraday["Dividends"] != 0.0).any()

        df_intraday_divs = df_intraday["Dividends"][df_intraday["Dividends"] != 0]
        df_intraday_divs.index = df_intraday_divs.index.floor('D')
        assert df_daily_divs.index.equals(df_intraday_divs.index)
        test_run = True

    if not test_run:
        pytest.skip("No TASE tickers had a dividend in last 60 days")


def test_dailyWithEvents():
    start_d = _dt.date(2022, 1, 1)
    end_d = _dt.date(2023, 1, 1)
    tkr_div_dates = {
        'BHP.AX': [_dt.date(2022, 9, 1), _dt.date(2022, 2, 24)],
        'IMP.JO': [_dt.date(2022, 9, 21), _dt.date(2022, 3, 16)],
        'BP.L': [_dt.date(2022, 11, 10), _dt.date(2022, 8, 11),
                 _dt.date(2022, 5, 12), _dt.date(2022, 2, 17)],
        'INTC': [_dt.date(2022, 11, 4), _dt.date(2022, 8, 4),
                 _dt.date(2022, 5, 5), _dt.date(2022, 2, 4)],
    }
    for tkr, dates in tkr_div_dates.items():
        df = yf.Ticker(tkr).history(interval='1d', start=start_d, end=end_d)
        df_divs = df[df['Dividends'] != 0].sort_index(ascending=False)
        assert (df_divs.index.date == dates).all(), \
            f"ticker={tkr} got {df_divs.index.date} expected {dates}"


def test_dailyWithEvents_bugs():
    tkr1 = "QQQ"
    tkr2 = "GDX"
    start_d = "2014-12-29"
    end_d = "2020-11-29"
    df1 = yf.Ticker(tkr1).history(start=start_d, end=end_d, interval="1d", actions=True)
    df2 = yf.Ticker(tkr2).history(start=start_d, end=end_d, interval="1d", actions=True)
    assert ((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any()
    assert ((df2["Dividends"] > 0) | (df2["Stock Splits"] > 0)).any()
    assert df1.index.equals(df2.index), \
        f"QQQ missing: {df2.index.difference(df1.index)}, GDX missing: {df1.index.difference(df2.index)}"

    for tkr in [tkr1, tkr2]:
        df_a = yf.Ticker(tkr).history(start=start_d, end=end_d, interval="1d", actions=True)
        df_b = yf.Ticker(tkr).history(start=start_d, end=end_d, interval="1d", actions=False)
        assert ((df_a["Dividends"] > 0) | (df_a["Stock Splits"] > 0)).any()
        assert df_a.index.equals(df_b.index)

    div_dt = _pd.Timestamp(2022, 7, 21).tz_localize("America/New_York")
    df_dividends = _pd.DataFrame(data={"Dividends": [1.0]}, index=[div_dt])
    df_prices = _pd.DataFrame(
        data={c: [1.0] for c in yf.const._PRICE_COLNAMES_} | {'Volume': 0},
        index=[div_dt + _dt.timedelta(days=1)]
    )
    df_merged = yf.utils.safe_merge_dfs(df_prices, df_dividends, '1d')
    assert df_merged.shape[0] == 2
    assert df_merged[df_prices.columns].iloc[1:].equals(df_prices)
    assert df_merged.index[0] == div_dt


def test_weeklyWithEvents():
    tkr1 = "QQQ"
    tkr2 = "GDX"
    start_d = "2014-12-29"
    end_d = "2020-11-29"
    df1 = yf.Ticker(tkr1).history(start=start_d, end=end_d, interval="1wk", actions=True)
    df2 = yf.Ticker(tkr2).history(start=start_d, end=end_d, interval="1wk", actions=True)
    assert ((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any()
    assert ((df2["Dividends"] > 0) | (df2["Stock Splits"] > 0)).any()
    assert df1.index.equals(df2.index)

    for tkr in [tkr1, tkr2]:
        df_a = yf.Ticker(tkr).history(start=start_d, end=end_d, interval="1wk", actions=True)
        df_b = yf.Ticker(tkr).history(start=start_d, end=end_d, interval="1wk", actions=False)
        assert ((df_a["Dividends"] > 0) | (df_a["Stock Splits"] > 0)).any()
        assert df_a.index.equals(df_b.index)


def test_monthlyWithEvents():
    tkr1 = "QQQ"
    tkr2 = "GDX"
    start_d = "2014-12-29"
    end_d = "2020-11-29"
    df1 = yf.Ticker(tkr1).history(start=start_d, end=end_d, interval="1mo", actions=True)
    df2 = yf.Ticker(tkr2).history(start=start_d, end=end_d, interval="1mo", actions=True)
    assert ((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any()
    assert ((df2["Dividends"] > 0) | (df2["Stock Splits"] > 0)).any()
    assert df1.index.equals(df2.index)

    for tkr in [tkr1, tkr2]:
        df_a = yf.Ticker(tkr).history(start=start_d, end=end_d, interval="1mo", actions=True)
        df_b = yf.Ticker(tkr).history(start=start_d, end=end_d, interval="1mo", actions=False)
        assert ((df_a["Dividends"] > 0) | (df_a["Stock Splits"] > 0)).any()
        assert df_a.index.equals(df_b.index)


def test_monthlyWithEvents2():
    dfm = yf.Ticker("ABBV").history(period="max", interval="1mo")
    dfd = yf.Ticker("ABBV").history(period="max", interval="1d")
    dfd = dfd[dfd.index > dfm.index[0]]
    dfm_divs = dfm[dfm['Dividends'] != 0]
    dfd_divs = dfd[dfd['Dividends'] != 0]
    assert dfm_divs.shape[0] == dfd_divs.shape[0]


def test_tz_dst_ambiguous():
    try:
        yf.Ticker("ESLT.TA").history(start="2002-10-06", end="2002-10-09", interval="1d")
    except _tz.exceptions.AmbiguousTimeError:
        raise Exception("Ambiguous DST issue not resolved")


def test_dst_fix():
    tkr = "AGRO3.SA"
    dat = yf.Ticker(tkr)
    start = "2021-01-11"
    end = "2022-11-05"

    df = dat.history(start=start, end=end, interval="1d")
    assert ((df.index.weekday >= 0) & (df.index.weekday <= 4)).all(), \
        "Daily data contains weekend dates"

    df = dat.history(start=start, end=end, interval="1wk")
    assert (df.index.weekday == 0).all(), "Weekly data not aligned to Monday"


@pytest.mark.skip(reason="Need to investigate how to mock this properly.")
def test_prune_post_intraday_us():
    tkr = "AMZN"
    special_day = _dt.date(2024, 11, 29)
    time_early_close = _dt.time(13)
    dat = yf.Ticker(tkr)

    start_d = special_day - _dt.timedelta(days=7)
    end_d = special_day + _dt.timedelta(days=7)
    df = dat.history(start=start_d, end=end_d, interval="1h", prepost=False, keepna=True)
    tg_last_dt = df.loc[str(special_day)].index[-1]
    assert tg_last_dt.time() < time_early_close

    start_d = _dt.date(special_day.year, 1, 1)
    end_d = _dt.date(special_day.year + 1, 1, 1)
    df = dat.history(start=start_d, end=end_d, interval="1h", prepost=False, keepna=True)
    if df.empty:
        pytest.skip("TEST NEEDS UPDATE: 'special_day' needs to be LATEST Thanksgiving date")
    last_dts = _pd.Series(df.index).groupby(df.index.date).last()
    dfd = dat.history(start=start_d, end=end_d, interval='1d', prepost=False, keepna=True)
    assert _np.equal(dfd.index.date, _pd.to_datetime(last_dts.index).date).all()


@pytest.mark.skip(reason="Need to investigate how to mock this properly.")
def test_prune_post_intraday_asx():
    tkr = "BHP.AX"
    # No early closes in 2024
    dat = yf.Ticker(tkr)
    start_d = _dt.date(2024, 1, 1)
    end_d = _dt.date(2024 + 1, 1, 1)
    df = dat.history(start=start_d, end=end_d, interval="1h", prepost=False, keepna=True)
    last_dts = _pd.Series(df.index).groupby(df.index.date).last()
    dfd = dat.history(start=start_d, end=end_d, interval='1d', prepost=False, keepna=True)
    assert _np.equal(dfd.index.date, _pd.to_datetime(last_dts.index).date).all()


def test_weekly_2rows_fix():
    tkr = "AMZN"
    start = _dt.date.today() - _dt.timedelta(days=14)
    start -= _dt.timedelta(days=start.weekday())  # snap to Monday

    dat = yf.Ticker(tkr)
    df = dat.history(start=start, interval="1wk")
    assert (df.index.weekday == 0).all(), "Weekly bars not aligned to Monday"


def test_aggregate_capital_gains():
    tkr = "FXAIX"
    dat = yf.Ticker(tkr)
    start = "2017-12-31"
    end = "2019-12-31"
    interval = "3mo"
    dat.history(start=start, end=end, interval=interval)
    # No exception = pass


def test_transient_error_detection():
    assert _is_transient_error(socket.error("Network error"))
    assert _is_transient_error(TimeoutError("Timeout"))
    assert _is_transient_error(OSError("OS error"))

    assert not _is_transient_error(ValueError("Invalid"))
    assert not _is_transient_error(YFPricesMissingError('INVALID', ''))
    assert not _is_transient_error(KeyError("key"))


def test_invalid_ticker_raises_prices_missing_not_type_error():
    dat = yf.Ticker("TICKER_DOES_NOT_EXIST_XYZ")
    yf.config.debug.hide_exceptions = False
    with pytest.raises(YFPricesMissingError):
        dat.history(period="1mo")

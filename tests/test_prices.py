"""Integration tests for price history behavior."""

import datetime as _dt
import socket
from typing import cast
import unittest

import numpy as _np
import pandas as _pd
import pytz as _tz

from tests.context import SESSION_GBL
from tests.context import yfinance as yf
from yfinance.data import _is_transient_error
from yfinance.exceptions import YFPricesMissingError

_PRICE_COLUMNS = ["Open", "High", "Low", "Close", "Adj Close"]


def _as_datetime_index(index: _pd.Index) -> _pd.DatetimeIndex:
    assert isinstance(index, _pd.DatetimeIndex)
    return index


def _as_multi_index(index: _pd.Index) -> _pd.MultiIndex:
    assert isinstance(index, _pd.MultiIndex)
    return index


def _index_dates(index: _pd.Index) -> _np.ndarray:
    dt_index = _as_datetime_index(index)
    return _np.array([ts.date() for ts in dt_index], dtype=object)


def _index_times(index: _pd.Index) -> _np.ndarray:
    dt_index = _as_datetime_index(index)
    return _np.array([ts.time() for ts in dt_index], dtype=object)


def _index_weekdays(index: _pd.Index) -> _np.ndarray:
    dt_index = _as_datetime_index(index)
    return _np.array([ts.weekday() for ts in dt_index], dtype=int)


def _first_index_date(index: _pd.Index) -> _dt.date:
    dt_index = _as_datetime_index(index)
    return cast(_pd.Timestamp, list(dt_index)[0]).date()


def _last_index_date(index: _pd.Index) -> _dt.date:
    dt_index = _as_datetime_index(index)
    return cast(_pd.Timestamp, list(dt_index)[-1]).date()


def _ticker_timezone(dat: yf.Ticker) -> str:
    metadata = dat.get_history_metadata()
    tz_name = metadata.get("exchangeTimezoneName")
    assert isinstance(tz_name, str)
    return tz_name


class TestPriceHistory(unittest.TestCase):
    """Core price-history regression tests."""

    @classmethod
    def setUpClass(cls):
        """Attach the shared test session."""
        cls.session = SESSION_GBL

    @classmethod
    def tearDownClass(cls):
        """Close the shared test session."""
        if cls.session is not None:
            cls.session.close()

    def test_daily_index(self):
        """Ensure daily-like intervals are aligned to midnight."""
        tkrs = ["BHP.AX", "IMP.JO", "BP.L", "PNL.L", "INTC"]
        intervals = ["1d", "1wk", "1mo"]
        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)

            for interval in intervals:
                df = dat.history(period="5y", interval=interval)
                self.assertTrue((_index_times(df.index) == _dt.time(0)).all())

    def test_download_multi_large_interval(self):
        """Validate multi-ticker downloads for daily/weekly/monthly intervals."""
        tkrs = ["BHP.AX", "IMP.JO", "BP.L", "PNL.L", "INTC"]
        intervals = ["1d", "1wk", "1mo"]
        for interval in intervals:
            with self.subTest(interval):
                df = yf.download(tkrs, period="5y", interval=interval)
                assert df is not None

                self.assertTrue((_index_times(df.index) == _dt.time(0)).all())
                df_tkrs = _as_multi_index(df.columns).levels[1]
                self.assertEqual(sorted(tkrs), sorted(df_tkrs))

    def test_download_multi_small_interval(self):
        """Validate multi-ticker intraday download timezone normalization."""
        use_tkrs = ["AAPL", "0Q3.DE", "ATVI"]
        df = yf.download(use_tkrs, period="1d", interval="5m", auto_adjust=True)
        assert df is not None
        self.assertEqual(_as_datetime_index(df.index).tz, _dt.timezone.utc)

    def test_download_with_invalid_ticker(self):
        """Ensure one invalid ticker does not alter valid ticker values."""
        invalid_tkrs = ["AAPL", "ATVI"]
        valid_tkrs = ["AAPL", "INTC"]

        start_d = _dt.date.today() - _dt.timedelta(days=30)
        data_invalid_sym = yf.download(invalid_tkrs, start=start_d, auto_adjust=True)
        data_valid_sym = yf.download(valid_tkrs, start=start_d, auto_adjust=True)
        assert data_invalid_sym is not None
        assert data_valid_sym is not None

        dt_compare = _as_datetime_index(data_valid_sym.index)[0]
        self.assertEqual(
            data_invalid_sym["Close"]["AAPL"][dt_compare],
            data_valid_sym["Close"]["AAPL"][dt_compare],
        )

    def test_duplicating_hourly(self):
        """Ensure hourly history does not duplicate final hour rows."""
        tkrs = ["IMP.JO", "BHG.JO", "SSW.JO", "BP.L", "INTC"]
        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)
            tz_name = _ticker_timezone(dat)

            dt_utc = _pd.Timestamp.now("UTC")
            dt_local = dt_utc.astimezone(_tz.timezone(tz_name))
            start_d = dt_local.date() - _dt.timedelta(days=7)
            df = dat.history(start=start_d, interval="1h")

            dt_index = _as_datetime_index(df.index)
            dt0 = cast(_pd.Timestamp, dt_index[-2])
            dt1 = cast(_pd.Timestamp, dt_index[-1])
            try:
                self.assertNotEqual(dt0.hour, dt1.hour)
            except AssertionError:
                print("Ticker =", tkr)
                raise

    def test_duplicating_daily(self):
        """Ensure daily history does not duplicate adjacent rows."""
        tkrs = ["IMP.JO", "BHG.JO", "SSW.JO", "BP.L", "INTC"]
        test_run = False
        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)
            tz_name = _ticker_timezone(dat)

            dt_utc = _pd.Timestamp.now("UTC")
            dt_local = dt_utc.astimezone(_tz.timezone(tz_name))
            if dt_local.time() < _dt.time(17, 0):
                continue
            test_run = True

            df = dat.history(start=dt_local.date() - _dt.timedelta(days=7), interval="1d")
            dt_index = _as_datetime_index(df.index)
            dt0 = dt_index[-2]
            dt1 = dt_index[-1]
            try:
                self.assertNotEqual(dt0, dt1)
            except AssertionError:
                print("Ticker =", tkr)
                raise

        if not test_run:
            self.skipTest(
                "Skipping test_duplicating_daily() because this only fails right "
                "after market close."
            )

    def test_duplicating_weekly(self):
        """Ensure weekly history rolls into a new ISO week."""
        tkrs = ["MSFT", "IWO", "VFINX", "^GSPC", "BTC-USD"]
        test_run = False
        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)
            tz_name = _ticker_timezone(dat)

            dt_local = _tz.timezone(tz_name).localize(_dt.datetime.now())
            if dt_local.date().weekday() not in [1, 2, 3, 4]:
                continue
            test_run = True

            df = dat.history(start=dt_local.date() - _dt.timedelta(days=7), interval="1wk")
            dt_index = _as_datetime_index(df.index)
            dt0 = dt_index[-2]
            dt1 = dt_index[-1]
            try:
                self.assertNotEqual(dt0.isocalendar().week, dt1.isocalendar().week)
            except AssertionError:
                print(f"Ticker={tkr}: Last two rows within same week:")
                print(df.iloc[df.shape[0] - 2 :])
                raise

        if not test_run:
            self.skipTest(
                "Skipping test_duplicating_weekly() because it cannot fail on "
                "Monday/weekends."
            )

    def test_prices_events_merge(self):
        """Ensure future dividend rows are merged into daily price data."""
        tkr = "INTC"
        start_d = _dt.date(2022, 1, 1)
        end_d = _dt.date(2023, 1, 1)
        df = yf.Ticker(tkr, session=self.session).history(
            interval="1d", start=start_d, end=end_d
        )
        div = 1.0
        future_div_dt = df.index[-1] + _dt.timedelta(days=1)
        if future_div_dt.weekday() in [5, 6]:
            future_div_dt += _dt.timedelta(days=1) * (7 - future_div_dt.weekday())
        divs = _pd.DataFrame(data={"Dividends": [div]}, index=[future_div_dt])
        df2 = yf.utils.safe_merge_dfs(
            df.drop(["Dividends", "Stock Splits"], axis=1), divs, "1d"
        )
        self.assertIn(future_div_dt, df2.index)
        self.assertIn("Dividends", df2.columns)
        self.assertEqual(df2["Dividends"].iloc[-1], div)

    def test_prices_events_merge_bug(self):
        """Reproduce and guard intraday merge bug with future dividends."""
        interval = "30m"
        df_index = []
        day = 13
        for hour in range(0, 16):
            for minute in [0, 30]:
                df_index.append(_dt.datetime(2023, 9, day, hour, minute))
        df_index.append(_dt.datetime(2023, 9, day, 16))
        df = _pd.DataFrame(index=df_index)
        df.index = _pd.to_datetime(df.index)
        df["Close"] = 1.0

        div = 1.0
        future_div_dt = _dt.datetime(2023, 9, 14, 10)
        divs = _pd.DataFrame(data={"Dividends": [div]}, index=[future_div_dt])

        yf.utils.safe_merge_dfs(df, divs, interval)

    def test_intraday_with_events(self):
        """Ensure intraday data preserves dividend events."""
        tkrs = ["BHP.AX", "IMP.JO", "BP.L", "PNL.L", "INTC"]
        test_run = False
        for tkr in tkrs:
            start_d = _dt.date.today() - _dt.timedelta(days=59)
            df_daily = yf.Ticker(tkr, session=self.session).history(
                start=start_d,
                end=None,
                interval="1d",
                actions=True,
            )
            df_daily_divs = cast(_pd.Series, df_daily["Dividends"][df_daily["Dividends"] != 0])
            if df_daily_divs.shape[0] == 0:
                continue

            start_d = _first_index_date(df_daily_divs.index)
            end_d = _last_index_date(df_daily_divs.index) + _dt.timedelta(days=1)
            df_intraday = yf.Ticker(tkr, session=self.session).history(
                start=start_d,
                end=end_d,
                interval="15m",
                actions=True,
            )
            self.assertTrue((df_intraday["Dividends"] != 0.0).any())

            df_intraday_divs = cast(
                _pd.Series,
                df_intraday["Dividends"][df_intraday["Dividends"] != 0],
            )
            intraday_div_index = _as_datetime_index(df_intraday_divs.index)
            df_intraday_divs.index = _pd.to_datetime([dt.date() for dt in intraday_div_index])
            self.assertTrue(df_daily_divs.index.equals(df_intraday_divs.index))

            test_run = True

        if not test_run:
            self.skipTest(
                "Skipping test_intraday_with_events() because no tickers had a "
                "dividend in the last 60 days."
            )

    def test_intraday_with_events_tase(self):
        """Ensure intraday TASE data keeps dividends despite pre-market release."""
        tase_tkrs = ["ICL.TA", "ESLT.TA", "ONE.TA", "MGDL.TA"]
        test_run = False
        for tkr in tase_tkrs:
            start_d = _dt.date.today() - _dt.timedelta(days=59)
            df_daily = yf.Ticker(tkr, session=self.session).history(
                start=start_d,
                end=None,
                interval="1d",
                actions=True,
            )
            df_daily_divs = cast(_pd.Series, df_daily["Dividends"][df_daily["Dividends"] != 0])
            if df_daily_divs.shape[0] == 0:
                continue

            start_d = _first_index_date(df_daily_divs.index)
            end_d = _last_index_date(df_daily_divs.index) + _dt.timedelta(days=1)
            df_intraday = yf.Ticker(tkr, session=self.session).history(
                start=start_d,
                end=end_d,
                interval="15m",
                actions=True,
            )
            self.assertTrue((df_intraday["Dividends"] != 0.0).any())

            df_intraday_divs = cast(
                _pd.Series,
                df_intraday["Dividends"][df_intraday["Dividends"] != 0],
            )
            intraday_div_index = _as_datetime_index(df_intraday_divs.index)
            df_intraday_divs.index = _pd.to_datetime([dt.date() for dt in intraday_div_index])
            self.assertTrue(df_daily_divs.index.equals(df_intraday_divs.index))

            test_run = True

        if not test_run:
            self.skipTest(
                "Skipping test_intraday_with_events_tase() because no tickers had "
                "a dividend in the last 60 days."
            )

    def test_daily_with_events(self):
        """Validate known daily dividend dates for selected tickers."""
        start_d = _dt.date(2022, 1, 1)
        end_d = _dt.date(2023, 1, 1)

        tkr_div_dates = {
            "BHP.AX": [_dt.date(2022, 9, 1), _dt.date(2022, 2, 24)],
            "IMP.JO": [_dt.date(2022, 9, 21), _dt.date(2022, 3, 16)],
            "BP.L": [
                _dt.date(2022, 11, 10),
                _dt.date(2022, 8, 11),
                _dt.date(2022, 5, 12),
                _dt.date(2022, 2, 17),
            ],
            "INTC": [
                _dt.date(2022, 11, 4),
                _dt.date(2022, 8, 4),
                _dt.date(2022, 5, 5),
                _dt.date(2022, 2, 4),
            ],
        }

        for tkr, dates in tkr_div_dates.items():
            df = yf.Ticker(tkr, session=self.session).history(
                interval="1d", start=start_d, end=end_d
            )
            df_divs = df[df["Dividends"] != 0].sort_index(ascending=False)
            div_dates = _index_dates(df_divs.index)
            try:
                self.assertTrue((div_dates == dates).all())
            except AssertionError:
                print(f"- ticker = {tkr}")
                print("- response:")
                print(div_dates)
                print("- answer:")
                print(dates)
                raise

    def test_daily_with_events_bugs(self):
        """Cover daily event merge regressions from known issues."""
        tkr1 = "QQQ"
        tkr2 = "GDX"
        start_d = "2014-12-29"
        end_d = "2020-11-29"
        df1 = yf.Ticker(tkr1).history(start=start_d, end=end_d, interval="1d", actions=True)
        df2 = yf.Ticker(tkr2).history(start=start_d, end=end_d, interval="1d", actions=True)
        self.assertTrue(((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any())
        self.assertTrue(((df2["Dividends"] > 0) | (df2["Stock Splits"] > 0)).any())
        try:
            self.assertTrue(df1.index.equals(df2.index))
        except AssertionError:
            missing_from_df1 = df2.index.difference(df1.index)
            missing_from_df2 = df1.index.difference(df2.index)
            print(f"{tkr1} missing these dates: {missing_from_df1}")
            print(f"{tkr2} missing these dates: {missing_from_df2}")
            raise

        for tkr in [tkr1, tkr2]:
            df1 = yf.Ticker(tkr, session=self.session).history(
                start=start_d, end=end_d, interval="1d", actions=True
            )
            df2 = yf.Ticker(tkr, session=self.session).history(
                start=start_d, end=end_d, interval="1d", actions=False
            )
            self.assertTrue(((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any())
            try:
                self.assertTrue(df1.index.equals(df2.index))
            except AssertionError:
                missing_from_df1 = df2.index.difference(df1.index)
                missing_from_df2 = df1.index.difference(df2.index)
                print(f"{tkr}-with-events missing these dates: {missing_from_df1}")
                print(f"{tkr}-without-events missing these dates: {missing_from_df2}")
                raise

        div_dt = _pd.Timestamp(2022, 7, 21).tz_localize("America/New_York")
        df_dividends = _pd.DataFrame(data={"Dividends": [1.0]}, index=[div_dt])
        df_prices = _pd.DataFrame(
            data={c: [1.0] for c in _PRICE_COLUMNS} | {"Volume": 0},
            index=[div_dt + _dt.timedelta(days=1)],
        )
        df_merged = yf.utils.safe_merge_dfs(df_prices, df_dividends, "1d")
        self.assertEqual(df_merged.shape[0], 2)
        self.assertTrue(df_merged[df_prices.columns].iloc[1:].equals(df_prices))
        self.assertEqual(df_merged.index[0], div_dt)


class TestPriceHistoryAdditional(unittest.TestCase):
    """Additional price-history regression tests."""

    @classmethod
    def setUpClass(cls):
        """Attach the shared test session."""
        cls.session = SESSION_GBL

    @classmethod
    def tearDownClass(cls):
        """Close the shared test session."""
        if cls.session is not None:
            cls.session.close()

    def test_weekly_with_events(self):
        """Cover weekly event merge regressions from known issues."""
        tkr1 = "QQQ"
        tkr2 = "GDX"
        start_d = "2014-12-29"
        end_d = "2020-11-29"
        df1 = yf.Ticker(tkr1).history(start=start_d, end=end_d, interval="1wk", actions=True)
        df2 = yf.Ticker(tkr2).history(start=start_d, end=end_d, interval="1wk", actions=True)
        self.assertTrue(((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any())
        self.assertTrue(((df2["Dividends"] > 0) | (df2["Stock Splits"] > 0)).any())
        try:
            self.assertTrue(df1.index.equals(df2.index))
        except AssertionError:
            missing_from_df1 = df2.index.difference(df1.index)
            missing_from_df2 = df1.index.difference(df2.index)
            print(f"{tkr1} missing these dates: {missing_from_df1}")
            print(f"{tkr2} missing these dates: {missing_from_df2}")
            raise

        for tkr in [tkr1, tkr2]:
            df1 = yf.Ticker(tkr, session=self.session).history(
                start=start_d, end=end_d, interval="1wk", actions=True
            )
            df2 = yf.Ticker(tkr, session=self.session).history(
                start=start_d, end=end_d, interval="1wk", actions=False
            )
            self.assertTrue(((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any())
            try:
                self.assertTrue(df1.index.equals(df2.index))
            except AssertionError:
                missing_from_df1 = df2.index.difference(df1.index)
                missing_from_df2 = df1.index.difference(df2.index)
                print(f"{tkr}-with-events missing these dates: {missing_from_df1}")
                print(f"{tkr}-without-events missing these dates: {missing_from_df2}")
                raise

    def test_monthly_with_events(self):
        """Cover monthly event merge regressions from known issues."""
        tkr1 = "QQQ"
        tkr2 = "GDX"
        start_d = "2014-12-29"
        end_d = "2020-11-29"
        df1 = yf.Ticker(tkr1).history(start=start_d, end=end_d, interval="1mo", actions=True)
        df2 = yf.Ticker(tkr2).history(start=start_d, end=end_d, interval="1mo", actions=True)
        self.assertTrue(((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any())
        self.assertTrue(((df2["Dividends"] > 0) | (df2["Stock Splits"] > 0)).any())
        try:
            self.assertTrue(df1.index.equals(df2.index))
        except AssertionError:
            missing_from_df1 = df2.index.difference(df1.index)
            missing_from_df2 = df1.index.difference(df2.index)
            print(f"{tkr1} missing these dates: {missing_from_df1}")
            print(f"{tkr2} missing these dates: {missing_from_df2}")
            raise

        for tkr in [tkr1, tkr2]:
            df1 = yf.Ticker(tkr, session=self.session).history(
                start=start_d, end=end_d, interval="1mo", actions=True
            )
            df2 = yf.Ticker(tkr, session=self.session).history(
                start=start_d, end=end_d, interval="1mo", actions=False
            )
            self.assertTrue(((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any())
            try:
                self.assertTrue(df1.index.equals(df2.index))
            except AssertionError:
                missing_from_df1 = df2.index.difference(df1.index)
                missing_from_df2 = df1.index.difference(df2.index)
                print(f"{tkr}-with-events missing these dates: {missing_from_df1}")
                print(f"{tkr}-without-events missing these dates: {missing_from_df2}")
                raise

    def test_monthly_with_events_2(self):
        """Ensure monthly/dividend merge consistency for long histories."""
        dfm = yf.Ticker("ABBV").history(period="max", interval="1mo")
        dfd = yf.Ticker("ABBV").history(period="max", interval="1d")
        dfd = dfd[dfd.index > dfm.index[0]]
        dfm_divs = dfm[dfm["Dividends"] != 0]
        dfd_divs = dfd[dfd["Dividends"] != 0]
        self.assertEqual(dfm_divs.shape[0], dfd_divs.shape[0])

    def test_tz_dst_ambiguous(self):
        """Ensure ambiguous DST dates do not raise timezone ambiguity errors."""
        try:
            yf.Ticker("ESLT.TA", session=self.session).history(
                start="2002-10-06",
                end="2002-10-09",
                interval="1d",
            )
        except _tz.exceptions.AmbiguousTimeError as exc:
            raise AssertionError("Ambiguous DST issue not resolved") from exc

    def test_dst_fix(self):
        """Validate daily/weekly timezone correction around DST transitions."""
        tkr = "AGRO3.SA"
        dat = yf.Ticker(tkr, session=self.session)
        start = "2021-01-11"
        end = "2022-11-05"

        df = dat.history(start=start, end=end, interval="1d")
        weekdays = _index_weekdays(df.index)
        self.assertTrue(((weekdays >= 0) & (weekdays <= 4)).all())

        df = dat.history(start=start, end=end, interval="1wk")
        try:
            self.assertTrue((_index_weekdays(df.index) == 0).all())
        except AssertionError:
            print("Weekly data not aligned to Monday")
            raise

    def test_prune_post_intraday_us(self):
        """Ensure U.S. half-day intraday pruning behavior remains correct."""
        tkr = "AMZN"
        special_day = _dt.date(2024, 11, 29)
        time_early_close = _dt.time(13)
        dat = yf.Ticker(tkr, session=self.session)

        start_d = special_day - _dt.timedelta(days=7)
        end_d = special_day + _dt.timedelta(days=7)
        df = dat.history(
            start=start_d,
            end=end_d,
            interval="1h",
            prepost=False,
            keepna=True,
        )
        tg_last_dt = df.loc[str(special_day)].index[-1]
        self.assertTrue(tg_last_dt.time() < time_early_close)

        start_d = _dt.date(special_day.year, 1, 1)
        end_d = _dt.date(special_day.year + 1, 1, 1)
        df = dat.history(
            start=start_d,
            end=end_d,
            interval="1h",
            prepost=False,
            keepna=True,
        )
        if df.empty:
            self.skipTest(
                "TEST NEEDS UPDATE: 'special_day' should be the latest "
                "Thanksgiving date."
            )
        df_dates = _index_dates(df.index)
        last_dts = _pd.Series(df.index).groupby(df_dates).last()
        dfd = dat.history(
            start=start_d,
            end=end_d,
            interval="1d",
            prepost=False,
            keepna=True,
        )
        self.assertTrue(
            _np.equal(
                _index_dates(dfd.index),
                _index_dates(_pd.to_datetime(last_dts.index)),
            ).all()
        )

    def test_prune_post_intraday_asx(self):
        """Ensure ASX intraday sessions are not over-pruned."""
        tkr = "BHP.AX"
        dat = yf.Ticker(tkr, session=self.session)

        end_d = _dt.date.today() - _dt.timedelta(days=1)
        start_d = end_d - _dt.timedelta(days=180)
        df = dat.history(
            start=start_d,
            end=end_d,
            interval="1h",
            prepost=False,
            keepna=True,
        )
        if df.empty or not hasattr(df.index, "date"):
            self.skipTest("No 1h data available for BHP.AX in this date range")
        df_dates = _index_dates(df.index)
        last_dts = _pd.Series(df.index).groupby(df_dates).last()
        dfd = dat.history(
            start=start_d,
            end=end_d,
            interval="1d",
            prepost=False,
            keepna=True,
        )
        self.assertTrue(
            _np.equal(
                _index_dates(dfd.index),
                _index_dates(_pd.to_datetime(last_dts.index)),
            ).all()
        )

    def test_weekly_2rows_fix(self):
        """Ensure two-row weekly histories still align to Monday."""
        tkr = "AMZN"
        start = _dt.date.today() - _dt.timedelta(days=14)
        start -= _dt.timedelta(days=start.weekday())

        dat = yf.Ticker(tkr)
        df = dat.history(start=start, interval="1wk")
        self.assertTrue((_index_weekdays(df.index) == 0).all())

    def test_aggregate_capital_gains(self):
        """Ensure capital gains aggregation path runs without errors."""
        tkr = "FXAIX"
        dat = yf.Ticker(tkr, session=self.session)
        start = "2017-12-31"
        end = "2019-12-31"
        interval = "3mo"
        dat.history(start=start, end=end, interval=interval)

    def test_transient_error_detection(self):
        """Validate transient-error detection for retryable exceptions."""
        self.assertTrue(_is_transient_error(socket.error("Network error")))
        self.assertTrue(_is_transient_error(TimeoutError("Timeout")))
        self.assertTrue(_is_transient_error(OSError("OS error")))

        self.assertFalse(_is_transient_error(ValueError("Invalid")))
        self.assertFalse(_is_transient_error(YFPricesMissingError("INVALID", "")))
        self.assertFalse(_is_transient_error(KeyError("key")))


if __name__ == "__main__":
    unittest.main()

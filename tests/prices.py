from .context import yfinance as yf

import unittest

import datetime as _dt
import pytz as _tz
import numpy as _np
import pandas as _pd

import requests_cache


class TestPriceHistory(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = requests_cache.CachedSession(backend='memory')

    @classmethod
    def tearDownClass(cls):
        if cls.session is not None:
            cls.session.close()

    def test_daily_index(self):
        tkrs = ["BHP.AX", "IMP.JO", "BP.L", "PNL.L", "INTC"]
        intervals = ["1d", "1wk", "1mo"]
        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)

            for interval in intervals:
                df = dat.history(period="5y", interval=interval)

                f = df.index.time == _dt.time(0)
                self.assertTrue(f.all())

    def test_duplicatingHourly(self):
        tkrs = ["IMP.JO", "BHG.JO", "SSW.JO", "BP.L", "INTC"]
        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)
            tz = dat._get_ticker_tz(debug_mode=False, proxy=None, timeout=None)

            dt_utc = _tz.timezone("UTC").localize(_dt.datetime.utcnow())
            dt = dt_utc.astimezone(_tz.timezone(tz))
            start_d = dt.date() - _dt.timedelta(days=7)
            df = dat.history(start=start_d, interval="1h")

            dt0 = df.index[-2]
            dt1 = df.index[-1]
            try:
                self.assertNotEqual(dt0.hour, dt1.hour)
            except:
                print("Ticker = ", tkr)
                raise

    def test_duplicatingDaily(self):
        tkrs = ["IMP.JO", "BHG.JO", "SSW.JO", "BP.L", "INTC"]
        test_run = False
        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)
            tz = dat._get_ticker_tz(debug_mode=False, proxy=None, timeout=None)

            dt_utc = _tz.timezone("UTC").localize(_dt.datetime.utcnow())
            dt = dt_utc.astimezone(_tz.timezone(tz))
            if dt.time() < _dt.time(17, 0):
                continue
            test_run = True

            df = dat.history(start=dt.date() - _dt.timedelta(days=7), interval="1d")

            dt0 = df.index[-2]
            dt1 = df.index[-1]
            try:
                self.assertNotEqual(dt0, dt1)
            except:
                print("Ticker = ", tkr)
                raise

        if not test_run:
            self.skipTest("Skipping test_duplicatingDaily() because only expected to fail just after market close")

    def test_duplicatingWeekly(self):
        tkrs = ['MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']
        test_run = False
        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)
            tz = dat._get_ticker_tz(debug_mode=False, proxy=None, timeout=None)

            dt = _tz.timezone(tz).localize(_dt.datetime.now())
            if dt.date().weekday() not in [1, 2, 3, 4]:
                continue
            test_run = True

            df = dat.history(start=dt.date() - _dt.timedelta(days=7), interval="1wk")
            dt0 = df.index[-2]
            dt1 = df.index[-1]
            try:
                self.assertNotEqual(dt0.week, dt1.week)
            except:
                print("Ticker={}: Last two rows within same week:".format(tkr))
                print(df.iloc[df.shape[0] - 2:])
                raise

        if not test_run:
            self.skipTest("Skipping test_duplicatingWeekly() because not possible to fail Monday/weekend")

    def test_intraDayWithEvents(self):
        # TASE dividend release pre-market, doesn't merge nicely with intra-day data so check still present

        tase_tkrs = ["ICL.TA", "ESLT.TA", "ONE.TA", "MGDL.TA"]
        test_run = False
        for tkr in tase_tkrs:
            start_d = _dt.date.today() - _dt.timedelta(days=59)
            end_d = None
            df_daily = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="1d", actions=True)
            df_daily_divs = df_daily["Dividends"][df_daily["Dividends"] != 0]
            if df_daily_divs.shape[0] == 0:
                # self.skipTest("Skipping test_intraDayWithEvents() because 'ICL.TA' has no dividend in last 60 days")
                continue

            last_div_date = df_daily_divs.index[-1]
            start_d = last_div_date.date()
            end_d = last_div_date.date() + _dt.timedelta(days=1)
            df = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="15m", actions=True)
            self.assertTrue((df["Dividends"] != 0.0).any())
            test_run = True
            break

        if not test_run:
            self.skipTest("Skipping test_intraDayWithEvents() because no tickers had a dividend in last 60 days")

    def test_dailyWithEvents(self):
        # Reproduce issue #521
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
        except:
            missing_from_df1 = df2.index.difference(df1.index)
            missing_from_df2 = df1.index.difference(df2.index)
            print("{} missing these dates: {}".format(tkr1, missing_from_df1))
            print("{} missing these dates: {}".format(tkr2, missing_from_df2))
            raise

        # Test that index same with and without events:
        tkrs = [tkr1, tkr2]
        for tkr in tkrs:
            df1 = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="1d", actions=True)
            df2 = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="1d", actions=False)
            self.assertTrue(((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any())
            try:
                self.assertTrue(df1.index.equals(df2.index))
            except:
                missing_from_df1 = df2.index.difference(df1.index)
                missing_from_df2 = df1.index.difference(df2.index)
                print("{}-with-events missing these dates: {}".format(tkr, missing_from_df1))
                print("{}-without-events missing these dates: {}".format(tkr, missing_from_df2))
                raise

    def test_weeklyWithEvents(self):
        # Reproduce issue #521
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
        except:
            missing_from_df1 = df2.index.difference(df1.index)
            missing_from_df2 = df1.index.difference(df2.index)
            print("{} missing these dates: {}".format(tkr1, missing_from_df1))
            print("{} missing these dates: {}".format(tkr2, missing_from_df2))
            raise

        # Test that index same with and without events:
        tkrs = [tkr1, tkr2]
        for tkr in tkrs:
            df1 = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="1wk", actions=True)
            df2 = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="1wk", actions=False)
            self.assertTrue(((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any())
            try:
                self.assertTrue(df1.index.equals(df2.index))
            except:
                missing_from_df1 = df2.index.difference(df1.index)
                missing_from_df2 = df1.index.difference(df2.index)
                print("{}-with-events missing these dates: {}".format(tkr, missing_from_df1))
                print("{}-without-events missing these dates: {}".format(tkr, missing_from_df2))
                raise

    def test_monthlyWithEvents(self):
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
        except:
            missing_from_df1 = df2.index.difference(df1.index)
            missing_from_df2 = df1.index.difference(df2.index)
            print("{} missing these dates: {}".format(tkr1, missing_from_df1))
            print("{} missing these dates: {}".format(tkr2, missing_from_df2))
            raise

        # Test that index same with and without events:
        tkrs = [tkr1, tkr2]
        for tkr in tkrs:
            df1 = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="1mo", actions=True)
            df2 = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="1mo", actions=False)
            self.assertTrue(((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any())
            try:
                self.assertTrue(df1.index.equals(df2.index))
            except:
                missing_from_df1 = df2.index.difference(df1.index)
                missing_from_df2 = df1.index.difference(df2.index)
                print("{}-with-events missing these dates: {}".format(tkr, missing_from_df1))
                print("{}-without-events missing these dates: {}".format(tkr, missing_from_df2))
                raise

    def test_tz_dst_ambiguous(self):
        # Reproduce issue #1100
        try:
            yf.Ticker("ESLT.TA", session=self.session).history(start="2002-10-06", end="2002-10-09", interval="1d")
        except _tz.exceptions.AmbiguousTimeError:
            raise Exception("Ambiguous DST issue not resolved")

    def test_dst_fix(self):
        # Daily intervals should start at time 00:00. But for some combinations of date and timezone, 
        # Yahoo has time off by few hours (e.g. Brazil 23:00 around Jan-2022). Suspect DST problem.
        # The clue is (a) minutes=0 and (b) hour near 0. 
        # Obviously Yahoo meant 00:00, so ensure this doesn't affect date conversion.

        # The correction is successful if no days are weekend, and weekly data begins Monday

        tkr = "AGRO3.SA"
        dat = yf.Ticker(tkr, session=self.session)
        start = "2021-01-11"
        end = "2022-11-05"

        interval = "1d"
        df = dat.history(start=start, end=end, interval=interval)
        self.assertTrue(((df.index.weekday >= 0) & (df.index.weekday <= 4)).all())

        interval = "1wk"
        df = dat.history(start=start, end=end, interval=interval)
        try:
            self.assertTrue((df.index.weekday == 0).all())
        except:
            print("Weekly data not aligned to Monday")
            raise

    def test_prune_post_intraday_us(self):
        # Half-day before USA Thanksgiving. Yahoo normally 
        # returns an interval starting when regular trading closes, 
        # even if prepost=False.

        # Setup
        tkr = "AMZN"
        interval = "1h"
        interval_td = _dt.timedelta(hours=1)
        time_open = _dt.time(9, 30)
        time_close = _dt.time(16)
        special_day = _dt.date(2022, 11, 25)
        time_early_close = _dt.time(13)
        dat = yf.Ticker(tkr, session=self.session)

        # Run
        start_d = special_day - _dt.timedelta(days=7)
        end_d = special_day + _dt.timedelta(days=7)
        df = dat.history(start=start_d, end=end_d, interval=interval, prepost=False, keepna=True)
        tg_last_dt = df.loc[str(special_day)].index[-1]
        self.assertTrue(tg_last_dt.time() < time_early_close)

        # Test no other afternoons (or mornings) were pruned
        start_d = _dt.date(special_day.year, 1, 1)
        end_d = _dt.date(special_day.year+1, 1, 1)
        df = dat.history(start=start_d, end=end_d, interval="1h", prepost=False, keepna=True)
        last_dts = _pd.Series(df.index).groupby(df.index.date).last()
        f_early_close = (last_dts+interval_td).dt.time < time_close
        early_close_dates = last_dts.index[f_early_close].values
        self.assertEqual(len(early_close_dates), 1)
        self.assertEqual(early_close_dates[0], special_day)

        first_dts = _pd.Series(df.index).groupby(df.index.date).first()
        f_late_open = first_dts.dt.time > time_open
        late_open_dates = first_dts.index[f_late_open]
        self.assertEqual(len(late_open_dates), 0)

    def test_prune_post_intraday_omx(self):
        # Half-day before Sweden Christmas. Yahoo normally 
        # returns an interval starting when regular trading closes, 
        # even if prepost=False.
        # If prepost=False, test that yfinance is removing prepost intervals.

        # Setup
        tkr = "AEC.ST"
        interval = "1h"
        interval_td = _dt.timedelta(hours=1)
        time_open = _dt.time(9)
        time_close = _dt.time(17,30)
        special_day = _dt.date(2022, 12, 23)
        time_early_close = _dt.time(13, 2)
        dat = yf.Ticker(tkr, session=self.session)

        # Half trading day Jan 5, Apr 14, May 25, Jun 23, Nov 4, Dec 23, Dec 30
        half_days = [_dt.date(special_day.year, x[0], x[1]) for x in [(1,5), (4,14), (5,25), (6,23), (11,4), (12,23), (12,30)]]

        # Yahoo has incorrectly classified afternoon of 2022-04-13 as post-market.
        # Nothing yfinance can do because Yahoo doesn't return data with prepost=False.
        # But need to handle in this test.
        expected_incorrect_half_days = [_dt.date(2022,4,13)]
        half_days = sorted(half_days+expected_incorrect_half_days)

        # Run
        start_d = special_day - _dt.timedelta(days=7)
        end_d = special_day + _dt.timedelta(days=7)
        df = dat.history(start=start_d, end=end_d, interval=interval, prepost=False, keepna=True)
        tg_last_dt = df.loc[str(special_day)].index[-1]
        self.assertTrue(tg_last_dt.time() < time_early_close)

        # Test no other afternoons (or mornings) were pruned
        start_d = _dt.date(special_day.year, 1, 1)
        end_d = _dt.date(special_day.year+1, 1, 1)
        df = dat.history(start=start_d, end=end_d, interval="1h", prepost=False, keepna=True)
        last_dts = _pd.Series(df.index).groupby(df.index.date).last()
        f_early_close = (last_dts+interval_td).dt.time < time_close
        early_close_dates = last_dts.index[f_early_close].values
        unexpected_early_close_dates = [d for d in early_close_dates if not d in half_days]
        self.assertEqual(len(unexpected_early_close_dates), 0)
        self.assertEqual(len(early_close_dates), len(half_days))
        self.assertTrue(_np.equal(early_close_dates, half_days).all())

        first_dts = _pd.Series(df.index).groupby(df.index.date).first()
        f_late_open = first_dts.dt.time > time_open
        late_open_dates = first_dts.index[f_late_open]
        self.assertEqual(len(late_open_dates), 0)

    def test_prune_post_intraday_asx(self):
        # Setup
        tkr = "BHP.AX"
        interval = "1h"
        interval_td = _dt.timedelta(hours=1)
        time_open = _dt.time(10)
        time_close = _dt.time(16,12)
        # No early closes in 2022
        dat = yf.Ticker(tkr, session=self.session)

        # Test no afternoons (or mornings) were pruned
        start_d = _dt.date(2022, 1, 1)
        end_d = _dt.date(2022+1, 1, 1)
        df = dat.history(start=start_d, end=end_d, interval="1h", prepost=False, keepna=True)
        last_dts = _pd.Series(df.index).groupby(df.index.date).last()
        f_early_close = (last_dts+interval_td).dt.time < time_close
        early_close_dates = last_dts.index[f_early_close].values
        self.assertEqual(len(early_close_dates), 0)

        first_dts = _pd.Series(df.index).groupby(df.index.date).first()
        f_late_open = first_dts.dt.time > time_open
        late_open_dates = first_dts.index[f_late_open]
        self.assertEqual(len(late_open_dates), 0)

    def test_weekly_2rows_fix(self):
        tkr = "AMZN"
        start = _dt.date.today() - _dt.timedelta(days=14)
        start -= _dt.timedelta(days=start.weekday())

        dat = yf.Ticker(tkr)
        df = dat.history(start=start, interval="1wk")
        self.assertTrue((df.index.weekday == 0).all())

class TestPriceRepair(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = requests_cache.CachedSession(backend='memory')

    @classmethod
    def tearDownClass(cls):
        if cls.session is not None:
            cls.session.close()

    def test_reconstruct_2m(self):
        # 2m repair requires 1m data.
        # Yahoo restricts 1m fetches to 7 days max within last 30 days.
        # Need to test that '_reconstruct_intervals_batch()' can handle this.

        tkrs = ["BHP.AX", "IMP.JO", "BP.L", "PNL.L", "INTC"]

        dt_now = _pd.Timestamp.utcnow()
        td_7d = _dt.timedelta(days=7)
        td_60d = _dt.timedelta(days=60)

        # Round time for 'requests_cache' reuse
        dt_now = dt_now.ceil("1h")

        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)
            end_dt = dt_now
            start_dt = end_dt - td_60d
            df = dat.history(start=start_dt, end=end_dt, interval="2m", repair=True)

    def test_repair_100x_weekly(self):
        # Setup:
        tkr = "PNL.L"
        dat = yf.Ticker(tkr, session=self.session)
        tz_exchange = dat.fast_info["timezone"]

        data_cols = ["Low", "High", "Open", "Close", "Adj Close"]
        df = _pd.DataFrame(data={"Open":      [470.5, 473.5, 474.5, 470],
                                 "High":      [476,   476.5, 477,   480],
                                 "Low":       [470.5, 470,   465.5, 468.26],
                                 "Close":     [475,   473.5, 472,   473.5],
                                 "Adj Close": [475,   473.5, 472,   473.5],
                                 "Volume": [2295613, 2245604, 3000287, 2635611]},
                                index=_pd.to_datetime([_dt.date(2022, 10, 24),
                                                       _dt.date(2022, 10, 17),
                                                       _dt.date(2022, 10, 10),
                                                       _dt.date(2022, 10, 3)]))
        df = df.sort_index()
        df.index.name = "Date"
        df_bad = df.copy()
        df_bad.loc["2022-10-24", "Close"] *= 100
        df_bad.loc["2022-10-17", "Low"] *= 100
        df_bad.loc["2022-10-03", "Open"] *= 100
        df.index = df.index.tz_localize(tz_exchange)
        df_bad.index = df_bad.index.tz_localize(tz_exchange)

        # Run test

        df_repaired = dat._fix_unit_mixups(df_bad, "1wk", tz_exchange, prepost=False)

        # First test - no errors left
        for c in data_cols:
            try:
                self.assertTrue(_np.isclose(df_repaired[c], df[c], rtol=1e-2).all())
            except:
                print(df[c])
                print(df_repaired[c])
                raise


        # Second test - all differences should be either ~1x or ~100x
        ratio = df_bad[data_cols].values / df[data_cols].values
        ratio = ratio.round(2)
        # - round near-100 ratio to 100:
        f = ratio > 90
        ratio[f] = (ratio[f] / 10).round().astype(int) * 10  # round ratio to nearest 10
        # - now test
        f_100 = ratio == 100
        f_1 = ratio == 1
        self.assertTrue((f_100 | f_1).all())

    def test_repair_100x_weekly_preSplit(self):
        # PNL.L has a stock-split in 2022. Sometimes requesting data before 2022 is not split-adjusted.

        tkr = "PNL.L"
        dat = yf.Ticker(tkr, session=self.session)
        tz_exchange = dat.fast_info["timezone"]

        data_cols = ["Low", "High", "Open", "Close", "Adj Close"]
        df = _pd.DataFrame(data={"Open":      [400,   398,    392.5,   417],
                                 "High":      [421,   425,    419,     420.5],
                                 "Low":       [400,   380.5,  376.5,   396],
                                 "Close":     [410,   409.5,  402,     399],
                                 "Adj Close": [398.02, 397.53, 390.25, 387.34],
                                 "Volume": [3232600, 3773900, 10835000, 4257900]},
                                index=_pd.to_datetime([_dt.date(2020, 3, 30),
                                                       _dt.date(2020, 3, 23),
                                                       _dt.date(2020, 3, 16),
                                                       _dt.date(2020, 3, 9)]))
        df = df.sort_index()
        # Simulate data missing split-adjustment:
        df[data_cols] *= 100.0
        df["Volume"] *= 0.01
        #
        df.index.name = "Date"
        # Create 100x errors:
        df_bad = df.copy()
        df_bad.loc["2020-03-30", "Close"] *= 100
        df_bad.loc["2020-03-23", "Low"] *= 100
        df_bad.loc["2020-03-09", "Open"] *= 100
        df.index = df.index.tz_localize(tz_exchange)
        df_bad.index = df_bad.index.tz_localize(tz_exchange)

        df_repaired = dat._fix_unit_mixups(df_bad, "1wk", tz_exchange, prepost=False)

        # First test - no errors left
        for c in data_cols:
            try:
                self.assertTrue(_np.isclose(df_repaired[c], df[c], rtol=1e-2).all())
            except:
                print("Mismatch in column", c)
                print("- df_repaired:")
                print(df_repaired[c])
                print("- answer:")
                print(df[c])
                raise

        # Second test - all differences should be either ~1x or ~100x
        ratio = df_bad[data_cols].values / df[data_cols].values
        ratio = ratio.round(2)
        # - round near-100 ratio to 100:
        f = ratio > 90
        ratio[f] = (ratio[f] / 10).round().astype(int) * 10  # round ratio to nearest 10
        # - now test
        f_100 = ratio == 100
        f_1 = ratio == 1
        self.assertTrue((f_100 | f_1).all())

    def test_repair_100x_daily(self):
        tkr = "PNL.L"
        dat = yf.Ticker(tkr, session=self.session)
        tz_exchange = dat.fast_info["timezone"]

        data_cols = ["Low", "High", "Open", "Close", "Adj Close"]
        df = _pd.DataFrame(data={"Open":      [478,    476,   476,   472],
                                 "High":      [478,    477.5, 477,   475],
                                 "Low":       [474.02, 474,   473,   470.75],
                                 "Close":     [475.5,  475.5, 474.5, 475],
                                 "Adj Close": [475.5,  475.5, 474.5, 475],
                                 "Volume": [436414, 485947, 358067, 287620]},
                            index=_pd.to_datetime([_dt.date(2022, 11, 1),
                                                   _dt.date(2022, 10, 31),
                                                   _dt.date(2022, 10, 28),
                                                   _dt.date(2022, 10, 27)]))
        df = df.sort_index()
        df.index.name = "Date"
        df_bad = df.copy()
        df_bad.loc["2022-11-01", "Close"] *= 100
        df_bad.loc["2022-10-31", "Low"] *= 100
        df_bad.loc["2022-10-27", "Open"] *= 100
        df.index = df.index.tz_localize(tz_exchange)
        df_bad.index = df_bad.index.tz_localize(tz_exchange)

        df_repaired = dat._fix_unit_mixups(df_bad, "1d", tz_exchange, prepost=False)

        # First test - no errors left
        for c in data_cols:
            self.assertTrue(_np.isclose(df_repaired[c], df[c], rtol=1e-2).all())

        # Second test - all differences should be either ~1x or ~100x
        ratio = df_bad[data_cols].values / df[data_cols].values
        ratio = ratio.round(2)
        # - round near-100 ratio to 100:
        f = ratio > 90
        ratio[f] = (ratio[f] / 10).round().astype(int) * 10  # round ratio to nearest 10
        # - now test
        f_100 = ratio == 100
        f_1 = ratio == 1
        self.assertTrue((f_100 | f_1).all())

    def test_repair_zeroes_daily(self):
        tkr = "BBIL.L"
        dat = yf.Ticker(tkr, session=self.session)
        tz_exchange = dat.fast_info["timezone"]

        df_bad = _pd.DataFrame(data={"Open":      [0,      102.04, 102.04],
                                     "High":      [0,      102.1,  102.11],
                                     "Low":       [0,      102.04, 102.04],
                                     "Close":     [103.03, 102.05, 102.08],
                                     "Adj Close": [102.03, 102.05, 102.08],
                                     "Volume": [560, 137, 117]},
                                index=_pd.to_datetime([_dt.datetime(2022, 11, 1),
                                                       _dt.datetime(2022, 10, 31),
                                                       _dt.datetime(2022, 10, 30)]))
        df_bad = df_bad.sort_index()
        df_bad.index.name = "Date"
        df_bad.index = df_bad.index.tz_localize(tz_exchange)

        repaired_df = dat._fix_zeroes(df_bad, "1d", tz_exchange, prepost=False)

        correct_df = df_bad.copy()
        correct_df.loc["2022-11-01", "Open"] = 102.080002
        correct_df.loc["2022-11-01", "Low"] = 102.032501
        correct_df.loc["2022-11-01", "High"] = 102.080002
        for c in ["Open", "Low", "High", "Close"]:
            self.assertTrue(_np.isclose(repaired_df[c], correct_df[c], rtol=1e-8).all())

    def test_repair_zeroes_hourly(self):
        tkr = "INTC"
        dat = yf.Ticker(tkr, session=self.session)
        tz_exchange = dat.fast_info["timezone"]

        correct_df = dat.history(period="1wk", interval="1h", auto_adjust=False, repair=True)

        df_bad = correct_df.copy()
        bad_idx = correct_df.index[10]
        df_bad.loc[bad_idx, "Open"] = _np.nan
        df_bad.loc[bad_idx, "High"] = _np.nan
        df_bad.loc[bad_idx, "Low"] = _np.nan
        df_bad.loc[bad_idx, "Close"] = _np.nan
        df_bad.loc[bad_idx, "Adj Close"] = _np.nan
        df_bad.loc[bad_idx, "Volume"] = 0

        repaired_df = dat._fix_zeroes(df_bad, "1h", tz_exchange, prepost=False)

        for c in ["Open", "Low", "High", "Close"]:
            try:
                self.assertTrue(_np.isclose(repaired_df[c], correct_df[c], rtol=1e-7).all())
            except:
                print("COLUMN", c)
                print("- repaired_df")
                print(repaired_df)
                print("- correct_df[c]:")
                print(correct_df[c])
                print("- diff:")
                print(repaired_df[c] - correct_df[c])
                raise

if __name__ == '__main__':
    unittest.main()

# # Run tests sequentially:
# import inspect
# test_src = inspect.getsource(TestPriceHistory)
# unittest.TestLoader.sortTestMethodsUsing = lambda _, x, y: (
#     test_src.index(f"def {x}") - test_src.index(f"def {y}")
# )
# unittest.main(verbosity=2)

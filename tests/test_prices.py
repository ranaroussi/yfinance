from .context import yfinance as yf
from .context import session_gbl

import unittest

import os
import datetime as _dt
import pytz as _tz
import numpy as _np
import pandas as _pd


class TestPriceHistory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = session_gbl

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

    def test_download(self):
        tkrs = ["BHP.AX", "IMP.JO", "BP.L", "PNL.L", "INTC"]
        intervals = ["1d", "1wk", "1mo"]
        for interval in intervals:
            df = yf.download(tkrs, period="5y", interval=interval)

            f = df.index.time == _dt.time(0)
            self.assertTrue(f.all())

            df_tkrs = df.columns.levels[1]
            self.assertEqual(sorted(tkrs), sorted(df_tkrs))

    def test_download_with_invalid_ticker(self):
        #Checks if using an invalid symbol gives the same output as not using an invalid symbol in combination with a valid symbol (AAPL)
        #Checks to make sure that invalid symbol handling for the date column is the same as the base case (no invalid symbols)

        invalid_tkrs = ["AAPL", "ATVI"] #AAPL exists and ATVI does not exist
        valid_tkrs = ["AAPL", "INTC"] #AAPL and INTC both exist

        data_invalid_sym = yf.download(invalid_tkrs, start='2023-11-16', end='2023-11-17')
        data_valid_sym = yf.download(valid_tkrs, start='2023-11-16', end='2023-11-17')

        self.assertEqual(data_invalid_sym['Close']['AAPL']['2023-11-16'],data_valid_sym['Close']['AAPL']['2023-11-16'])

    def test_duplicatingHourly(self):
        tkrs = ["IMP.JO", "BHG.JO", "SSW.JO", "BP.L", "INTC"]
        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)
            tz = dat._get_ticker_tz(proxy=None, timeout=None)

            dt_utc = _pd.Timestamp.utcnow()
            dt = dt_utc.astimezone(_tz.timezone(tz))
            start_d = dt.date() - _dt.timedelta(days=7)
            df = dat.history(start=start_d, interval="1h")

            dt0 = df.index[-2]
            dt1 = df.index[-1]
            try:
                self.assertNotEqual(dt0.hour, dt1.hour)
            except AssertionError:
                print("Ticker = ", tkr)
                raise

    def test_duplicatingDaily(self):
        tkrs = ["IMP.JO", "BHG.JO", "SSW.JO", "BP.L", "INTC"]
        test_run = False
        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)
            tz = dat._get_ticker_tz(proxy=None, timeout=None)

            dt_utc = _pd.Timestamp.utcnow()
            dt = dt_utc.astimezone(_tz.timezone(tz))
            if dt.time() < _dt.time(17, 0):
                continue
            test_run = True

            df = dat.history(start=dt.date() - _dt.timedelta(days=7), interval="1d")

            dt0 = df.index[-2]
            dt1 = df.index[-1]
            try:
                self.assertNotEqual(dt0, dt1)
            except AssertionError:
                print("Ticker = ", tkr)
                raise

        if not test_run:
            self.skipTest("Skipping test_duplicatingDaily() because only expected to fail just after market close")

    def test_duplicatingWeekly(self):
        tkrs = ['MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']
        test_run = False
        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)
            tz = dat._get_ticker_tz(proxy=None, timeout=None)

            dt = _tz.timezone(tz).localize(_dt.datetime.now())
            if dt.date().weekday() not in [1, 2, 3, 4]:
                continue
            test_run = True

            df = dat.history(start=dt.date() - _dt.timedelta(days=7), interval="1wk")
            dt0 = df.index[-2]
            dt1 = df.index[-1]
            try:
                self.assertNotEqual(dt0.week, dt1.week)
            except AssertionError:
                print("Ticker={}: Last two rows within same week:".format(tkr))
                print(df.iloc[df.shape[0] - 2:])
                raise

        if not test_run:
            self.skipTest("Skipping test_duplicatingWeekly() because not possible to fail Monday/weekend")

    def test_pricesEventsMerge(self):
        # Test case: dividend occurs after last row in price data
        tkr = 'INTC'
        start_d = _dt.date(2022, 1, 1)
        end_d = _dt.date(2023, 1, 1)
        df = yf.Ticker(tkr, session=self.session).history(interval='1d', start=start_d, end=end_d)
        div = 1.0
        future_div_dt = df.index[-1] + _dt.timedelta(days=1)
        if future_div_dt.weekday() in [5, 6]:
            future_div_dt += _dt.timedelta(days=1) * (7 - future_div_dt.weekday())
        divs = _pd.DataFrame(data={"Dividends":[div]}, index=[future_div_dt])
        df2 = yf.utils.safe_merge_dfs(df.drop(['Dividends', 'Stock Splits'], axis=1), divs, '1d')
        self.assertIn(future_div_dt, df2.index)
        self.assertIn("Dividends", df2.columns)
        self.assertEqual(df2['Dividends'].iloc[-1], div)

    def test_pricesEventsMerge_bug(self):
        # Reproduce exception when merging intraday prices with future dividend
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
        divs = _pd.DataFrame(data={"Dividends":[div]}, index=[future_div_dt])

        yf.utils.safe_merge_dfs(df, divs, interval)
        # No exception = test pass

    def test_intraDayWithEvents(self):
        tkrs = ["BHP.AX", "IMP.JO", "BP.L", "PNL.L", "INTC"]
        test_run = False
        for tkr in tkrs:
            start_d = _dt.date.today() - _dt.timedelta(days=59)
            end_d = None
            df_daily = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="1d", actions=True)
            df_daily_divs = df_daily["Dividends"][df_daily["Dividends"] != 0]
            if df_daily_divs.shape[0] == 0:
                continue

            last_div_date = df_daily_divs.index[-1]
            start_d = last_div_date.date()
            end_d = last_div_date.date() + _dt.timedelta(days=1)
            df_intraday = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="15m", actions=True)
            self.assertTrue((df_intraday["Dividends"] != 0.0).any())

            df_intraday_divs = df_intraday["Dividends"][df_intraday["Dividends"] != 0]
            df_intraday_divs.index = df_intraday_divs.index.floor('D')
            self.assertTrue(df_daily_divs.equals(df_intraday_divs))

            test_run = True

        if not test_run:
            self.skipTest("Skipping test_intraDayWithEvents() because no tickers had a dividend in last 60 days")

    def test_intraDayWithEvents_tase(self):
        # TASE dividend release pre-market, doesn't merge nicely with intra-day data so check still present

        tase_tkrs = ["ICL.TA", "ESLT.TA", "ONE.TA", "MGDL.TA"]
        test_run = False
        for tkr in tase_tkrs:
            start_d = _dt.date.today() - _dt.timedelta(days=59)
            end_d = None
            df_daily = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="1d", actions=True)
            df_daily_divs = df_daily["Dividends"][df_daily["Dividends"] != 0]
            if df_daily_divs.shape[0] == 0:
                continue

            last_div_date = df_daily_divs.index[-1]
            start_d = last_div_date.date()
            end_d = last_div_date.date() + _dt.timedelta(days=1)
            df_intraday = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="15m", actions=True)
            self.assertTrue((df_intraday["Dividends"] != 0.0).any())

            df_intraday_divs = df_intraday["Dividends"][df_intraday["Dividends"] != 0]
            df_intraday_divs.index = df_intraday_divs.index.floor('D')
            self.assertTrue(df_daily_divs.equals(df_intraday_divs))

            test_run = True

        if not test_run:
            self.skipTest("Skipping test_intraDayWithEvents_tase() because no tickers had a dividend in last 60 days")

    def test_dailyWithEvents(self):
        start_d = _dt.date(2022, 1, 1)
        end_d = _dt.date(2023, 1, 1)

        tkr_div_dates = {'BHP.AX': [_dt.date(2022, 9, 1), _dt.date(2022, 2, 24)],  # Yahoo claims 23-Feb but wrong because DST
                         'IMP.JO': [_dt.date(2022, 9, 21), _dt.date(2022, 3, 16)],
                         'BP.L': [_dt.date(2022, 11, 10), _dt.date(2022, 8, 11), _dt.date(2022, 5, 12),
                                  _dt.date(2022, 2, 17)],
                         'INTC': [_dt.date(2022, 11, 4), _dt.date(2022, 8, 4), _dt.date(2022, 5, 5),
                                  _dt.date(2022, 2, 4)]}

        for tkr, dates in tkr_div_dates.items():
            df = yf.Ticker(tkr, session=self.session).history(interval='1d', start=start_d, end=end_d)
            df_divs = df[df['Dividends'] != 0].sort_index(ascending=False)
            try:
                self.assertTrue((df_divs.index.date == dates).all())
            except AssertionError:
                print(f'- ticker = {tkr}')
                print('- response:')
                print(df_divs.index.date)
                print('- answer:')
                print(dates)
                raise

    def test_dailyWithEvents_bugs(self):
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
        except AssertionError:
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
            except AssertionError:
                missing_from_df1 = df2.index.difference(df1.index)
                missing_from_df2 = df1.index.difference(df2.index)
                print("{}-with-events missing these dates: {}".format(tkr, missing_from_df1))
                print("{}-without-events missing these dates: {}".format(tkr, missing_from_df2))
                raise

        # Reproduce issue #1634 - 1d dividend out-of-range, should be prepended to prices
        div_dt = _pd.Timestamp(2022, 7, 21).tz_localize("America/New_York")
        df_dividends = _pd.DataFrame(data={"Dividends":[1.0]}, index=[div_dt])
        df_prices = _pd.DataFrame(data={c:[1.0] for c in yf.const._PRICE_COLNAMES_}|{'Volume':0}, index=[div_dt+_dt.timedelta(days=1)])
        df_merged = yf.utils.safe_merge_dfs(df_prices, df_dividends, '1d')
        self.assertEqual(df_merged.shape[0], 2)
        self.assertTrue(df_merged[df_prices.columns].iloc[1:].equals(df_prices))
        self.assertEqual(df_merged.index[0], div_dt)

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
        except AssertionError:
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
            except AssertionError:
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
        except AssertionError:
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
            except AssertionError:
                missing_from_df1 = df2.index.difference(df1.index)
                missing_from_df2 = df1.index.difference(df2.index)
                print("{}-with-events missing these dates: {}".format(tkr, missing_from_df1))
                print("{}-without-events missing these dates: {}".format(tkr, missing_from_df2))
                raise

    def test_monthlyWithEvents2(self):
        # Simply check no exception from internal merge
        dfm = yf.Ticker("ABBV").history(period="max", interval="1mo")
        dfd = yf.Ticker("ABBV").history(period="max", interval="1d")
        dfd = dfd[dfd.index > dfm.index[0]]
        dfm_divs = dfm[dfm['Dividends'] != 0]
        dfd_divs = dfd[dfd['Dividends'] != 0]
        self.assertEqual(dfm_divs.shape[0], dfd_divs.shape[0])

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
        except AssertionError:
            print("Weekly data not aligned to Monday")
            raise

    def test_prune_post_intraday_us(self):
        # Half-day at USA Thanksgiving. Yahoo normally
        # returns an interval starting when regular trading closes,
        # even if prepost=False.

        # Setup
        tkr = "AMZN"
        special_day = _dt.date(2023, 11, 24)
        time_early_close = _dt.time(13)
        dat = yf.Ticker(tkr, session=self.session)

        # Run
        start_d = special_day - _dt.timedelta(days=7)
        end_d = special_day + _dt.timedelta(days=7)
        df = dat.history(start=start_d, end=end_d, interval="1h", prepost=False, keepna=True)
        tg_last_dt = df.loc[str(special_day)].index[-1]
        self.assertTrue(tg_last_dt.time() < time_early_close)

        # Test no other afternoons (or mornings) were pruned
        start_d = _dt.date(special_day.year, 1, 1)
        end_d = _dt.date(special_day.year+1, 1, 1)
        df = dat.history(start=start_d, end=end_d, interval="1h", prepost=False, keepna=True)
        last_dts = _pd.Series(df.index).groupby(df.index.date).last()
        dfd = dat.history(start=start_d, end=end_d, interval='1d', prepost=False, keepna=True)
        self.assertTrue(_np.equal(dfd.index.date, _pd.to_datetime(last_dts.index).date).all())

    def test_prune_post_intraday_asx(self):
        # Setup
        tkr = "BHP.AX"
        # No early closes in 2023
        dat = yf.Ticker(tkr, session=self.session)

        # Test no other afternoons (or mornings) were pruned
        start_d = _dt.date(2023, 1, 1)
        end_d = _dt.date(2023+1, 1, 1)
        df = dat.history(start=start_d, end=end_d, interval="1h", prepost=False, keepna=True)
        last_dts = _pd.Series(df.index).groupby(df.index.date).last()
        dfd = dat.history(start=start_d, end=end_d, interval='1d', prepost=False, keepna=True)
        self.assertTrue(_np.equal(dfd.index.date, _pd.to_datetime(last_dts.index).date).all())

    def test_weekly_2rows_fix(self):
        tkr = "AMZN"
        start = _dt.date.today() - _dt.timedelta(days=14)
        start -= _dt.timedelta(days=start.weekday())

        dat = yf.Ticker(tkr)
        df = dat.history(start=start, interval="1wk")
        self.assertTrue((df.index.weekday == 0).all())

    def test_aggregate_capital_gains(self):
        # Setup
        tkr = "FXAIX"
        dat = yf.Ticker(tkr, session=self.session)
        start = "2017-12-31"
        end = "2019-12-31"
        interval = "3mo"

        dat.history(start=start, end=end, interval=interval)


class TestPriceRepair(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = session_gbl

    @classmethod
    def tearDownClass(cls):
        if cls.session is not None:
            cls.session.close()

    def test_types(self):
        tkr = 'INTC'
        dat = yf.Ticker(tkr, session=self.session)

        data = dat.history(period="3mo", interval="1d", prepost=True, repair=True)
        self.assertIsInstance(data, _pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

        reconstructed = dat._lazy_load_price_history()._reconstruct_intervals_batch(data, "1wk", True)
        self.assertIsInstance(reconstructed, _pd.DataFrame, "data has wrong type")
        self.assertFalse(data.empty, "data is empty")

    def test_reconstruct_2m(self):
        # 2m repair requires 1m data.
        # Yahoo restricts 1m fetches to 7 days max within last 30 days.
        # Need to test that '_reconstruct_intervals_batch()' can handle this.

        tkrs = ["BHP.AX", "IMP.JO", "BP.L", "PNL.L", "INTC"]

        dt_now = _pd.Timestamp.utcnow()
        td_60d = _dt.timedelta(days=60)

        # Round time for 'requests_cache' reuse
        dt_now = dt_now.ceil("1h")

        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)
            end_dt = dt_now
            start_dt = end_dt - td_60d
            dat.history(start=start_dt, end=end_dt, interval="2m", repair=True)

    def test_repair_100x_random_weekly(self):
        # Setup:
        tkr = "PNL.L"
        dat = yf.Ticker(tkr, session=self.session)
        tz_exchange = dat.fast_info["timezone"]
        hist = dat._lazy_load_price_history()

        data_cols = ["Low", "High", "Open", "Close", "Adj Close"]
        df = _pd.DataFrame(data={"Open":      [470.5, 473.5, 474.5, 470],
                                 "High":      [476,   476.5, 477,   480],
                                 "Low":       [470.5, 470,   465.5, 468.26],
                                 "Close":     [475,   473.5, 472,   473.5],
                                 "Adj Close": [470.1, 468.6, 467.1, 468.6],
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

        df_repaired = hist._fix_unit_random_mixups(df_bad, "1wk", tz_exchange, prepost=False)

        # First test - no errors left
        for c in data_cols:
            try:
                self.assertTrue(_np.isclose(df_repaired[c], df[c], rtol=1e-2).all())
            except AssertionError:
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

        self.assertTrue("Repaired?" in df_repaired.columns)
        self.assertFalse(df_repaired["Repaired?"].isna().any())

    def test_repair_100x_random_weekly_preSplit(self):
        # PNL.L has a stock-split in 2022. Sometimes requesting data before 2022 is not split-adjusted.

        tkr = "PNL.L"
        dat = yf.Ticker(tkr, session=self.session)
        tz_exchange = dat.fast_info["timezone"]
        hist = dat._lazy_load_price_history()

        data_cols = ["Low", "High", "Open", "Close", "Adj Close"]
        df = _pd.DataFrame(data={"Open":      [400,    398,    392.5,  417],
                                 "High":      [421,    425,    419,    420.5],
                                 "Low":       [400,    380.5,  376.5,  396],
                                 "Close":     [410,    409.5,  402,    399],
                                 "Adj Close": [393.91, 393.43, 386.22, 383.34],
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

        df_repaired = hist._fix_unit_random_mixups(df_bad, "1wk", tz_exchange, prepost=False)

        # First test - no errors left
        for c in data_cols:
            try:
                self.assertTrue(_np.isclose(df_repaired[c], df[c], rtol=1e-2).all())
            except AssertionError:
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

        self.assertTrue("Repaired?" in df_repaired.columns)
        self.assertFalse(df_repaired["Repaired?"].isna().any())

    def test_repair_100x_random_daily(self):
        tkr = "PNL.L"
        dat = yf.Ticker(tkr, session=self.session)
        tz_exchange = dat.fast_info["timezone"]
        hist = dat._lazy_load_price_history()

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

        df_repaired = hist._fix_unit_random_mixups(df_bad, "1d", tz_exchange, prepost=False)

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

        self.assertTrue("Repaired?" in df_repaired.columns)
        self.assertFalse(df_repaired["Repaired?"].isna().any())

    def test_repair_100x_block_daily(self):
        # Some 100x errors are not sporadic.
        # Sometimes Yahoo suddenly shifts from cents->$ from some recent date.

        tkrs = ['AET.L', 'SSW.JO']
        for tkr in tkrs:
            for interval in ['1d', '1wk']:
                dat = yf.Ticker(tkr, session=self.session)
                tz_exchange = dat.fast_info["timezone"]
                hist = dat._lazy_load_price_history()

                data_cols = ["Low", "High", "Open", "Close", "Adj Close"]
                _dp = os.path.dirname(__file__)
                fp = os.path.join(_dp, "data", tkr.replace('.','-') + '-' + interval + "-100x-error.csv")
                if not os.path.isfile(fp):
                    continue
                df_bad = _pd.read_csv(fp, index_col="Date")
                df_bad.index = _pd.to_datetime(df_bad.index, utc=True).tz_convert(tz_exchange)
                df_bad = df_bad.sort_index()

                df = df_bad.copy()
                fp = os.path.join(_dp, "data", tkr.replace('.','-') + '-' + interval + "-100x-error-fixed.csv")
                df = _pd.read_csv(fp, index_col="Date")
                df.index = _pd.to_datetime(df.index, utc=True).tz_convert(tz_exchange)
                df = df.sort_index()

                df_repaired = hist._fix_unit_switch(df_bad, interval, tz_exchange)
                df_repaired = df_repaired.sort_index()

                # First test - no errors left
                for c in data_cols:
                    try:
                        self.assertTrue(_np.isclose(df_repaired[c], df[c], rtol=1e-2).all())
                    except:
                        print("- repaired:")
                        print(df_repaired[c])
                        print("- correct:")
                        print(df[c])
                        print(f"TEST FAIL on column '{c}' (tkr={tkr} interval={interval})")
                        raise

                # Second test - all differences should be either ~1x or ~100x
                ratio = df_bad[data_cols].values / df[data_cols].values
                ratio = ratio.round(2)
                # - round near-100 ratio to 100:
                f = ratio > 90
                ratio[f] = (ratio[f] / 10).round().astype(int) * 10  # round ratio to nearest 10
                # - now test
                f_100 = (ratio == 100) | (ratio == 0.01)
                f_1 = ratio == 1
                self.assertTrue((f_100 | f_1).all())

                self.assertTrue("Repaired?" in df_repaired.columns)
                self.assertFalse(df_repaired["Repaired?"].isna().any())

    def test_repair_zeroes_daily(self):
        tkr = "BBIL.L"
        dat = yf.Ticker(tkr, session=self.session)
        hist = dat._lazy_load_price_history()
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

        repaired_df = hist._fix_zeroes(df_bad, "1d", tz_exchange, prepost=False)

        correct_df = df_bad.copy()
        correct_df.loc["2022-11-01", "Open"] = 102.080002
        correct_df.loc["2022-11-01", "Low"] = 102.032501
        correct_df.loc["2022-11-01", "High"] = 102.080002
        for c in ["Open", "Low", "High", "Close"]:
            self.assertTrue(_np.isclose(repaired_df[c], correct_df[c], rtol=1e-8).all())

        self.assertTrue("Repaired?" in repaired_df.columns)
        self.assertFalse(repaired_df["Repaired?"].isna().any())

    def test_repair_zeroes_daily_adjClose(self):
        # Test that 'Adj Close' is reconstructed correctly,
        # particularly when a dividend occurred within 1 day.

        tkr = "INTC"
        df = _pd.DataFrame(data={"Open":      [28.95, 28.65, 29.55, 29.62, 29.25],
                                 "High":      [29.12, 29.27, 29.65, 31.17, 30.30],
                                 "Low":       [28.21, 28.43, 28.61, 29.53, 28.80],
                                 "Close":     [28.24, 29.05, 28.69, 30.32, 30.19],
                                 "Adj Close": [28.12, 28.93, 28.57, 29.83, 29.70],
                                 "Volume":    [36e6, 51e6, 49e6, 58e6, 62e6],
                                 "Dividends": [0, 0, 0.365, 0, 0]},
                           index=_pd.to_datetime([_dt.datetime(2023, 2, 8),
                                                  _dt.datetime(2023, 2, 7),
                                                  _dt.datetime(2023, 2, 6),
                                                  _dt.datetime(2023, 2, 3),
                                                  _dt.datetime(2023, 2, 2)]))
        df = df.sort_index()
        df.index.name = "Date"
        dat = yf.Ticker(tkr, session=self.session)
        tz_exchange = dat.fast_info["timezone"]
        df.index = df.index.tz_localize(tz_exchange)
        hist = dat._lazy_load_price_history()

        rtol = 5e-3
        for i in [0, 1, 2]:
            df_slice = df.iloc[i:i+3]
            for j in range(3):
                df_slice_bad = df_slice.copy()
                df_slice_bad.loc[df_slice_bad.index[j], "Adj Close"] = 0.0

                df_slice_bad_repaired = hist._fix_zeroes(df_slice_bad, "1d", tz_exchange, prepost=False)
                for c in ["Close", "Adj Close"]:
                    self.assertTrue(_np.isclose(df_slice_bad_repaired[c], df_slice[c], rtol=rtol).all())
                self.assertTrue("Repaired?" in df_slice_bad_repaired.columns)
                self.assertFalse(df_slice_bad_repaired["Repaired?"].isna().any())

    def test_repair_zeroes_hourly(self):
        tkr = "INTC"
        dat = yf.Ticker(tkr, session=self.session)
        tz_exchange = dat.fast_info["timezone"]
        hist = dat._lazy_load_price_history()

        correct_df = hist.history(period="5d", interval="1h", auto_adjust=False, repair=True)

        df_bad = correct_df.copy()
        bad_idx = correct_df.index[10]
        df_bad.loc[bad_idx, "Open"] = _np.nan
        df_bad.loc[bad_idx, "High"] = _np.nan
        df_bad.loc[bad_idx, "Low"] = _np.nan
        df_bad.loc[bad_idx, "Close"] = _np.nan
        df_bad.loc[bad_idx, "Adj Close"] = _np.nan
        df_bad.loc[bad_idx, "Volume"] = 0

        repaired_df = hist._fix_zeroes(df_bad, "1h", tz_exchange, prepost=False)

        for c in ["Open", "Low", "High", "Close"]:
            try:
                self.assertTrue(_np.isclose(repaired_df[c], correct_df[c], rtol=1e-7).all())
            except AssertionError:
                print("COLUMN", c)
                print("- repaired_df")
                print(repaired_df)
                print("- correct_df[c]:")
                print(correct_df[c])
                print("- diff:")
                print(repaired_df[c] - correct_df[c])
                raise

        self.assertTrue("Repaired?" in repaired_df.columns)
        self.assertFalse(repaired_df["Repaired?"].isna().any())

    def test_repair_bad_stock_splits(self):
        # Stocks that split in 2022 but no problems in Yahoo data,
        # so repair should change nothing
        good_tkrs = ['AMZN', 'DXCM', 'FTNT', 'GOOG', 'GME', 'PANW', 'SHOP', 'TSLA']
        good_tkrs += ['AEI', 'GHI', 'IRON', 'LXU', 'NUZE', 'RSLS', 'TISI']
        good_tkrs += ['BOL.ST', 'TUI1.DE']
        intervals = ['1d', '1wk', '1mo', '3mo']
        for tkr in good_tkrs:
            for interval in intervals:
                dat = yf.Ticker(tkr, session=self.session)
                tz_exchange = dat.fast_info["timezone"]
                hist = dat._lazy_load_price_history()

                _dp = os.path.dirname(__file__)
                df_good = dat.history(start='2020-01-01', end=_dt.date.today(), interval=interval, auto_adjust=False)

                repaired_df = hist._fix_bad_stock_splits(df_good, interval, tz_exchange)

                # Expect no change from repair
                df_good = df_good.sort_index()
                repaired_df = repaired_df.sort_index()
                for c in ["Open", "Low", "High", "Close", "Adj Close", "Volume"]:
                    try:
                        self.assertTrue((repaired_df[c].to_numpy() == df_good[c].to_numpy()).all())
                    except:
                        print(f"tkr={tkr} interval={interval} COLUMN={c}")
                        df_dbg = df_good[[c]].join(repaired_df[[c]], lsuffix='.good', rsuffix='.repaired')
                        f_diff = repaired_df[c].to_numpy() != df_good[c].to_numpy()
                        print(df_dbg[f_diff | _np.roll(f_diff, 1) | _np.roll(f_diff, -1)])
                        raise

        bad_tkrs = ['4063.T', 'ALPHA.PA', 'AV.L', 'CNE.L', 'MOB.ST', 'SPM.MI']
        bad_tkrs.append('LA.V')  # special case - stock split error is 3 years ago! why not fixed?
        for tkr in bad_tkrs:
            dat = yf.Ticker(tkr, session=self.session)
            tz_exchange = dat.fast_info["timezone"]
            hist = dat._lazy_load_price_history()

            _dp = os.path.dirname(__file__)
            interval = '1d'
            fp = os.path.join(_dp, "data", tkr.replace('.','-')+'-'+interval+"-bad-stock-split.csv")
            if not os.path.isfile(fp):
                interval = '1wk'
                fp = os.path.join(_dp, "data", tkr.replace('.','-')+'-'+interval+"-bad-stock-split.csv")
            df_bad = _pd.read_csv(fp, index_col="Date")
            df_bad.index = _pd.to_datetime(df_bad.index, utc=True)

            repaired_df = hist._fix_bad_stock_splits(df_bad, "1d", tz_exchange)

            fp = os.path.join(_dp, "data", tkr.replace('.','-')+'-'+interval+"-bad-stock-split-fixed.csv")
            correct_df = _pd.read_csv(fp, index_col="Date")
            correct_df.index = _pd.to_datetime(correct_df.index)

            repaired_df = repaired_df.sort_index()
            correct_df = correct_df.sort_index()
            for c in ["Open", "Low", "High", "Close", "Adj Close", "Volume"]:
                try:
                    self.assertTrue(_np.isclose(repaired_df[c], correct_df[c], rtol=5e-6).all())
                except AssertionError:
                    print(f"tkr={tkr} COLUMN={c}")
                    # print("- repaired_df")
                    # print(repaired_df)
                    # print("- correct_df[c]:")
                    # print(correct_df[c])
                    # print("- diff:")
                    # print(repaired_df[c] - correct_df[c])
                    raise

        # Had very high price volatility in Jan-2021 around split date that could
        # be mistaken for missing stock split adjustment. And old logic did think
        # column 'High' required fixing - wrong!
        sketchy_tkrs = ['FIZZ']
        intervals = ['1wk']
        for tkr in sketchy_tkrs:
            for interval in intervals:
                dat = yf.Ticker(tkr, session=self.session)
                tz_exchange = dat.fast_info["timezone"]
                hist = dat._lazy_load_price_history()

                _dp = os.path.dirname(__file__)
                df_good = hist.history(start='2020-11-30', end='2021-04-01', interval=interval, auto_adjust=False)

                repaired_df = hist._fix_bad_stock_splits(df_good, interval, tz_exchange)

                # Expect no change from repair
                df_good = df_good.sort_index()
                repaired_df = repaired_df.sort_index()
                for c in ["Open", "Low", "High", "Close", "Adj Close", "Volume"]:
                    try:
                        self.assertTrue((repaired_df[c].to_numpy() == df_good[c].to_numpy()).all())
                    except AssertionError:
                        print(f"tkr={tkr} interval={interval} COLUMN={c}")
                        df_dbg = df_good[[c]].join(repaired_df[[c]], lsuffix='.good', rsuffix='.repaired')
                        f_diff = repaired_df[c].to_numpy() != df_good[c].to_numpy()
                        print(df_dbg[f_diff | _np.roll(f_diff, 1) | _np.roll(f_diff, -1)])
                        raise

    def test_repair_missing_div_adjust(self):
        tkr = '8TRA.DE'

        dat = yf.Ticker(tkr, session=self.session)
        tz_exchange = dat.fast_info["timezone"]
        hist = dat._lazy_load_price_history()

        _dp = os.path.dirname(__file__)
        df_bad = _pd.read_csv(os.path.join(_dp, "data", tkr.replace('.','-')+"-1d-missing-div-adjust.csv"), index_col="Date")
        df_bad.index = _pd.to_datetime(df_bad.index)

        repaired_df = hist._fix_missing_div_adjust(df_bad, "1d", tz_exchange)

        correct_df = _pd.read_csv(os.path.join(_dp, "data", tkr.replace('.','-')+"-1d-missing-div-adjust-fixed.csv"), index_col="Date")
        correct_df.index = _pd.to_datetime(correct_df.index)

        repaired_df = repaired_df.sort_index()
        correct_df = correct_df.sort_index()
        for c in ["Open", "Low", "High", "Close", "Adj Close", "Volume"]:
            try:
                self.assertTrue(_np.isclose(repaired_df[c], correct_df[c], rtol=5e-6).all())
            except:
                print(f"tkr={tkr} COLUMN={c}")
                print("- repaired_df")
                print(repaired_df)
                print("- correct_df[c]:")
                print(correct_df[c])
                print("- diff:")
                print(repaired_df[c] - correct_df[c])
                raise


if __name__ == '__main__':
    unittest.main()

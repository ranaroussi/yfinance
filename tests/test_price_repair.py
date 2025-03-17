from tests.context import yfinance as yf
from tests.context import session_gbl

import unittest

import os
import datetime as _dt
import numpy as _np
import pandas as _pd


class TestPriceRepairAssumptions(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = session_gbl
        cls.dp = os.path.dirname(__file__)

    @classmethod
    def tearDownClass(cls):
        if cls.session is not None:
            cls.session.close()

    def test_resampling(self):
        for tkr in ['GOOGL', 'GLEN.L', '2330.TW']:
            dat = yf.Ticker(tkr, session=self.session)

            intervals = ['1d', '1wk', '1mo', '3mo']
            periods = ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd']#, 'max']
            # Yahoo handles period=max weird. For tkr=INTC, interval=1d starts 5 years before interval=1mo
            for i in range(len(intervals)):
                interval = intervals[i]
                if interval == '1d':
                    continue
                for j in range(i, len(periods)):
                    period = periods[j]

                    df_truth = dat.history(interval=interval, period=period)
                    # df_1d = dat.history(interval='1d', period=period)
                    # dfr = dat._lazy_load_price_history()._resample(df_1d, '1d', interval, period)
                    dfr = dat.history(interval=interval, period=period, repair=True)

                    debug = False
                    if len(dfr) != len(df_truth):
                        if dfr.index[1] == df_truth.index[0]:
                            # print("  - resampled has extra row at start")
                            pass
                        elif dfr.index[0] == df_truth.index[1]:
                            print("  - resampled missing a row at start")
                            debug = True
                        else:
                            print("  - resampled index different length")
                            debug = True
                    elif (dfr.index != df_truth.index).all():
                        print("  - resampled index mismatch:")
                        print(dfr.index == df_truth.index)
                        debug = True
                    else:
                        # vol_match = dfr['Volume'] == df_truth['Volume']
                        vol_diff_pct0 = (dfr['Volume'].iloc[0] - df_truth['Volume'].iloc[0])/df_truth['Volume'].iloc[0]
                        vol_diff_pct1 = (dfr['Volume'].iloc[-1] - df_truth['Volume'].iloc[-1])/df_truth['Volume'].iloc[-1]
                        vol_diff_pct = _np.array([vol_diff_pct0, vol_diff_pct1])
                        vol_match = vol_diff_pct > -0.23
                        vol_match_nmatch = _np.sum(vol_match)
                        vol_match_ndiff = len(vol_match) - vol_match_nmatch
                        if vol_match.all():
                            # print("  - volume match 100%")
                            pass
                        elif vol_match_ndiff == 1 and not vol_match[0]:
                            # print("  - volume almost-perfect match, only first different")
                            # debug = True
                            pass
                        else:
                            # print(f"  - volume match {vol_match_nmatch}/{len(vol_match)} {vol_match.to_numpy()}")
                            print(f"  - volume significantly different in first row: vol_diff_pct={vol_diff_pct*100}%")
                            debug = True

                    if debug:
                        print("- investigate:")
                        print("- df_truth:")
                        print(df_truth)#[['Open', 'Close', 'Volume']])
                        df_1d = dat.history(interval='1d', period=period)
                        print("- df_1d:")
                        print(df_1d)#[['Open', 'Close', 'Volume']])
                        print("- dfr:")
                        print(dfr)#[['Open', 'Close', 'Volume']])
                        return



class TestPriceRepair(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        cls.session = session_gbl
        cls.dp = os.path.dirname(__file__)

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
        df = _pd.DataFrame(data={"Open":      [470.5,   473.5, 474.5, 470],
                                 "High":      [476,     476.5, 477,   480],
                                 "Low":       [470.5,   470,   465.5, 468.26],
                                 "Close":     [475,     473.5, 472,   473.5],
                                 "Adj Close": [474.865, 468.6, 467.1, 468.6],
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
                                 "Adj Close": [409.75, 393.43, 386.22, 383.34],
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
        for c in data_cols:
            df[c] = df[c].astype('float')
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
        intervals = ['1d', '1wk']
        # tkrs = ['AET.L']
        # intervals = ['1wk']
        # import yfinance as yf ; yf.enable_debug_mode()

        for tkr in tkrs:
            for interval in intervals:
                dat = yf.Ticker(tkr, session=self.session)
                tz_exchange = dat.fast_info["timezone"]
                hist = dat._lazy_load_price_history()

                data_cols = ["Low", "High", "Open", "Close", "Adj Close"]
                fp = os.path.join(self.dp, "data", tkr.replace('.','-') + '-' + interval + "-100x-error.csv")
                if not os.path.isfile(fp):
                    continue
                df_bad = _pd.read_csv(fp, index_col="Date")
                df_bad.index = _pd.to_datetime(df_bad.index, utc=True).tz_convert(tz_exchange)
                df_bad = df_bad.sort_index()

                df = df_bad.copy()
                fp = os.path.join(self.dp, "data", tkr.replace('.','-') + '-' + interval + "-100x-error-fixed.csv")
                df = _pd.read_csv(fp, index_col="Date")
                df.index = _pd.to_datetime(df.index, utc=True).tz_convert(tz_exchange)
                df = df.sort_index()

                df_repaired = hist._fix_unit_switch(df_bad, interval, tz_exchange)
                df_repaired = df_repaired.sort_index()

                # First test - no errors left
                for c in data_cols:
                    try:
                        self.assertTrue(_np.isclose(df_repaired[c], df[c], rtol=1e-2).all())
                    except Exception:
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

        df_bad = _pd.DataFrame(data={"Open":      [0,      114.37, 114.20],
                                     "High":      [0,      114.40, 114.40],
                                     "Low":       [0,      114.36, 114.20],
                                     "Close":     [114.39, 114.38, 114.45],
                                     "Adj Close": [114.39, 114.38, 114.45],
                                     "Volume":    [9,      15666,  1094]},
                               index=_pd.to_datetime([_dt.datetime(2025, 3, 17),
                                                      _dt.datetime(2025, 3, 14),
                                                      _dt.datetime(2025, 3, 13)]))
        df_bad = df_bad.sort_index()
        df_bad.index.name = "Date"
        df_bad.index = df_bad.index.tz_localize(tz_exchange)

        repaired_df = hist._fix_zeroes(df_bad, "1d", tz_exchange, prepost=False)

        correct_df = df_bad.copy()
        correct_df.loc["2025-03-17", "Open"] = 114.62
        correct_df.loc["2025-03-17", "High"] = 114.62
        correct_df.loc["2025-03-17", "Low"] = 114.41
        for c in ["Open", "Low", "High", "Close"]:
            self.assertTrue(_np.isclose(repaired_df[c], correct_df[c], rtol=1e-7).all())

        self.assertTrue("Repaired?" in repaired_df.columns)
        self.assertFalse(repaired_df["Repaired?"].isna().any())

    def test_repair_zeroes_daily_adjClose(self):
        # Test that 'Adj Close' is reconstructed correctly,
        # particularly when a dividend occurred within 1 day.

        self.skipTest("Currently failing because Yahoo returning slightly different data for interval 1d vs 1h on day Aug 6 2024")

        tkr = "INTC"
        df = _pd.DataFrame(data={"Open":      [2.020000e+01, 2.032000e+01, 1.992000e+01, 1.910000e+01, 2.008000e+01],
                                 "High":      [2.039000e+01, 2.063000e+01, 2.025000e+01, 2.055000e+01, 2.015000e+01],
                                 "Low":       [1.929000e+01, 1.975000e+01, 1.895000e+01, 1.884000e+01, 1.950000e+01],
                                 "Close":     [2.011000e+01, 1.983000e+01, 1.899000e+01, 2.049000e+01, 1.971000e+01],
                                 "Adj Close": [1.998323e+01, 1.970500e+01, 1.899000e+01, 2.049000e+01, 1.971000e+01],
                                 "Volume":    [1.473857e+08, 1.066704e+08, 9.797230e+07, 9.683680e+07, 7.639450e+07],
                                 "Dividends": [0.000000e+00, 0.000000e+00, 1.250000e-01, 0.000000e+00, 0.000000e+00]},
                           index=_pd.to_datetime([_dt.datetime(2024, 8, 9),
                                                  _dt.datetime(2024, 8, 8),
                                                  _dt.datetime(2024, 8, 7),
                                                  _dt.datetime(2024, 8, 6),
                                                  _dt.datetime(2024, 8, 5)]))
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
        good_tkrs += ['AEI', 'GHI', 'IRON', 'LXU', 'RSLS', 'TISI']
        good_tkrs += ['BOL.ST', 'TUI1.DE']
        intervals = ['1d', '1wk', '1mo', '3mo']
        for tkr in good_tkrs:
            for interval in intervals:
                dat = yf.Ticker(tkr, session=self.session)
                tz_exchange = dat.fast_info["timezone"]
                hist = dat._lazy_load_price_history()

                df_good = dat.history(start='2020-01-01', end=_dt.date.today(), interval=interval, auto_adjust=False)

                repaired_df = hist._fix_bad_stock_splits(df_good, interval, tz_exchange)

                # Expect no change from repair
                df_good = df_good.sort_index()
                repaired_df = repaired_df.sort_index()
                for c in ["Open", "Low", "High", "Close", "Adj Close", "Volume"]:
                    try:
                        self.assertTrue((repaired_df[c].to_numpy() == df_good[c].to_numpy()).all())
                    except Exception:
                        print(f"tkr={tkr} interval={interval} COLUMN={c}")
                        df_dbg = df_good[[c]].join(repaired_df[[c]], lsuffix='.good', rsuffix='.repaired')
                        f_diff = repaired_df[c].to_numpy() != df_good[c].to_numpy()
                        print(df_dbg[f_diff | _np.roll(f_diff, 1) | _np.roll(f_diff, -1)])
                        raise

        bad_tkrs = ['4063.T', 'AV.L', 'CNE.L', 'MOB.ST', 'SPM.MI']
        bad_tkrs.append('LA.V')  # special case - stock split error is 3 years ago! why not fixed?
        for tkr in bad_tkrs:
            dat = yf.Ticker(tkr, session=self.session)
            tz_exchange = dat.fast_info["timezone"]
            hist = dat._lazy_load_price_history()

            interval = '1d'
            fp = os.path.join(self.dp, "data", tkr.replace('.','-')+'-'+interval+"-bad-stock-split.csv")
            if not os.path.isfile(fp):
                interval = '1wk'
                fp = os.path.join(self.dp, "data", tkr.replace('.','-')+'-'+interval+"-bad-stock-split.csv")
            df_bad = _pd.read_csv(fp, index_col="Date")
            df_bad.index = _pd.to_datetime(df_bad.index, utc=True)

            repaired_df = hist._fix_bad_stock_splits(df_bad, "1d", tz_exchange)

            fp = os.path.join(self.dp, "data", tkr.replace('.','-')+'-'+interval+"-bad-stock-split-fixed.csv")
            correct_df = _pd.read_csv(fp, index_col="Date")
            correct_df.index = _pd.to_datetime(correct_df.index, utc=True)

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

    def test_repair_bad_div_adjusts(self):
        interval = '1d'
        bad_tkrs = []
        false_positives = []

        # Tickers are not random. Either their errors were really bad, or
        # they discovered bugs/gaps in repair logic.

        # bad_tkrs += ['MPCC.OL']  # has yahoo fixed?

        # These tickers were exceptionally bad
        bad_tkrs += ['LSC.L']
        bad_tkrs += ['TEM.L']

        # Other special sits
        bad_tkrs += ['KME.MI']  # 2023 dividend paid to savings share, not common/preferred
        bad_tkrs += ['REL.L']  # 100x div also missing adjust
        bad_tkrs.append('4063.T')  # Div with same-day split not split adjusted

        # Adj too small
        bad_tkrs += ['ADIG.L']
        bad_tkrs += ['CLC.L']
        bad_tkrs += ['RGL.L']
        bad_tkrs += ['SERE.L']

        # Div 100x
        bad_tkrs += ['ABDP.L']
        bad_tkrs += ['ELCO.L']
        bad_tkrs += ['PSH.L']

        # Div 100x and adjust too big
        bad_tkrs += ['SCR.TO']

        # Div 0.01x
        bad_tkrs += ['NVT.L']
        bad_tkrs += ['TENT.L']

        # Missing div adjusts:
        bad_tkrs += ['1398.HK']
        bad_tkrs += ['3988.HK']
        bad_tkrs += ['KEN.TA']

        # Phantom divs
        bad_tkrs += ['KAP.IL']  # 1x 1d phantom div, and false positives 0.01x in 1wk
        bad_tkrs += ['SAND']
        bad_tkrs += ['TEM.L']
        bad_tkrs += ['TEP.PA']

        # Maybe test tickers with mix of adj-too-small and 100x

        false_positives += ['CALM']  # tiny div on 2023-10-31
        false_positives += ['EWG']  # tiny div 2022-12-13
        false_positives += ['HSBK.IL']  # normal divs but 1wk volatility uncovered logic bug
        false_positives += ['IBE.MC']  # 2x 0.01x divs only detected when compared to others. pass
        false_positives += ['KMR.L']
        false_positives += ['TISG.MI']

        for tkr in false_positives:
            # Nothing should change
            dat = yf.Ticker(tkr, session=self.session)
            hist = dat._lazy_load_price_history()
            hist.history(period='1mo')  # init metadata for currency
            currency = hist._history_metadata['currency']
            tz = hist._history_metadata['exchangeTimezoneName']

            fp = os.path.join(self.dp, "data", tkr.replace('.','-') + '-' + interval + "-no-bad-divs.csv")
            if not os.path.isfile(fp):
                continue
            df = _pd.read_csv(fp, index_col='Datetime')
            df.index = _pd.to_datetime(df.index, utc=True).tz_convert(tz)

            repaired_df = hist._fix_bad_div_adjust(df, interval, currency)

            c = 'Dividends'
            self.assertTrue(_np.isclose(repaired_df[c].to_numpy(), df[c].to_numpy(), rtol=1e-12, equal_nan=True).all())
            c = 'Adj Close'
            try:
                f_close = _np.isclose(repaired_df[c].to_numpy(), df[c].to_numpy(), rtol=1e-12, equal_nan=True)
                self.assertTrue(f_close.all())
            except Exception:
                f_diff = ~f_close
                print(f"tkr={tkr} interval={interval}")
                print("- repaired_df:")
                print(repaired_df[c][f_diff])
                print("- df:")
                print(df[c][f_diff])
                print("- diff:")
                print(repaired_df[c][f_diff] - df[c][f_diff])
                raise

        for tkr in bad_tkrs:
            dat = yf.Ticker(tkr, session=self.session)
            hist = dat._lazy_load_price_history()
            hist.history(period='1mo')  # init metadata for currency
            currency = hist._history_metadata['currency']
            tz = hist._history_metadata['exchangeTimezoneName']

            fp = os.path.join(self.dp, "data", tkr.replace('.','-') + '-' + interval + "-bad-div.csv")
            if not os.path.isfile(fp):
                continue
            df_bad = _pd.read_csv(fp, index_col='Datetime')
            df_bad.index = _pd.to_datetime(df_bad.index, utc=True).tz_convert(tz)
            fp = os.path.join(self.dp, "data", tkr.replace('.','-') + '-' + interval + "-bad-div-fixed.csv")
            correct_df = _pd.read_csv(fp, index_col='Datetime')
            correct_df.index = _pd.to_datetime(correct_df.index, utc=True).tz_convert(tz)

            repaired_df = hist._fix_bad_div_adjust(df_bad, interval, currency)

            c = 'Dividends'
            f_close = _np.isclose(repaired_df[c].to_numpy(), correct_df[c].to_numpy(), rtol=1e-12, equal_nan=True)
            try:
                self.assertTrue(f_close.all())
            except Exception:
                f_diff = ~f_close
                print(f"tkr={tkr} interval={interval}")
                print("- repaired_df:")
                print(repaired_df[c][f_diff])
                print("- correct_df:")
                print(correct_df[c][f_diff])
                print("- diff:")
                print(repaired_df[c][f_diff] - correct_df[c][f_diff])
                raise

            c = 'Adj Close'
            try:
                f_close = _np.isclose(repaired_df[c].to_numpy(), correct_df[c].to_numpy(), rtol=5e-7, equal_nan=True)
                self.assertTrue(f_close.all())
            except Exception:
                f_diff = ~f_close
                print(f"tkr={tkr} interval={interval}")
                print("- repaired_df:")
                print(repaired_df[c][f_diff])
                print("- correct_df:")
                print(correct_df[c][f_diff])
                print("- diff:")
                print(repaired_df[c][f_diff] - correct_df[c][f_diff])
                raise


if __name__ == '__main__':
    unittest.main()

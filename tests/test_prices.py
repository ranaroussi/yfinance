import datetime as _dt
import socket
import unittest

import numpy as _np
import polars as _pl
import pytz as _tz

from tests.context import session_gbl
from tests.context import yfinance as yf


class TestPriceHistory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.session = session_gbl

    @classmethod
    def tearDownClass(cls):
        if cls.session is not None:
            cls.session.close()

    def _get_date_col(self, df):
        """Return the name of the date/datetime column."""
        if "Datetime" in df.columns:
            return "Datetime"
        if "Date" in df.columns:
            return "Date"
        raise ValueError(f"No date column found in {df.columns}")

    def test_daily_index(self):
        tkrs = ["BHP.AX", "IMP.JO", "BP.L", "PNL.L", "INTC"]
        intervals = ["1d", "1wk", "1mo"]
        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)

            for interval in intervals:
                df = dat.history(period="5y", interval=interval)
                date_col = self._get_date_col(df)
                # Daily/weekly/monthly data should have date values with time == midnight
                # In polars the Date type has no time component; Datetime type may have time.
                if isinstance(df[date_col].dtype, _pl.Datetime):
                    times = df[date_col].dt.time()
                    midnight = _dt.time(0)
                    self.assertTrue(
                        all(t == midnight for t in times.to_list()),
                        f"{tkr} {interval}: not all times are midnight",
                    )
                # else: Date dtype has no time component, implicitly midnight

    def test_download_multi_large_interval(self):
        tkrs = ["BHP.AX", "IMP.JO", "BP.L", "PNL.L", "INTC"]
        intervals = ["1d", "1wk", "1mo"]
        for interval in intervals:
            with self.subTest(interval):
                df = yf.download(tkrs, period="5y", interval=interval)
                date_col = self._get_date_col(df)

                if isinstance(df[date_col].dtype, _pl.Datetime):
                    times = df[date_col].dt.time()
                    midnight = _dt.time(0)
                    self.assertTrue(
                        all(t == midnight for t in times.to_list()),
                        f"{interval}: not all times are midnight",
                    )

                # Check all tickers present
                if "Ticker" in df.columns:
                    df_tkrs = df["Ticker"].unique().to_list()
                    self.assertEqual(sorted(tkrs), sorted(df_tkrs))

    def test_download_multi_small_interval(self):
        use_tkrs = ["AAPL", "0Q3.DE", "ATVI"]
        df = yf.download(use_tkrs, period="1d", interval="5m", auto_adjust=True)
        date_col = self._get_date_col(df)
        # UTC timezone expected
        if isinstance(df[date_col].dtype, _pl.Datetime):
            self.assertEqual(df[date_col].dtype.time_zone, "UTC")

    def test_download_with_invalid_ticker(self):
        # Checks if using an invalid symbol gives the same output as not using an invalid
        # symbol in combination with a valid symbol (AAPL)
        invalid_tkrs = ["AAPL", "ATVI"]  # AAPL exists and ATVI does not exist
        valid_tkrs = ["AAPL", "INTC"]  # AAPL and INTC both exist

        start_d = _dt.date.today() - _dt.timedelta(days=30)
        data_invalid_sym = yf.download(invalid_tkrs, start=start_d, auto_adjust=True)
        data_valid_sym = yf.download(valid_tkrs, start=start_d, auto_adjust=True)

        date_col = self._get_date_col(data_invalid_sym)
        dt_compare = data_invalid_sym[date_col][0]

        # Get AAPL Close for both, filtered to the same first date
        inv_aapl = data_invalid_sym.filter(
            (_pl.col("Ticker") == "AAPL") & (_pl.col(date_col) == dt_compare)
        )["Close"][0]
        val_aapl = data_valid_sym.filter(
            (_pl.col("Ticker") == "AAPL") & (_pl.col(date_col) == dt_compare)
        )["Close"][0]
        self.assertEqual(inv_aapl, val_aapl)

    def test_duplicatingHourly(self):
        tkrs = ["IMP.JO", "BHG.JO", "SSW.JO", "BP.L", "INTC"]
        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)
            tz = dat._get_ticker_tz(timeout=None)

            dt_utc = _dt.datetime.now(_dt.timezone.utc)
            dt = dt_utc.astimezone(_tz.timezone(tz))
            start_d = dt.date() - _dt.timedelta(days=7)
            df = dat.history(start=start_d, interval="1h")

            date_col = self._get_date_col(df)
            dt0 = df[date_col][-2]
            dt1 = df[date_col][-1]
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
            tz = dat._get_ticker_tz(timeout=None)

            dt_utc = _dt.datetime.now(_dt.timezone.utc)
            dt = dt_utc.astimezone(_tz.timezone(tz))
            if dt.time() < _dt.time(17, 0):
                continue
            test_run = True

            df = dat.history(start=dt.date() - _dt.timedelta(days=7), interval="1d")
            date_col = self._get_date_col(df)
            dt0 = df[date_col][-2]
            dt1 = df[date_col][-1]
            try:
                self.assertNotEqual(dt0, dt1)
            except AssertionError:
                print("Ticker = ", tkr)
                raise

        if not test_run:
            self.skipTest(
                "Skipping test_duplicatingDaily() because only expected to fail just after market close"
            )

    def test_duplicatingWeekly(self):
        tkrs = ["MSFT", "IWO", "VFINX", "^GSPC", "BTC-USD"]
        test_run = False
        for tkr in tkrs:
            dat = yf.Ticker(tkr, session=self.session)
            tz = dat._get_ticker_tz(timeout=None)

            dt = _tz.timezone(tz).localize(_dt.datetime.now())
            if dt.date().weekday() not in [1, 2, 3, 4]:
                continue
            test_run = True

            df = dat.history(start=dt.date() - _dt.timedelta(days=7), interval="1wk")
            date_col = self._get_date_col(df)
            dt0 = df[date_col][-2]
            dt1 = df[date_col][-1]
            try:
                # dt0 and dt1 are date or datetime objects; get ISO week number
                d0 = dt0 if isinstance(dt0, _dt.date) else dt0.date()
                d1 = dt1 if isinstance(dt1, _dt.date) else dt1.date()
                self.assertNotEqual(d0.isocalendar()[1], d1.isocalendar()[1])
            except AssertionError:
                print("Ticker={}: Last two rows within same week:".format(tkr))
                print(df[-2:])
                raise

        if not test_run:
            self.skipTest(
                "Skipping test_duplicatingWeekly() because not possible to fail Monday/weekend"
            )

    def test_pricesEventsMerge(self):
        # Test case: dividend occurs after last row in price data
        # This test exercises yf.utils.safe_merge_dfs which still uses pandas internally
        import pandas as _pd_compat

        tkr = "INTC"
        start_d = _dt.date(2022, 1, 1)
        end_d = _dt.date(2023, 1, 1)
        df_pl = yf.Ticker(tkr, session=self.session).history(
            interval="1d", start=start_d, end=end_d
        )
        date_col = self._get_date_col(df_pl)

        # Convert last date to a pandas Timestamp for safe_merge_dfs (pandas internal util)
        last_date_val = df_pl[date_col][-1]
        if isinstance(last_date_val, _dt.date) and not isinstance(
            last_date_val, _dt.datetime
        ):
            last_date_val = _dt.datetime(
                last_date_val.year,
                last_date_val.month,
                last_date_val.day,
                tzinfo=_tz.timezone("America/New_York"),
            )
        future_div_dt = last_date_val + _dt.timedelta(days=1)
        if future_div_dt.weekday() in [5, 6]:
            future_div_dt += _dt.timedelta(days=(7 - future_div_dt.weekday()))

        # safe_merge_dfs is a pandas-internal utility; skip if pandas not available
        try:
            import pandas as _pd_inner
        except ImportError:
            self.skipTest("safe_merge_dfs requires pandas internally")
            return

        divs = _pd_inner.DataFrame(data={"Dividends": [1.0]}, index=[future_div_dt])
        # Convert polars df to pandas for the merge utility
        from yfinance.scrapers.history import _pl_to_pd

        df_pd = _pl_to_pd(df_pl)
        df2 = yf.utils.safe_merge_dfs(
            df_pd.drop(["Dividends", "Stock Splits"], axis=1, errors="ignore"),
            divs,
            "1d",
        )
        self.assertIn(future_div_dt, df2.index)
        self.assertIn("Dividends", df2.columns)
        self.assertEqual(df2["Dividends"].iloc[-1], 1.0)

    def test_pricesEventsMerge_bug(self):
        # Reproduce exception when merging intraday prices with future dividend
        # safe_merge_dfs is pandas-internal; test using pandas directly
        try:
            import pandas as _pd_inner
        except ImportError:
            self.skipTest("safe_merge_dfs requires pandas internally")
            return

        interval = "30m"
        df_index = []
        d = 13
        for h in range(0, 16):
            for m in [0, 30]:
                df_index.append(_dt.datetime(2023, 9, d, h, m))
        df_index.append(_dt.datetime(2023, 9, d, 16))
        df = _pd_inner.DataFrame(index=df_index)
        df.index = _pd_inner.to_datetime(df.index)
        df["Close"] = 1.0

        div = 1.0
        future_div_dt = _dt.datetime(2023, 9, 14, 10)
        divs = _pd_inner.DataFrame(data={"Dividends": [div]}, index=[future_div_dt])

        yf.utils.safe_merge_dfs(df, divs, interval)
        # No exception = test pass

    def test_intraDayWithEvents(self):
        tkrs = ["BHP.AX", "IMP.JO", "BP.L", "PNL.L", "INTC"]
        test_run = False
        for tkr in tkrs:
            start_d = _dt.date.today() - _dt.timedelta(days=59)
            end_d = None
            df_daily = yf.Ticker(tkr, session=self.session).history(
                start=start_d, end=end_d, interval="1d", actions=True
            )
            date_col = self._get_date_col(df_daily)
            df_daily_divs = df_daily.filter(_pl.col("Dividends") != 0)
            if df_daily_divs.height == 0:
                continue

            # Get the date range from dividend dates
            first_div_date = df_daily_divs[date_col][0]
            last_div_date = df_daily_divs[date_col][-1]

            if isinstance(first_div_date, _dt.datetime):
                start_d2 = first_div_date.date()
                end_d2 = last_div_date.date() + _dt.timedelta(days=1)
            else:
                start_d2 = first_div_date
                end_d2 = last_div_date + _dt.timedelta(days=1)

            df_intraday = yf.Ticker(tkr, session=self.session).history(
                start=start_d2, end=end_d2, interval="15m", actions=True
            )
            self.assertTrue(
                (df_intraday["Dividends"] != 0.0).any(),
                f"{tkr}: no intraday dividends found",
            )

            intra_date_col = self._get_date_col(df_intraday)
            df_intraday_divs = df_intraday.filter(_pl.col("Dividends") != 0)

            # Floor intraday datetimes to dates for comparison
            intra_div_dates = sorted(
                [
                    v.date() if isinstance(v, _dt.datetime) else v
                    for v in df_intraday_divs[intra_date_col].to_list()
                ]
            )
            daily_div_dates = sorted(
                [
                    v.date() if isinstance(v, _dt.datetime) else v
                    for v in df_daily_divs[date_col].to_list()
                ]
            )
            self.assertEqual(intra_div_dates, daily_div_dates)
            test_run = True

        if not test_run:
            self.skipTest(
                "Skipping test_intraDayWithEvents() because no tickers had a dividend in last 60 days"
            )

    def test_intraDayWithEvents_tase(self):
        # TASE dividend release pre-market, doesn't merge nicely with intra-day data so check still present
        tase_tkrs = ["ICL.TA", "ESLT.TA", "ONE.TA", "MGDL.TA"]
        test_run = False
        for tkr in tase_tkrs:
            start_d = _dt.date.today() - _dt.timedelta(days=59)
            end_d = None
            df_daily = yf.Ticker(tkr, session=self.session).history(
                start=start_d, end=end_d, interval="1d", actions=True
            )
            date_col = self._get_date_col(df_daily)
            df_daily_divs = df_daily.filter(_pl.col("Dividends") != 0)
            if df_daily_divs.height == 0:
                continue

            first_div_date = df_daily_divs[date_col][0]
            last_div_date = df_daily_divs[date_col][-1]

            if isinstance(first_div_date, _dt.datetime):
                start_d2 = first_div_date.date()
                end_d2 = last_div_date.date() + _dt.timedelta(days=1)
            else:
                start_d2 = first_div_date
                end_d2 = last_div_date + _dt.timedelta(days=1)

            df_intraday = yf.Ticker(tkr, session=self.session).history(
                start=start_d2, end=end_d2, interval="15m", actions=True
            )
            self.assertTrue(
                (df_intraday["Dividends"] != 0.0).any(),
                f"{tkr}: no intraday dividends found",
            )

            intra_date_col = self._get_date_col(df_intraday)
            df_intraday_divs = df_intraday.filter(_pl.col("Dividends") != 0)

            intra_div_dates = sorted(
                [
                    v.date() if isinstance(v, _dt.datetime) else v
                    for v in df_intraday_divs[intra_date_col].to_list()
                ]
            )
            daily_div_dates = sorted(
                [
                    v.date() if isinstance(v, _dt.datetime) else v
                    for v in df_daily_divs[date_col].to_list()
                ]
            )
            self.assertEqual(intra_div_dates, daily_div_dates)
            test_run = True

        if not test_run:
            self.skipTest(
                "Skipping test_intraDayWithEvents_tase() because no tickers had a dividend in last 60 days"
            )

    def test_dailyWithEvents(self):
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
            date_col = self._get_date_col(df)
            df_divs = df.filter(_pl.col("Dividends") != 0).sort(
                date_col, descending=True
            )
            try:
                div_dates = [
                    v.date() if isinstance(v, _dt.datetime) else v
                    for v in df_divs[date_col].to_list()
                ]
                self.assertEqual(div_dates, dates)
            except AssertionError:
                print(f"- ticker = {tkr}")
                print("- response:")
                print(div_dates)
                print("- answer:")
                print(dates)
                raise

    def test_dailyWithEvents_bugs(self):
        # Reproduce issue #521
        tkr1 = "QQQ"
        tkr2 = "GDX"
        start_d = "2014-12-29"
        end_d = "2020-11-29"
        df1 = yf.Ticker(tkr1).history(
            start=start_d, end=end_d, interval="1d", actions=True
        )
        df2 = yf.Ticker(tkr2).history(
            start=start_d, end=end_d, interval="1d", actions=True
        )
        date_col1 = self._get_date_col(df1)
        date_col2 = self._get_date_col(df2)

        self.assertTrue(((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any())
        self.assertTrue(((df2["Dividends"] > 0) | (df2["Stock Splits"] > 0)).any())

        try:
            dates1 = set(df1[date_col1].to_list())
            dates2 = set(df2[date_col2].to_list())
            self.assertEqual(dates1, dates2)
        except AssertionError:
            missing_from_df1 = dates2 - dates1
            missing_from_df2 = dates1 - dates2
            print("{} missing these dates: {}".format(tkr1, missing_from_df1))
            print("{} missing these dates: {}".format(tkr2, missing_from_df2))
            raise

        # Test that index same with and without events:
        tkrs = [tkr1, tkr2]
        for tkr in tkrs:
            df1 = yf.Ticker(tkr, session=self.session).history(
                start=start_d, end=end_d, interval="1d", actions=True
            )
            df2 = yf.Ticker(tkr, session=self.session).history(
                start=start_d, end=end_d, interval="1d", actions=False
            )
            dc1 = self._get_date_col(df1)
            dc2 = self._get_date_col(df2)
            self.assertTrue(((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any())
            try:
                dates1 = set(df1[dc1].to_list())
                dates2 = set(df2[dc2].to_list())
                self.assertEqual(dates1, dates2)
            except AssertionError:
                missing_from_df1 = dates2 - dates1
                missing_from_df2 = dates1 - dates2
                print(
                    "{}-with-events missing these dates: {}".format(
                        tkr, missing_from_df1
                    )
                )
                print(
                    "{}-without-events missing these dates: {}".format(
                        tkr, missing_from_df2
                    )
                )
                raise

        # Reproduce issue #1634 - 1d dividend out-of-range, should be prepended to prices
        # This sub-test uses pandas-internal safe_merge_dfs
        try:
            import pandas as _pd_inner
        except ImportError:
            return  # skip sub-test if pandas unavailable

        div_dt = _pd_inner.Timestamp(2022, 7, 21).tz_localize("America/New_York")
        df_dividends = _pd_inner.DataFrame(data={"Dividends": [1.0]}, index=[div_dt])
        df_prices = _pd_inner.DataFrame(
            data={c: [1.0] for c in yf.const._PRICE_COLNAMES_} | {"Volume": 0},
            index=[div_dt + _dt.timedelta(days=1)],
        )
        df_merged = yf.utils.safe_merge_dfs(df_prices, df_dividends, "1d")
        self.assertEqual(df_merged.shape[0], 2)
        self.assertTrue(df_merged[df_prices.columns].iloc[1:].equals(df_prices))
        self.assertEqual(df_merged.index[0], div_dt)

    def test_weeklyWithEvents(self):
        # Reproduce issue #521
        tkr1 = "QQQ"
        tkr2 = "GDX"
        start_d = "2014-12-29"
        end_d = "2020-11-29"
        df1 = yf.Ticker(tkr1).history(
            start=start_d, end=end_d, interval="1wk", actions=True
        )
        df2 = yf.Ticker(tkr2).history(
            start=start_d, end=end_d, interval="1wk", actions=True
        )
        date_col1 = self._get_date_col(df1)
        date_col2 = self._get_date_col(df2)

        self.assertTrue(((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any())
        self.assertTrue(((df2["Dividends"] > 0) | (df2["Stock Splits"] > 0)).any())
        try:
            dates1 = set(df1[date_col1].to_list())
            dates2 = set(df2[date_col2].to_list())
            self.assertEqual(dates1, dates2)
        except AssertionError:
            missing_from_df1 = dates2 - dates1
            missing_from_df2 = dates1 - dates2
            print("{} missing these dates: {}".format(tkr1, missing_from_df1))
            print("{} missing these dates: {}".format(tkr2, missing_from_df2))
            raise

        # Test that index same with and without events:
        tkrs = [tkr1, tkr2]
        for tkr in tkrs:
            df1 = yf.Ticker(tkr, session=self.session).history(
                start=start_d, end=end_d, interval="1wk", actions=True
            )
            df2 = yf.Ticker(tkr, session=self.session).history(
                start=start_d, end=end_d, interval="1wk", actions=False
            )
            dc1 = self._get_date_col(df1)
            dc2 = self._get_date_col(df2)
            self.assertTrue(((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any())
            try:
                dates1 = set(df1[dc1].to_list())
                dates2 = set(df2[dc2].to_list())
                self.assertEqual(dates1, dates2)
            except AssertionError:
                missing_from_df1 = dates2 - dates1
                missing_from_df2 = dates1 - dates2
                print(
                    "{}-with-events missing these dates: {}".format(
                        tkr, missing_from_df1
                    )
                )
                print(
                    "{}-without-events missing these dates: {}".format(
                        tkr, missing_from_df2
                    )
                )
                raise

    def test_monthlyWithEvents(self):
        tkr1 = "QQQ"
        tkr2 = "GDX"
        start_d = "2014-12-29"
        end_d = "2020-11-29"
        df1 = yf.Ticker(tkr1).history(
            start=start_d, end=end_d, interval="1mo", actions=True
        )
        df2 = yf.Ticker(tkr2).history(
            start=start_d, end=end_d, interval="1mo", actions=True
        )
        date_col1 = self._get_date_col(df1)
        date_col2 = self._get_date_col(df2)

        self.assertTrue(((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any())
        self.assertTrue(((df2["Dividends"] > 0) | (df2["Stock Splits"] > 0)).any())
        try:
            dates1 = set(df1[date_col1].to_list())
            dates2 = set(df2[date_col2].to_list())
            self.assertEqual(dates1, dates2)
        except AssertionError:
            missing_from_df1 = dates2 - dates1
            missing_from_df2 = dates1 - dates2
            print("{} missing these dates: {}".format(tkr1, missing_from_df1))
            print("{} missing these dates: {}".format(tkr2, missing_from_df2))
            raise

        # Test that index same with and without events:
        tkrs = [tkr1, tkr2]
        for tkr in tkrs:
            df1 = yf.Ticker(tkr, session=self.session).history(
                start=start_d, end=end_d, interval="1mo", actions=True
            )
            df2 = yf.Ticker(tkr, session=self.session).history(
                start=start_d, end=end_d, interval="1mo", actions=False
            )
            dc1 = self._get_date_col(df1)
            dc2 = self._get_date_col(df2)
            self.assertTrue(((df1["Dividends"] > 0) | (df1["Stock Splits"] > 0)).any())
            try:
                dates1 = set(df1[dc1].to_list())
                dates2 = set(df2[dc2].to_list())
                self.assertEqual(dates1, dates2)
            except AssertionError:
                missing_from_df1 = dates2 - dates1
                missing_from_df2 = dates1 - dates2
                print(
                    "{}-with-events missing these dates: {}".format(
                        tkr, missing_from_df1
                    )
                )
                print(
                    "{}-without-events missing these dates: {}".format(
                        tkr, missing_from_df2
                    )
                )
                raise

    def test_monthlyWithEvents2(self):
        # Simply check no exception from internal merge
        dfm = yf.Ticker("ABBV").history(period="max", interval="1mo")
        dfd = yf.Ticker("ABBV").history(period="max", interval="1d")
        dcm = self._get_date_col(dfm)
        dcd = self._get_date_col(dfd)

        first_monthly_date = dfm[dcm][0]
        dfd = dfd.filter(_pl.col(dcd) > first_monthly_date)

        dfm_divs = dfm.filter(_pl.col("Dividends") != 0)
        dfd_divs = dfd.filter(_pl.col("Dividends") != 0)
        self.assertEqual(dfm_divs.height, dfd_divs.height)

    def test_tz_dst_ambiguous(self):
        # Reproduce issue #1100
        try:
            yf.Ticker("ESLT.TA", session=self.session).history(
                start="2002-10-06", end="2002-10-09", interval="1d"
            )
        except _tz.exceptions.AmbiguousTimeError:
            raise Exception("Ambiguous DST issue not resolved")

    def test_dst_fix(self):
        # Daily intervals should start at time 00:00. But for some combinations of date
        # and timezone, Yahoo has time off by few hours. Suspect DST problem.
        # The correction is successful if no days are weekend, and weekly data begins Monday

        tkr = "AGRO3.SA"
        dat = yf.Ticker(tkr, session=self.session)
        start = "2021-01-11"
        end = "2022-11-05"

        interval = "1d"
        df = dat.history(start=start, end=end, interval=interval)
        date_col = self._get_date_col(df)
        weekdays = [
            v.weekday() if isinstance(v, _dt.date) else v.date().weekday()
            for v in df[date_col].to_list()
        ]
        self.assertTrue(all(0 <= wd <= 4 for wd in weekdays))

        interval = "1wk"
        df = dat.history(start=start, end=end, interval=interval)
        date_col = self._get_date_col(df)
        try:
            weekdays = [
                v.weekday() if isinstance(v, _dt.date) else v.date().weekday()
                for v in df[date_col].to_list()
            ]
            self.assertTrue(all(wd == 0 for wd in weekdays))
        except AssertionError:
            print("Weekly data not aligned to Monday")
            raise

    def test_prune_post_intraday_us(self):
        # Half-day at USA Thanksgiving.
        tkr = "AMZN"
        special_day = _dt.date(2024, 11, 29)
        time_early_close = _dt.time(13)
        dat = yf.Ticker(tkr, session=self.session)

        start_d = special_day - _dt.timedelta(days=7)
        end_d = special_day + _dt.timedelta(days=7)
        df = dat.history(
            start=start_d, end=end_d, interval="1h", prepost=False, keepna=True
        )
        date_col = self._get_date_col(df)

        # Get last entry on special_day
        special_day_rows = df.filter(
            _pl.col(date_col).cast(_pl.Date) == special_day
            if isinstance(df[date_col].dtype, _pl.Datetime)
            else _pl.col(date_col) == special_day
        )
        tg_last_dt = special_day_rows[date_col][-1]
        if isinstance(tg_last_dt, _dt.datetime):
            last_time = tg_last_dt.time().replace(tzinfo=None)
        else:
            last_time = _dt.time(0)
        self.assertTrue(last_time < time_early_close)

        # Test no other afternoons (or mornings) were pruned
        start_d = _dt.date(special_day.year, 1, 1)
        end_d = _dt.date(special_day.year + 1, 1, 1)
        df = dat.history(
            start=start_d, end=end_d, interval="1h", prepost=False, keepna=True
        )
        if df.is_empty():
            self.skipTest(
                "TEST NEEDS UPDATE: 'special_day' needs to be LATEST Thanksgiving date"
            )

        date_col = self._get_date_col(df)
        # Get last datetime per date
        df_with_date = df.with_columns(_pl.col(date_col).cast(_pl.Date).alias("_date"))
        last_dts = (
            df_with_date.group_by("_date")
            .agg(_pl.col(date_col).last().alias("last_dt"))
            .sort("_date")
        )

        dfd = dat.history(
            start=start_d, end=end_d, interval="1d", prepost=False, keepna=True
        )
        dfd_date_col = self._get_date_col(dfd)
        dfd_dates = [
            v if isinstance(v, _dt.date) else v.date()
            for v in dfd[dfd_date_col].to_list()
        ]
        last_dates = last_dts["_date"].to_list()
        self.assertTrue(_np.equal(dfd_dates, last_dates).all())

    def test_prune_post_intraday_asx(self):
        tkr = "BHP.AX"
        dat = yf.Ticker(tkr, session=self.session)

        start_d = _dt.date(2024, 1, 1)
        end_d = _dt.date(2025, 1, 1)
        df = dat.history(
            start=start_d, end=end_d, interval="1h", prepost=False, keepna=True
        )
        date_col = self._get_date_col(df)

        df_with_date = df.with_columns(_pl.col(date_col).cast(_pl.Date).alias("_date"))
        last_dts = (
            df_with_date.group_by("_date")
            .agg(_pl.col(date_col).last().alias("last_dt"))
            .sort("_date")
        )

        dfd = dat.history(
            start=start_d, end=end_d, interval="1d", prepost=False, keepna=True
        )
        dfd_date_col = self._get_date_col(dfd)
        dfd_dates = [
            v if isinstance(v, _dt.date) else v.date()
            for v in dfd[dfd_date_col].to_list()
        ]
        last_dates = last_dts["_date"].to_list()
        self.assertTrue(_np.equal(dfd_dates, last_dates).all())

    def test_weekly_2rows_fix(self):
        tkr = "AMZN"
        start = _dt.date.today() - _dt.timedelta(days=14)
        start -= _dt.timedelta(days=start.weekday())

        dat = yf.Ticker(tkr)
        df = dat.history(start=start, interval="1wk")
        date_col = self._get_date_col(df)
        weekdays = [
            v.weekday() if isinstance(v, _dt.date) else v.date().weekday()
            for v in df[date_col].to_list()
        ]
        self.assertTrue(all(wd == 0 for wd in weekdays))

    def test_aggregate_capital_gains(self):
        # Setup
        tkr = "FXAIX"
        dat = yf.Ticker(tkr, session=self.session)
        start = "2017-12-31"
        end = "2019-12-31"
        interval = "3mo"

        dat.history(start=start, end=end, interval=interval)

    def test_transient_error_detection(self):
        """Test that _is_transient_error correctly identifies transient vs permanent errors"""
        from yfinance.data import _is_transient_error
        from yfinance.exceptions import YFPricesMissingError

        # Transient errors (should retry)
        self.assertTrue(_is_transient_error(socket.error("Network error")))
        self.assertTrue(_is_transient_error(TimeoutError("Timeout")))
        self.assertTrue(_is_transient_error(OSError("OS error")))

        # Permanent errors (should NOT retry)
        self.assertFalse(_is_transient_error(ValueError("Invalid")))
        self.assertFalse(_is_transient_error(YFPricesMissingError("INVALID", "")))
        self.assertFalse(_is_transient_error(KeyError("key")))

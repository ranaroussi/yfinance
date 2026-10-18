"""
Tests for utils

To run all tests in suite from commandline:
   python -m unittest tests.utils

Specific test class:
   python -m unittest tests.utils.TestTicker

"""
from datetime import datetime
from unittest import TestSuite

import warnings

import numpy as np
import pandas as pd

import unittest

from yfinance.utils import (
    is_valid_period_format,
    _dts_in_same_interval,
    _parse_user_dt,
    _interval_to_timedelta,
    fix_Yahoo_returning_live_separate,
    auto_adjust,
    back_adjust,
)


class TestPandas(unittest.TestCase):
    date_strings = ["2024-08-07 09:05:00+02:00", "2024-08-07 09:05:00-04:00"]

    @unittest.expectedFailure
    def test_mixed_timezones_to_datetime_fails(self):
        series = pd.Series(self.date_strings)
        series = series.map(pd.Timestamp)
        converted = pd.to_datetime(series)
        self.assertIsNotNone(converted[0].tz)

    def test_mixed_timezones_to_datetime(self):
        series = pd.Series(self.date_strings)
        series = series.map(pd.Timestamp)
        converted = pd.to_datetime(series, utc=True)
        self.assertIsNotNone(converted[0].tz)
        i = 0
        for dt in converted:
            dt: datetime
            ts: pd.Timestamp = series[i]
            self.assertEqual(dt.isoformat(), ts.tz_convert(tz="UTC").isoformat())
            i += 1


class TestUtils(unittest.TestCase):
    def test_is_valid_period_format_valid(self):
        self.assertTrue(is_valid_period_format("1d"))
        self.assertTrue(is_valid_period_format("5wk"))
        self.assertTrue(is_valid_period_format("12mo"))
        self.assertTrue(is_valid_period_format("2y"))

    def test_is_valid_period_format_invalid(self):
        self.assertFalse(is_valid_period_format("1m"))    # Incorrect suffix
        self.assertFalse(is_valid_period_format("2wks"))  # Incorrect suffix
        self.assertFalse(is_valid_period_format("10"))    # Missing suffix
        self.assertFalse(is_valid_period_format("abc"))   # Invalid string
        self.assertFalse(is_valid_period_format(""))      # Empty string

    def test_is_valid_period_format_edge_cases(self):
        self.assertFalse(is_valid_period_format(None))    # None input
        self.assertFalse(is_valid_period_format("0d"))    # Zero is invalid
        self.assertTrue(is_valid_period_format("999mo"))  # Large number valid


class TestDateIntervalCheck(unittest.TestCase):
    def test_same_day(self):
        dt1 = pd.Timestamp("2024-10-15 10:00:00")
        dt2 = pd.Timestamp("2024-10-15 14:30:00")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1d"))
        
    def test_different_days(self):
        dt1 = pd.Timestamp("2024-10-15 10:00:00")
        dt2 = pd.Timestamp("2024-10-16 09:00:00")
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1d"))
    
    def test_same_week_mid_week(self):
        # Wednesday and Friday in same week
        dt1 = pd.Timestamp("2024-10-16")  # Wednesday
        dt2 = pd.Timestamp("2024-10-18")  # Friday
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1wk"))
    
    def test_different_weeks(self):
        dt1 = pd.Timestamp("2024-10-14")  # Monday week 42
        dt2 = pd.Timestamp("2024-10-21")  # Monday week 43
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1wk"))
    
    def test_week_year_boundary(self):
        # Week 52 of 2024 spans into 2025
        dt1 = pd.Timestamp("2024-12-30")  # Monday in week 1 (ISO calendar)
        dt2 = pd.Timestamp("2025-01-03")  # Friday in week 1 (ISO calendar)
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1wk"))
    
    def test_same_month(self):
        dt1 = pd.Timestamp("2024-10-01")
        dt2 = pd.Timestamp("2024-10-31")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1mo"))
    
    def test_different_months(self):
        dt1 = pd.Timestamp("2024-10-31")
        dt2 = pd.Timestamp("2024-11-01")
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1mo"))
    
    def test_month_year_boundary(self):
        dt1 = pd.Timestamp("2024-12-15")
        dt2 = pd.Timestamp("2025-01-15")
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1mo"))

    def test_same_month_different_year(self):
        # Same calendar month but different years must not be treated
        # as the same monthly interval.
        dt1 = pd.Timestamp("2024-12-15")
        dt2 = pd.Timestamp("2025-12-15")
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "1mo"))

        dt3 = pd.Timestamp("2020-06-01")
        dt4 = pd.Timestamp("2025-06-01")
        self.assertFalse(_dts_in_same_interval(dt3, dt4, "1mo"))

    def test_standard_quarters(self):
        q1_start = datetime(2023, 1, 1)
        self.assertTrue(_dts_in_same_interval(q1_start, datetime(2023, 1, 15), '3mo'))
        self.assertTrue(_dts_in_same_interval(q1_start, datetime(2023, 3, 31), '3mo'))
        self.assertFalse(_dts_in_same_interval(q1_start, datetime(2023, 4, 1), '3mo'))
        self.assertFalse(_dts_in_same_interval(q1_start, datetime(2022, 1, 15), '3mo'))  # Previous year
        self.assertFalse(_dts_in_same_interval(q1_start, datetime(2024, 1, 15), '3mo'))  # Next year
        
        q2_start = datetime(2023, 4, 1)
        self.assertTrue(_dts_in_same_interval(q2_start, datetime(2023, 5, 15), '3mo'))
        self.assertTrue(_dts_in_same_interval(q2_start, datetime(2023, 6, 30), '3mo'))
        self.assertFalse(_dts_in_same_interval(q2_start, datetime(2023, 7, 1), '3mo'))
    
    def test_nonstandard_quarters(self):
        q1_start = datetime(2023, 2, 1)
        # Same quarter
        self.assertTrue(_dts_in_same_interval(q1_start, datetime(2023, 3, 1), '3mo'))
        self.assertTrue(_dts_in_same_interval(q1_start, datetime(2023, 4, 25), '3mo'))
        # Different quarters
        self.assertFalse(_dts_in_same_interval(q1_start, datetime(2023, 1, 25), '3mo'))  # Before quarter start
        self.assertFalse(_dts_in_same_interval(q1_start, datetime(2023, 6, 1), '3mo'))  # Start of next quarter
        self.assertFalse(_dts_in_same_interval(q1_start, datetime(2023, 9, 1), '3mo'))  # Start of Q3
        
        q2_start = datetime(2023, 5, 1)
        self.assertTrue(_dts_in_same_interval(q2_start, datetime(2023, 6, 1), '3mo'))
        self.assertTrue(_dts_in_same_interval(q2_start, datetime(2023, 7, 25), '3mo'))
        self.assertFalse(_dts_in_same_interval(q2_start, datetime(2023, 8, 1), '3mo'))
    
    def test_cross_year_quarters(self):
        q4_start = datetime(2023, 11, 1)
        
        # Same quarter, different year
        self.assertTrue(_dts_in_same_interval(q4_start, datetime(2023, 11, 15), '3mo'))
        self.assertTrue(_dts_in_same_interval(q4_start, datetime(2024, 1, 15), '3mo'))
        self.assertTrue(_dts_in_same_interval(q4_start, datetime(2024, 1, 25), '3mo'))
        
        # Different quarters
        self.assertFalse(_dts_in_same_interval(q4_start, datetime(2024, 2, 1), '3mo'))  # Start of next quarter
        self.assertFalse(_dts_in_same_interval(q4_start, datetime(2023, 10, 14), '3mo'))  # Before quarter start
    
    def test_hourly_interval(self):
        dt1 = pd.Timestamp("2024-10-15 14:00:00")
        dt2 = pd.Timestamp("2024-10-15 14:59:59")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1h"))
        
        dt3 = pd.Timestamp("2024-10-15 15:00:00")
        self.assertFalse(_dts_in_same_interval(dt1, dt3, "1h"))
    
    def test_custom_intervals(self):
        # Test 4 hour interval
        dt1 = pd.Timestamp("2024-10-15 10:00:00")
        dt2 = pd.Timestamp("2024-10-15 13:59:59")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "4h"))
        
        dt3 = pd.Timestamp("2024-10-15 14:00:00")
        self.assertFalse(_dts_in_same_interval(dt1, dt3, "4h"))
        
    def test_minute_intervals(self):
        dt1 = pd.Timestamp("2024-10-15 10:30:00")
        dt2 = pd.Timestamp("2024-10-15 10:30:45")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "1min"))
        
        dt3 = pd.Timestamp("2024-10-15 10:31:00")
        self.assertFalse(_dts_in_same_interval(dt1, dt3, "1min"))

    def test_no_generic_timedelta_deprecation_warning(self):
        # Regression for #2882: sub-day intervals must not trigger numpy's
        # "'generic' unit for NumPy timedelta is deprecated" DeprecationWarning.
        intervals = ["1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"]
        for interval in intervals:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                _interval_to_timedelta(interval)
            generic = [w for w in caught
                       if issubclass(w.category, DeprecationWarning)
                       and "generic" in str(w.message)]
            self.assertEqual(generic, [], f"generic-unit warning for {interval!r}")

    def test_interval_to_timedelta_values(self):
        # Lock the sub-day interval parsing behaviour.
        self.assertEqual(_interval_to_timedelta("1m"), pd.Timedelta(minutes=1))
        self.assertEqual(_interval_to_timedelta("30m"), pd.Timedelta(minutes=30))
        self.assertEqual(_interval_to_timedelta("90m"), pd.Timedelta(minutes=90))
        self.assertEqual(_interval_to_timedelta("1h"), pd.Timedelta(hours=1))
        self.assertEqual(_interval_to_timedelta("4h"), pd.Timedelta(hours=4))

    def test_parse_user_dt(self):
        exchange_tz = "US/Eastern"
        dtstr = "2024-01-04"
        epoch = 1704344400  # output of `pd.Timestamp("2024-01-04", tz="US/Eastern").timestamp()`
        expected = pd.Timestamp(dtstr, tz=exchange_tz)

        self.assertEqual(_parse_user_dt(epoch, exchange_tz), expected)
        self.assertEqual(_parse_user_dt(dtstr, exchange_tz), expected)
        self.assertEqual(_parse_user_dt(datetime(year=2024, month=1, day=4).date(), exchange_tz), expected)
        self.assertEqual(_parse_user_dt(datetime(year=2024, month=1, day=4), exchange_tz), expected)
        with self.assertRaises(ValueError):
            self.assertEqual(_parse_user_dt(float(epoch), exchange_tz), expected)

    def test_parse_user_dt_midnight_in_dst_gap(self):
        # Brazil's clocks jumped 00:00 -> 01:00 on 2018-11-04, so midnight did not exist.
        self.assertEqual(_parse_user_dt("2018-11-04", "America/Sao_Paulo"),
                         pd.Timestamp("2018-11-04 01:00", tz="America/Sao_Paulo"))


class TestMultiDayInterval(unittest.TestCase):
    def test_multiday_interval_does_not_raise(self):
        # _interval_to_timedelta returns a relativedelta for day intervals,
        # which cannot be compared against the Timedelta (dt2 - dt1), so a
        # multi-day interval used to raise TypeError here.
        dt1 = pd.Timestamp("2024-01-01")
        dt2 = pd.Timestamp("2024-01-08")
        self.assertTrue(_dts_in_same_interval(dt1, dt2, "10d"))
        self.assertFalse(_dts_in_same_interval(dt1, dt2, "5d"))

    def test_multiday_interval_does_not_warn(self):
        # Building the comparison Timedelta must not trigger numpy's
        # "generic unit" DeprecationWarning on pandas 2.x + numpy>=2.5.
        dt1 = pd.Timestamp("2024-01-01")
        dt2 = pd.Timestamp("2024-01-04")
        for interval in ("3d", "5d", "10d"):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                _dts_in_same_interval(dt1, dt2, interval)
            generic = [w for w in caught
                       if issubclass(w.category, DeprecationWarning)
                       and "generic" in str(w.message)]
            self.assertEqual(generic, [], f"generic-unit warning for {interval!r}")


class TestFixYahooReturningLiveSeparate(unittest.TestCase):
    def test_repair_unit_mixup_uses_currency_divide(self):
        # Yahoo can return the live row in the sub-unit (100x, or 1000x for
        # Kuwaiti fils). The older row must be rescaled by the same factor
        # that detected the mixup, else the merged bar is still off.
        tz = "Asia/Kuwait"
        idx = pd.DatetimeIndex([pd.Timestamp("2024-06-03", tz=tz),
                                pd.Timestamp("2024-06-05 10:15", tz=tz)])
        for currency, m in [("GBp", 100), ("KWF", 1000)]:
            for f in (m, 1.0/m):
                with self.subTest(currency=currency, live_row_factor=f):
                    df = pd.DataFrame({
                        "Open": [1.0, f*1.0], "High": [1.01, f*1.01],
                        "Low": [0.99, f*0.99], "Close": [1.0, f*1.0],
                        "Adj Close": [1.0, f*1.0], "Volume": [100, 50],
                        "Dividends": [0.0, 0.0], "Stock Splits": [0.0, 0.0]}, index=idx)
                    df, _ = fix_Yahoo_returning_live_separate(df, "1wk", tz, prepost=False, repair=True, currency=currency)
                    self.assertEqual(len(df), 1)
                    row = df.iloc[0]
                    self.assertAlmostEqual(row["Open"], f*1.0)
                    self.assertAlmostEqual(row["High"], f*1.01)
                    self.assertAlmostEqual(row["Low"], f*0.99)
                    self.assertAlmostEqual(row["Close"], f*1.0)


class TestAdjustRatio(unittest.TestCase):
    # 1.0 dividend with ex-date on row 3: rows 0-2 scaled by 1 - 1/102
    closes = np.array([100.0, 101.0, 102.0, 100.0, 99.0, 98.0])
    dividends = np.array([0.0, 0.0, 0.0, 1.0, 0.0, 0.0])
    step = 1.0 - 1.0 / 102.0

    def _make_df(self, adj_factors, noise=None):
        # Yahoo 'adjclose' = Close * factor, plus float32-level noise
        noise = np.zeros(len(self.closes)) if noise is None else np.asarray(noise)
        idx = pd.date_range("2020-01-01", periods=len(self.closes), freq="D", tz="UTC")
        return pd.DataFrame({
            "Open": self.closes - 0.5, "High": self.closes + 1.0,
            "Low": self.closes - 1.0, "Close": self.closes,
            "Adj Close": self.closes * np.asarray(adj_factors) * (1 + noise),
            "Volume": 1000, "Dividends": self.dividends, "Stock Splits": 0.0}, index=idx)

    def test_auto_adjust_removes_adjclose_noise(self):
        factors = [self.step] * 3 + [1.0] * 3
        expected = self.closes * np.array(factors)
        outputs = []
        for noise in ([3e-7, -2e-7, 1e-7, -4e-7, 2e-7, 0.0], [-1e-7, 4e-7, -3e-7, 1e-7, 0.0, -2e-7]):
            df = auto_adjust(self._make_df(factors, noise))
            np.testing.assert_allclose(df["Close"].to_numpy(), expected, rtol=1e-15)
            np.testing.assert_allclose(df["Open"].to_numpy(), (self.closes - 0.5) * np.array(factors), rtol=1e-15)
            outputs.append(df["Close"].to_numpy())
        # Different noise in, identical prices out
        self.assertTrue(np.array_equal(outputs[0], outputs[1]))

    def test_auto_adjust_anchor_before_later_dividend(self):
        # Range ends before a later dividend: anchor comes from Yahoo's factor
        factors = [0.97 * self.step] * 3 + [0.97] * 3
        df = auto_adjust(self._make_df(factors))
        np.testing.assert_allclose(df["Close"].to_numpy(), self.closes * np.array(factors), rtol=1e-12)

    def test_auto_adjust_keeps_adjclose_when_disagreeing(self):
        # Only denoise: real disagreements with Yahoo's adjclose must be left untouched
        noise = [3e-7, -2e-7, 1e-7, -4e-7, 2e-7, 0.0]

        # Dividend missing from df, but present in Yahoo's adjclose
        df_in = self._make_df([self.step] * 3 + [1.0] * 3, noise)
        df_in["Dividends"] = 0.0
        np.testing.assert_allclose(auto_adjust(df_in)["Close"].to_numpy(), df_in["Adj Close"].to_numpy(), rtol=1e-15)

        # Yahoo's adjclose ignoring a dividend (e.g. GBp tickers)
        df_in = self._make_df([1.0] * 6, noise)
        df_out = auto_adjust(df_in)
        np.testing.assert_allclose(df_out["Close"].to_numpy()[:3], df_in["Adj Close"].to_numpy()[:3], rtol=1e-15)
        # Rows after the dividend still agree with Yahoo, so are denoised
        np.testing.assert_allclose(df_out["Close"].to_numpy()[3:], self.closes[3:], rtol=1e-15)

    def test_auto_adjust_fallbacks_to_adjclose(self):
        factors = [self.step] * 3 + [1.0] * 3
        noise = [3e-7, -2e-7, 1e-7, -4e-7, 2e-7, 0.0]

        # No dividends column
        df_in = self._make_df(factors, noise).drop(columns="Dividends")
        np.testing.assert_array_equal(auto_adjust(df_in)["Close"].to_numpy(), df_in["Adj Close"].to_numpy())

        # Unusable dividend: not smaller than previous close
        df_in = self._make_df(factors, noise)
        df_in.loc[df_in.index[3], "Dividends"] = 500.0
        np.testing.assert_allclose(auto_adjust(df_in)["Close"].to_numpy(), df_in["Adj Close"].to_numpy(), rtol=1e-15)

    def test_back_adjust_uses_same_factor(self):
        factors = [self.step] * 3 + [1.0] * 3
        df = back_adjust(self._make_df(factors, [3e-7, -2e-7, 1e-7, -4e-7, 2e-7, 0.0]))
        np.testing.assert_array_equal(df["Close"].to_numpy(), self.closes)
        np.testing.assert_allclose(df["Open"].to_numpy(), (self.closes - 0.5) * np.array(factors), rtol=1e-15)


if __name__ == "__main__":
    unittest.main()


def suite():
    ts: TestSuite = unittest.TestSuite()
    ts.addTest(TestPandas("Test pandas"))
    ts.addTest(TestUtils("Test utils"))
    return ts


if __name__ == '__main__':
    unittest.main()

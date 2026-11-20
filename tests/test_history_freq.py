import unittest

import numpy as np
import pandas as pd

from yfinance.scrapers.history import _INTERVAL_FREQ, _can_conform_to_freq, _set_index_freq


class TestHistoryIndexFreq(unittest.TestCase):
    def _make_df(self, index):
        if isinstance(index, pd.DatetimeIndex) and index.freq is not None:
            index = pd.DatetimeIndex(index.to_list(), tz=index.tz)
        return pd.DataFrame({"Close": np.arange(len(index), dtype=float)}, index=index)

    def test_conforming_intervals_set_expected_freq(self):
        cases = [
            ("1m", pd.date_range("2026-11-16 09:30", periods=10, freq="1min", tz="America/New_York"), pd.offsets.Minute(1)),
            ("2m", pd.date_range("2026-11-16 09:30", periods=10, freq="2min", tz="America/New_York"), pd.offsets.Minute(2)),
            ("5m", pd.date_range("2026-11-16 09:30", periods=10, freq="5min", tz="America/New_York"), pd.offsets.Minute(5)),
            ("15m", pd.date_range("2026-11-16 09:30", periods=10, freq="15min", tz="America/New_York"), pd.offsets.Minute(15)),
            ("30m", pd.date_range("2026-11-16 09:30", periods=10, freq="30min", tz="America/New_York"), pd.offsets.Minute(30)),
            ("60m", pd.date_range("2026-11-16 09:30", periods=6, freq="60min", tz="America/New_York"), pd.offsets.Minute(60)),
            ("90m", pd.date_range("2026-11-16 09:30", periods=6, freq="90min", tz="America/New_York"), pd.offsets.Minute(90)),
            ("1h", pd.date_range("2026-11-16 09:30", periods=6, freq="1h", tz="America/New_York"), pd.offsets.Hour(1)),
            ("1d", pd.date_range("2026-11-16", periods=5, freq="B", tz="America/New_York"), pd.offsets.BDay()),
            ("1wk", pd.date_range("2026-10-05", periods=6, freq="W-MON", tz="America/New_York"), pd.offsets.Week(weekday=0)),
            ("1mo", pd.date_range("2026-01-01", periods=12, freq="MS", tz="America/New_York"), pd.offsets.MonthBegin()),
            ("3mo", pd.date_range("2025-02-01", periods=6, freq="QS-FEB", tz="America/New_York"), pd.offsets.QuarterBegin(startingMonth=2)),
        ]
        for interval, idx, expected in cases:
            with self.subTest(interval=interval):
                df = self._make_df(idx)
                self.assertIsNone(df.index.freq)
                _set_index_freq(df, interval)
                self.assertEqual(df.index.freq, expected)

    def test_dst_transitions_and_tz_naive_preserve_freq(self):
        # Spring forward (2026-03-08) and fall back (2026-11-01) in America/New_York
        daily_spring = pd.date_range("2026-03-05", periods=5, freq="B", tz="America/New_York")
        df_spring = self._make_df(daily_spring)
        _set_index_freq(df_spring, "1d")
        self.assertEqual(df_spring.index.freq, pd.offsets.BDay())

        daily_fall = pd.date_range("2026-10-29", periods=5, freq="B", tz="America/New_York")
        df_fall = self._make_df(daily_fall)
        _set_index_freq(df_fall, "1d")
        self.assertEqual(df_fall.index.freq, pd.offsets.BDay())

        hourly_spring = pd.date_range("2026-03-08 00:00", periods=6, freq="1h", tz="America/New_York")
        df_hr_spring = self._make_df(hourly_spring)
        _set_index_freq(df_hr_spring, "1h")
        self.assertEqual(df_hr_spring.index.freq, pd.offsets.Hour(1))

        hourly_fall = pd.date_range("2026-11-01 00:00", periods=6, freq="1h", tz="America/New_York")
        df_hr_fall = self._make_df(hourly_fall)
        _set_index_freq(df_hr_fall, "1h")
        self.assertEqual(df_hr_fall.index.freq, pd.offsets.Hour(1))

        # tz-naive conforming daily index
        naive_daily = pd.date_range("2026-11-16", periods=5, freq="B")
        df_naive = self._make_df(naive_daily)
        _set_index_freq(df_naive, "1d")
        self.assertEqual(df_naive.index.freq, pd.offsets.BDay())

    def test_gaps_rejected_in_fast_path_without_error(self):
        # Long daily history with exchange holidays dropped
        full_bday = pd.date_range("1962-01-02", periods=16800, freq="B", tz="America/New_York")
        mask = np.ones(len(full_bday), dtype=bool)
        mask[100::28] = False
        daily_with_holidays = full_bday[mask]

        # Crypto 7-day daily index (includes weekends)
        crypto_daily = pd.date_range("2026-11-01", periods=14, freq="D", tz="UTC")

        # Weekly with missing week
        wk_gap = pd.date_range("2025-01-06", periods=40, freq="W-MON", tz="America/New_York").delete(15)

        # Monthly & quarterly with missing bar
        mo_gap = pd.date_range("2020-01-01", periods=60, freq="MS", tz="America/New_York").delete(20)
        q_gap = pd.date_range("2020-01-01", periods=20, freq="QS-JAN", tz="America/New_York").delete(5)

        # Intraday 5m with overnight gap
        d1 = pd.date_range("2026-11-16 09:30", periods=5, freq="5min", tz="America/New_York")
        d2 = pd.date_range("2026-11-17 09:30", periods=5, freq="5min", tz="America/New_York")
        intra_gap = d1.append(d2)

        non_conforming = [
            ("1d", daily_with_holidays),
            ("1d", crypto_daily),
            ("1wk", wk_gap),
            ("1mo", mo_gap),
            ("3mo", q_gap),
            ("5m", intra_gap),
        ]
        for interval, idx in non_conforming:
            with self.subTest(interval=interval, n=len(idx)):
                freq = _INTERVAL_FREQ[interval]
                if interval == "3mo":
                    freq = pd.offsets.QuarterBegin(startingMonth=idx[0].month)
                self.assertFalse(_can_conform_to_freq(idx, interval, freq))
                df = self._make_df(idx)
                _set_index_freq(df, interval)
                self.assertIsNone(df.index.freq)

    def test_edge_cases(self):
        # Empty DataFrame
        empty_df = pd.DataFrame(index=pd.DatetimeIndex([], tz="America/New_York"))
        _set_index_freq(empty_df, "1d")
        self.assertIsNone(empty_df.index.freq)

        # Non-DatetimeIndex
        range_df = pd.DataFrame({"Close": [1.0, 2.0]})
        _set_index_freq(range_df, "1d")

        # Single-row weekday vs weekend
        single_wkday = self._make_df(pd.DatetimeIndex(["2026-11-16"], tz="America/New_York"))
        _set_index_freq(single_wkday, "1d")
        self.assertEqual(single_wkday.index.freq, pd.offsets.BDay())

        single_wknd = self._make_df(pd.DatetimeIndex(["2026-11-15"], tz="America/New_York"))
        _set_index_freq(single_wknd, "1d")
        self.assertIsNone(single_wknd.index.freq)

        # NaT at start, at end, and with 3mo
        for interval, nat_idx in [
            ("1d", pd.DatetimeIndex([pd.NaT, pd.Timestamp("2026-11-17", tz="America/New_York")])),
            ("1d", pd.DatetimeIndex([pd.Timestamp("2026-11-16", tz="America/New_York"), pd.NaT])),
            ("3mo", pd.DatetimeIndex([pd.NaT])),
        ]:
            df_nat = self._make_df(nat_idx)
            _set_index_freq(df_nat, interval)
            self.assertIsNone(df_nat.index.freq)

        # Descending and duplicate timestamps
        desc_idx = pd.date_range("2026-11-16", periods=5, freq="B", tz="America/New_York")[::-1]
        df_desc = self._make_df(desc_idx)
        _set_index_freq(df_desc, "1d")
        self.assertIsNone(df_desc.index.freq)

        for dup_idx in [
            pd.DatetimeIndex(["2026-11-16", "2026-11-16"], tz="America/New_York"),
            pd.DatetimeIndex(["2026-11-16", "2026-11-17", "2026-11-17"], tz="America/New_York"),
        ]:
            df_dup = self._make_df(dup_idx)
            _set_index_freq(df_dup, "1d")
            self.assertIsNone(df_dup.index.freq)

        # Tick subtraction spanning >292y on datetime64[ns] must not raise OutOfBoundsDatetime
        wide_ns_idx = pd.DatetimeIndex(["1700-01-01", "2200-01-01"]).as_unit("ns")
        df_wide = self._make_df(wide_ns_idx)
        _set_index_freq(df_wide, "1h")
        self.assertIsNone(df_wide.index.freq)

        # Same calendar business days but mismatched time-of-day -> caught by pandas fallback
        mixed_time = pd.DatetimeIndex([
            pd.Timestamp("2026-11-16 00:00", tz="America/New_York"),
            pd.Timestamp("2026-11-17 09:30", tz="America/New_York"),
        ])
        df_mixed = self._make_df(mixed_time)
        _set_index_freq(df_mixed, "1d")
        self.assertIsNone(df_mixed.index.freq)

        # Unknown interval
        df_unk = self._make_df(pd.date_range("2026-11-16", periods=5, freq="B"))
        _set_index_freq(df_unk, "2d")
        self.assertIsNone(df_unk.index.freq)


if __name__ == "__main__":
    unittest.main()

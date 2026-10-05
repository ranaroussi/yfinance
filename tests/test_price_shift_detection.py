"""Offline coverage for price-shift detection and its repair caller."""

from contextlib import ExitStack
from pathlib import Path
import unittest
from unittest import mock

import numpy as np
import pandas as pd

from yfinance.scrapers.history import PriceHistory
from yfinance.exceptions import YFRateLimitError


class TestPriceShiftDetection(unittest.TestCase):
    def setUp(self):
        self.history = PriceHistory(None, 'TEST', 'UTC', session=object())
        self.history._history_metadata = {}

    def _frame(self):
        index = pd.date_range('2024-01-01', periods=40, tz='UTC')[::-1]
        return pd.DataFrame({
            'Open': 10.0, 'High': 10.0, 'Low': 10.0, 'Close': 10.0,
            'Adj Close': 10.0, 'Volume': 1000, 'Dividends': 0.0,
            'Stock Splits': 0.0,
        }, index=index)

    def test_direction_and_reciprocal(self):
        for interval in ['1d', '1wk', '1mo', '3mo', '1h']:
            for ratio in [100.0, 0.01]:
                for change in [100.0, 0.01]:
                    with self.subTest(interval=interval, ratio=ratio, change=change):
                        frame = self._frame()
                        frame.iloc[20:, :5] *= ratio
                        original = frame.copy(deep=True)
                        detection = self.history._detect_price_shifts(frame, interval, change)
                        self.assertIsNotNone(detection)
                        np.testing.assert_array_equal(np.flatnonzero(detection.up),
                                                      [20] if ratio > 1 else [])
                        np.testing.assert_array_equal(np.flatnonzero(detection.down),
                                                      [20] if ratio < 1 else [])
                        pd.testing.assert_index_equal(detection.workings.index, frame.index)
                        pd.testing.assert_frame_equal(frame, original)

    def test_volume_denoised_in_separate_price_blocks(self):
        frame = self._frame()
        frame.iloc[20:, :5] *= 100
        frame.iloc[20:, frame.columns.get_loc('Volume')] = 10
        detection = self.history._detect_price_shifts(frame, '1d', 100)
        self.assertIsNotNone(detection)
        original = frame.copy(deep=True)
        workings = detection.workings.copy(deep=True)
        threshold, ratios = self.history._estimate_volume_shift_threshold(
            frame, '1d', 100, detection)
        self.assertAlmostEqual(threshold, 20.8)
        self.assertEqual(ratios[20], 0.01)
        self.assertTrue((np.delete(ratios, 20) == 1).all())
        pd.testing.assert_frame_equal(frame, original)
        pd.testing.assert_frame_equal(detection.workings, workings)

    def test_detection_does_not_require_repair_columns(self):
        frame = self._frame().drop(columns=['Dividends', 'Stock Splits', 'Volume'])
        frame.iloc[20:, :5] *= 100
        detection = self.history._detect_price_shifts(frame, '1d', 100)
        self.assertIsNotNone(detection)
        np.testing.assert_array_equal(np.flatnonzero(detection.up), [20])

    def test_price_detection_ignores_volume_values(self):
        frame = self._frame()
        frame.iloc[20:, :5] *= 100
        expected = self.history._detect_price_shifts(frame, '1d', 100)
        frame['Volume'] = 'not numeric'
        detection = self.history._detect_price_shifts(frame, '1d', 100)
        np.testing.assert_array_equal(detection.up, expected.up)
        np.testing.assert_array_equal(detection.down, expected.down)
        pd.testing.assert_frame_equal(detection.workings, expected.workings)

    def test_repair_preserves_filtering_order(self):
        frame = self._frame()
        frame.iloc[20:, :5] *= 100
        frame['Volume'] = np.arange(len(frame)) + 1000
        names = ['_detect_price_shifts', '_estimate_volume_shift_threshold',
                 '_filter_price_shifts_volume',
                 '_filter_price_shifts_that_match_local_stdev', '_verify_price_shifts_volume']
        manager = mock.Mock()
        with ExitStack() as stack:
            for name in names:
                wrapped = stack.enter_context(mock.patch.object(
                    self.history, name, wraps=getattr(self.history, name)))
                manager.attach_mock(wrapped, name)
            self.history._fix_prices_sudden_change(frame, '1d', 'UTC', 100, unit_switch=True)
        self.assertEqual([call[0] for call in manager.mock_calls], names)
        estimate_args = manager.mock_calls[1].args
        screen_args = manager.mock_calls[2].args
        self.assertIs(estimate_args[3], screen_args[2])
        threshold = manager.mock_calls[-1].args[4]
        self.assertEqual(threshold, self.history._estimate_volume_shift_threshold(*estimate_args)[0])

    def test_volume_spike_filters_genuine_price_drop(self):
        frame = self._frame()
        frame.iloc[20:, :5] *= 100
        frame['Volume'] = np.arange(len(frame)) + 1000
        frame.iloc[19, frame.columns.get_loc('Volume')] = 100000
        detection = self.history._detect_price_shifts(frame, '1d', 100)
        self.assertIsNotNone(detection)
        with mock.patch.object(self.history, '_estimate_volume_shift_threshold') as estimate, \
                mock.patch.object(self.history, '_denoise_volume') as denoise:
            self.assertIsNone(self.history._filter_price_shifts_volume(frame, '1d', detection))
        estimate.assert_not_called()
        denoise.assert_not_called()
        self.assertTrue(detection.up[20])  # Screening must not mutate candidates.

    def test_individual_ohlc_masks(self):
        frame = self._frame()
        frame.iloc[20:, frame.columns.get_indexer(['Open', 'High'])] *= 100
        original = frame.copy(deep=True)
        detection = self.history._detect_price_shifts(
            frame, '1d', 100, correct_columns_individually=True)
        self.assertIsNotNone(detection)
        expected = np.zeros((len(frame), 4), dtype=bool)
        expected[20, :2] = True
        np.testing.assert_array_equal(detection.up, expected)
        self.assertFalse(detection.down.any())
        verified = self.history._filter_price_shifts_volume(frame, '1d', detection)
        self.assertFalse(hasattr(verified, 'volume_threshold'))
        self.assertEqual(self.history._estimate_volume_shift_threshold(
            frame, '1d', 100, detection), (None, None))
        pd.testing.assert_frame_equal(frame, original)

    def test_no_signal(self):
        frame = self._frame()
        self.assertIsNone(self.history._detect_price_shifts(frame, '1d', 100))
        self.assertIsNone(self.history._detect_price_shifts(frame.iloc[:0], '1d', 100))
        # Price detection remains independent of volume availability.
        frame.iloc[20:, :5] *= 100
        frame['Volume'] = 0
        detection = self.history._detect_price_shifts(frame, '1d', 100)
        self.assertIsNotNone(detection)
        self.assertIsNone(self.history._filter_price_shifts_volume(frame, '1d', detection))
        self.assertEqual(self.history._estimate_volume_shift_threshold(
            frame, '1d', 100, detection), (None, None))

    def test_unknown_multiple_preserves_row_order(self):
        frame = self._frame().drop(columns=['Volume'])
        frame.iloc[20:, :5] *= 15
        for ordered in [frame, frame.iloc[::-1]]:
            with self.subTest(ascending=ordered.index.is_monotonic_increasing):
                original = ordered.copy(deep=True)
                detection = self.history._detect_price_shifts(
                    ordered, '1d', min_change=2)
                self.assertIsNotNone(detection)
                expected_up = [20] if ordered.index.is_monotonic_decreasing else []
                expected_down = [20] if ordered.index.is_monotonic_increasing else []
                np.testing.assert_array_equal(np.flatnonzero(detection.up), expected_up)
                np.testing.assert_array_equal(np.flatnonzero(detection.down), expected_down)
                pd.testing.assert_index_equal(detection.workings.index, ordered.index)
                pd.testing.assert_frame_equal(ordered, original)

    def test_unknown_multiple_threshold_is_strict(self):
        for ratio, expected in [(2, False), (0.5, False), (2.01, True), (0.49, True)]:
            with self.subTest(ratio=ratio):
                frame = self._frame()
                frame.iloc[20:, :5] *= ratio
                detection = self.history._detect_price_shifts(
                    frame, '1d', min_change=2)
                self.assertEqual(detection is not None, expected)
        with self.assertRaises(ValueError):
            self.history._detect_price_shifts(self._frame(), '1d', min_change=1)

    def test_volume_verification_distinguishes_split_from_unit_switch(self):
        up = np.zeros(40, dtype=bool)
        down = up.copy()
        down[20] = True
        ranges = [(20, 40, 'split')]
        original = list(ranges)
        for multiplier in [1, 15]:
            vol = np.arange(40, dtype=float) + 1000
            vol[20:] *= multiplier
            for unit_switch in [False, True]:
                with self.subTest(multiplier=multiplier, unit_switch=unit_switch):
                    confirmed = self.history._verify_price_shifts_volume(
                        vol, ranges, up, down, 3.8, unit_switch=unit_switch)
                    expected = multiplier == 1 if unit_switch else multiplier == 15
                    self.assertEqual(confirmed, original if expected else [])
                    self.assertEqual(ranges, original)

    def test_volume_verification_stdev_fallback(self):
        vol = np.arange(40, dtype=float) + 1000
        vol[20:] *= 3
        up = np.zeros(40, dtype=bool)
        down = up.copy()
        down[20] = True
        ranges = [(20, 40, 'split')]
        # Ratio misses the 15x-derived threshold, but separation in SDs passes.
        self.assertEqual(self.history._verify_price_shifts_volume(
            vol, ranges, up, down, 3.8), ranges)

    def test_volume_verification_allows_unavailable_range_samples(self):
        up = np.zeros(40, dtype=bool)
        down = up.copy()
        down[20] = True
        ranges = [(20, 40, 'split')]
        vol = np.zeros(40)
        self.assertEqual(self.history._verify_price_shifts_volume(
            vol, ranges, up, down, 3.8), ranges)

    def test_adjusted_prices_avoid_dividend_false_positive(self):
        frame = self._frame()
        frame.iloc[20:, :4] *= 3
        self.assertIsNone(self.history._detect_price_shifts(frame, '1d', 2))
        self.assertIsNone(self.history._detect_price_shifts(frame, '1d', min_change=2))

    def test_unexplained_shift_ignores_large_dividend_without_intraday_fetch(self):
        frame = self._frame()
        frame.index = pd.date_range(end=pd.Timestamp.now('UTC').normalize(), periods=40)[::-1]
        frame.iloc[20:, :4] *= 3
        frame.iloc[:20, frame.columns.get_loc('Volume')] *= 3
        frame.iloc[19, frame.columns.get_loc('Dividends')] = 20
        # Raw prices drop 3x while volume moves inversely, but adjusted prices
        # show no shift. Do not request calibration for this dividend event.
        with mock.patch.object(self.history, 'history') as fetch:
            repaired = self.history._fix_unexplained_level_shifts(frame, '1d', 'UTC')
        fetch.assert_not_called()
        pd.testing.assert_frame_equal(repaired, frame.sort_index())

    def test_denoise_volume(self):
        volume = np.array([0, 10, 0, 30, 0])
        original = volume.copy()
        np.testing.assert_array_equal(self.history._denoise_volume(volume), [10, 20, 30, 30, 30])
        np.testing.assert_array_equal(volume, original)
        np.testing.assert_array_equal(self.history._denoise_volume([]), [])
        np.testing.assert_array_equal(self.history._denoise_volume([0, 0]), [0, 0])


class TestPriceShiftRepairFixtures(unittest.TestCase):
    def _load(self, name, tz):
        frame = pd.read_csv(Path(__file__).parent / 'data' / name, index_col='Date')
        frame.index = pd.to_datetime(frame.index, utc=True).tz_convert(tz)
        return frame.sort_index()

    def test_unit_switch(self):
        for ticker, tz, currency in [('AET.L', 'Europe/London', 'GBp'),
                                     ('SSW.JO', 'Africa/Johannesburg', 'ZAc')]:
            with self.subTest(ticker=ticker):
                history = PriceHistory(None, ticker, tz, session=object())
                history._history_metadata = {'currency': currency}
                prefix = ticker.replace('.', '-') + '-1d-100x-error'
                frame = self._load(prefix + '.csv', tz)
                original = frame.copy(deep=True)
                expected = self._load(prefix + '-fixed.csv', tz)
                repaired = history._fix_unit_switch(frame.copy(), '1d', tz).sort_index()
                columns = ['Open', 'High', 'Low', 'Close', 'Adj Close']
                np.testing.assert_allclose(repaired[columns], expected[columns], rtol=1e-2)
                self.assertIn('Repaired?', repaired)
                self.assertFalse(repaired['Repaired?'].isna().any())
                pd.testing.assert_frame_equal(frame, original)

    def test_bad_stock_split(self):
        tz = 'America/New_York'
        history = PriceHistory(None, 'NRDY', tz, session=object())
        frame = self._load('NRDY-1d-bad-stock-split.csv', tz)
        expected = self._load('NRDY-1d-bad-stock-split-fixed.csv', tz)
        for size in [len(frame), 27]:
            with self.subTest(size=size):
                repaired = history._fix_bad_stock_splits(frame.iloc[-size:].copy(), '1d', tz).sort_index()
                columns = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
                np.testing.assert_allclose(repaired[columns], expected.iloc[-size:][columns], rtol=5e-5)

    def test_unexplained_level_shift_reuses_price_detector(self):
        tz = 'America/New_York'
        for ticker, suffix, bad in [('SOXS', 'bad-level-shift', True),
                                    ('KALA', 'no-bad-level-shift', False)]:
            with self.subTest(ticker=ticker):
                frame = self._load(f'{ticker}-1d-{suffix}.csv', tz)
                fine = pd.read_csv(Path(__file__).parent / 'data' / f'{ticker}-1h-{suffix}.csv',
                                   index_col='Datetime')
                fine.index = pd.to_datetime(fine.index, utc=True).tz_convert(tz)
                expected = self._load(f'{ticker}-1d-{suffix}-fixed.csv', tz) if bad else frame.copy()
                weeks = max(0, (pd.Timestamp.now(tz).date() - frame.index[-1].date()).days // 7 - 1)
                for data in [frame, fine, expected]:
                    data.index = (data.index.tz_localize(None) + pd.Timedelta(weeks=weeks)).tz_localize(tz)
                requested = []

                def fake_history(*args, **kwargs):
                    requested.append(kwargs['interval'])
                    start = pd.Timestamp(kwargs['start']).tz_localize(tz)
                    end = pd.Timestamp(kwargs['end']).tz_localize(tz)
                    return fine[(fine.index >= start) & (fine.index < end)]

                history = PriceHistory(None, ticker, tz, session=object())
                original = frame.copy(deep=True)
                with mock.patch.object(history, 'history', side_effect=fake_history), \
                     mock.patch.object(history, '_detect_price_shifts',
                                       wraps=history._detect_price_shifts) as detector:
                    repaired = history._fix_unexplained_level_shifts(frame, '1d', tz).sort_index()
                self.assertEqual(requested, ['1h'])
                self.assertEqual(detector.call_args_list[0].kwargs,
                                 {'min_change': 2.0})
                self.assertTrue(detector.call_args_list[0].args[0].index.is_monotonic_increasing)
                columns = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
                np.testing.assert_allclose(repaired[columns], expected[columns], rtol=1e-6)
                np.testing.assert_array_equal(repaired['Dividends'], frame['Dividends'])
                if bad:
                    boundary = pd.Timestamp('2026-05-26', tz=tz) + pd.Timedelta(weeks=weeks)
                    self.assertTrue(repaired.loc[repaired.index < boundary, 'Repaired?'].all())
                    self.assertFalse(repaired.loc[repaired.index >= boundary, 'Repaired?'].any())
                elif 'Repaired?' in repaired:
                    self.assertFalse(repaired['Repaired?'].any())
                pd.testing.assert_frame_equal(frame, original)



class TestUnexplainedLevelShiftBatch(unittest.TestCase):
    def _data(self, multiples=(15, 3, 1)):
        index = pd.bdate_range(end=pd.Timestamp.now('UTC').normalize(), periods=60)
        prices = 100 + np.arange(60) * 0.1
        truth = pd.DataFrame({c: prices.copy() for c in
                              ['Open', 'High', 'Low', 'Close', 'Adj Close']}, index=index)
        truth['Volume'] = 60000 + (np.arange(60) % 7) * 1500
        truth['Dividends'] = 0.0
        truth['Stock Splits'] = 0.0
        factors = np.repeat(multiples, 20)
        frame = truth.copy()
        columns = ['Open', 'High', 'Low', 'Close', 'Adj Close']
        frame[columns] = frame[columns].mul(factors, axis=0)
        frame['Volume'] = (truth['Volume'] / factors).astype(int)
        fine = truth[['Open', 'High', 'Low', 'Close']].copy()
        fine.index += pd.Timedelta(hours=9, minutes=30)
        return frame, fine, truth

    def test_multiple_shifts_share_one_two_year_fetch(self):
        frame, fine, truth = self._data()
        history = PriceHistory(None, 'TEST', 'UTC', session=object())
        original = frame.copy(deep=True)
        with mock.patch.object(history, 'history', return_value=fine) as fetch, \
             mock.patch.object(history, '_fix_prices_sudden_change',
                               wraps=history._fix_prices_sudden_change) as repair:
            repaired = history._fix_unexplained_level_shifts(frame, '1d', 'UTC')
        fetch.assert_called_once()
        kwargs = fetch.call_args.kwargs
        self.assertEqual(kwargs['interval'], '1h')
        self.assertEqual((kwargs['end'] - kwargs['start']).days, 730)
        self.assertLessEqual(kwargs['start'], frame.index[0].date())
        self.assertGreater(kwargs['end'], frame.index[-1].date())
        self.assertFalse(kwargs['auto_adjust'])
        self.assertFalse(kwargs['repair'])
        self.assertEqual([call.args[3] for call in repair.call_args_list], [3, 15])
        columns = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
        np.testing.assert_allclose(repaired[columns], truth[columns], rtol=1e-12)
        self.assertTrue(repaired['Repaired?'].iloc[:40].all())
        self.assertFalse(repaired['Repaired?'].iloc[40:].any())
        pd.testing.assert_frame_equal(frame, original)

    def test_volume_rejection_is_delegated_to_repair(self):
        frame, fine, truth = self._data((3, 1, 1))
        frame['Volume'] = truth['Volume']
        original = frame.copy(deep=True)
        history = PriceHistory(None, 'TEST', 'UTC', session=object())
        with mock.patch.object(history, 'history', return_value=fine) as fetch, \
             mock.patch.object(history, '_fix_prices_sudden_change',
                               wraps=history._fix_prices_sudden_change) as repair:
            repaired = history._fix_unexplained_level_shifts(frame, '1d', 'UTC')
        fetch.assert_called_once()
        repair.assert_called_once()
        pd.testing.assert_frame_equal(repaired, original)

    def test_ineligible_candidates_do_not_fetch(self):
        for reason in ['no_shift', 'too_old', 'near_split', 'spike', 'insufficient_sides',
                       'zero_volume', 'missing_volume']:
            with self.subTest(reason=reason):
                frame, _, truth = self._data()
                if reason == 'no_shift':
                    frame = truth.copy()
                elif reason == 'too_old':
                    frame.index -= pd.Timedelta(weeks=200)
                elif reason == 'near_split':
                    frame.iloc[[20, 40], frame.columns.get_loc('Stock Splits')] = 3
                elif reason in ['zero_volume', 'missing_volume']:
                    frame['Volume'] = 0 if reason == 'zero_volume' else np.nan
                else:
                    frame = truth.copy()
                    columns = ['Open', 'High', 'Low', 'Close', 'Adj Close']
                    if reason == 'spike':
                        frame.loc[frame.index[30], columns] *= 15
                    else:
                        frame.loc[frame.index[:2], columns] *= 15
                history = PriceHistory(None, 'TEST', 'UTC', session=object())
                with mock.patch.object(history, 'history') as fetch:
                    repaired = history._fix_unexplained_level_shifts(frame, '1d', 'UTC')
                fetch.assert_not_called()
                pd.testing.assert_frame_equal(repaired, frame)

    def test_unavailable_reference_is_not_refetched_and_restores_state(self):
        for outcome in ['none', 'empty', 'invalid', 'exception', 'rate_limit']:
            with self.subTest(outcome=outcome):
                frame, fine, _ = self._data()
                history = PriceHistory(None, 'TEST', 'UTC', session=object())
                fields = ['_history_metadata', '_history_metadata_formatted',
                          '_history_metadata_lazy', '_dividends', '_splits', '_capital_gains']
                state = {name: getattr(history, name) for name in fields}

                def fake_history(*args, **kwargs):
                    for name in fields:
                        setattr(history, name, object())
                    if outcome == 'exception':
                        raise RuntimeError('reference unavailable')
                    if outcome == 'rate_limit':
                        raise YFRateLimitError()
                    if outcome == 'none':
                        return None
                    if outcome == 'empty':
                        return fine.iloc[:0]
                    return fine * 0

                with mock.patch.object(history, 'history', side_effect=fake_history) as fetch, \
                     mock.patch.object(history, '_fix_prices_sudden_change') as repair:
                    if outcome == 'rate_limit':
                        with self.assertRaises(YFRateLimitError):
                            history._fix_unexplained_level_shifts(frame, '1d', 'UTC')
                    else:
                        repaired = history._fix_unexplained_level_shifts(frame, '1d', 'UTC')
                        pd.testing.assert_frame_equal(repaired, frame)
                fetch.assert_called_once()
                repair.assert_not_called()
                for name, value in state.items():
                    self.assertIs(getattr(history, name), value)

    def test_partial_reference_does_not_trigger_another_fetch(self):
        frame, fine, _ = self._data()
        history = PriceHistory(None, 'TEST', 'UTC', session=object())
        with mock.patch.object(history, 'history', return_value=fine.iloc[-20:]) as fetch, \
             mock.patch.object(history, '_fix_prices_sudden_change') as repair:
            repaired = history._fix_unexplained_level_shifts(frame, '1d', 'UTC')
        fetch.assert_called_once()
        repair.assert_not_called()
        pd.testing.assert_frame_equal(repaired, frame)

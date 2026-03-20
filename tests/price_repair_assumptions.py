"""Assumption tests for price repair."""

import numpy as _np
import pandas as _pd

from tests.price_repair_support import PriceRepairTestCase


def volume_mismatch_debug(resampled: _pd.DataFrame, truth: _pd.DataFrame) -> str | None:
    """Return a human-readable mismatch reason when volume diverges."""
    volume_diff = _np.array(
        [
            (resampled["Volume"].iloc[0] - truth["Volume"].iloc[0]) / truth["Volume"].iloc[0],
            (resampled["Volume"].iloc[-1] - truth["Volume"].iloc[-1]) / truth["Volume"].iloc[-1],
        ]
    )
    volume_match = volume_diff > -0.32
    diff_count = len(volume_match) - int(_np.sum(volume_match))
    if volume_match.all() or (diff_count == 1 and not volume_match[0]):
        return None
    return f"volume significantly different in first row: vol_diff_pct={volume_diff * 100}%"


def resample_mismatch_debug(resampled: _pd.DataFrame, truth: _pd.DataFrame) -> str | None:
    """Return a mismatch reason when repaired resampling diverges from truth."""
    if len(resampled) != len(truth):
        if resampled.index[1] == truth.index[0]:
            return None
        if resampled.index[0] == truth.index[1]:
            return "resampled missing a row at start"
        return "resampled index different length"
    if (resampled.index != truth.index).all():
        print(resampled.index == truth.index)
        return "resampled index mismatch"
    return volume_mismatch_debug(resampled, truth)


class TestPriceRepairAssumptions(PriceRepairTestCase):
    """Validate assumptions behind the repair implementation."""

    def test_resampling(self):
        """Ensure repaired resampling closely matches Yahoo interval results."""
        intervals = ['1d', '1wk', '1mo', '3mo']
        periods = ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd']

        for ticker_symbol in ['GOOGL', 'GLEN.L', '2330.TW']:
            ticker = self.get_history_parts(ticker_symbol)[0]
            for interval_index, interval in enumerate(intervals):
                if interval == '1d':
                    continue
                for period in periods[interval_index:]:
                    with self.subTest(ticker=ticker_symbol, interval=interval, period=period):
                        truth = ticker.history(interval=interval, period=period)
                        repaired = ticker.history(interval=interval, period=period, repair=True)
                        debug_message = resample_mismatch_debug(repaired, truth)
                        if debug_message is None:
                            continue
                        print(f"  - {debug_message}")
                        print("- investigate:")
                        print(f"  - interval = {interval}")
                        print(f"  - period = {period}")
                        print("- df_truth:")
                        print(truth)
                        print("- df_1d:")
                        print(ticker.history(interval='1d', period=period))
                        print("- dfr:")
                        print(repaired)
                        self.fail(debug_message)

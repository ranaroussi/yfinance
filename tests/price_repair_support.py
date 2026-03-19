"""Shared helpers for price repair tests."""

import os
import unittest
from typing import Any, cast

import numpy as _np
import pandas as _pd

from tests.context import SESSION_GBL, yfinance as yf


def as_datetime_index(index: _pd.Index) -> _pd.DatetimeIndex:
    """Narrow a pandas index to a datetime index."""
    assert isinstance(index, _pd.DatetimeIndex)
    return index


def call_private(obj: Any, name: str, *args: Any, **kwargs: Any) -> Any:
    """Call a private method or fetch a private attribute for test coverage."""
    member = getattr(obj, name)
    if callable(member):
        return member(*args, **kwargs)
    return member


def to_dataframe(value: Any) -> _pd.DataFrame:
    """Narrow a value to a dataframe."""
    assert isinstance(value, _pd.DataFrame)
    return value


class PriceRepairTestCase(unittest.TestCase):
    """Base class for price repair integration tests."""

    session = None
    dp = ""

    @classmethod
    def setUpClass(cls):
        """Share the test session and data directory."""
        cls.session = SESSION_GBL
        cls.dp = os.path.dirname(__file__)

    @classmethod
    def tearDownClass(cls):
        """Close the shared session when available."""
        if cls.session is not None:
            cls.session.close()

    def get_history_parts(self, ticker_symbol: str):
        """Create a ticker plus its timezone and lazy history object."""
        ticker = yf.Ticker(ticker_symbol, session=self.session)
        timezone = ticker.fast_info["timezone"]
        history = call_private(ticker, "_lazy_load_price_history")
        return ticker, timezone, history

    def read_csv_frame(
        self,
        filename: str,
        *,
        index_col: str,
        timezone: str | None = None,
    ) -> _pd.DataFrame:
        """Read a fixture dataframe and normalize the index."""
        frame = cast(_pd.DataFrame, _pd.read_csv(filename, index_col=index_col))
        frame.index = _pd.to_datetime(frame.index, utc=True)
        if timezone is not None:
            frame.index = as_datetime_index(frame.index).tz_convert(timezone)
        return frame.sort_index()

    def assert_repaired_flag(self, frame: _pd.DataFrame):
        """Ensure the repair flag column is present and populated."""
        self.assertIn("Repaired?", frame.columns)
        self.assertFalse(frame["Repaired?"].isna().any())

    def assert_close_columns(
        self,
        actual: _pd.DataFrame,
        expected: _pd.DataFrame,
        columns: list[str],
        *,
        rtol: float,
    ):
        """Assert numeric columns are close and print useful context on failure."""
        for column in columns:
            try:
                self.assertTrue(_np.isclose(actual[column], expected[column], rtol=rtol).all())
            except AssertionError:
                print(f"column={column}")
                print("- actual:")
                print(actual[column])
                print("- expected:")
                print(expected[column])
                raise

    def assert_ratio_matches_expected(
        self,
        raw_frame: _pd.DataFrame,
        expected: _pd.DataFrame,
        columns: list[str],
        *,
        include_inverse: bool = False,
    ):
        """Ensure corrupted values differ by only 1x, 100x, or optionally 0.01x."""
        ratio = raw_frame[columns].to_numpy(dtype=float) / expected[columns].to_numpy(dtype=float)
        ratio = ratio.round(2)
        large_ratio = ratio > 90
        ratio[large_ratio] = (ratio[large_ratio] / 10).round().astype(int) * 10
        is_expected = ratio == 1
        if include_inverse:
            is_expected |= (ratio == 100) | (ratio == 0.01)
        else:
            is_expected |= ratio == 100
        self.assertTrue(is_expected.all())

    def history_metadata(self, history: Any) -> dict[str, str]:
        """Return initialized history metadata with a precise type."""
        metadata = cast(dict[str, str] | None, call_private(history, "_history_metadata"))
        assert metadata is not None
        return metadata

"""
Polars compatibility helpers replacing common pandas idioms used throughout yfinance.

This module provides:
- Helper expressions for common datetime conversions
- empty_ohlcv() replacing utils.empty_df()
- now_utc() replacing pd.Timestamp.now('UTC')
- Utility functions for common DataFrame operations
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Sequence

import polars as pl


def from_unix_s(col: str) -> pl.Expr:
    """Convert an integer unix-seconds column to UTC Datetime (microsecond precision)."""
    return pl.col(col).cast(pl.Int64).mul(1_000_000).cast(pl.Datetime("us", "UTC"))


def from_unix_ms(col: str) -> pl.Expr:
    """Convert an integer unix-milliseconds column to UTC Datetime."""
    return pl.col(col).cast(pl.Int64).mul(1_000).cast(pl.Datetime("us", "UTC"))


def localize_utc(col: str) -> pl.Expr:
    """Interpret a naive Datetime column as UTC (no conversion)."""
    return pl.col(col).dt.replace_time_zone("UTC")


def convert_tz(col: str, tz: str) -> pl.Expr:
    """Convert a tz-aware Datetime column to another timezone."""
    return pl.col(col).dt.convert_time_zone(tz)


def now_utc() -> datetime:
    """Return the current UTC-aware datetime (replaces pd.Timestamp.now('UTC'))."""
    return datetime.now(timezone.utc)


def today_utc() -> date:
    """Return today's date in UTC."""
    return datetime.now(timezone.utc).date()


def empty_ohlcv(date_col: str = "Datetime") -> pl.DataFrame:
    """
    Return a zero-row OHLCV DataFrame.
    Replaces pandas utils.empty_df().
    The date column uses UTC Datetime with microsecond precision.
    """
    return pl.DataFrame(
        {
            date_col: pl.Series([], dtype=pl.Datetime("us", "UTC")),
            "Open": pl.Series([], dtype=pl.Float64),
            "High": pl.Series([], dtype=pl.Float64),
            "Low": pl.Series([], dtype=pl.Float64),
            "Close": pl.Series([], dtype=pl.Float64),
            "Volume": pl.Series([], dtype=pl.Int64),
            "Dividends": pl.Series([], dtype=pl.Float64),
            "Stock Splits": pl.Series([], dtype=pl.Float64),
        }
    )


def df_is_empty(df: pl.DataFrame) -> bool:
    """Check if a DataFrame is empty (replaces df.empty)."""
    return df.height == 0


def sort_by_date(df: pl.DataFrame, date_col: str = "Datetime") -> pl.DataFrame:
    """Sort a DataFrame by its date column (replaces df.sort_index())."""
    return df.sort(date_col)


def filter_date_range(
    df: pl.DataFrame,
    start: datetime | date | str | None,
    end: datetime | date | str | None,
    date_col: str = "Datetime",
) -> pl.DataFrame:
    """
    Filter a DataFrame to a date range (replaces df.loc[start:end] on DatetimeIndex).
    start and end are inclusive.
    """
    exprs = []
    if start is not None:
        exprs.append(pl.col(date_col) >= pl.lit(start).cast(pl.Datetime("us", "UTC")))
    if end is not None:
        exprs.append(pl.col(date_col) <= pl.lit(end).cast(pl.Datetime("us", "UTC")))
    if not exprs:
        return df
    return df.filter(pl.all_horizontal(exprs))


def rename_columns(df: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame:
    """Rename columns, ignoring keys not present (replaces df.rename(columns=..., errors='ignore'))."""
    actual = {k: v for k, v in mapping.items() if k in df.columns}
    return df.rename(actual) if actual else df


def drop_all_null_rows(df: pl.DataFrame) -> pl.DataFrame:
    """Drop rows where all values are null (replaces df.dropna(how='all'))."""
    return df.filter(~pl.all_horizontal(pl.all().is_null()))


def reorder_columns(df: pl.DataFrame, order: Sequence[str]) -> pl.DataFrame:
    """Reorder columns to match order, keeping only columns that exist."""
    cols = [c for c in order if c in df.columns]
    return df.select(cols)


def to_pandas_bridge(df: pl.DataFrame):
    """
    Convert a polars DataFrame to pandas (soft compatibility bridge).
    Requires pandas to be installed (optional dependency).
    """
    try:
        return df.to_pandas()
    except ImportError as e:
        raise ImportError(
            "pandas is required for to_pandas() conversion. "
            "Install it with: uv add pandas  or  pip install 'yfinance[pandas]'"
        ) from e

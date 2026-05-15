"""DataFrame backend dispatch.

Strategy: yfinance pipelines are pandas-native end-to-end. Polars
support is implemented as a thin output adapter — when
``YfConfig.dataframe.backend == "polars"`` we convert the pandas
DataFrame (or Series) at the public-API boundary via
``df_to_backend`` / ``series_to_backend``. There is **one** code path
internally; polars is never used to build or transform frames.

This directly answers the maintenance concern raised on PR #2808:
a parallel polars implementation doubles surface area; an output
adapter does not.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Union

import pandas as pd

from .config import YfConfig

if TYPE_CHECKING:
    import polars as pl  # noqa: F401

# Public return-type aliases for methods whose runtime type depends on
# ``YfConfig.dataframe.backend``. Both polars symbols are quoted so
# nothing is imported at runtime when the optional dep is absent.
DataFrameLike = Union[pd.DataFrame, "pl.DataFrame"]
SeriesLike = Union[pd.Series, "pl.DataFrame"]


def current_backend() -> str:
    """Active dataframe backend: ``"pandas"`` (default) or ``"polars"``."""
    return YfConfig.dataframe.backend or "pandas"


def _polars():
    """Lazy-import polars. Raises a clear error if the user opted into
    ``polars`` without installing the optional dependency."""
    try:
        import polars as pl
    except ImportError as e:
        raise ImportError(
            "polars backend requires the optional dependency. "
            "Install with: pip install yfinance[polars]"
        ) from e
    return pl


def empty_df() -> Any:
    """Empty DataFrame in the active backend."""
    if current_backend() == "polars":
        return _polars().DataFrame()
    return pd.DataFrame()


def df_to_backend(df: pd.DataFrame, *, index_as_column: str | None = None) -> Any:
    """Convert a pandas DataFrame to the active backend at an output
    boundary. Polars has no row index concept, so we always promote the
    pandas index to a column on the polars side: ``index_as_column``
    overrides the column name when provided, otherwise the existing
    index name (or ``"index"``) is kept. Pass-through for pandas.
    """
    if current_backend() == "pandas":
        return df
    pl = _polars()
    if df.empty and len(df.columns) == 0:
        return pl.DataFrame()
    df_reset = df.copy()
    if index_as_column is not None and df_reset.index.name != index_as_column:
        df_reset.index.name = index_as_column
    if isinstance(df_reset.index, pd.RangeIndex) and df_reset.index.name is None:
        return pl.from_pandas(df_reset)
    return pl.from_pandas(df_reset.reset_index())


def series_to_backend(s: pd.Series, *, index_as_column: str | None = None, value_name: str | None = None) -> Any:
    """Convert a pandas Series to the active backend. Polars Series have
    no index, so the result is a two-column frame
    ``(index_as_column, value_name)`` when ``index_as_column`` is given.
    Pass-through for pandas backend.
    """
    if current_backend() == "pandas":
        return s
    pl = _polars()
    if index_as_column is not None:
        df = s.reset_index()
        if value_name is not None and df.columns[-1] != value_name:
            df = df.rename(columns={df.columns[-1]: value_name})
        if df.columns[0] != index_as_column:
            df = df.rename(columns={df.columns[0]: index_as_column})
        return pl.from_pandas(df)
    return pl.from_pandas(s.to_frame())

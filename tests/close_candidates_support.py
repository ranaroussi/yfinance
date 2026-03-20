"""Shared helpers for close-candidate issue regression tests."""

from typing import Any
import unittest

import pandas as pd


def call_private(obj: Any, name: str, *args: Any, **kwargs: Any) -> Any:
    """Call a private API from tests without direct protected-member syntax."""
    member = getattr(obj, name)
    if callable(member):
        return member(*args, **kwargs)
    return member


class SessionTickerTestCase(unittest.TestCase):
    """Base class for ticker tests that use the shared session."""

    session = None


def require_dataframe(
    frame: pd.DataFrame | None,
    message: str = "Expected DataFrame",
) -> pd.DataFrame:
    """Narrow optional dataframe results for test assertions."""
    if frame is None:
        raise AssertionError(message)
    return frame


def require_datetime_index(
    index: pd.Index,
    message: str = "Expected DatetimeIndex",
) -> pd.DatetimeIndex:
    """Narrow generic pandas indexes when tests require datetimes."""
    if not isinstance(index, pd.DatetimeIndex):
        raise AssertionError(message)
    return index


def make_mm_suggest_payload(*entries: tuple[str, str, str, str, str, str]) -> str:
    """Build a compact Yahoo mmSuggest payload for ISIN regression tests."""
    rendered_entries = ",".join(
        (
            f'new Array("{name}", "{category}", "{keywords}", '
            f'"{bias}", "{extension}", "{entry_ids}")'
        )
        for name, category, keywords, bias, extension, entry_ids in entries
    )
    return (
        'mmSuggestDeliver(0, new Array("Name", "Category", "Keywords", "Bias", '
        '"Extension", "IDs"), new Array('
        f"{rendered_entries}))"
    )

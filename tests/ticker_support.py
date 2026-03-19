"""Shared helpers for ticker integration tests."""

from datetime import datetime, timedelta
import unittest
from typing import Any, Optional, TypeVar, Union, get_args

import pandas as pd

from tests.context import SESSION_GBL, yfinance as yf
from yfinance.config import YF_CONFIG
from yfinance.exceptions import (
    YFDataException,
    YFInvalidPeriodError,
    YFNotImplementedError,
    YFPricesMissingError,
    YFTickerMissingError,
    YFTzMissingError,
)


ticker_attributes = (
    ("major_holders", pd.DataFrame),
    ("institutional_holders", pd.DataFrame),
    ("mutualfund_holders", pd.DataFrame),
    ("insider_transactions", pd.DataFrame),
    ("insider_purchases", pd.DataFrame),
    ("insider_roster_holders", pd.DataFrame),
    ("splits", pd.Series),
    ("actions", pd.DataFrame),
    ("shares", pd.DataFrame),
    ("info", dict),
    ("calendar", dict),
    ("recommendations", Union[pd.DataFrame, dict]),
    ("recommendations_summary", Union[pd.DataFrame, dict]),
    ("upgrades_downgrades", Union[pd.DataFrame, dict]),
    ("ttm_cashflow", pd.DataFrame),
    ("quarterly_cashflow", pd.DataFrame),
    ("cashflow", pd.DataFrame),
    ("quarterly_balance_sheet", pd.DataFrame),
    ("balance_sheet", pd.DataFrame),
    ("ttm_income_stmt", pd.DataFrame),
    ("quarterly_income_stmt", pd.DataFrame),
    ("income_stmt", pd.DataFrame),
    ("analyst_price_targets", dict),
    ("earnings_estimate", pd.DataFrame),
    ("revenue_estimate", pd.DataFrame),
    ("earnings_history", pd.DataFrame),
    ("eps_trend", pd.DataFrame),
    ("eps_revisions", pd.DataFrame),
    ("growth_estimates", pd.DataFrame),
    ("sustainability", pd.DataFrame),
    ("options", tuple),
    ("news", Any),
    ("earnings_dates", pd.DataFrame),
)


T = TypeVar("T")


def call_private(obj: Any, name: str, *args: Any, **kwargs: Any) -> Any:
    """Call a private API from tests without direct protected-member syntax."""
    member = getattr(obj, name)
    if callable(member):
        return member(*args, **kwargs)
    return member


def assert_attribute_type(
    test_case: unittest.TestCase,
    instance: Any,
    attribute_name: str,
    expected_type: Any,
):
    """Assert that a ticker attribute returns the expected type when implemented."""
    try:
        attribute = getattr(instance, attribute_name)
    except YFNotImplementedError:
        return
    if attribute is None or expected_type is Any:
        return
    error_message = f"{attribute_name} type is {type(attribute)} not {expected_type}"
    if getattr(expected_type, '__origin__', None) is Union:
        test_case.assertTrue(isinstance(attribute, get_args(expected_type)), error_message)
    else:
        test_case.assertEqual(type(attribute), expected_type, error_message)


def as_dataframe(value: Any) -> pd.DataFrame:
    """Narrow a value to a dataframe."""
    assert isinstance(value, pd.DataFrame)
    return value


def as_non_none(value: Optional[T]) -> T:
    """Narrow an optional value to a non-None value."""
    assert value is not None
    return value


class SessionTickerTestCase(unittest.TestCase):
    """Base class for ticker tests that use the shared session."""

    session = None

    @classmethod
    def setUpClass(cls):
        """Attach the shared test session."""
        cls.session = SESSION_GBL

    @classmethod
    def tearDownClass(cls):
        """Close the shared session if one exists."""
        if cls.session is not None:
            cls.session.close()


__all__ = [
    "YFDataException",
    "YFInvalidPeriodError",
    "YFNotImplementedError",
    "YFPricesMissingError",
    "YFTickerMissingError",
    "YFTzMissingError",
    "YF_CONFIG",
    "SessionTickerTestCase",
    "as_dataframe",
    "as_non_none",
    "assert_attribute_type",
    "call_private",
    "datetime",
    "pd",
    "ticker_attributes",
    "timedelta",
    "yf",
]

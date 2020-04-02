"""
Unit tests for yfinance package.
"""
# --- Imports

import yfinance
from yfinance import *


# --- Tests

def test_modules():
    """
    Test for expected modules.
    """
    assert yfinance.base
    assert yfinance.shared
    assert yfinance.ticker
    assert yfinance.utils
    assert yfinance.multi
    assert yfinance.tickers


def test_types():
    """
    Test for expected types.
    """
    assert Ticker
    assert isinstance(Ticker, type)

    assert Tickers
    assert isinstance(Tickers, type)


def test_api():
    """
    Test for expected API.
    """
    assert download
    assert callable(download)

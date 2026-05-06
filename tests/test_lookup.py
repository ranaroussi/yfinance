import pytest
import pandas as pd
import yfinance as yf


@pytest.fixture
def lookup():
    return yf.Lookup(query="A")


def test_get_all(lookup):
    result = lookup.get_all(count=5)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 5


def test_get_stock(lookup):
    result = lookup.get_stock(count=5)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 5


def test_get_mutualfund(lookup):
    result = lookup.get_mutualfund(count=5)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 5


def test_get_etf(lookup):
    result = lookup.get_etf(count=5)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 5


def test_get_index(lookup):
    result = lookup.get_index(count=5)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 5


def test_get_future(lookup):
    result = lookup.get_future(count=5)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 5


def test_get_currency(lookup):
    result = lookup.get_currency(count=5)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 5


def test_get_cryptocurrency(lookup):
    result = lookup.get_cryptocurrency(count=5)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 5


def test_large_all(lookup):
    result = lookup.get_all(count=1000)
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1000

#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Yahoo! Finance market data downloader (+fix for Pandas Datareader)
# https://github.com/ranaroussi/yfinance

"""
Sanity check for most common library uses all working

- Stock: Microsoft
- ETF: Russell 2000 Growth
- Mutual fund: Vanguard 500 Index fund
- Index: S&P500
- Currency BTC-USD
"""

from __future__ import print_function
import yfinance as yf


def test_basic():
    for symbol in ['MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']:
        print(">>", symbol, end=' ... ')
        ticker = yf.Ticker(symbol)

        # always should have info and history for valid symbols
        assert(ticker.info is not None and ticker.info != {})
        assert(ticker.history(period="max").empty is False)

        # following should always gracefully handled, no crashes
        ticker.cashflow
        ticker.balance_sheet
        ticker.financials
        ticker.sustainability
        ticker.major_holders
        ticker.institutional_holders
        ticker.mutualfund_holders

        print("OK")


def test_holders():
    # Ford has institutional investors table and mutual fund holders
    ticker = yf.Ticker('F')
    print(">> F", end=" ... ")
    assert(ticker.info is not None and ticker.info != {})
    assert(not ticker.major_holders.empty)
    assert(not ticker.institutional_holders.empty)
    print("OK")


def test_financials():
    ticker = yf.Ticker('MSFT')
    assert(not ticker.financials.empty)


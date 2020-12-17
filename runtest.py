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


def test_yfinance():

    # MSFT
    ticker = yf.Ticker('MSFT')
    print(">> MSFT", end=" ... ")
    assert(ticker.info is not None and ticker.info != {})
    assert(ticker.major_holders is not None)
    assert(ticker.institutional_holders is not None)
    assert(ticker.mutualfund_holders is not None)
    assert(len(ticker.balance_sheet.index) > 0)
    assert(len(ticker.financials.index) > 0)
    assert(len(ticker.cashflow.index) > 0)
    info = ticker.get_info(None, True)
    assert(isinstance(info, dict))
    print("OK")

    # NESN.SW has no institutional investors table but it does have mutual fund holders
    ticker = yf.Ticker('NESN.SW')
    print(">> NESN.SW", end=" ... ")
    assert(ticker.info is not None and ticker.info != {})
    assert(ticker.major_holders is not None)
    assert(ticker.institutional_holders is not None)
    assert(ticker.mutualfund_holders is None)
    assert(len(ticker.balance_sheet.index) > 0)
    assert(len(ticker.financials.index) > 0)
    assert(len(ticker.cashflow.index) > 0)
    info = ticker.get_info(None, True)
    assert(isinstance(info, dict))
    print("OK")

    for symbol in ['KO', 'MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']:
        print(">>", symbol, end=' ... ')
        ticker = yf.Ticker(symbol)

        # always should have info and history for valid symbols
        assert(ticker.info is not None and ticker.info != {})
        assert(ticker.history(period="max").empty is False)

        # following should always gracefully handled, no crashes
        cf = ticker.cashflow
        bs = ticker.balance_sheet
        fi = ticker.financials
        su = ticker.sustainability
        mh = ticker.major_holders
        ih = ticker.institutional_holders
        mh = ticker.mutualfund_holders

        print("OK")

    # Ford has institutional investors table and mutual fund holders
    ticker = yf.Ticker('F')
    print(">> F", end=" ... ")
    assert(ticker.info is not None and ticker.info != {})
    assert(ticker.major_holders is not None)
    assert(ticker.institutional_holders is not None)
    assert(ticker.mutualfund_holders is not None)
    assert(len(ticker.balance_sheet.index) > 0)
    assert(len(ticker.financials.index) > 0)
    assert(len(ticker.cashflow.index) > 0)
    info = ticker.get_info(None, True)
    assert(isinstance(info, dict))
    print("OK")

    # NKLA has institutional investors table and mutual fund holders
    ticker = yf.Ticker('NKLA')
    print(">> NKLA", end=" ... ")
    assert(ticker.info is not None and ticker.info != {})
    assert(ticker.major_holders is not None)
    assert(ticker.institutional_holders is not None)
    assert(ticker.mutualfund_holders is not None)
    assert(len(ticker.balance_sheet.index) > 0)
    assert(len(ticker.financials.index) > 0)
    assert(len(ticker.cashflow.index) > 0)
    info = ticker.get_info(None, True)
    assert(isinstance(info, dict))
    print("OK")


if __name__ == "__main__":
    test_yfinance()

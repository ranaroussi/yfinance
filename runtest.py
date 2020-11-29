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
- Currency: BTC-USD
- Stock: Nestle
"""

from __future__ import print_function
import yfinance as yf


def test_yfinance():
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

    # NESN.SW has no institutional holders table
    ticker = yf.Ticker('NESN.SW')
    print(">> NESN.SW", end=" ... ")
    assert(ticker.info is not None and ticker.info != {})
    assert(ticker.major_holders is not None)
    assert(ticker.mutualfund_holders is not None)
    assert(ticker.institutional_holders is None)
    print("OK")

if __name__ == "__main__":
    test_yfinance()

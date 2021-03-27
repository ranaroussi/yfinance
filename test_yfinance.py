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

import yfinance as yf


symbols = ['MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']
tickers = [yf.Ticker(symbol) for symbol in symbols]

class TestTicker:
    def test_info_history(self):
        for ticker in tickers:
            # always should have info and history for valid symbols
            assert(ticker.info is not None and ticker.info != {})
            assert(ticker.history(period="max").empty is False)

    def test_attributes(self):
        for ticker in tickers:
            # following should always gracefully handled, no crashes
            ticker.cashflow
            ticker.balance_sheet
            ticker.financials
            ticker.sustainability
            ticker.major_holders
            ticker.institutional_holders
            ticker.mutualfund_holders

    def test_holders(self):
        for ticker in tickers:
            assert(ticker.info is not None and ticker.info != {})
            assert(ticker.major_holders is not None)
            assert(ticker.institutional_holders is not None)

    def test_balance_sheet(self):
        for ticker in tickers:
            assert(ticker.balance_sheet is not None)
            assert(ticker.balance_sheet == ticker.get_balancesheet())

    def test_quarterly_balance_sheet(self):
        for ticker in tickers:
            assert(ticker.quarterly_balance_sheet is not None)
            assert(ticker.quarterly_balance_sheet == ticker.get_balancesheet(freq='quarterly'))

    def test_balancesheet(self):
        for ticker in tickers:
            assert(ticker.balancesheet is not None)
            assert(ticker.balancesheet == ticker.get_balancesheet())

    def test_quarterly_balancesheet(self):
        for ticker in tickers:
            assert(ticker.quarterly_balancesheet is not None)
            assert(ticker.quarterly_balancesheet == ticker.get_balancesheet('quarterly'))

class TestTickers:
    def test_nothing(self):
        pass

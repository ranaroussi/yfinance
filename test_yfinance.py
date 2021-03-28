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
import unittest

symbols = ['MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']
tickers = [yf.Ticker(symbol) for symbol in symbols]

class TestTicker(unittest.TestCase):
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

    def test_balance(self):
        '''
            Tests ticker.balance_sheet & ticker.balancesheet
            Unified in same function due to same name 
        '''
        for ticker in tickers:
            self.assertIsNotNone(ticker.balance_sheet)
            self.assertIsNotNone(ticker.balancesheet)
            self.assertIs(ticker.balancesheet, ticker.get_balancesheet())
            self.assertIs(ticker.balance_sheet, ticker.get_balancesheet())

    def test_quarterly_balance(self):
        '''
            Tests ticker.quarterly_balance_sheet & ticker.quarterly_balancesheet
            Unified in same function due to same name 
        '''
        for ticker in tickers:
            self.assertIsNotNone(ticker.quarterly_balance_sheet)
            self.assertIsNotNone(ticker.quarterly_balancesheet)
            self.assertIs(ticker.quarterly_balancesheet, ticker.get_balancesheet(freq='quarterly'))
            self.assertIs(ticker.quarterly_balance_sheet, ticker.get_balancesheet(freq='quarterly'))

class TestTickers:
    def test_nothing(self):
        pass

if __name__ == '__main__':
    unittest.main()
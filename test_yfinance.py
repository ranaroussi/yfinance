#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# yfinance - market data downloader
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
import datetime

session = None
import requests_cache ; session = requests_cache.CachedSession("yfinance.cache", expire_after=24*60*60)

# Good symbols = all attributes should work
good_symbols = ['MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']
good_tickers = [yf.Ticker(symbol, session=session) for symbol in good_symbols]
# Dodgy symbols = Yahoo data incomplete, so exclude from some tests
dodgy_symbols = ["G7W.DU"]
dodgy_tickers = [yf.Ticker(symbol, session=session) for symbol in dodgy_symbols]
symbols = good_symbols + dodgy_symbols
tickers = good_tickers + dodgy_tickers
# Delisted = no data expected but yfinance shouldn't raise exception
delisted_symbols = ["BRK.B", "SDLP"]
delisted_tickers = [yf.Ticker(symbol, session=session) for symbol in delisted_symbols]


class TestTicker(unittest.TestCase):
    def setUp(self):
        d_today = datetime.date.today()
        d_today -= datetime.timedelta(days=30)
        self.start_d = datetime.date(d_today.year, d_today.month, 1)

    def test_info_history(self):
        # always should have info and history for valid symbols
        for ticker in tickers:
            assert(ticker.info is not None and ticker.info != {})
            history = ticker.history(period="1mo")
            assert(history.empty is False and history is not None)
        histories = yf.download(symbols, period="1mo", session=session)
        assert(histories.empty is False and histories is not None)

        for ticker in tickers:
            assert(ticker.info is not None and ticker.info != {})
            history = ticker.history(start=self.start_d)
            assert(history.empty is False and history is not None)
        histories = yf.download(symbols, start=self.start_d, session=session)
        assert(histories.empty is False and histories is not None)

    def test_info_history_nofail(self):
        # should not throw Exception for delisted tickers, just print a message
        for ticker in delisted_tickers:
            history = ticker.history(period="1mo")
        histories = yf.download(delisted_symbols, period="1mo", session=session)
        histories = yf.download(delisted_symbols[0], period="1mo", session=session)
        histories = yf.download(delisted_symbols[1], period="1mo")#, session=session)
        for ticker in delisted_tickers:
            history = ticker.history(start=self.start_d)
        histories = yf.download(delisted_symbols, start=self.start_d, session=session)
        histories = yf.download(delisted_symbols[0], start=self.start_d, session=session)
        histories = yf.download(delisted_symbols[1], start=self.start_d, session=session)

    def test_attributes(self):
        for ticker in tickers:
            ticker.isin
            ticker.major_holders
            ticker.institutional_holders
            ticker.mutualfund_holders
            ticker.dividends
            ticker.splits
            ticker.actions
            ticker.info
            ticker.info["trailingPegRatio"]
            ticker.calendar
            ticker.recommendations
            ticker.earnings
            ticker.quarterly_earnings
            ticker.financials
            ticker.quarterly_financials
            ticker.balance_sheet
            ticker.quarterly_balance_sheet
            ticker.cashflow
            ticker.quarterly_cashflow
            ticker.sustainability
            ticker.options
            ticker.news
            ticker.shares
            ticker.earnings_history
            ticker.earnings_dates

    def test_attributes_nofail(self):
        # should not throw Exception for delisted tickers, just print a message
        for ticker in delisted_tickers:
            ticker.isin
            ticker.major_holders
            ticker.institutional_holders
            ticker.mutualfund_holders
            ticker.dividends
            ticker.splits
            ticker.actions
            ticker.info
            ticker.calendar
            ticker.recommendations
            ticker.earnings
            ticker.quarterly_earnings
            ticker.financials
            ticker.quarterly_financials
            ticker.balance_sheet
            ticker.quarterly_balance_sheet
            ticker.cashflow
            ticker.quarterly_cashflow
            ticker.sustainability
            ticker.options
            ticker.news
            ticker.shares
            ticker.earnings_history
            ticker.earnings_dates

    def test_holders(self):
        for ticker in good_tickers:
            assert(ticker.major_holders is not None)
            assert(ticker.institutional_holders is not None)


if __name__ == '__main__':
    unittest.main()

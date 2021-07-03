#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Yahoo! Finance market data downloader (+fix for Pandas Datareader)
# https://github.com/ranaroussi/yfinance

"""
- Stock: Microsoft
- ETF: Russell 2000 Growth
- Mutual fund: Vanguard 500 Index fund
- Index: S&P500
- Currency BTC-USD
"""

import yfinance as yf

SYMBOLS = ['MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']
TICKERS = yf.Tickers(' '.join(SYMBOLS))

def test_tickers_all_symbols_present():
    for symbol in SYMBOLS:
        assert symbol in TICKERS.tickers
        assert TICKERS.tickers.get(symbol)

def test_tickers_history():
    assert len(TICKERS.history()) > 0

def test_wrong_symbols():
    wrong_symbols = ['***', '???']
    tickers = yf.Tickers(' '.join(wrong_symbols))
    for symbol in wrong_symbols:
        assert len(tickers.tickers.get(symbol.upper()).history()) == 0

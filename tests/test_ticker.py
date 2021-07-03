#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Yahoo! Finance market data downloader (+fix for Pandas Datareader)
# https://github.com/ranaroussi/yfinance

"""
- Stock: Microsoft
"""

import yfinance as yf

TICKER = yf.Ticker('MSFT')

def test_info():
    assert TICKER.info

def test_history_max():
    assert len(TICKER.history(period="max")) > 0

def test_cashflow():
    assert len(TICKER.cashflow) > 0

def test_quarterly_cashflow():
    assert len(TICKER.quarterly_cashflow) > 0

def test_balance_sheet():
    assert len(TICKER.balance_sheet) > 0

def test_quarterly_balance_sheet():
    assert len(TICKER.quarterly_balance_sheet) > 0

def test_financials():
    assert len(TICKER.financials) > 0

def test_quarterly_financials():
    assert len(TICKER.quarterly_financials) > 0

def test_sustainability():
    assert len(TICKER.sustainability) > 0

def test_major_holders():
    assert len(TICKER.major_holders) > 0

def test_institutional_holders():
    assert len(TICKER.institutional_holders) > 0

def test_mutualfund_holders():
    assert len(TICKER.mutualfund_holders) > 0

def test_dividends():
    assert len(TICKER.dividends) > 0

def test_splits():
    assert len(TICKER.splits) > 0

def test_actions():
    assert len(TICKER.actions) > 0

def test_calendar():
    assert len(TICKER.calendar) > 0

def test_recommendations():
    assert len(TICKER.recommendations) > 0

def test_earnings():
    assert len(TICKER.earnings) > 0

def test_quarterly_earnings():
    assert len(TICKER.quarterly_earnings) > 0

def test_wrong_symbol():
    ticker = yf.Ticker('===')
    assert len(ticker.history()) == 0

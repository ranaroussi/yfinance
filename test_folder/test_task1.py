#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import yfinance as yf
import unittest

symbols = ['MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']
tickers = [yf.Ticker(symbol) for symbol in symbols]

class TestEvent1:

  def test_event_cal(self):

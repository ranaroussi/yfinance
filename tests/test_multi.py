#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Yahoo! Finance market data downloader (+fix for Pandas Datareader)
# https://github.com/ranaroussi/yfinance

import yfinance as yf

def test_multi():
    symbols = "A AA AAC AACE AACG AACO AACP AACQ AAIC AAL AAMC AAME AAN AAOI AAON AAP AAPL AAQC"
    data = yf.download(symbols, start="2010-01-01", end="2011-01-01")
    assert len(data) > 0

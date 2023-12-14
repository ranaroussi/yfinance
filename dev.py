# -*- coding: UTF-8 -*-
"""
    Unit Test Lab
    2023-12-14
    Description:
    
"""

from yfinance import Ticker
from pprint import pprint
ticker = Ticker("AAPL")
print(ticker.ticker)
pprint(ticker.calendar)

ticker = Ticker("LLY")
print(ticker.ticker)
pprint(ticker.calendar)

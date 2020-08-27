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


# import pickle
from pathlib import Path
# import functools
import pandas as pd
try:
    from urllib.parse import quote as urlencode
except ImportError:
    from urllib import quote as urlencode


import multiprocessing
from multiprocessing import Pool as ProcessPool
from multiprocessing.pool import ThreadPool as ThreadPool
# from multiprocessing.pool import ThreadPool as ProcessPool 
from yfinance.utils import ProgressBar, retry

from multiprocessing.sharedctypes import Value

import time
from functools import partial


def download(fn, url, path='.test_data_cache'):
    path = Path(path)
    path.mkdir(exist_ok=True)
    filepath = path / urlencode(url, safe='')
    if not filepath.exists():
        filepath.touch()
        res = fn(url)
        res.to_csv(filepath)
    return pd.read_csv(filepath)



URL_TICKER_LIST = 'ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt'
URL_100_MOST_POPULAR = 'https://etfdb.com/compare/volume/'

# broken on yahoo finance -- not the library's fault.
# known_to_be_broken = {'BLNKW', 'KBLMW'} 
known_to_be_broken = set() 
broken = []

count = Value('i', 0)

def get_test_symbols():
    nasdaq = set(download(lambda url: pd.read_csv(url, sep='|'), URL_TICKER_LIST).Symbol.unique())
    _100_most_popular = set(download(lambda url: pd.read_html(url)[0], URL_100_MOST_POPULAR).Symbol.unique())
    more_stuff = {'IWO', 'GC=F', 'CL=F', 'BELP'}
    vanguard_mutual = {'VTSAX', 'VFIAX', 'VFINX', 'VGSLX', 'VMMSX', 'VTRIX', 'VHGEX', 'VSTCX', 'VWUSX', 'VDADX'} 
    vanguard_etf = {'VTI', 'VIG', 'VV', 'MGV', 'VONE', 'VTV', 'VTWO', 'VB'}
    rates = {'^IRX', '^FVX', '^TNX', '^TYX'}
    indices = {'^GSPC', '^DJI', '^IXIC', '^VIX', '^FTSE', '^N225', '^HSI'}
    currency = {'BTCUSD=X', 'ETHUSD=X', 'EURUSD=X', 'HKD=X', 'DX-Y.NYB'}

    symbols = set().union(  nasdaq, _100_most_popular, 
                            more_stuff, vanguard_mutual, 
                            vanguard_etf, rates, indices, currency)
    # symbols = set().union( more_stuff, rates, indices, currency)

    return symbols

# @retry(total=4)
def test_ticker(symbol, let_slide=known_to_be_broken):
    # print(">>", symbol, end=' ... ')
    ticker = yf.Ticker(symbol)

    # following should always gracefully handled, no crashes
    try:
        # always should have info and history for valid symbols
        assert(ticker.info is not None and ticker.info != {})
        if symbol not in let_slide:
            assert (ticker.history(period="max").empty is False)
        ticker.cashflow
        ticker.balance_sheet
        ticker.financials
        ticker.sustainability
        ticker.major_holders
        ticker.institutional_holders

        return symbol, (ticker.history(period="max"), ticker.cashflow, 
            ticker.balance_sheet, ticker.financials, ticker.sustainability, 
            ticker.major_holders, ticker.institutional_holders )

    except Exception as e:
        print(e)
        print(e.url)
        broken.append(symbol)
        return symbol, None, None, None, None, None, None, None


    


def test_tickers(symbols, let_slide, threads, total=None):
    print(f'\ttesting {len(symbols)} symbols')
    global count
    with ThreadPool(threads) as tp:
        for res in tp.imap_unordered(test_ticker, symbols):
            # print('res:',res)
            count.value += 1
            if count.value % 100 == 0:
                print(f'test {count.value}/{len(symbols) if total is None else total} symbols', end='\r', flush=True)

def test_yfinance(processes=4, threads_per_process=200):
    symbols = get_test_symbols()
 
    print(f'testing {len(symbols)} symbols')

    
    with ProcessPool(processes) as pp:
        func = partial(test_tickers, 
                            let_slide=known_to_be_broken, 
                            threads=threads_per_process,
                            total=len(symbols)
                        )
        chunksize = len(symbols)//processes
        symbols = list(symbols)
        chunks = [symbols[i*chunksize:(i+1)*chunksize] for i in range(processes)]
        chunks[-1] += symbols[processes*chunksize:]
        [_ for _ in pp.imap_unordered(func, chunks)]
        
    
    print('done' + ' '*76, end='\r', flush=True)

    try:
        assert len(broken) == 0, f'{len(broken)} tickers failed...'
        print('all passed :-)')
    except Exception as e:
        print(e)
        print('>>>>>>> Broken Tickers: <<<<<<<<<<<<')
        print(broken)

if __name__ == "__main__":
    test_yfinance()

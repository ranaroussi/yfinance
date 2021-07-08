#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Yahoo! Finance market data downloader (+fix for Pandas Datareader)
# https://github.com/ranaroussi/yfinance
#
# Copyright 2017-2019 Ran Aroussi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import time as _time
import concurrent.futures as _futures
import pandas as _pd

from . import Ticker, utils
from . import shared


def download(tickers, start=None, end=None, actions=False, threads=True,
             group_by='column', auto_adjust=False, back_adjust=False,
             progress=True, period="max", show_errors=True, interval="1d", prepost=False,
             proxy=None, rounding=False, **kwargs):
    """Download yahoo tickers
    :Parameters:
        tickers : str, list
            List of tickers to download
        period : str
            Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
            Either Use period parameter or use start and end
        interval : str
            Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
            Intraday data cannot extend last 60 days
        start: str
            Download start date string (YYYY-MM-DD) or _datetime.
            Default is 1900-01-01
        end: str
            Download end date string (YYYY-MM-DD) or _datetime.
            Default is now
        group_by : str
            Group by 'ticker' or 'column' (default)
        prepost : bool
            Include Pre and Post market data in results?
            Default is False
        auto_adjust: bool
            Adjust all OHLC automatically? Default is False
        actions: bool
            Download dividend + stock splits data. Default is False
        threads: bool / int
            How many threads to use for mass downloading. Default is True
        proxy: str
            Optional. Proxy server URL scheme. Default is None
        rounding: bool
            Optional. Round values to 2 decimal places?
        show_errors: bool
            Optional. Doesn't print errors if True
    """

    # create ticker list
    tickers = tickers if isinstance(
        tickers, (list, set, tuple)) else tickers.replace(',', ' ').split()

    tickers = list(set([ticker.upper() for ticker in tickers]))

    if progress:
        shared._PROGRESS_BAR = utils.ProgressBar(len(tickers), 'completed')

    # reset dfs
    dfs = {}
    shared._ERRORS = {}

    # download using threads
    if threads:
        with _futures.ThreadPoolExecutor() as executor:
            futures = []
            for i, ticker in enumerate(tickers):
                futures.append(
                    executor.submit(_download_one_threaded, ticker=ticker, period=period,
                                    interval=interval, start=start, end=end, prepost=prepost,
                                    actions=actions, auto_adjust=auto_adjust,
                                    back_adjust=back_adjust,
                                    progress=(progress and i > 0), proxy=proxy,
                                    rounding=rounding
                    )
                )

            for future in _futures.as_completed(futures):
                ticker, data = future.result()
                dfs[ticker.upper()] = data
                if progress:
                    shared._PROGRESS_BAR.animate()

    # download synchronously
    else:
        for i, ticker in enumerate(tickers):
            data = _download_one(ticker, period=period, interval=interval,
                                 start=start, end=end, prepost=prepost,
                                 actions=actions, auto_adjust=auto_adjust,
                                 back_adjust=back_adjust, proxy=proxy,
                                 rounding=rounding)
            dfs[ticker.upper()] = data
            if progress:
                shared._PROGRESS_BAR.animate()

    if progress:
        shared._PROGRESS_BAR.completed()

    if shared._ERRORS and show_errors:
        print('\n%.f Failed download%s:' % (
            len(shared._ERRORS), 's' if len(shared._ERRORS) > 1 else ''))
        # print(shared._ERRORS)
        print("\n".join(['- %s: %s' %
                         v for v in list(shared._ERRORS.items())]))

    try:
        data = _pd.concat(dfs.values(), axis=1,
                          keys=dfs.keys())
    except Exception:
        _realign_dfs(dfs)
        data = _pd.concat(dfs.values(), axis=1,
                          keys=dfs.keys())

    if group_by == 'column':
        data.columns = data.columns.swaplevel(0, 1)
        data.sort_index(level=0, axis=1, inplace=True)

    return data


def _realign_dfs(dfs):
    idx_len = 0
    idx = None

    for df in dfs.values():
        if len(df) > idx_len:
            idx_len = len(df)
            idx = df.index

    for key in dfs.keys():
        try:
            dfs[key] = _pd.DataFrame(
                index=idx, data=dfs[key]).drop_duplicates()
        except Exception:
            dfs[key] = _pd.concat([
                utils.empty_df(idx), dfs[key].dropna()
            ], axis=0, sort=True)

        # remove duplicate index
        dfs[key] = dfs[key].loc[
            ~dfs[key].index.duplicated(keep='last')]


def _download_one_threaded(ticker, start=None, end=None,
                           auto_adjust=False, back_adjust=False,
                           actions=False, progress=True, period="max",
                           interval="1d", prepost=False, proxy=None,
                           rounding=False):

    data = _download_one(ticker, start, end, auto_adjust, back_adjust,
                         actions, period, interval, prepost, proxy, rounding)

    return ticker, data


def _download_one(ticker, start=None, end=None,
                  auto_adjust=False, back_adjust=False,
                  actions=False, period="max", interval="1d",
                  prepost=False, proxy=None, rounding=False):

    return Ticker(ticker).history(period=period, interval=interval,
                                  start=start, end=end, prepost=prepost,
                                  actions=actions, auto_adjust=auto_adjust,
                                  back_adjust=back_adjust, proxy=proxy,
                                  rounding=rounding, many=True)

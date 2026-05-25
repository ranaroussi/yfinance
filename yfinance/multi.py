#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# yfinance - market data downloader
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

import logging
import threading
import time as _time
import traceback
from typing import Union

import multitasking as _multitasking
import pandas as _pd
import numpy as _np
from ._http import new_session

from . import Ticker, utils
from .data import YfData
from .config import YfConfig
from .const import period_default


class _DownloadCtx:
    """Per-call scratch state for download(). Concurrent calls each get
    their own instance, so no shared mutation between threads."""
    __slots__ = ('dfs', 'errors', 'tracebacks', 'isins', 'progress_bar', 'lock')

    def __init__(self):
        self.dfs = {}
        self.errors = {}
        self.tracebacks = {}
        self.isins = {}
        self.progress_bar = None
        self.lock = threading.Lock()

@utils.log_indent_decorator
def download(tickers, start=None, end=None, actions=False, threads=True,
             ignore_tz=None, group_by='column', auto_adjust=True, back_adjust=False,
             repair=False, keepna=False, progress=True, period=period_default, interval="1d",
             prepost=False, rounding=False, timeout=10, session=None,
             multi_level_index=True) -> Union[_pd.DataFrame, None]:
    """
    Download yahoo tickers
    :Parameters:
        tickers : str, list
            List of tickers to download
        period : str
            Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
            Default: '1mo' if start & end None
            Either Use period parameter or use start and end
        interval : str
            Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
            Intraday data cannot extend last 60 days
        start: str
            Download start date string (YYYY-MM-DD) or _datetime, inclusive.
            Default is 99 years ago
            E.g. for start="2020-01-01", the first data point will be on "2020-01-01"
        end: str
            Download end date string (YYYY-MM-DD) or _datetime, exclusive.
            Default is now
            E.g. for end="2023-01-01", the last data point will be on "2022-12-31"
        group_by : str
            Group by 'ticker' or 'column' (default)
        prepost : bool
            Include Pre and Post market data in results?
            Default is False
        auto_adjust: bool
            Adjust all OHLC automatically? Default is True
        repair: bool
            Detect currency unit 100x mixups and attempt repair
            Default is False
        keepna: bool
            Keep NaN rows returned by Yahoo?
            Default is False
        actions: bool
            Download dividend + stock splits data. Default is False
        threads: bool / int
            How many threads to use for mass downloading. Default is True
        ignore_tz: bool
            When combining from different timezones, ignore that part of datetime.
            Default depends on interval. Intraday = False. Day+ = True.
        rounding: bool
            Optional. Round values to 2 decimal places?
        timeout: None or float
            If not None stops waiting for a response after given number of
            seconds. (Can also be a fraction of a second e.g. 0.01)
        session: None or Session
            Optional. Pass your own session object to be used for all requests
        multi_level_index: bool
            Optional. Always return a MultiIndex DataFrame? Default is True
    """
    return _download_impl(
        _DownloadCtx(),
        tickers, start=start, end=end, actions=actions, threads=threads,
        ignore_tz=ignore_tz, group_by=group_by, auto_adjust=auto_adjust,
        back_adjust=back_adjust, repair=repair, keepna=keepna, progress=progress,
        period=period, interval=interval, prepost=prepost, rounding=rounding,
        timeout=timeout, session=session, multi_level_index=multi_level_index,
    )


def _download_impl(ctx, tickers, start=None, end=None, actions=False, threads=True,
                   ignore_tz=None, group_by='column', auto_adjust=True, back_adjust=False,
                   repair=False, keepna=False, progress=True, period=period_default, interval="1d",
                   prepost=False, rounding=False, timeout=10, session=None,
                   multi_level_index=True):
    logger = utils.get_yf_logger()
    session = session or new_session()

    YfData(session=session)

    if logger.isEnabledFor(logging.DEBUG):
        if threads:
            # multi-threaded log messages would interleave; serialize.
            logger.debug('Disabling multithreading because DEBUG logging enabled')
            threads = False
        if progress:
            progress = False

    if ignore_tz is None:
        ignore_tz = interval[-1] not in ('m', 'h')

    tickers = tickers if isinstance(
        tickers, (list, set, tuple)) else tickers.replace(',', ' ').split()

    _tickers_ = []
    for ticker in tickers:
        if utils.is_isin(ticker):
            isin = ticker
            ticker = utils.get_ticker_by_isin(ticker)
            ctx.isins[ticker] = isin
        _tickers_.append(ticker)

    tickers = list(set([t.upper() for t in _tickers_]))

    if progress:
        ctx.progress_bar = utils.ProgressBar(len(tickers), 'completed')

    if threads:
        if threads is True:
            threads = min([len(tickers), _multitasking.cpu_count() * 2])
        _multitasking.set_max_threads(threads)
        for i, ticker in enumerate(tickers):
            _download_one_threaded(ctx, ticker, period=period, interval=interval,
                                   start=start, end=end, prepost=prepost,
                                   actions=actions, auto_adjust=auto_adjust,
                                   back_adjust=back_adjust, repair=repair, keepna=keepna,
                                   progress=(progress and i > 0),
                                   rounding=rounding, timeout=timeout)
        while True:
            with ctx.lock:
                if len(ctx.dfs) >= len(tickers):
                    break
            _time.sleep(0.01)
    else:
        for i, ticker in enumerate(tickers):
            _download_one(ctx, ticker, period=period, interval=interval,
                          start=start, end=end, prepost=prepost,
                          actions=actions, auto_adjust=auto_adjust,
                          back_adjust=back_adjust, repair=repair, keepna=keepna,
                          rounding=rounding, timeout=timeout)
            if progress:
                ctx.progress_bar.animate()

    if progress:
        ctx.progress_bar.completed()

    if ctx.errors:
        logger.error('\n%.f Failed download%s:' % (
            len(ctx.errors), 's' if len(ctx.errors) > 1 else ''))

        errors = {}
        for ticker, err in ctx.errors.items():
            err = err.replace(f'${ticker}: ', '')
            errors.setdefault(err, []).append(ticker)
        for err, syms in errors.items():
            logger.error(f'{syms}: ' + err)

        tbs = {}
        for ticker, tb in ctx.tracebacks.items():
            tb = tb.replace(f'${ticker}: ', '')
            tbs.setdefault(tb, []).append(ticker)
        for tb, syms in tbs.items():
            logger.debug(f'{syms}: ' + tb)

    if ignore_tz:
        for tkr, df in ctx.dfs.items():
            if df is not None and df.shape[0] > 0:
                df.index = df.index.tz_localize(None)
    ctx.dfs = reindex_dfs(ctx.dfs, ignore_tz)
    try:
        data = _pd.concat(ctx.dfs.values(), axis=1, sort=True,
                          keys=ctx.dfs.keys(), names=['Ticker', 'Price'])
    except Exception:
        data = _pd.concat(ctx.dfs.values(), axis=1, sort=True,
                          keys=ctx.dfs.keys(), names=['Ticker', 'Price'])
    data.rename(columns=ctx.isins, inplace=True)

    if group_by == 'column' and isinstance(data.columns, _pd.MultiIndex):
        data.columns = data.columns.swaplevel(0, 1)
        data.sort_index(level=0, axis=1, inplace=True)

    if not multi_level_index and len(tickers) == 1:
        data = data.droplevel(0 if group_by == 'ticker' else 1, axis=1).rename_axis(None, axis=1)

    return data

def reindex_dfs(dfs, ignore_tz):
    if ignore_tz:
        for tkr in dfs.keys():
            if (dfs[tkr] is not None) and (not dfs[tkr].empty):
                dfs[tkr].index = dfs[tkr].index.tz_localize(None)
    else:
        # Align each df to most common timezone.
        # Compare strings since np.unique can't handle tz objects
        tzs = [str(df.index.tz) for df in dfs.values() if df is not None and not df.empty]
        if tzs:
            # Find most common timezone
            unique_tzs, counts = _np.unique(tzs, return_counts=True)
            tz_mode = unique_tzs[counts.argmax()]
            for tkr in dfs.keys():
                if (dfs[tkr] is not None) and (not dfs[tkr].empty):
                    dfs[tkr].index = dfs[tkr].index.tz_convert(tz_mode)

    idx = None
    for df in dfs.values():
        if df is not None and not df.empty:
            idx = df.index if idx is None else idx.union(df.index)
    if idx is None:
        idx = _pd.DatetimeIndex([])
    for key, df in dfs.items():
        dfs[key] = df.reindex(idx)

    return dfs

@_multitasking.task
def _download_one_threaded(ctx, ticker, start=None, end=None,
                           auto_adjust=False, back_adjust=False, repair=False,
                           actions=False, progress=True, period=None,
                           interval="1d", prepost=False,
                           keepna=False, rounding=False, timeout=10):
    _download_one(ctx, ticker, start, end, auto_adjust, back_adjust, repair,
                  actions, period, interval, prepost, rounding,
                  keepna, timeout)
    if progress:
        ctx.progress_bar.animate()


def _download_one(ctx, ticker, start=None, end=None,
                  auto_adjust=False, back_adjust=False, repair=False,
                  actions=False, period=None, interval="1d",
                  prepost=False, rounding=False,
                  keepna=False, timeout=10):
    data = None
    sym = ticker.upper()

    backup = YfConfig.network.hide_exceptions
    YfConfig.network.hide_exceptions = False
    try:
        tkr = Ticker(ticker)
        data = tkr.history(
            period=period, interval=interval,
            start=start, end=end, prepost=prepost,
            actions=actions, auto_adjust=auto_adjust,
            back_adjust=back_adjust, repair=repair,
            rounding=rounding, keepna=keepna, timeout=timeout
        )
        with ctx.lock:
            ctx.dfs[sym] = data
            # PriceHistory records soft errors (e.g. delisted, missing tz)
            # without raising; surface them so download() can log them.
            ph = tkr._price_history
            if ph is not None and ph._last_error is not None:
                ctx.errors[sym] = ph._last_error
    except Exception as e:
        with ctx.lock:
            ctx.dfs[sym] = utils.empty_df()
            ctx.errors[sym] = repr(e)
            ctx.tracebacks[sym] = traceback.format_exc()

    YfConfig.network.hide_exceptions = backup

    return data

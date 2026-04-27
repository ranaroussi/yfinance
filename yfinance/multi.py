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
import time as _time
import traceback
from typing import Union

import multitasking as _multitasking
import polars as _pl
from curl_cffi import requests

from . import Ticker, shared, utils
from .config import YfConfig
from .const import period_default
from .data import YfData


@utils.log_indent_decorator
def download(
    tickers,
    start=None,
    end=None,
    actions=False,
    threads=True,
    ignore_tz=None,
    group_by="column",
    auto_adjust=True,
    back_adjust=False,
    repair=False,
    keepna=False,
    progress=True,
    period=period_default,
    interval="1d",
    prepost=False,
    rounding=False,
    timeout=10,
    session=None,
    multi_level_index=True,
) -> Union[_pl.DataFrame, None]:
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
            NOTE: In the polars migration this parameter is retained for API
            compatibility but has no effect — output is always long-form with a
            'Ticker' column.
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
            Optional. Retained for API compatibility; has no effect in polars output.
    """
    shared._LOCK.acquire()
    try:
        return _download_impl(
            tickers,
            start=start,
            end=end,
            actions=actions,
            threads=threads,
            ignore_tz=ignore_tz,
            group_by=group_by,
            auto_adjust=auto_adjust,
            back_adjust=back_adjust,
            repair=repair,
            keepna=keepna,
            progress=progress,
            period=period,
            interval=interval,
            prepost=prepost,
            rounding=rounding,
            timeout=timeout,
            session=session,
            multi_level_index=multi_level_index,
        )
    finally:
        shared._LOCK.release()


def _download_impl(
    tickers,
    start=None,
    end=None,
    actions=False,
    threads=True,
    ignore_tz=None,
    group_by="column",
    auto_adjust=True,
    back_adjust=False,
    repair=False,
    keepna=False,
    progress=True,
    period=period_default,
    interval="1d",
    prepost=False,
    rounding=False,
    timeout=10,
    session=None,
    multi_level_index=True,
):
    logger = utils.get_yf_logger()
    session = session or requests.Session(impersonate="chrome")

    # Ensure data initialised with session.
    YfData(session=session)

    if logger.isEnabledFor(logging.DEBUG):
        if threads:
            logger.debug("Disabling multithreading because DEBUG logging enabled")
            threads = False
        if progress:
            progress = False

    if ignore_tz is None:
        # Set default value depending on interval
        if interval[-1] in ["m", "h"]:
            # Intraday
            ignore_tz = False
        else:
            ignore_tz = True

    # create ticker list
    tickers = (
        tickers
        if isinstance(tickers, (list, set, tuple))
        else tickers.replace(",", " ").split()
    )

    # accept isin as ticker
    shared._ISINS = {}
    _tickers_ = []
    for ticker in tickers:
        if utils.is_isin(ticker):
            isin = ticker
            ticker = utils.get_ticker_by_isin(ticker)
            shared._ISINS[ticker] = isin
        _tickers_.append(ticker)

    tickers = _tickers_

    tickers = list(set([ticker.upper() for ticker in tickers]))

    if progress:
        shared._PROGRESS_BAR = utils.ProgressBar(len(tickers), "completed")

    # reset shared._DFS
    shared._DFS = {}
    shared._ERRORS = {}
    shared._TRACEBACKS = {}

    # download using threads
    if threads:
        if threads is True:
            threads = min([len(tickers), _multitasking.cpu_count() * 2])
        _multitasking.set_max_threads(threads)
        for i, ticker in enumerate(tickers):
            _download_one_threaded(
                ticker,
                period=period,
                interval=interval,
                start=start,
                end=end,
                prepost=prepost,
                actions=actions,
                auto_adjust=auto_adjust,
                back_adjust=back_adjust,
                repair=repair,
                keepna=keepna,
                progress=(progress and i > 0),
                rounding=rounding,
                timeout=timeout,
            )
        while len(shared._DFS) < len(tickers):
            _time.sleep(0.01)
    # download synchronously
    else:
        for i, ticker in enumerate(tickers):
            data = _download_one(
                ticker,
                period=period,
                interval=interval,
                start=start,
                end=end,
                prepost=prepost,
                actions=actions,
                auto_adjust=auto_adjust,
                back_adjust=back_adjust,
                repair=repair,
                keepna=keepna,
                rounding=rounding,
                timeout=timeout,
            )
            if progress:
                shared._PROGRESS_BAR.animate()

    if progress:
        shared._PROGRESS_BAR.completed()

    if shared._ERRORS:
        logger = utils.get_yf_logger()
        logger.error(
            "\n%.f Failed download%s:"
            % (len(shared._ERRORS), "s" if len(shared._ERRORS) > 1 else "")
        )

        errors = {}
        for ticker in shared._ERRORS:
            err = shared._ERRORS[ticker]
            err = err.replace(f"${ticker}: ", "")
            if err not in errors:
                errors[err] = [ticker]
            else:
                errors[err].append(ticker)
        for err in errors.keys():
            logger.error(f"{errors[err]}: " + err)

        tbs = {}
        for ticker in shared._TRACEBACKS:
            tb = shared._TRACEBACKS[ticker]
            tb = tb.replace(f"${ticker}: ", "")
            if tb not in tbs:
                tbs[tb] = [ticker]
            else:
                tbs[tb].append(ticker)
        for tb in tbs.keys():
            logger.debug(f"{tbs[tb]}: " + tb)

    # Strip timezone from Datetime column when ignore_tz is set
    if ignore_tz:
        for tkr in shared._DFS.keys():
            df = shared._DFS[tkr]
            if df is not None and not df.is_empty():
                if "Datetime" in df.columns:
                    shared._DFS[tkr] = df.with_columns(
                        _pl.col("Datetime").dt.replace_time_zone(None)
                    )

    # Attempt concat; on failure realign then retry
    try:
        frames = []
        for ticker, df in shared._DFS.items():
            if df is not None and not df.is_empty():
                df = df.with_columns(_pl.lit(ticker).alias("Ticker"))
                frames.append(df)

        if not frames:
            return _pl.DataFrame()

        data = _pl.concat(frames, how="diagonal")
    except Exception:
        _realign_dfs()
        frames = []
        for ticker, df in shared._DFS.items():
            if df is not None and not df.is_empty():
                df = df.with_columns(_pl.lit(ticker).alias("Ticker"))
                frames.append(df)

        if not frames:
            return _pl.DataFrame()

        data = _pl.concat(frames, how="diagonal")

    # Determine sort column
    if "Datetime" in data.columns:
        data = data.sort("Datetime")
    elif "Date" in data.columns:
        data = data.sort("Date")

    # Switch ticker names back to ISINs if applicable
    if shared._ISINS:
        data = data.with_columns(_pl.col("Ticker").replace(shared._ISINS))

    return data


def _realign_dfs():
    # Find the union of all Datetime values across DFS entries
    datetime_col = None
    for df in shared._DFS.values():
        if df is not None and not df.is_empty():
            if "Datetime" in df.columns:
                datetime_col = "Datetime"
            elif "Date" in df.columns:
                datetime_col = "Date"
            break

    if datetime_col is None:
        return

    valid_dfs = [
        df.select(datetime_col)
        for df in shared._DFS.values()
        if df is not None and not df.is_empty()
    ]

    if not valid_dfs:
        return

    all_datetimes = _pl.concat(valid_dfs).unique().sort(datetime_col)

    for key in shared._DFS.keys():
        df = shared._DFS[key]
        if df is None or df.is_empty():
            continue
        try:
            # Left-join to align to union of all datetime values
            shared._DFS[key] = all_datetimes.join(df, on=datetime_col, how="left")
            # Remove duplicate datetime rows, keeping last
            shared._DFS[key] = shared._DFS[key].unique(
                subset=[datetime_col], keep="last"
            )
        except Exception:
            shared._DFS[key] = df


@_multitasking.task
def _download_one_threaded(
    ticker,
    start=None,
    end=None,
    auto_adjust=False,
    back_adjust=False,
    repair=False,
    actions=False,
    progress=True,
    period=None,
    interval="1d",
    prepost=False,
    keepna=False,
    rounding=False,
    timeout=10,
):
    _download_one(
        ticker,
        start,
        end,
        auto_adjust,
        back_adjust,
        repair,
        actions,
        period,
        interval,
        prepost,
        rounding,
        keepna,
        timeout,
    )
    if progress:
        shared._PROGRESS_BAR.animate()


def _download_one(
    ticker,
    start=None,
    end=None,
    auto_adjust=False,
    back_adjust=False,
    repair=False,
    actions=False,
    period=None,
    interval="1d",
    prepost=False,
    rounding=False,
    keepna=False,
    timeout=10,
):
    data = None

    backup = YfConfig.network.hide_exceptions
    YfConfig.network.hide_exceptions = False
    try:
        data = Ticker(ticker).history(
            period=period,
            interval=interval,
            start=start,
            end=end,
            prepost=prepost,
            actions=actions,
            auto_adjust=auto_adjust,
            back_adjust=back_adjust,
            repair=repair,
            rounding=rounding,
            keepna=keepna,
            timeout=timeout,
        )
        shared._DFS[ticker.upper()] = data
    except Exception as e:
        shared._DFS[ticker.upper()] = utils.empty_df()
        shared._ERRORS[ticker.upper()] = repr(e)
        shared._TRACEBACKS[ticker.upper()] = traceback.format_exc()

    YfConfig.network.hide_exceptions = backup

    return data


def download_to_dict(df: _pl.DataFrame) -> dict[str, _pl.DataFrame]:
    """
    Convert a long-form download() result into a dict keyed by ticker symbol.
    Each value is a DataFrame with the 'Ticker' column removed.

    Example:
        result = yf.download(["AAPL", "MSFT"])
        by_ticker = yf.download_to_dict(result)
        aapl_df = by_ticker["AAPL"]
    """
    if "Ticker" not in df.columns:
        return {"": df}
    return {
        ticker: df.filter(_pl.col("Ticker") == ticker).drop("Ticker")
        for ticker in df["Ticker"].unique().sort().to_list()
    }

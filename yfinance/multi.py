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

"""Bulk download helpers for fetching multiple Yahoo Finance tickers."""

from __future__ import print_function

import logging
import time as _time
import traceback
from typing import Union, cast

import multitasking as _multitasking
import pandas as _pd
from curl_cffi import requests

from .options import bind_options
from . import utils
from . import shared
from .config import YF_CONFIG as YfConfig
from .data import YfData
from .exceptions import YFException
from .ticker import Ticker

_SHARED_DFS_ATTR = "_DFS"
_SHARED_PROGRESS_BAR_ATTR = "_PROGRESS_BAR"
_SHARED_ERRORS_ATTR = "_ERRORS"
_SHARED_TRACEBACKS_ATTR = "_TRACEBACKS"
_SHARED_ISINS_ATTR = "_ISINS"

_DOWNLOAD_ARG_NAMES = (
    "start",
    "end",
    "actions",
    "threads",
    "ignore_tz",
    "group_by",
    "auto_adjust",
    "back_adjust",
    "repair",
    "keepna",
    "progress",
    "period",
    "interval",
    "prepost",
    "rounding",
    "timeout",
    "session",
    "multi_level_index",
)
_DOWNLOAD_DEFAULTS = {
    "start": None,
    "end": None,
    "actions": False,
    "threads": True,
    "ignore_tz": None,
    "group_by": "column",
    "auto_adjust": True,
    "back_adjust": False,
    "repair": False,
    "keepna": False,
    "progress": True,
    "period": None,
    "interval": "1d",
    "prepost": False,
    "rounding": False,
    "timeout": 10,
    "session": None,
    "multi_level_index": True,
}

_DOWNLOAD_ONE_THREADED_ARG_NAMES = (
    "start",
    "end",
    "auto_adjust",
    "back_adjust",
    "repair",
    "actions",
    "progress",
    "period",
    "interval",
    "prepost",
    "keepna",
    "rounding",
    "timeout",
)
_DOWNLOAD_ONE_THREADED_DEFAULTS = {
    "start": None,
    "end": None,
    "auto_adjust": False,
    "back_adjust": False,
    "repair": False,
    "actions": False,
    "progress": True,
    "period": None,
    "interval": "1d",
    "prepost": False,
    "keepna": False,
    "rounding": False,
    "timeout": 10,
}

_DOWNLOAD_ONE_ARG_NAMES = (
    "start",
    "end",
    "auto_adjust",
    "back_adjust",
    "repair",
    "actions",
    "period",
    "interval",
    "prepost",
    "rounding",
    "keepna",
    "timeout",
)
_DOWNLOAD_ONE_DEFAULTS = {
    "start": None,
    "end": None,
    "auto_adjust": False,
    "back_adjust": False,
    "repair": False,
    "actions": False,
    "period": None,
    "interval": "1d",
    "prepost": False,
    "rounding": False,
    "keepna": False,
    "timeout": 10,
}

_RECOVERABLE_EXCEPTIONS = (
    AssertionError,
    AttributeError,
    IndexError,
    KeyError,
    OSError,
    RuntimeError,
    TypeError,
    ValueError,
    requests.exceptions.RequestException,
    YFException,
)


def _parse_options(function_name, arg_names, defaults, args, kwargs):
    options, remaining_kwargs = bind_options(function_name, arg_names, defaults, args, kwargs)
    if remaining_kwargs:
        unexpected = ", ".join(sorted(remaining_kwargs))
        raise TypeError(f"{function_name}() got unexpected keyword argument(s): {unexpected}")

    return options


def _shared_get(attr_name):
    return getattr(shared, attr_name)


def _shared_set(attr_name, value):
    setattr(shared, attr_name, value)


def _get_dfs():
    return _shared_get(_SHARED_DFS_ATTR)


def _get_errors():
    return _shared_get(_SHARED_ERRORS_ATTR)


def _get_tracebacks():
    return _shared_get(_SHARED_TRACEBACKS_ATTR)


def _get_isins():
    return _shared_get(_SHARED_ISINS_ATTR)


def _get_progress_bar():
    return _shared_get(_SHARED_PROGRESS_BAR_ATTR)


def _set_progress_bar(progress_bar):
    _shared_set(_SHARED_PROGRESS_BAR_ATTR, progress_bar)


def _set_dfs(value):
    _shared_set(_SHARED_DFS_ATTR, value)


def _set_errors(value):
    _shared_set(_SHARED_ERRORS_ATTR, value)


def _set_tracebacks(value):
    _shared_set(_SHARED_TRACEBACKS_ATTR, value)


def _set_isins(value):
    _shared_set(_SHARED_ISINS_ATTR, value)


def _normalise_tickers(tickers):
    if isinstance(tickers, (list, set, tuple)):
        return list(tickers)
    return tickers.replace(",", " ").split()


def _resolve_isins(tickers):
    symbols = []
    isin_map = {}
    for ticker in tickers:
        resolved_ticker = ticker
        if utils.is_isin(ticker):
            isin = ticker
            resolved_ticker = utils.get_ticker_by_isin(ticker)
            isin_map[resolved_ticker] = isin
        symbols.append(resolved_ticker)

    return symbols, isin_map


def _initialise_shared_state(tickers, progress):
    _set_dfs({})
    _set_errors({})
    _set_tracebacks({})
    if progress:
        _set_progress_bar(utils.ProgressBar(len(tickers), "completed"))
    else:
        _set_progress_bar(None)


def _adjust_runtime_options_for_logging(options, logger):
    if not logger.isEnabledFor(logging.DEBUG):
        return options

    updated = dict(options)
    if updated["threads"]:
        logger.debug("Disabling multithreading because DEBUG logging enabled")
        updated["threads"] = False
    if updated["progress"]:
        updated["progress"] = False

    return updated


def _resolve_ignore_tz(ignore_tz, interval):
    if ignore_tz is not None:
        return ignore_tz
    return interval[-1] not in ["m", "h"]


def _download_all_tickers(tickers, options):
    if options["threads"]:
        _download_all_tickers_threaded(tickers, options)
        return
    _download_all_tickers_synchronously(tickers, options)


def _download_all_tickers_threaded(tickers, options):
    threads = options["threads"]
    if threads is True:
        threads = min(len(tickers), _multitasking.cpu_count() * 2)

    _multitasking.set_max_threads(threads)
    for index, ticker in enumerate(tickers):
        _download_one_threaded(
            ticker,
            period=options["period"],
            interval=options["interval"],
            start=options["start"],
            end=options["end"],
            prepost=options["prepost"],
            actions=options["actions"],
            auto_adjust=options["auto_adjust"],
            back_adjust=options["back_adjust"],
            repair=options["repair"],
            keepna=options["keepna"],
            progress=(options["progress"] and index > 0),
            rounding=options["rounding"],
            timeout=options["timeout"],
        )

    while len(_get_dfs()) < len(tickers):
        _time.sleep(0.01)


def _download_all_tickers_synchronously(tickers, options):
    progress_bar = _get_progress_bar()
    for ticker in tickers:
        _download_one(
            ticker,
            period=options["period"],
            interval=options["interval"],
            start=options["start"],
            end=options["end"],
            prepost=options["prepost"],
            actions=options["actions"],
            auto_adjust=options["auto_adjust"],
            back_adjust=options["back_adjust"],
            repair=options["repair"],
            keepna=options["keepna"],
            rounding=options["rounding"],
            timeout=options["timeout"],
        )
        if options["progress"] and progress_bar is not None:
            progress_bar.animate()


def _complete_progress_bar(progress):
    progress_bar = _get_progress_bar()
    if progress and progress_bar is not None:
        progress_bar.completed()


def _log_distinct_download_problems(logger):
    errors = _get_errors()
    if not errors:
        return

    failed_count = len(errors)
    logger.error("\n%d Failed download%s:", failed_count, "s" if failed_count > 1 else "")

    grouped_errors = {}
    for ticker, error_message in errors.items():
        normalized_error = error_message.replace(f"${ticker}: ", "")
        grouped_errors.setdefault(normalized_error, []).append(ticker)
    for error_message, symbols in grouped_errors.items():
        logger.error("%s: %s", symbols, error_message)

    grouped_tracebacks = {}
    for ticker, traceback_message in _get_tracebacks().items():
        normalized_traceback = traceback_message.replace(f"${ticker}: ", "")
        grouped_tracebacks.setdefault(normalized_traceback, []).append(ticker)
    for traceback_message, symbols in grouped_tracebacks.items():
        logger.debug("%s: %s", symbols, traceback_message)


def _remove_timezone_from_index_if_needed(ignore_tz):
    if not ignore_tz:
        return

    for _, dataframe in _get_dfs().items():
        if dataframe is not None and dataframe.shape[0] > 0:
            dataframe.index = dataframe.index.tz_localize(None)


def _concat_download_frames(dataframes):
    return _pd.concat(
        dataframes.values(),
        axis=1,
        sort=True,
        keys=dataframes.keys(),
        names=["Ticker", "Price"],
    )


def _create_download_dataframe(ignore_tz):
    dataframes = _get_dfs()
    if not dataframes:
        return _pd.DataFrame()

    try:
        data = _concat_download_frames(dataframes)
    except (KeyError, TypeError, ValueError):
        _realign_dfs()
        data = _concat_download_frames(_get_dfs())

    data.index = _pd.to_datetime(data.index, utc=not ignore_tz)
    return data

@utils.log_indent_decorator
def download(tickers, *args, **kwargs) -> Union[_pd.DataFrame, None]:
    """
    Download yahoo tickers
    :Parameters:
        tickers : str, list
            List of tickers to download
        period : str
            Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
            Default: 1mo
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
    options = _parse_options(
        "download",
        _DOWNLOAD_ARG_NAMES,
        _DOWNLOAD_DEFAULTS,
        args,
        kwargs,
    )
    logger = utils.get_yf_logger()
    options["session"] = options["session"] or requests.Session(impersonate="chrome")
    options = _adjust_runtime_options_for_logging(options, logger)
    options["ignore_tz"] = _resolve_ignore_tz(options["ignore_tz"], options["interval"])

    # Ensure data initialised with session.
    YfData(session=options["session"])

    parsed_tickers = _normalise_tickers(tickers)
    parsed_tickers, isin_map = _resolve_isins(parsed_tickers)
    parsed_tickers = list({ticker.upper() for ticker in parsed_tickers})
    _set_isins(isin_map)
    _initialise_shared_state(parsed_tickers, options["progress"])

    _download_all_tickers(parsed_tickers, options)
    _complete_progress_bar(options["progress"])
    _log_distinct_download_problems(logger)
    _remove_timezone_from_index_if_needed(options["ignore_tz"])

    data = _create_download_dataframe(options["ignore_tz"])
    # switch names back to isins if applicable
    data = cast(_pd.DataFrame, data.rename(cast(dict[str, str], _get_isins()), axis=1))

    if options["group_by"] == "column":
        if isinstance(data.columns, _pd.MultiIndex):
            data.columns = data.columns.swaplevel(0, 1)
            data.sort_index(level=0, axis=1, inplace=True)

    if not options["multi_level_index"] and len(parsed_tickers) == 1:
        level = 0 if options["group_by"] == "ticker" else 1
        data = cast(_pd.DataFrame, data.droplevel(level, axis=1))
        data = data.rename_axis(None, axis=1)

    return data


def _realign_dfs():
    dfs = _get_dfs()
    idx_len = 0
    idx = None
    for dataframe in dfs.values():
        if dataframe is not None and len(dataframe) > idx_len:
            idx_len = len(dataframe)
            idx = dataframe.index

    if idx is None:
        return

    for key, dataframe in list(dfs.items()):
        aligned_dataframe = _align_dataframe_to_index(dataframe, idx)
        dfs[key] = aligned_dataframe.loc[~aligned_dataframe.index.duplicated(keep="last")]


def _align_dataframe_to_index(dataframe, index):
    if dataframe is None:
        return utils.empty_df(index)

    if dataframe.shape[0] == len(index):
        return _pd.DataFrame(index=index, data=dataframe).drop_duplicates()

    return _pd.concat([utils.empty_df(index), dataframe.dropna()], axis=0, sort=True)


@_multitasking.task
def _download_one_threaded(ticker, *args, **kwargs):
    options = _parse_options(
        "_download_one_threaded",
        _DOWNLOAD_ONE_THREADED_ARG_NAMES,
        _DOWNLOAD_ONE_THREADED_DEFAULTS,
        args,
        kwargs,
    )
    _download_one(
        ticker,
        start=options["start"],
        end=options["end"],
        auto_adjust=options["auto_adjust"],
        back_adjust=options["back_adjust"],
        repair=options["repair"],
        actions=options["actions"],
        period=options["period"],
        interval=options["interval"],
        prepost=options["prepost"],
        rounding=options["rounding"],
        keepna=options["keepna"],
        timeout=options["timeout"],
    )
    progress_bar = _get_progress_bar()
    if options["progress"] and progress_bar is not None:
        progress_bar.animate()


def _download_one(ticker, *args, **kwargs):
    options = _parse_options(
        "_download_one",
        _DOWNLOAD_ONE_ARG_NAMES,
        _DOWNLOAD_ONE_DEFAULTS,
        args,
        kwargs,
    )
    data = None

    dfs = _get_dfs()
    errors = _get_errors()
    tracebacks = _get_tracebacks()
    symbol = ticker.upper()

    prev_hide = YfConfig.debug.hide_exceptions
    try:
        YfConfig.debug.hide_exceptions = False
        data = Ticker(ticker).history(
            period=options["period"],
            interval=options["interval"],
            start=options["start"],
            end=options["end"],
            prepost=options["prepost"],
            actions=options["actions"],
            auto_adjust=options["auto_adjust"],
            back_adjust=options["back_adjust"],
            repair=options["repair"],
            rounding=options["rounding"],
            keepna=options["keepna"],
            timeout=options["timeout"],
        )
        dfs[symbol] = data
    except _RECOVERABLE_EXCEPTIONS as error:
        dfs[symbol] = utils.empty_df()
        errors[symbol] = repr(error)
        tracebacks[symbol] = traceback.format_exc()
    finally:
        YfConfig.debug.hide_exceptions = prev_hide

    return data

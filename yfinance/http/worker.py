"""Bulk download workers for fetching multiple Yahoo Finance tickers.

State isolation
---------------
All per-call mutable state (result frames, errors, tracebacks, ISIN map,
progress bar) lives on a :class:`~yfinance.http.manager.DownloadManager`
instance created at the start of each :func:`download` call.  Workers receive
the manager explicitly rather than reading from module-level globals, so
concurrent download() calls cannot overwrite each other's results.
"""

from __future__ import print_function

import logging
import traceback
from typing import Union, cast

import multitasking as _multitasking
import pandas as _pd
from curl_cffi import requests

from ..options import bind_options
from .. import utils
from ..config import YF_CONFIG as YfConfig
from ..data import YfData
from ..exceptions import YFException
from ..scrapers.history.client import PriceHistory
from ..utils_tz import get_ticker_tz
from .manager import DownloadManager

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


def _download_all_tickers(manager: DownloadManager, options: dict) -> None:
    prev_raise_on_error = YfConfig.debug.raise_on_error
    YfConfig.debug.raise_on_error = True
    try:
        if options["threads"]:
            _download_all_tickers_threaded(manager, options)
        else:
            _download_all_tickers_synchronously(manager, options)
    finally:
        YfConfig.debug.raise_on_error = prev_raise_on_error


def _download_all_tickers_threaded(manager: DownloadManager, options: dict) -> None:
    tickers = manager.tickers
    threads = options["threads"]
    if threads is True:
        threads = min(len(tickers), _multitasking.cpu_count() * 2)

    _multitasking.set_max_threads(threads)
    for index, ticker in enumerate(tickers):
        _download_one_threaded(
            ticker,
            manager,
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

    manager.wait_for_completion()


def _download_all_tickers_synchronously(manager: DownloadManager, options: dict) -> None:
    for ticker in manager.tickers:
        _download_one(ticker, manager, **{k: options[k] for k in _DOWNLOAD_ONE_ARG_NAMES})
        if options["progress"]:
            manager.animate_progress()


def _log_distinct_download_problems(manager: DownloadManager, logger) -> None:
    if not manager.errors:
        return

    failed_count = len(manager.errors)
    logger.error("\n%d Failed download%s:", failed_count, "s" if failed_count > 1 else "")

    grouped_errors = {}
    for ticker, error_message in manager.errors.items():
        normalized_error = error_message.replace(f"${ticker}: ", "")
        grouped_errors.setdefault(normalized_error, []).append(ticker)
    for error_message, symbols in grouped_errors.items():
        logger.error("%s: %s", symbols, error_message)

    grouped_tracebacks = {}
    for ticker, traceback_message in manager.tracebacks.items():
        normalized_traceback = traceback_message.replace(f"${ticker}: ", "")
        grouped_tracebacks.setdefault(normalized_traceback, []).append(ticker)
    for traceback_message, symbols in grouped_tracebacks.items():
        logger.debug("%s: %s", symbols, traceback_message)


def _remove_timezone_from_index_if_needed(dfs: dict, ignore_tz: bool) -> None:
    if not ignore_tz:
        return
    for _, dataframe in dfs.items():
        if dataframe is not None and dataframe.shape[0] > 0:
            dataframe.index = dataframe.index.tz_localize(None)


def _concat_download_frames(dataframes: dict) -> _pd.DataFrame:
    return _pd.concat(
        dataframes.values(),
        axis=1,
        sort=True,
        keys=dataframes.keys(),
        names=["Ticker", "Price"],
    )


def _create_download_dataframe(dfs: dict, ignore_tz: bool) -> _pd.DataFrame:
    if not dfs:
        return _pd.DataFrame()

    try:
        data = _concat_download_frames(dfs)
    except (KeyError, TypeError, ValueError):
        _realign_dfs(dfs)
        data = _concat_download_frames(dfs)

    data.index = _pd.to_datetime(data.index, utc=not ignore_tz)
    return data


def _realign_dfs(dfs: dict) -> None:
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


def _align_dataframe_to_index(dataframe, index) -> _pd.DataFrame:
    if dataframe is None:
        return utils.empty_df(index)

    if dataframe.shape[0] == len(index):
        return _pd.DataFrame(index=index, data=dataframe).drop_duplicates()

    return _pd.concat([utils.empty_df(index), dataframe.dropna()], axis=0, sort=True)


@_multitasking.task
def _download_one_threaded(ticker: str, manager: DownloadManager, *args, **kwargs) -> None:
    options = _parse_options(
        "_download_one_threaded",
        _DOWNLOAD_ONE_THREADED_ARG_NAMES,
        _DOWNLOAD_ONE_THREADED_DEFAULTS,
        args,
        kwargs,
    )
    _download_one(
        ticker,
        manager,
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
    if options["progress"]:
        manager.animate_progress()


def _download_one(ticker: str, manager: DownloadManager, *args, **kwargs) -> None:
    options = _parse_options(
        "_download_one",
        _DOWNLOAD_ONE_ARG_NAMES,
        _DOWNLOAD_ONE_DEFAULTS,
        args,
        kwargs,
    )
    symbol = ticker.upper()
    data = None

    try:
        data_client = YfData()
        timezone = get_ticker_tz(data_client, symbol, timeout=10)
        data = PriceHistory(data_client, symbol, timezone).history(
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
        manager.record(symbol, data)
    except _RECOVERABLE_EXCEPTIONS as error:
        manager.record(
            symbol,
            utils.empty_df(),
            error=repr(error),
            traceback_str=traceback.format_exc(),
        )


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

    manager = DownloadManager(parsed_tickers, show_progress=options["progress"])
    manager.isins = isin_map

    _download_all_tickers(manager, options)
    manager.complete_progress()
    _log_distinct_download_problems(manager, logger)
    _remove_timezone_from_index_if_needed(manager.dfs, options["ignore_tz"])

    data = _create_download_dataframe(manager.dfs, options["ignore_tz"])
    data = cast(_pd.DataFrame, data.rename(cast(dict[str, str], manager.isins), axis=1))

    if options["group_by"] == "column":
        if isinstance(data.columns, _pd.MultiIndex):
            data.columns = data.columns.swaplevel(0, 1)
            data.sort_index(level=0, axis=1, inplace=True)

    if not options["multi_level_index"] and len(parsed_tickers) == 1:
        level = 0 if options["group_by"] == "ticker" else 1
        data = cast(_pd.DataFrame, data.droplevel(level, axis=1))
        data = data.rename_axis(None, axis=1)

    return data

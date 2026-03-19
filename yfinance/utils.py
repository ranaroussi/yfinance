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
"""Core utility helpers used across yfinance modules."""

from __future__ import print_function

import datetime as _datetime
import logging
import re as _re
import threading
from functools import wraps
from inspect import getmembers
from types import FunctionType
import warnings

import numpy as _np
import pandas as _pd
import pytz as _tz
from dateutil.relativedelta import relativedelta
from pytz import UnknownTimeZoneError

from yfinance import const
from yfinance.config import YF_CONFIG as YfConfig
from yfinance.exceptions import YFException
from yfinance.utils_doc import (
    ProgressBar as _ProgressBar,
    dynamic_docstring as _dynamic_docstring,
    generate_list_table_from_dict as _generate_list_table_from_dict,
    generate_list_table_from_dict_universal as _generate_list_table_from_dict_universal,
)
from yfinance.utils_financial import (
    build_template as _build_template,
    camel2title as _camel2title,
    format_annual_financial_statement as _format_annual_financial_statement,
    format_quarterly_financial_statement as _format_quarterly_financial_statement,
    retrieve_financial_details as _retrieve_financial_details,
    snake_case_to_camel_case as _snake_case_to_camel_case,
)
from yfinance.utils_price import (
    _LiveSeparateContext,
    _MergeContext,
    fix_yahoo_returning_live_separate_impl as _fix_yahoo_returning_live_separate_impl,
    format_history_metadata_impl as _format_history_metadata_impl,
    safe_merge_dfs_impl as _safe_merge_dfs_impl,
)

ProgressBar = _ProgressBar
dynamic_docstring = _dynamic_docstring
generate_list_table_from_dict = _generate_list_table_from_dict
generate_list_table_from_dict_universal = _generate_list_table_from_dict_universal
build_template = _build_template
camel2title = _camel2title
format_annual_financial_statement = _format_annual_financial_statement
format_quarterly_financial_statement = _format_quarterly_financial_statement
retrieve_financial_details = _retrieve_financial_details
snake_case_to_camel_case = _snake_case_to_camel_case

_PRICE_COLNAMES = getattr(const, "_PRICE_COLNAMES_")


# From https://stackoverflow.com/a/59128615
def attributes(obj):
    """Return public non-method attributes for an object."""
    disallowed_names = {
        name for name, value in getmembers(type(obj)) if isinstance(value, FunctionType)
    }
    return {
        name: getattr(obj, name)
        for name in dir(obj)
        if name[0] != "_" and name not in disallowed_names and hasattr(obj, name)
    }


# Logging
# Note: most of this logic is adding indentation with function depth,
#       so that DEBUG log is readable.
class IndentLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that prepends indentation to multi-line debug output."""

    def process(self, msg, kwargs):
        if get_yf_logger().isEnabledFor(logging.DEBUG):
            indent_raw = (
                self.extra.get("indent", 0) if isinstance(self.extra, dict) else 0
            )
            indent = indent_raw if isinstance(indent_raw, int) else 0
            i = " " * indent
            if not isinstance(msg, str):
                msg = str(msg)
            msg = "\n".join([i + m for m in msg.split("\n")])
        return msg, kwargs


_indentation_level = threading.local()


class IndentationContext:
    """Context manager that increments/decrements the current log indentation."""

    def __init__(self, increment=1):
        self.increment = increment

    def __enter__(self):
        _indentation_level.indent = (
            getattr(_indentation_level, "indent", 0) + self.increment
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        _indentation_level.indent -= self.increment


def get_indented_logger(name=None):
    """Return an indentation-aware logger adapter for the given logger name."""
    # Never cache the returned value; that would desync indentation depth.
    return IndentLoggerAdapter(
        logging.getLogger(name), {"indent": getattr(_indentation_level, "indent", 0)}
    )


def log_indent_decorator(func):
    """Decorator that logs entry/exit with nested indentation."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_indented_logger("yfinance")
        logger.debug(f"Entering {func.__name__}()")

        with IndentationContext():
            result = func(*args, **kwargs)

        logger.debug(f"Exiting {func.__name__}()")
        return result

    return wrapper


class MultiLineFormatter(logging.Formatter):
    """Formatter that preserves level-prefix alignment for multi-line messages."""

    # The 'fmt' formatting further down is only applied to first line
    # of log message, specifically the padding after %level%.
    # For multi-line messages, need to manually copy over padding.
    def __init__(self, fmt):
        super().__init__(fmt)
        # Extract amount of padding
        match = _re.search(r"%\(levelname\)-(\d+)s", fmt)
        self.level_length = int(match.group(1)) if match else 0

    def format(self, record):
        original = super().format(record)
        lines = original.split("\n")
        levelname = lines[0].split(" ")[0]
        if len(lines) <= 1:
            return original
        # Apply padding to all lines below first
        formatted = [lines[0]]
        if self.level_length == 0:
            padding = " " * len(levelname)
        else:
            padding = " " * self.level_length
        padding += " "  # +1 for space between level and message
        formatted.extend(padding + line for line in lines[1:])
        return "\n".join(formatted)


_LOGGER_STATE = {"logger": None, "indented": False}


def _apply_yf_log_context(record: logging.LogRecord) -> bool:
    """Normalize contextual fields on yfinance log records."""
    msg = record.msg
    yf_cat = getattr(record, "yf_cat", None)
    if yf_cat is not None:
        msg = f"{yf_cat}: {msg}"
    yf_interval = getattr(record, "yf_interval", None)
    if yf_interval is not None:
        msg = f"{yf_interval}: {msg}"
    yf_symbol = getattr(record, "yf_symbol", None)
    if yf_symbol is not None:
        msg = f"{yf_symbol}: {msg}"
    record.msg = msg
    return True


def get_yf_logger():
    """Return the yfinance logger with current debug/indent configuration."""
    if _LOGGER_STATE["indented"] and not YfConfig.debug.logging:
        _disable_debug_mode()
    elif YfConfig.debug.logging and not _LOGGER_STATE["indented"]:
        _enable_debug_mode()

    if _LOGGER_STATE["indented"]:
        _LOGGER_STATE["logger"] = get_indented_logger("yfinance")
    elif _LOGGER_STATE["logger"] is None:
        logger = logging.getLogger("yfinance")
        logger.addFilter(_apply_yf_log_context)
        _LOGGER_STATE["logger"] = logger
    return _LOGGER_STATE["logger"]


def enable_debug_mode():
    """Enable debug logging with indentation-aware formatting (deprecated)."""
    warnings.warn(
        "enable_debug_mode() is replaced by: yf.config.debug.logging = True (or False to disable)",
        DeprecationWarning,
    )
    _enable_debug_mode()


def _enable_debug_mode():
    if not _LOGGER_STATE["indented"]:
        base_logger = logging.getLogger("yfinance")
        base_logger.setLevel(logging.DEBUG)
        if base_logger.handlers is None or len(base_logger.handlers) == 0:
            h = logging.StreamHandler()
            # Ensure different level strings don't interfere with indentation
            formatter = MultiLineFormatter(fmt="%(levelname)-8s %(message)s")
            h.setFormatter(formatter)
            base_logger.addHandler(h)
        _LOGGER_STATE["logger"] = get_indented_logger()
        _LOGGER_STATE["indented"] = True


def _disable_debug_mode():
    if _LOGGER_STATE["indented"]:
        base_logger = logging.getLogger("yfinance")
        base_logger.setLevel(logging.NOTSET)
        _LOGGER_STATE["logger"] = None
        _LOGGER_STATE["indented"] = False


def is_isin(string):
    """Return True when string matches the ISIN identifier format."""
    return bool(_re.match("^([A-Z]{2})([A-Z0-9]{9})([0-9])$", string))


def get_all_by_isin(isin):
    """Resolve ticker/news metadata for a valid ISIN."""
    if not is_isin(isin):
        raise ValueError("Invalid ISIN number")

    search_class = __import__("yfinance.search", fromlist=["Search"]).Search
    search = search_class(query=isin, max_results=1)

    # Extract the first quote and news
    ticker = search.quotes[0] if search.quotes else {}
    news = search.news

    return {
        "ticker": {
            "symbol": ticker.get("symbol", ""),
            "shortname": ticker.get("shortname", ""),
            "longname": ticker.get("longname", ""),
            "type": ticker.get("quoteType", ""),
            "exchange": ticker.get("exchDisp", ""),
        },
        "news": news,
    }


def get_ticker_by_isin(isin):
    """Return ticker symbol for a valid ISIN, else empty string."""
    data = get_all_by_isin(isin)
    return data.get("ticker", {}).get("symbol", "")


def get_info_by_isin(isin):
    """Return ticker metadata dictionary for a valid ISIN."""
    data = get_all_by_isin(isin)
    return data.get("ticker", {})


def get_news_by_isin(isin):
    """Return related news payload for a valid ISIN."""
    data = get_all_by_isin(isin)
    return data.get("news", {})


def empty_df(index=None):
    """Create an empty OHLCV dataframe with a Date index."""
    if index is None:
        index = []
    empty = _pd.DataFrame(
        index=index,
        data={
            "Open": _np.nan,
            "High": _np.nan,
            "Low": _np.nan,
            "Close": _np.nan,
            "Adj Close": _np.nan,
            "Volume": _np.nan,
        },
    )
    empty.index.name = "Date"
    return empty


def empty_earnings_dates_df():
    """Create an empty dataframe for earnings-date results."""
    empty = _pd.DataFrame(
        columns=[
            "Symbol",
            "Company",
            "Earnings Date",
            "EPS Estimate",
            "Reported EPS",
            "Surprise(%)",
        ]
    )
    return empty


globals()["snake_case_2_camelCase"] = snake_case_to_camel_case


def _parse_user_dt(dt, exchange_tz=_tz.utc):
    """Normalize user datetime input to exchange timezone-aware Timestamp."""
    if isinstance(dt, int):
        dt = _pd.Timestamp(dt, unit="s", tz=exchange_tz)
    else:
        # Convert str/date -> datetime, set tzinfo=exchange, get timestamp:
        if isinstance(dt, str):
            dt = _datetime.datetime.strptime(str(dt), "%Y-%m-%d")
        if isinstance(dt, _datetime.date) and not isinstance(dt, _datetime.datetime):
            dt = _datetime.datetime.combine(dt, _datetime.time(0))
        if isinstance(dt, _datetime.datetime):
            if dt.tzinfo is None:
                # Assume user is referring to exchange's timezone
                dt = _pd.Timestamp(dt).tz_localize(exchange_tz)
            else:
                dt = _pd.Timestamp(dt).tz_convert(exchange_tz)
        else:  # if we reached here, then it hasn't been any known type
            raise ValueError(f"Unable to parse input dt {dt} of type {type(dt)}")
    return dt


parse_user_dt = _parse_user_dt


def _interval_to_timedelta(interval):
    """Convert a Yahoo interval token into a Timedelta/relativedelta."""
    if interval[-1] == "d":
        return relativedelta(days=int(interval[:-1]))
    if interval[-2:] == "wk":
        return relativedelta(weeks=int(interval[:-2]))
    if interval[-2:] == "mo":
        return relativedelta(months=int(interval[:-2]))
    if interval[-1] == "y":
        return relativedelta(years=int(interval[:-1]))
    return _pd.Timedelta(interval)


interval_to_timedelta = _interval_to_timedelta


def is_valid_period_format(period):
    """Check if the provided period has a valid format."""
    if period is None:
        return False

    # Regex pattern to match valid period formats like '1d', '2wk', '3mo', '1y'
    valid_pattern = r"^[1-9]\d*(d|wk|mo|y)$"
    return bool(_re.match(valid_pattern, period))


def auto_adjust(data):
    """Backfill OHLC columns using Adj Close as adjustment anchor."""
    col_order = data.columns
    df = data.copy()
    ratio = (df["Adj Close"] / df["Close"]).to_numpy()
    df["Adj Open"] = df["Open"] * ratio
    df["Adj High"] = df["High"] * ratio
    df["Adj Low"] = df["Low"] * ratio

    df.drop(["Open", "High", "Low", "Close"], axis=1, inplace=True)

    df.rename(
        columns={
            "Adj Open": "Open",
            "Adj High": "High",
            "Adj Low": "Low",
            "Adj Close": "Close",
        },
        inplace=True,
    )

    return df[[c for c in col_order if c in df.columns]]


def back_adjust(data):
    """back-adjusted data to mimic true historical prices"""

    col_order = data.columns
    df = data.copy()
    ratio = df["Adj Close"] / df["Close"]
    df["Adj Open"] = df["Open"] * ratio
    df["Adj High"] = df["High"] * ratio
    df["Adj Low"] = df["Low"] * ratio

    df.drop(["Open", "High", "Low", "Adj Close"], axis=1, inplace=True)

    df.rename(
        columns={"Adj Open": "Open", "Adj High": "High", "Adj Low": "Low"}, inplace=True
    )

    return df[[c for c in col_order if c in df.columns]]


def parse_quotes(data):
    """Parse Yahoo chart quote payload into a sorted OHLCV dataframe."""
    timestamps = data["timestamp"]
    ohlc = data["indicators"]["quote"][0]
    volumes = ohlc["volume"]
    opens = ohlc["open"]
    closes = ohlc["close"]
    lows = ohlc["low"]
    highs = ohlc["high"]

    adjclose = closes
    if "adjclose" in data["indicators"]:
        adjclose = data["indicators"]["adjclose"][0]["adjclose"]

    quotes = _pd.DataFrame(
        {
            "Open": opens,
            "High": highs,
            "Low": lows,
            "Close": closes,
            "Adj Close": adjclose,
            "Volume": volumes,
        }
    )

    quotes.index = _pd.to_datetime(timestamps, unit="s")
    quotes.sort_index(inplace=True)

    return quotes


def parse_actions(data):
    """Parse Yahoo corporate-action payload into dividend/split dataframes."""
    dividends = None
    capital_gains = None
    splits = None

    if "events" in data:
        if "dividends" in data["events"] and len(data["events"]["dividends"]) > 0:
            dividends = _pd.DataFrame(data=list(data["events"]["dividends"].values()))
            dividends.set_index("date", inplace=True)
            dividends.index = _pd.to_datetime(dividends.index, unit="s")
            dividends.sort_index(inplace=True)
            if "currency" in dividends.columns and (dividends["currency"] == "").all():
                # Currency column useless, drop it.
                dividends = dividends.drop("currency", axis=1)
            dividends = dividends.rename(columns={"amount": "Dividends"})

        if "capitalGains" in data["events"] and len(data["events"]["capitalGains"]) > 0:
            capital_gains = _pd.DataFrame(
                data=list(data["events"]["capitalGains"].values())
            )
            capital_gains.set_index("date", inplace=True)
            capital_gains.index = _pd.to_datetime(capital_gains.index, unit="s")
            capital_gains.sort_index(inplace=True)
            capital_gains.columns = ["Capital Gains"]

        if "splits" in data["events"] and len(data["events"]["splits"]) > 0:
            splits = _pd.DataFrame(data=list(data["events"]["splits"].values()))
            splits.set_index("date", inplace=True)
            splits.index = _pd.to_datetime(splits.index, unit="s")
            splits.sort_index(inplace=True)
            splits["Stock Splits"] = splits["numerator"] / splits["denominator"]
            splits = splits[["Stock Splits"]]

    if dividends is None:
        dividends = _pd.DataFrame(columns=["Dividends"], index=_pd.DatetimeIndex([]))
    if capital_gains is None:
        capital_gains = _pd.DataFrame(
            columns=["Capital Gains"], index=_pd.DatetimeIndex([])
        )
    if splits is None:
        splits = _pd.DataFrame(columns=["Stock Splits"], index=_pd.DatetimeIndex([]))

    return dividends, splits, capital_gains


def set_df_tz(df, _interval, tz):
    """Ensure dataframe index is localized to UTC then converted to target tz."""
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    df.index = df.index.tz_convert(tz)
    return df


def fix_yahoo_returning_prepost_unrequested(quotes, interval, trading_periods):
    """Drop rows returned by Yahoo outside regular requested trading hours."""
    # Sometimes Yahoo returns post-market data despite not requesting it.
    # Normally happens on half-day early closes.
    #
    # And sometimes returns pre-market data despite not requesting it.
    # E.g. some London tickers.
    tps_df = trading_periods.copy()
    tps_df["_date"] = tps_df.index.date
    quotes["_date"] = quotes.index.date
    idx = quotes.index.copy()
    quotes = quotes.merge(tps_df, how="left")
    quotes.index = idx
    # "end" = end of regular trading hours (including any auction)
    f_drop = quotes.index >= quotes["end"]
    td = _interval_to_timedelta(interval)
    f_drop = f_drop | (quotes.index + td <= quotes["start"])
    if f_drop.any():
        # When printing report, ignore rows that were already NaNs:
        # f_na = quotes[["Open","Close"]].isna().all(axis=1)
        # n_nna = quotes.shape[0] - _np.sum(f_na)
        # n_drop_nna = _np.sum(f_drop & ~f_na)
        # quotes_dropped = quotes[f_drop]
        # if debug and n_drop_nna > 0:
        #     print("Dropping intervals outside regular trading hours")
        quotes = quotes[~f_drop]
    quotes = quotes.drop(["_date", "start", "end"], axis=1)
    return quotes


def fix_yahoo_returning_live_separate(
    quotes, interval, tz_exchange, prepost, repair_context=None
):
    """Merge Yahoo's split live row into the prior interval when required."""
    repair_context = repair_context or {}
    repair = repair_context.get("repair", False)
    currency = repair_context.get("currency")
    return _fix_yahoo_returning_live_separate_impl(
        quotes,
        _LiveSeparateContext(
            interval=interval,
            tz_exchange=tz_exchange,
            prepost=prepost,
            repair=repair,
            currency=currency,
            price_colnames=_PRICE_COLNAMES,
        ),
    )


def _fix_yahoo_returning_live_separate_legacy(*args, **kwargs):
    """Backward-compatible adapter for legacy public function signature."""
    if len(args) < 4:
        raise TypeError("Expected at least 4 positional arguments")
    quotes, interval, tz_exchange, prepost, *rest = args
    if len(rest) > 2:
        raise TypeError("Expected at most 6 positional arguments")

    repair = kwargs.pop("repair", rest[0] if len(rest) >= 1 else False)
    currency = kwargs.pop("currency", rest[1] if len(rest) >= 2 else None)
    if kwargs:
        raise TypeError(f"Unexpected keyword arguments: {sorted(kwargs.keys())}")
    repair_context = {"repair": repair, "currency": currency}
    return fix_yahoo_returning_live_separate(
        quotes,
        interval,
        tz_exchange,
        prepost,
        repair_context=repair_context,
    )


globals()["fix_Yahoo_returning_live_separate"] = (
    _fix_yahoo_returning_live_separate_legacy
)
globals()["fix_Yahoo_returning_prepost_unrequested"] = (
    fix_yahoo_returning_prepost_unrequested
)


def safe_merge_dfs(df_main, df_sub, interval):
    """Safely merge event dataframes into a prices dataframe by interval."""
    def _interval_to_timedelta_strict(interval: str) -> _pd.Timedelta:
        result = _interval_to_timedelta(interval)
        if not isinstance(result, _pd.Timedelta):
            raise TypeError(f"Expected Timedelta, got {type(result)} for interval {interval!r}")
        return result

    return _safe_merge_dfs_impl(
        df_main,
        df_sub,
        interval,
        _MergeContext(
            interval_to_timedelta=_interval_to_timedelta_strict,
            logger_getter=get_yf_logger,
            price_colnames=_PRICE_COLNAMES,
            exception_cls=YFException,
        ),
    )


def fix_yahoo_dst_issue(df, interval):
    """Correct known Yahoo DST timestamp offsets for daily/weekly data."""
    if interval in ["1d", "1w", "1wk"]:
        # Intervals should start at 00:00. Some date/timezone combinations from
        # Yahoo are shifted by hours (e.g. Brazil 23:00 around Jan-2022).
        # The clue is (a) minutes=0 and (b) hour near 0.
        # Obviously Yahoo meant 00:00, so ensure this doesn't affect date conversion:
        f_pre_midnight = (df.index.minute == 0) & (df.index.hour.isin([22, 23]))
        dst_error_hours = _np.array([0] * df.shape[0])
        dst_error_hours[f_pre_midnight] = 24 - df.index[f_pre_midnight].hour
        df.index += _pd.to_timedelta(dst_error_hours, "h")
    return df


def is_valid_timezone(tz: str) -> bool:
    """Return True when timezone string can be resolved by pytz."""
    try:
        _tz.timezone(tz)
    except UnknownTimeZoneError:
        return False
    return True


globals()["fix_Yahoo_dst_issue"] = fix_yahoo_dst_issue


def format_history_metadata(md, trading_periods_only=True, **kwargs):
    """Normalize and timezone-convert chart metadata returned by Yahoo."""
    if "tradingPeriodsOnly" in kwargs:
        trading_periods_only = kwargs.pop("tradingPeriodsOnly")
    if kwargs:
        raise TypeError(f"Unexpected keyword arguments: {sorted(kwargs.keys())}")
    return _format_history_metadata_impl(md, trading_periods_only=trading_periods_only)

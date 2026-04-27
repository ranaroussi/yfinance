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

import datetime as _datetime
import logging
import re
import re as _re
import sys as _sys
import threading
import warnings
from datetime import date as _date
from datetime import datetime, timedelta, timezone
from functools import wraps
from inspect import getmembers
from types import FunctionType
from typing import List, Optional
from zoneinfo import ZoneInfo

import numpy as _np
import polars as _pl
import pytz as _tz
from dateutil.relativedelta import relativedelta
from pytz import UnknownTimeZoneError

from yfinance import const
from yfinance.config import YfConfig
from yfinance.exceptions import YFException


# From https://stackoverflow.com/a/59128615
def attributes(obj):
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
    def process(self, msg, kwargs):
        if get_yf_logger().isEnabledFor(logging.DEBUG):
            i = " " * self.extra["indent"]
            if not isinstance(msg, str):
                msg = str(msg)
            msg = "\n".join([i + m for m in msg.split("\n")])
        return msg, kwargs


_indentation_level = threading.local()


class IndentationContext:
    def __init__(self, increment=1):
        self.increment = increment

    def __enter__(self):
        _indentation_level.indent = (
            getattr(_indentation_level, "indent", 0) + self.increment
        )

    def __exit__(self, exc_type, exc_val, exc_tb):
        _indentation_level.indent -= self.increment


def get_indented_logger(name=None):
    # Never cache the returned value! Will break indentation.
    return IndentLoggerAdapter(
        logging.getLogger(name), {"indent": getattr(_indentation_level, "indent", 0)}
    )


def log_indent_decorator(func):
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
        else:
            # Apply padding to all lines below first
            formatted = [lines[0]]
            if self.level_length == 0:
                padding = " " * len(levelname)
            else:
                padding = " " * self.level_length
            padding += " "  # +1 for space between level and message
            formatted.extend(padding + line for line in lines[1:])
            return "\n".join(formatted)


yf_logger = None
yf_log_indented = False


class YFLogFormatter(logging.Filter):
    # Help be consistent with structuring YF log messages
    def filter(self, record):
        msg = record.msg
        if hasattr(record, "yf_cat"):
            msg = f"{record.yf_cat}: {msg}"
        if hasattr(record, "yf_interval"):
            msg = f"{record.yf_interval}: {msg}"
        if hasattr(record, "yf_symbol"):
            msg = f"{record.yf_symbol}: {msg}"
        record.msg = msg
        return True


def get_yf_logger():
    global yf_logger
    global yf_log_indented

    if yf_log_indented and not YfConfig.debug.logging:
        _disable_debug_mode()
    elif YfConfig.debug.logging and not yf_log_indented:
        _enable_debug_mode()

    if yf_log_indented:
        yf_logger = get_indented_logger("yfinance")
    elif yf_logger is None:
        yf_logger = logging.getLogger("yfinance")
        yf_logger.addFilter(YFLogFormatter())
    return yf_logger


def enable_debug_mode():
    warnings.warn(
        "enable_debug_mode() is replaced by: yf.config.debug.logging = True (or False to disable)",
        DeprecationWarning,
    )
    _enable_debug_mode()


def _enable_debug_mode():
    global yf_logger
    global yf_log_indented
    if not yf_log_indented:
        yf_logger = logging.getLogger("yfinance")
        yf_logger.setLevel(logging.DEBUG)
        if yf_logger.handlers is None or len(yf_logger.handlers) == 0:
            h = logging.StreamHandler()
            # Ensure different level strings don't interfere with indentation
            formatter = MultiLineFormatter(fmt="%(levelname)-8s %(message)s")
            h.setFormatter(formatter)
            yf_logger.addHandler(h)
        yf_logger = get_indented_logger()
        yf_log_indented = True


def _disable_debug_mode():
    global yf_logger
    global yf_log_indented
    if yf_log_indented:
        yf_logger = logging.getLogger("yfinance")
        yf_logger.setLevel(logging.NOTSET)
        yf_logger = None
        yf_log_indented = False


def is_isin(string):
    return bool(_re.match("^([A-Z]{2})([A-Z0-9]{9})([0-9])$", string))


def get_all_by_isin(isin):
    if not (is_isin(isin)):
        raise ValueError("Invalid ISIN number")

    # Deferred this to prevent circular imports
    from .search import Search

    search = Search(query=isin, max_results=1)

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
    data = get_all_by_isin(isin)
    return data.get("ticker", {}).get("symbol", "")


def get_info_by_isin(isin):
    data = get_all_by_isin(isin)
    return data.get("ticker", {})


def get_news_by_isin(isin):
    data = get_all_by_isin(isin)
    return data.get("news", {})


def empty_df(date_col: str = "Datetime") -> _pl.DataFrame:
    """Return a zero-row OHLCV DataFrame (replaces the pandas DatetimeIndex version)."""
    return _pl.DataFrame(
        {
            date_col: _pl.Series([], dtype=_pl.Datetime("us", "UTC")),
            "Open": _pl.Series([], dtype=_pl.Float64),
            "High": _pl.Series([], dtype=_pl.Float64),
            "Low": _pl.Series([], dtype=_pl.Float64),
            "Close": _pl.Series([], dtype=_pl.Float64),
            "Volume": _pl.Series([], dtype=_pl.Int64),
            "Dividends": _pl.Series([], dtype=_pl.Float64),
            "Stock Splits": _pl.Series([], dtype=_pl.Float64),
        }
    )


def empty_earnings_dates_df() -> _pl.DataFrame:
    return _pl.DataFrame(
        {
            "Symbol": _pl.Series([], dtype=_pl.Utf8),
            "Company": _pl.Series([], dtype=_pl.Utf8),
            "Earnings Date": _pl.Series([], dtype=_pl.Datetime("us", "UTC")),
            "EPS Estimate": _pl.Series([], dtype=_pl.Float64),
            "Reported EPS": _pl.Series([], dtype=_pl.Float64),
            "Surprise(%)": _pl.Series([], dtype=_pl.Float64),
        }
    )


def build_template(data):
    """
    build_template returns the details required to rebuild any of the yahoo finance financial statements in the same order as the yahoo finance webpage. The function is built to be used on the "FinancialTemplateStore" json which appears in any one of the three yahoo finance webpages: "/financials", "/cash-flow" and "/balance-sheet".

    Returns:
        - template_annual_order: The order that annual figures should be listed in.
        - template_ttm_order: The order that TTM (Trailing Twelve Month) figures should be listed in.
        - template_order: The order that quarterlies should be in (note that quarterlies have no pre-fix - hence why this is required).
        - level_detail: The level of each individual line item. E.g. for the "/financials" webpage, "Total Revenue" is a level 0 item and is the summation of "Operating Revenue" and "Excise Taxes" which are level 1 items.

    """
    template_ttm_order = []  # Save the TTM (Trailing Twelve Months) ordering to an object.
    template_annual_order = []  # Save the annual ordering to an object.
    template_order = []  # Save the ordering to an object (this can be utilized for quarterlies)
    level_detail = []  # Record the level of each line item of the income statement ("Operating Revenue" and "Excise Taxes" sum to return "Total Revenue" we need to keep track of this)

    def traverse(node, level):
        """
        A recursive function that visits a node and its children.

        Args:
            node: The current node in the data structure.
            level: The depth of the current node in the data structure.
        """
        if level > 5:  # Stop when level is above 5
            return
        template_ttm_order.append(f"trailing{node['key']}")
        template_annual_order.append(f"annual{node['key']}")
        template_order.append(f"{node['key']}")
        level_detail.append(level)
        if "children" in node:  # Check if the node has children
            for child in node["children"]:  # If yes, traverse each child
                traverse(child, level + 1)  # Increment the level by 1 for each child

    for key in data["template"]:  # Loop through the data
        traverse(key, 0)  # Call the traverse function with initial level being 0

    return template_ttm_order, template_annual_order, template_order, level_detail


def retrieve_financial_details(data):
    """
    retrieve_financial_details returns all of the available financial details under the
    "QuoteTimeSeriesStore" for any of the following three yahoo finance webpages:
    "/financials", "/cash-flow" and "/balance-sheet".

    Returns:
        - TTM_dicts: A dictionary full of all of the available Trailing Twelve Month figures, this can easily be converted to a dataframe.
        - Annual_dicts: A dictionary full of all of the available Annual figures, this can easily be converted to a dataframe.
    """
    TTM_dicts = []  # Save a dictionary object to store the TTM financials.
    Annual_dicts = []  # Save a dictionary object to store the Annual financials.

    for key, timeseries in data.get(
        "timeSeries", {}
    ).items():  # Loop through the time series data to grab the key financial figures.
        try:
            if timeseries:
                time_series_dict = {"index": key}
                for each in timeseries:  # Loop through the years
                    if not each:
                        continue
                    time_series_dict[each.get("asOfDate")] = each.get("reportedValue")
                if "trailing" in key:
                    TTM_dicts.append(time_series_dict)
                elif "annual" in key:
                    Annual_dicts.append(time_series_dict)
        except KeyError as e:
            print(f"An error occurred while processing the key: {e}")
    return TTM_dicts, Annual_dicts


def format_annual_financial_statement(
    level_detail, annual_dicts, annual_order, ttm_dicts=None, ttm_order=None
):
    """
    format_annual_financial_statement formats any annual financial statement

    Returns:
        - _statement: A fully formatted annual financial statement as a polars DataFrame.
          Contains 'metric', 'level_detail' columns plus date columns.
    """
    import polars as _pl

    def _dicts_to_pl(dicts, order, prefix_strip):
        # Build a dict of {metric: {date: value}} then pivot
        rows = []
        for d in dicts:
            rows.append(d)
        # Reorder according to order list
        order_stripped = [o.replace(prefix_strip, "", 1) for o in order]
        # Build per-row data
        all_keys = set()
        for r in rows:
            all_keys.update(k for k in r.keys() if k != "index")
        date_cols = sorted(all_keys)

        index_to_row = {r["index"].replace(prefix_strip, "", 1): r for r in rows}
        records = []
        for metric in order_stripped:
            row = index_to_row.get(metric, {"index": metric})
            rec = {"metric": metric}
            for dc in date_cols:
                rec[dc] = row.get(dc, None)
            records.append(rec)

        if not records:
            return _pl.DataFrame({"metric": []})

        df = _pl.DataFrame(records)
        return df

    annual_df = _dicts_to_pl(annual_dicts, annual_order, "annual")

    if ttm_dicts and ttm_order:
        ttm_df = _dicts_to_pl(ttm_dicts, ttm_order, "trailing")
        # Rename TTM date columns with 'TTM ' prefix
        ttm_rename = {c: f"TTM {c}" for c in ttm_df.columns if c != "metric"}
        ttm_df = ttm_df.rename(ttm_rename)
        _statement = annual_df.join(ttm_df, on="metric", how="left")
    else:
        _statement = annual_df

    # Apply camel2title to metric column
    metrics = _statement["metric"].to_list()
    titled = camel2title(metrics)
    _statement = _statement.with_columns(_pl.Series("metric", titled))

    # Add level_detail column
    # level_detail list corresponds to the order
    n = min(len(level_detail), _statement.height)
    ld_col = level_detail[:n] + [None] * (_statement.height - n)
    _statement = _statement.with_columns(_pl.Series("level_detail", ld_col))

    # Sort columns descending (date columns), keep metric and level_detail first
    date_cols = [c for c in _statement.columns if c not in ("metric", "level_detail")]
    date_cols_sorted = sorted(date_cols, reverse=True)
    _statement = _statement.select(["metric", "level_detail"] + date_cols_sorted)

    # Drop rows where all date columns are null
    if date_cols_sorted:
        _statement = _statement.filter(
            ~_pl.all_horizontal([_pl.col(c).is_null() for c in date_cols_sorted])
        )

    return _statement


def format_quarterly_financial_statement(_statement, level_detail, order):
    """
    format_quarterly_financial_statements formats any quarterly financial statement

    Returns:
        - _statement: A fully formatted quarterly financial statement as a polars DataFrame.
    """
    # _statement is expected to be a polars DataFrame with a 'metric' column
    # Reorder rows according to order
    if "metric" in _statement.columns:
        order_df = _pl.DataFrame({"metric": order, "_order": list(range(len(order)))})
        _statement = (
            _statement.join(order_df, on="metric", how="left")
            .sort("_order")
            .drop("_order")
        )

    metrics = _statement["metric"].to_list()
    titled = camel2title(metrics)
    _statement = _statement.with_columns(_pl.Series("metric", titled))

    n = min(len(level_detail), _statement.height)
    ld_col = level_detail[:n] + [None] * (_statement.height - n)
    _statement = _statement.with_columns(_pl.Series("level_detail", ld_col))

    date_cols = [c for c in _statement.columns if c not in ("metric", "level_detail")]
    date_cols_sorted = sorted(date_cols, reverse=True)
    _statement = _statement.select(["metric", "level_detail"] + date_cols_sorted)

    if date_cols_sorted:
        _statement = _statement.filter(
            ~_pl.all_horizontal([_pl.col(c).is_null() for c in date_cols_sorted])
        )

    return _statement


def camel2title(
    strings: List[str], sep: str = " ", acronyms: Optional[List[str]] = None
) -> List[str]:
    if isinstance(strings, str) or not hasattr(strings, "__iter__"):
        raise TypeError("camel2title() 'strings' argument must be iterable of strings")
    if len(strings) == 0:
        return strings
    if not isinstance(strings[0], str):
        raise TypeError("camel2title() 'strings' argument must be iterable of strings")
    if not isinstance(sep, str) or len(sep) != 1:
        raise ValueError(
            f"camel2title() 'sep' argument = '{sep}' must be single character"
        )
    if _re.match("[a-zA-Z0-9]", sep):
        raise ValueError(
            f"camel2title() 'sep' argument = '{sep}' cannot be alpha-numeric"
        )
    if _re.escape(sep) != sep and sep not in {" ", "-"}:
        # Permit some exceptions, I don't understand why they get escaped
        raise ValueError(
            f"camel2title() 'sep' argument = '{sep}' cannot be special character"
        )

    if acronyms is None:
        pat = "([a-z])([A-Z])"
        rep = rf"\g<1>{sep}\g<2>"
        return [_re.sub(pat, rep, s).title() for s in strings]

    # Handling acronyms requires more care. Assumes Yahoo returns acronym strings upper-case
    if (
        isinstance(acronyms, str)
        or not hasattr(acronyms, "__iter__")
        or not isinstance(acronyms[0], str)
    ):
        raise TypeError("camel2title() 'acronyms' argument must be iterable of strings")
    for a in acronyms:
        if not _re.match("^[A-Z]+$", a):
            raise ValueError(
                f"camel2title() 'acronyms' argument must only contain upper-case, but '{a}' detected"
            )

    # Insert 'sep' between lower-then-upper-case
    pat = "([a-z])([A-Z])"
    rep = rf"\g<1>{sep}\g<2>"
    strings = [_re.sub(pat, rep, s) for s in strings]

    # Insert 'sep' after acronyms
    for a in acronyms:
        pat = f"({a})([A-Z][a-z])"
        rep = rf"\g<1>{sep}\g<2>"
        strings = [_re.sub(pat, rep, s) for s in strings]

    # Apply str.title() to non-acronym words
    strings = [s.split(sep) for s in strings]
    strings = [[j.title() if j not in acronyms else j for j in s] for s in strings]
    strings = [sep.join(s) for s in strings]

    return strings


def snake_case_2_camelCase(s):
    sc = s.split("_")[0] + "".join(x.title() for x in s.split("_")[1:])
    return sc


def _parse_user_dt(dt, exchange_tz=_tz.utc):
    """Parse user-provided datetime and return a timezone-aware datetime."""
    tz_name = exchange_tz if isinstance(exchange_tz, str) else str(exchange_tz)
    tz_obj = ZoneInfo(tz_name)

    if isinstance(dt, int):
        return datetime.fromtimestamp(dt, tz=tz_obj)
    else:
        if isinstance(dt, str):
            dt = _datetime.datetime.strptime(str(dt), "%Y-%m-%d")
        if isinstance(dt, _datetime.date) and not isinstance(dt, _datetime.datetime):
            dt = _datetime.datetime.combine(dt, _datetime.time(0))
        if isinstance(dt, _datetime.datetime):
            if dt.tzinfo is None:
                # Assume user is referring to exchange's timezone
                dt = dt.replace(tzinfo=tz_obj)
            else:
                dt = dt.astimezone(tz_obj)
        else:
            raise ValueError(f"Unable to parse input dt {dt} of type {type(dt)}")
    return dt


def _interval_to_timedelta(interval):
    if interval[-1] == "d":
        return relativedelta(days=int(interval[:-1]))
    elif interval[-2:] == "wk":
        return relativedelta(weeks=int(interval[:-2]))
    elif interval[-2:] == "mo":
        return relativedelta(months=int(interval[:-2]))
    elif interval[-1] == "y":
        return relativedelta(years=int(interval[:-1]))
    else:
        return _datetime.timedelta(
            seconds=_pl.Duration(interval).total_seconds()
            if False
            else _parse_interval_to_seconds(interval)
        )


def _parse_interval_to_seconds(interval):
    """Parse interval string like '1m', '5m', '1h' to seconds."""
    if interval.endswith("m"):
        return int(interval[:-1]) * 60
    elif interval.endswith("h"):
        return int(interval[:-1]) * 3600
    elif interval.endswith("s"):
        return int(interval[:-1])
    else:
        # Fall back: try converting to timedelta via relativedelta
        raise ValueError(f"Cannot parse interval: {interval}")


def is_valid_period_format(period):
    """Check if the provided period has a valid format."""
    if period is None:
        return False

    # Regex pattern to match valid period formats like '1d', '2wk', '3mo', '1y'
    valid_pattern = r"^[1-9]\d*(d|wk|mo|y)$"
    return bool(re.match(valid_pattern, period))


def auto_adjust(data: _pl.DataFrame) -> _pl.DataFrame:
    col_order = data.columns
    df = data.clone()
    ratio = (df["Adj Close"] / df["Close"]).to_numpy()
    df = df.with_columns(
        [
            (_pl.col("Open") * _pl.lit(_pl.Series(ratio))).alias("Open"),
            (_pl.col("High") * _pl.lit(_pl.Series(ratio))).alias("High"),
            (_pl.col("Low") * _pl.lit(_pl.Series(ratio))).alias("Low"),
            _pl.col("Adj Close").alias("Close"),
        ]
    )
    df = df.drop([c for c in ["Adj Close"] if c in df.columns])
    # Restore column order (excluding dropped cols)
    final_cols = [c for c in col_order if c in df.columns and c != "Adj Close"]
    df = df.select(final_cols)
    return df


def back_adjust(data: _pl.DataFrame) -> _pl.DataFrame:
    """back-adjusted data to mimic true historical prices"""
    col_order = data.columns
    df = data.clone()
    ratio = (df["Adj Close"] / df["Close"]).to_numpy()
    df = df.with_columns(
        [
            (_pl.col("Open") * _pl.lit(_pl.Series(ratio))).alias("Open"),
            (_pl.col("High") * _pl.lit(_pl.Series(ratio))).alias("High"),
            (_pl.col("Low") * _pl.lit(_pl.Series(ratio))).alias("Low"),
        ]
    )
    df = df.drop([c for c in ["Adj Close"] if c in df.columns])
    final_cols = [c for c in col_order if c in df.columns and c != "Adj Close"]
    df = df.select(final_cols)
    return df


def parse_quotes(data) -> _pl.DataFrame:
    timestamps = data["timestamp"]
    ohlc = data["indicators"]["quote"][0]
    opens = ohlc.get("open", [])
    highs = ohlc.get("high", [])
    lows = ohlc.get("low", [])
    closes = ohlc.get("close", [])
    volumes = ohlc.get("volume", [])
    adj_closes = data["indicators"].get("adjclose", [{}])[0].get("adjclose", [])

    # Convert Unix timestamps (seconds) to Polars Datetime(us, UTC)
    # 1 second = 1_000_000 microseconds
    ts_us = [t * 1_000_000 for t in timestamps]
    quotes = _pl.DataFrame(
        {
            "Datetime": _pl.Series(ts_us, dtype=_pl.Int64).cast(
                _pl.Datetime("us", "UTC")
            ),
            "Open": _pl.Series(
                opens if opens else [None] * len(timestamps), dtype=_pl.Float64
            ),
            "High": _pl.Series(
                highs if highs else [None] * len(timestamps), dtype=_pl.Float64
            ),
            "Low": _pl.Series(
                lows if lows else [None] * len(timestamps), dtype=_pl.Float64
            ),
            "Close": _pl.Series(
                closes if closes else [None] * len(timestamps), dtype=_pl.Float64
            ),
            "Volume": _pl.Series(
                volumes if volumes else [None] * len(timestamps), dtype=_pl.Float64
            ).cast(_pl.Int64),
        }
    )

    if adj_closes:
        quotes = quotes.with_columns(
            _pl.Series("Adj Close", adj_closes, dtype=_pl.Float64)
        )
    else:
        # Use Close as Adj Close when not present
        quotes = quotes.with_columns(_pl.col("Close").alias("Adj Close"))

    return quotes.sort("Datetime")


def parse_actions(data):
    dividends = None
    capital_gains = None
    splits = None

    if "events" in data:
        if "dividends" in data["events"] and len(data["events"]["dividends"]) > 0:
            div_list = list(data["events"]["dividends"].values())
            dates_us = [d["date"] * 1_000_000 for d in div_list]
            amounts = [d.get("amount", None) for d in div_list]
            dividends = _pl.DataFrame(
                {
                    "Date": _pl.Series(dates_us, dtype=_pl.Int64).cast(
                        _pl.Datetime("us", "UTC")
                    ),
                    "Dividends": _pl.Series(amounts, dtype=_pl.Float64),
                }
            )
            # Check for currency column
            if any("currency" in d for d in div_list):
                currencies = [d.get("currency", "") for d in div_list]
                if not all(c == "" for c in currencies):
                    dividends = dividends.with_columns(
                        _pl.Series("currency", currencies)
                    )
            dividends = dividends.sort("Date")

        if "capitalGains" in data["events"] and len(data["events"]["capitalGains"]) > 0:
            cg_list = list(data["events"]["capitalGains"].values())
            dates_us = [d["date"] * 1_000_000 for d in cg_list]
            amounts = [d.get("amount", None) for d in cg_list]
            capital_gains = _pl.DataFrame(
                {
                    "Date": _pl.Series(dates_us, dtype=_pl.Int64).cast(
                        _pl.Datetime("us", "UTC")
                    ),
                    "Capital Gains": _pl.Series(amounts, dtype=_pl.Float64),
                }
            )
            capital_gains = capital_gains.sort("Date")

        if "splits" in data["events"] and len(data["events"]["splits"]) > 0:
            sp_list = list(data["events"]["splits"].values())
            dates_us = [d["date"] * 1_000_000 for d in sp_list]
            split_ratios = [
                d.get("numerator", 1.0) / d.get("denominator", 1.0) for d in sp_list
            ]
            splits = _pl.DataFrame(
                {
                    "Date": _pl.Series(dates_us, dtype=_pl.Int64).cast(
                        _pl.Datetime("us", "UTC")
                    ),
                    "Stock Splits": _pl.Series(split_ratios, dtype=_pl.Float64),
                }
            )
            splits = splits.sort("Date")

    if dividends is None:
        dividends = _pl.DataFrame(
            {
                "Date": _pl.Series([], dtype=_pl.Datetime("us", "UTC")),
                "Dividends": _pl.Series([], dtype=_pl.Float64),
            }
        )
    if capital_gains is None:
        capital_gains = _pl.DataFrame(
            {
                "Date": _pl.Series([], dtype=_pl.Datetime("us", "UTC")),
                "Capital Gains": _pl.Series([], dtype=_pl.Float64),
            }
        )
    if splits is None:
        splits = _pl.DataFrame(
            {
                "Date": _pl.Series([], dtype=_pl.Datetime("us", "UTC")),
                "Stock Splits": _pl.Series([], dtype=_pl.Float64),
            }
        )

    return dividends, splits, capital_gains


def set_df_tz(
    df: _pl.DataFrame, interval: str, tz_exchange: str, date_col: str = "Datetime"
) -> _pl.DataFrame:
    if df.is_empty():
        return df
    dtype = df[date_col].dtype
    if isinstance(dtype, _pl.Datetime) and dtype.time_zone is None:
        df = df.with_columns(_pl.col(date_col).dt.replace_time_zone("UTC"))
    df = df.with_columns(_pl.col(date_col).dt.convert_time_zone(tz_exchange))
    return df


def fix_Yahoo_returning_prepost_unrequested(
    quotes: _pl.DataFrame, interval: str, tradingPeriods: _pl.DataFrame
) -> _pl.DataFrame:
    # Sometimes Yahoo returns post-market data despite not requesting it.
    # Normally happens on half-day early closes.
    # And sometimes returns pre-market data despite not requesting it.
    if quotes.is_empty():
        return quotes

    # tradingPeriods is a polars DataFrame with columns including 'start' and 'end'
    # and a 'Date' column (or index in pandas version — here it's a column)
    # Add _date column to quotes
    quotes = quotes.with_columns(_pl.col("Datetime").dt.date().alias("_date"))

    # Build tps_df from tradingPeriods
    # tradingPeriods may be a polars DataFrame with 'Date', 'start', 'end' columns
    if isinstance(tradingPeriods, _pl.DataFrame):
        tps_df = tradingPeriods.clone()
        if "Date" in tps_df.columns:
            tps_df = tps_df.with_columns(_pl.col("Date").dt.date().alias("_date"))
        elif "_date" not in tps_df.columns:
            # Assume first column is the date
            first_col = tps_df.columns[0]
            tps_df = tps_df.with_columns(_pl.col(first_col).dt.date().alias("_date"))
        tps_df = tps_df.select(
            [c for c in ["_date", "start", "end"] if c in tps_df.columns]
        )
    else:
        # Fallback: skip filtering
        quotes = quotes.drop(["_date"])
        return quotes

    # Left join quotes on _date
    quotes = quotes.join(tps_df, on="_date", how="left")

    # "end" = end of regular trading hours
    td = _interval_to_timedelta(interval)
    # Convert td to timedelta for comparison
    if isinstance(td, relativedelta):
        # Can't easily add relativedelta to a Datetime column in polars; approximate
        # For daily+ intervals, prepost filter is less critical
        # Just filter on >= end
        if "end" in quotes.columns:
            quotes = quotes.filter(_pl.col("Datetime") < _pl.col("end"))
    else:
        # intraday: td is a timedelta
        td_us = int(td.total_seconds() * 1_000_000)
        if "end" in quotes.columns and "start" in quotes.columns:
            # Normalize all datetimes to UTC for comparison
            dt_tz = quotes["Datetime"].dtype.time_zone
            start_tz = (
                quotes["start"].dtype.time_zone
                if isinstance(quotes["start"].dtype, _pl.Datetime)
                else None
            )
            end_tz = (
                quotes["end"].dtype.time_zone
                if isinstance(quotes["end"].dtype, _pl.Datetime)
                else None
            )

            # Convert Datetime to UTC if needed
            datetime_expr = _pl.col("Datetime")
            if dt_tz and dt_tz != "UTC":
                datetime_expr = _pl.col("Datetime").dt.convert_time_zone("UTC")

            # Convert start/end to UTC if needed
            start_expr = _pl.col("start")
            if start_tz and start_tz != "UTC":
                start_expr = _pl.col("start").dt.convert_time_zone("UTC")
            elif start_tz is None:
                start_expr = _pl.col("start").dt.replace_time_zone("UTC")

            end_expr = _pl.col("end")
            if end_tz and end_tz != "UTC":
                end_expr = _pl.col("end").dt.convert_time_zone("UTC")
            elif end_tz is None:
                end_expr = _pl.col("end").dt.replace_time_zone("UTC")

            quotes = quotes.with_columns(
                [
                    datetime_expr.alias("_dt_utc"),
                    start_expr.alias("_start_utc"),
                    end_expr.alias("_end_utc"),
                ]
            )
            quotes = quotes.filter(
                (_pl.col("_dt_utc") < _pl.col("_end_utc"))
                & (
                    (_pl.col("_dt_utc").cast(_pl.Int64) + td_us).cast(
                        _pl.Datetime("us", "UTC")
                    )
                    > _pl.col("_start_utc")
                )
            )
            quotes = quotes.drop(["_dt_utc", "_start_utc", "_end_utc"])

    # Drop helper columns
    drop_cols = [c for c in ["_date", "start", "end"] if c in quotes.columns]
    quotes = quotes.drop(drop_cols)
    return quotes


def _dts_in_same_interval(dt1, dt2, interval):
    # Check if second date dt2 in interval starting at dt1
    # dt1, dt2 are datetime objects

    if interval == "1d":
        last_rows_same_interval = dt1.date() == dt2.date()
    elif interval == "1wk":
        last_rows_same_interval = (dt2 - dt1).days < 7
    elif interval == "1mo":
        last_rows_same_interval = dt1.month == dt2.month
    elif interval == "3mo":
        shift = (dt1.month % 3) - 1
        q1 = (dt1.month - shift - 1) // 3 + 1
        q2 = (dt2.month - shift - 1) // 3 + 1
        year_diff = dt2.year - dt1.year
        quarter_diff = q2 - q1 + 4 * year_diff
        last_rows_same_interval = quarter_diff == 0
    else:
        diff = dt2 - dt1
        td = _interval_to_timedelta(interval)
        if isinstance(td, relativedelta):
            # Convert to approximate seconds
            last_rows_same_interval = diff.total_seconds() < 3600
        else:
            last_rows_same_interval = diff < td
    return last_rows_same_interval


def _pl_dt_to_datetime(val) -> datetime:
    """Convert a polars Datetime value to a Python datetime."""
    if isinstance(val, datetime):
        return val
    if hasattr(val, "to_pydatetime"):
        return val.to_pydatetime()
    # polars returns datetime directly from indexing
    return val


def fix_Yahoo_returning_live_separate(
    quotes: _pl.DataFrame,
    interval: str,
    tz_exchange: str,
    prepost: bool,
    repair: bool = False,
    currency: str = None,
):
    # Yahoo bug fix. If market is open today then Yahoo normally returns
    # todays data as a separate row from rest-of week/month interval in above row.
    # Fix = merge them together

    if interval[-1] not in ["m", "h"]:
        prepost = False

    dropped_row = None
    if quotes.height > 1:
        # Get last two datetimes, convert to exchange tz
        dt1_raw = quotes["Datetime"][-1]
        dt2_raw = quotes["Datetime"][-2]

        # Convert to Python datetime in exchange tz
        tz_obj = ZoneInfo(tz_exchange)
        if isinstance(dt1_raw, datetime):
            dt1 = dt1_raw.astimezone(tz_obj)
            dt2 = dt2_raw.astimezone(tz_obj)
        else:
            # polars Datetime -> Python datetime via timestamp
            dt1 = datetime.fromtimestamp(
                dt1_raw.timestamp(), tz=timezone.utc
            ).astimezone(tz_obj)
            dt2 = datetime.fromtimestamp(
                dt2_raw.timestamp(), tz=timezone.utc
            ).astimezone(tz_obj)

        if interval == "1d":
            # Similar bug in daily data except most data is simply duplicated
            if dt1.date() == dt2.date():
                # Last two rows are on same day. Drop second-to-last row
                dropped_row = quotes[-2]
                quotes = _pl.concat([quotes[:-2], quotes[-1:]])
        else:
            if _dts_in_same_interval(dt2, dt1, interval):
                # Last two rows are within same interval
                idx1_dt = quotes["Datetime"][-1]
                idx2_dt = quotes["Datetime"][-2]
                idx1_n = quotes.height - 1
                idx2_n = quotes.height - 2

                if idx1_dt == idx2_dt:
                    # Yahoo returning last interval duplicated
                    return quotes, None

                if prepost:
                    if dt1.second == 0:
                        return quotes, None

                # Stock splits product for last two rows
                ss_vals = quotes["Stock Splits"][-2:].to_list()
                ss_vals = [v if v != 0 else 1 for v in ss_vals]
                ss = ss_vals[0] * ss_vals[1]

                if repair:
                    if currency == "KWF":
                        currency_divide = 1000
                    else:
                        currency_divide = 100
                    if abs(ss / currency_divide - 1) > 0.25:
                        # Check price ratio
                        ratio_vals = {}
                        for c in const._PRICE_COLNAMES_:
                            if c in quotes.columns:
                                v1 = quotes[c][idx1_n]
                                v2 = quotes[c][idx2_n]
                                ratio_vals[c] = v1 / v2 if v2 else None

                        if ratio_vals:
                            ratios = [v for v in ratio_vals.values() if v is not None]
                            if ratios and all(
                                abs(r / currency_divide - 1) < 0.05 for r in ratios
                            ):
                                # newer prices are 100x
                                for c in const._PRICE_COLNAMES_:
                                    if c in quotes.columns:
                                        quotes = quotes.with_columns(
                                            _pl.when(
                                                _pl.int_range(0, quotes.height)
                                                == idx2_n
                                            )
                                            .then(_pl.col(c) * 100)
                                            .otherwise(_pl.col(c))
                                            .alias(c)
                                        )
                            elif ratios and all(
                                abs(r * currency_divide - 1) < 0.05 for r in ratios
                            ):
                                for c in const._PRICE_COLNAMES_:
                                    if c in quotes.columns:
                                        quotes = quotes.with_columns(
                                            _pl.when(
                                                _pl.int_range(0, quotes.height)
                                                == idx2_n
                                            )
                                            .then(_pl.col(c) * 0.01)
                                            .otherwise(_pl.col(c))
                                            .alias(c)
                                        )

                # Merge last row into second-to-last row
                open_val_last = quotes["Open"][-1]
                open_val_prev = quotes["Open"][-2]
                if open_val_prev is None or (
                    isinstance(open_val_prev, float) and _np.isnan(open_val_prev)
                ):
                    quotes = quotes.with_columns(
                        _pl.when(_pl.int_range(0, quotes.height) == idx2_n)
                        .then(open_val_last)
                        .otherwise(_pl.col("Open"))
                        .alias("Open")
                    )

                high_last = quotes["High"][-1]
                high_prev = quotes["High"][-2]
                if high_last is not None and not (
                    isinstance(high_last, float) and _np.isnan(high_last)
                ):
                    new_high = (
                        _np.nanmax([high_last, high_prev])
                        if high_prev is not None
                        else high_last
                    )
                    quotes = quotes.with_columns(
                        _pl.when(_pl.int_range(0, quotes.height) == idx2_n)
                        .then(float(new_high))
                        .otherwise(_pl.col("High"))
                        .alias("High")
                    )
                    if "Adj High" in quotes.columns:
                        adj_high_last = quotes["Adj High"][-1]
                        adj_high_prev = quotes["Adj High"][-2]
                        new_adj_high = (
                            _np.nanmax([adj_high_last, adj_high_prev])
                            if adj_high_prev is not None
                            else adj_high_last
                        )
                        quotes = quotes.with_columns(
                            _pl.when(_pl.int_range(0, quotes.height) == idx2_n)
                            .then(float(new_adj_high))
                            .otherwise(_pl.col("Adj High"))
                            .alias("Adj High")
                        )

                low_last = quotes["Low"][-1]
                low_prev = quotes["Low"][-2]
                if low_last is not None and not (
                    isinstance(low_last, float) and _np.isnan(low_last)
                ):
                    new_low = (
                        _np.nanmin([low_last, low_prev])
                        if low_prev is not None
                        else low_last
                    )
                    quotes = quotes.with_columns(
                        _pl.when(_pl.int_range(0, quotes.height) == idx2_n)
                        .then(float(new_low))
                        .otherwise(_pl.col("Low"))
                        .alias("Low")
                    )
                    if "Adj Low" in quotes.columns:
                        adj_low_last = quotes["Adj Low"][-1]
                        adj_low_prev = quotes["Adj Low"][-2]
                        new_adj_low = (
                            _np.nanmin([adj_low_last, adj_low_prev])
                            if adj_low_prev is not None
                            else adj_low_last
                        )
                        quotes = quotes.with_columns(
                            _pl.when(_pl.int_range(0, quotes.height) == idx2_n)
                            .then(float(new_adj_low))
                            .otherwise(_pl.col("Adj Low"))
                            .alias("Adj Low")
                        )

                close_last = quotes["Close"][-1]
                quotes = quotes.with_columns(
                    _pl.when(_pl.int_range(0, quotes.height) == idx2_n)
                    .then(close_last)
                    .otherwise(_pl.col("Close"))
                    .alias("Close")
                )
                if "Adj Close" in quotes.columns:
                    adj_close_last = quotes["Adj Close"][-1]
                    quotes = quotes.with_columns(
                        _pl.when(_pl.int_range(0, quotes.height) == idx2_n)
                        .then(adj_close_last)
                        .otherwise(_pl.col("Adj Close"))
                        .alias("Adj Close")
                    )

                vol_last = quotes["Volume"][-1]
                vol_prev = quotes["Volume"][-2]
                new_vol = (vol_prev or 0) + (vol_last or 0)
                quotes = quotes.with_columns(
                    _pl.when(_pl.int_range(0, quotes.height) == idx2_n)
                    .then(new_vol)
                    .otherwise(_pl.col("Volume"))
                    .alias("Volume")
                )

                div_last = quotes["Dividends"][-1]
                div_prev = quotes["Dividends"][-2]
                new_div = (div_prev or 0.0) + (div_last or 0.0)
                quotes = quotes.with_columns(
                    _pl.when(_pl.int_range(0, quotes.height) == idx2_n)
                    .then(new_div)
                    .otherwise(_pl.col("Dividends"))
                    .alias("Dividends")
                )

                if ss != 1.0:
                    quotes = quotes.with_columns(
                        _pl.when(_pl.int_range(0, quotes.height) == idx2_n)
                        .then(ss)
                        .otherwise(_pl.col("Stock Splits"))
                        .alias("Stock Splits")
                    )

                dropped_row = quotes[-1]
                quotes = quotes[:-1]

    return quotes, dropped_row


def safe_merge_dfs(
    df_main: _pl.DataFrame, df_sub: _pl.DataFrame, interval: str
) -> _pl.DataFrame:
    if df_main.is_empty():
        return df_main

    data_cols = [c for c in df_sub.columns if c not in df_main.columns and c != "Date"]
    data_col = data_cols[0]

    df_main = df_main.sort("Datetime")
    intraday = interval.endswith("m") or interval.endswith("s")

    td = _interval_to_timedelta(interval)

    # Helper to get date from Datetime for intraday
    def _get_dates_arr(df, col="Datetime"):
        return _np.array(
            [d.date() if hasattr(d, "date") else d for d in df[col].to_list()]
        )

    def _get_dts_arr(df, col="Datetime"):
        return _np.array(df[col].to_list())

    if intraday:
        main_dates = _get_dates_arr(df_main)
        sub_dates = _get_dates_arr(df_sub, "Date")

        # td for intraday should be a timedelta
        if isinstance(td, relativedelta):
            td_days = 1
            last_date_plus_td = main_dates[-1] + _datetime.timedelta(days=td_days)
        else:
            last_date_plus_td = main_dates[-1] + td

        indices = _np.searchsorted(
            _np.append(main_dates, [last_date_plus_td]), sub_dates, side="left"
        )
    else:
        main_dts = _get_dts_arr(df_main)
        sub_dts = _get_dts_arr(df_sub, "Date")

        if isinstance(td, relativedelta):
            # Convert to approximate timedelta
            last_dt = df_main["Datetime"][-1]
            if isinstance(last_dt, datetime):
                last_plus_td = last_dt + td
            else:
                last_plus_td = last_dt + _datetime.timedelta(days=1)
            append_arr = _np.append(main_dts, [last_plus_td])
        else:
            last_dt = df_main["Datetime"][-1]
            if isinstance(last_dt, datetime):
                last_plus_td = last_dt + td
            else:
                last_plus_td = last_dt + td
            append_arr = _np.append(main_dts, [last_plus_td])

        indices = _np.searchsorted(append_arr, sub_dts, side="right")
        indices -= 1  # Convert from [[i-1], [i]) to [[i], [i+1])

    # Handle out-of-range
    if intraday:
        main_dates_arr = _get_dates_arr(df_main)
        sub_dates_arr = _get_dates_arr(df_sub, "Date")
        for i in range(len(sub_dates_arr)):
            dt = sub_dates_arr[i]
            last_date = main_dates_arr[-1]
            if isinstance(td, relativedelta):
                last_plus = last_date + _datetime.timedelta(days=1)
            else:
                last_plus = last_date + td
            if dt < main_dates_arr[0] or dt >= last_plus:
                indices[i] = -1
    else:
        main_dts_arr = _get_dts_arr(df_main)
        sub_dts_arr = _get_dts_arr(df_sub, "Date")
        for i in range(len(sub_dts_arr)):
            dt = sub_dts_arr[i]
            first_dt = main_dts_arr[0]
            last_dt = main_dts_arr[-1]
            if isinstance(td, relativedelta):
                last_plus = last_dt + td
            else:
                last_plus = last_dt + td
            if dt < first_dt or dt >= last_plus:
                indices[i] = -1

    f_outOfRange = indices == -1
    if f_outOfRange.any():
        if intraday:
            # Discard out-of-range dividends in intraday data
            keep_mask = ~f_outOfRange
            sub_dates_list = df_sub["Date"].to_list()
            df_sub = df_sub.filter(_pl.Series(keep_mask.tolist()))
            if df_sub.is_empty():
                df_main = df_main.with_columns(_pl.lit(0.0).alias("Dividends"))
                return df_main

            # Recalc indices
            main_dates = _get_dates_arr(df_main)
            sub_dates = _get_dates_arr(df_sub, "Date")
            if isinstance(td, relativedelta):
                last_date_plus_td = main_dates[-1] + _datetime.timedelta(days=1)
            else:
                last_date_plus_td = main_dates[-1] + td
            indices = _np.searchsorted(
                _np.append(main_dates, [last_date_plus_td]), sub_dates, side="left"
            )
        else:
            empty_row_data = {**{c: None for c in const._PRICE_COLNAMES_}, "Volume": 0}
            if interval == "1d":
                # Add all out-of-range event dates
                sub_dates_list = df_sub["Date"].to_list()
                for i in _np.where(f_outOfRange)[0]:
                    dt = sub_dates_list[i]
                    get_yf_logger().debug(
                        f"Adding out-of-range {data_col} @ {dt} in new prices row of NaNs"
                    )
                    row_data = {"Datetime": dt}
                    for c in df_main.columns:
                        if c == "Datetime":
                            continue
                        if c in const._PRICE_COLNAMES_:
                            row_data[c] = None
                        elif c == "Volume":
                            row_data[c] = 0
                        else:
                            row_data[c] = None
                    empty_row = _pl.DataFrame(
                        {k: [v] for k, v in row_data.items()}, schema=df_main.schema
                    )
                    df_main = _pl.concat([df_main, empty_row])
            else:
                last_dt = df_main["Datetime"][-1]
                if isinstance(last_dt, datetime):
                    next_interval_start_dt = last_dt + td
                    if isinstance(td, relativedelta):
                        next_interval_end_dt = next_interval_start_dt + td
                    else:
                        next_interval_end_dt = next_interval_start_dt + td
                else:
                    next_interval_start_dt = last_dt
                    next_interval_end_dt = last_dt

                sub_dates_list = df_sub["Date"].to_list()
                for i in _np.where(f_outOfRange)[0]:
                    dt = sub_dates_list[i]
                    if next_interval_start_dt <= dt < next_interval_end_dt:
                        get_yf_logger().debug(
                            f"Adding out-of-range {data_col} @ {dt} in new prices row of NaNs"
                        )
                        row_data = {"Datetime": dt}
                        for c in df_main.columns:
                            if c == "Datetime":
                                continue
                            if c in const._PRICE_COLNAMES_:
                                row_data[c] = None
                            elif c == "Volume":
                                row_data[c] = 0
                            else:
                                row_data[c] = None
                        empty_row = _pl.DataFrame(
                            {k: [v] for k, v in row_data.items()}, schema=df_main.schema
                        )
                        df_main = _pl.concat([df_main, empty_row])

            df_main = df_main.sort("Datetime")

            # Re-calculate indices
            main_dts = _get_dts_arr(df_main)
            sub_dts = _get_dts_arr(df_sub, "Date")
            if isinstance(td, relativedelta):
                last_dt = df_main["Datetime"][-1]
                last_plus = last_dt + td if isinstance(last_dt, datetime) else last_dt
            else:
                last_dt = df_main["Datetime"][-1]
                last_plus = last_dt + td if isinstance(last_dt, datetime) else last_dt
            indices = _np.searchsorted(
                _np.append(main_dts, [last_plus]), sub_dts, side="right"
            )
            indices -= 1
            for i in range(len(sub_dts)):
                dt = sub_dts[i]
                if dt < main_dts[0] or dt >= last_plus:
                    indices[i] = -1

    f_outOfRange = indices == -1
    if f_outOfRange.any():
        if intraday or interval in ["1d", "1wk"]:
            out_dts = df_sub["Date"].filter(_pl.Series(f_outOfRange.tolist()))
            raise YFException(
                f"The following '{data_col}' events are out-of-range, did not expect with interval {interval}: {out_dts}"
            )
        get_yf_logger().debug(
            f"Discarding these {data_col} events:\n"
            + str(df_sub.filter(_pl.Series(f_outOfRange.tolist())))
        )
        keep_mask = ~f_outOfRange
        df_sub = df_sub.filter(_pl.Series(keep_mask.tolist()))
        indices = indices[keep_mask]

    def _reindex_events(df, new_index_dts, data_col_name):
        """Map df_sub rows to df_main Datetime values."""
        if len(new_index_dts) == len(set(new_index_dts)):
            # No duplicates
            df = df.with_columns(_pl.Series("Datetime", new_index_dts))
            return df

        df = df.with_columns(_pl.Series("Datetime", new_index_dts))
        if data_col_name in ["Dividends", "Capital Gains"]:
            df = df.group_by("Datetime").agg(_pl.col(data_col_name).sum())
        elif data_col_name == "Stock Splits":
            df = df.group_by("Datetime").agg(_pl.col(data_col_name).product())
        else:
            raise YFException(
                f"New index contains duplicates but unsure how to aggregate for '{data_col_name}'"
            )
        return df

    main_dts_list = df_main["Datetime"].to_list()
    new_index_dts = [main_dts_list[i] for i in indices]
    df_sub = _reindex_events(df_sub, new_index_dts, data_col)

    # Keep only Datetime and data_col in df_sub for join
    join_cols = ["Datetime", data_col] if data_col in df_sub.columns else df_sub.columns
    df_sub_join = df_sub.select([c for c in join_cols if c in df_sub.columns])

    df = df_main.join(df_sub_join, on="Datetime", how="left")
    f_na = df[data_col].is_null()
    data_lost = (~f_na).sum() < df_sub.height
    if data_lost:
        raise YFException("Data was lost in merge, investigate")

    return df


def fix_Yahoo_dst_issue(df: _pl.DataFrame, interval: str) -> _pl.DataFrame:
    if interval in ["1d", "1w", "1wk"]:
        # These intervals should start at time 00:00. But for some combinations of date and timezone,
        # Yahoo has time off by few hours (e.g. Brazil 23:00 around Jan-2022). Suspect DST problem.
        # The clue is (a) minutes=0 and (b) hour near 0.
        minutes = df["Datetime"].dt.minute()
        hours = df["Datetime"].dt.hour()
        f_pre_midnight = (minutes == 0) & (hours.is_in([22, 23]))
        f_pre_midnight_list = f_pre_midnight.to_list()
        hours_list = hours.to_list()

        dst_error_hours = _np.array([0] * df.height)
        for i, flag in enumerate(f_pre_midnight_list):
            if flag:
                dst_error_hours[i] = 24 - hours_list[i]

        # Add hours as microseconds to Datetime column
        dst_error_us = (dst_error_hours * 3_600 * 1_000_000).astype(_np.int64)
        df = df.with_columns(
            (_pl.col("Datetime").cast(_pl.Int64) + _pl.Series(dst_error_us.tolist()))
            .cast(_pl.Datetime("us", "UTC"))
            .alias("Datetime")
        )
    return df


def is_valid_timezone(tz: str) -> bool:
    try:
        _tz.timezone(tz)
    except UnknownTimeZoneError:
        return False
    return True


def format_history_metadata(md, tradingPeriodsOnly=True):
    if not isinstance(md, dict):
        return md
    if len(md) == 0:
        return md

    tz = md["exchangeTimezoneName"]

    if not tradingPeriodsOnly:
        for k in ["firstTradeDate", "regularMarketTime"]:
            if k in md and md[k] is not None:
                if isinstance(md[k], int):
                    md[k] = datetime.fromtimestamp(md[k], tz=timezone.utc).astimezone(
                        ZoneInfo(tz)
                    )

        if "currentTradingPeriod" in md:
            for m in ["regular", "pre", "post"]:
                if m in md["currentTradingPeriod"] and isinstance(
                    md["currentTradingPeriod"][m]["start"], int
                ):
                    for t in ["start", "end"]:
                        md["currentTradingPeriod"][m][t] = datetime.fromtimestamp(
                            md["currentTradingPeriod"][m][t], tz=timezone.utc
                        ).astimezone(ZoneInfo(tz))
                    del md["currentTradingPeriod"][m]["gmtoffset"]
                    del md["currentTradingPeriod"][m]["timezone"]

    if "tradingPeriods" in md:
        tps = md["tradingPeriods"]
        if tps == {"pre": [], "post": []}:
            # Ignore
            pass
        elif isinstance(tps, (list, dict)):
            tz_zi = ZoneInfo(tz)
            if isinstance(tps, list):
                # Only regular times
                flat = [
                    item
                    for sublist in tps
                    for item in (sublist if isinstance(sublist, list) else [sublist])
                ]
                rows = []
                for rec in flat:
                    rows.append(
                        {
                            "start": datetime.fromtimestamp(
                                rec["start"], tz=timezone.utc
                            ).astimezone(tz_zi),
                            "end": datetime.fromtimestamp(
                                rec["end"], tz=timezone.utc
                            ).astimezone(tz_zi),
                        }
                    )
                df = _pl.DataFrame(rows)
                df = df.with_columns(_pl.col("start").dt.date().alias("Date"))
            elif isinstance(tps, dict):
                # Includes pre- and post-market
                def _flatten(lst):
                    return [
                        item
                        for sublist in lst
                        for item in (
                            sublist if isinstance(sublist, list) else [sublist]
                        )
                    ]

                pre_rows = _flatten(tps.get("pre", []))
                post_rows = _flatten(tps.get("post", []))
                regular_rows = _flatten(tps.get("regular", []))

                rows = []
                for i, rec in enumerate(regular_rows):
                    row = {
                        "start": datetime.fromtimestamp(
                            rec["start"], tz=timezone.utc
                        ).astimezone(tz_zi),
                        "end": datetime.fromtimestamp(
                            rec["end"], tz=timezone.utc
                        ).astimezone(tz_zi),
                    }
                    if i < len(pre_rows):
                        row["pre_start"] = datetime.fromtimestamp(
                            pre_rows[i]["start"], tz=timezone.utc
                        ).astimezone(tz_zi)
                        row["pre_end"] = datetime.fromtimestamp(
                            pre_rows[i]["end"], tz=timezone.utc
                        ).astimezone(tz_zi)
                    if i < len(post_rows):
                        row["post_start"] = datetime.fromtimestamp(
                            post_rows[i]["start"], tz=timezone.utc
                        ).astimezone(tz_zi)
                        row["post_end"] = datetime.fromtimestamp(
                            post_rows[i]["end"], tz=timezone.utc
                        ).astimezone(tz_zi)
                    rows.append(row)
                df = _pl.DataFrame(rows)
                df = df.with_columns(_pl.col("start").dt.date().alias("Date"))

            md["tradingPeriods"] = df

    return md


class ProgressBar:
    def __init__(self, iterations, text="completed"):
        self.text = text
        self.iterations = iterations
        self.prog_bar = "[]"
        self.fill_char = "*"
        self.width = 50
        self.__update_amount(0)
        self.elapsed = 1

    def completed(self):
        if self.elapsed > self.iterations:
            self.elapsed = self.iterations
        self.update_iteration(1)
        print("\r" + str(self), end="", file=_sys.stderr)
        _sys.stderr.flush()
        print("", file=_sys.stderr)

    def animate(self, iteration=None):
        if iteration is None:
            self.elapsed += 1
            iteration = self.elapsed
        else:
            self.elapsed += iteration

        print("\r" + str(self), end="", file=_sys.stderr)
        _sys.stderr.flush()
        self.update_iteration()

    def update_iteration(self, val=None):
        val = val if val is not None else self.elapsed / float(self.iterations)
        self.__update_amount(val * 100.0)
        self.prog_bar += f"  {self.elapsed} of {self.iterations} {self.text}"

    def __update_amount(self, new_amount):
        percent_done = int(round((new_amount / 100.0) * 100.0))
        all_full = self.width - 2
        num_hashes = int(round((percent_done / 100.0) * all_full))
        self.prog_bar = (
            "[" + self.fill_char * num_hashes + " " * (all_full - num_hashes) + "]"
        )
        pct_place = (len(self.prog_bar) // 2) - len(str(percent_done))
        pct_string = f"{percent_done}%"
        self.prog_bar = self.prog_bar[0:pct_place] + (
            pct_string + self.prog_bar[pct_place + len(pct_string) :]
        )

    def __str__(self):
        return str(self.prog_bar)


def dynamic_docstring(placeholders: dict):
    """
    A decorator to dynamically update the docstring of a function or method.

    Args:
        placeholders (dict): A dictionary where keys are placeholder names and values are the strings to insert.
    """

    def decorator(func):
        if func.__doc__:
            docstring = func.__doc__
            # Replace each placeholder with its corresponding value
            for key, value in placeholders.items():
                docstring = docstring.replace(f"{{{key}}}", value)
            func.__doc__ = docstring
        return func

    return decorator


def _generate_table_configurations(title=None) -> str:
    import textwrap

    if title is None:
        title = "Permitted Keys/Values"
    table = textwrap.dedent(f"""
    .. list-table:: {title}
       :widths: 25 75
       :header-rows: 1

       * - Key
         - Values
    """)

    return table


def generate_list_table_from_dict(
    data: dict, bullets: bool = True, title: str = None
) -> str:
    """
    Generate a list-table for the docstring showing permitted keys/values.
    """
    table = _generate_table_configurations(title)
    for k in sorted(data.keys()):
        values = data[k]
        table += " " * 3 + f"* - {k}\n"
        lengths = [len(str(v)) for v in values]
        if bullets and max(lengths) > 5:
            table += " " * 5 + "-\n"
            for value in sorted(values):
                table += " " * 7 + f"- {value}\n"
        else:
            value_str = ", ".join(sorted(values))
            table += " " * 5 + f"- {value_str}\n"
    return table


def generate_list_table_from_dict_universal(
    data: dict, bullets: bool = True, title: str = None, concat_keys=[]
) -> str:
    """
    Generate a list-table for the docstring showing permitted keys/values.
    """
    table = _generate_table_configurations(title)
    for k in data.keys():
        values = data[k]

        table += " " * 3 + f"* - {k}\n"
        if isinstance(values, dict):
            table_add = ""

            concat_short_lines = k in concat_keys

            if bullets:
                k_keys = sorted(list(values.keys()))
                current_line = ""
                block_format = "query" in k_keys
                for i in range(len(k_keys)):
                    k2 = k_keys[i]
                    k2_values = values[k2]
                    k2_values_str = None
                    if isinstance(k2_values, set):
                        k2_values = list(k2_values)
                    elif isinstance(k2_values, dict) and len(k2_values) == 0:
                        k2_values = []
                    if isinstance(k2_values, list):
                        k2_values = sorted(k2_values)
                        all_scalar = all(
                            isinstance(k2v, (int, float, str)) for k2v in k2_values
                        )
                        if all_scalar:
                            k2_values_str = _re.sub(r"[{}\[\]']", "", str(k2_values))

                    if k2_values_str is None:
                        k2_values_str = str(k2_values)

                    if len(current_line) > 0 and (
                        len(current_line) + len(k2_values_str) > 40
                    ):
                        # new line
                        table_add += current_line + "\n"
                        current_line = ""

                    if concat_short_lines:
                        if current_line == "":
                            current_line += " " * 5
                            if i == 0:
                                # Only add dash to first
                                current_line += "- "
                            else:
                                current_line += "  "
                            # Don't draw bullet points:
                            current_line += "| "
                        else:
                            current_line += ".  "
                        current_line += f"{k2}: " + k2_values_str
                    else:
                        table_add += " " * 5
                        if i == 0:
                            # Only add dash to first
                            table_add += "- "
                        else:
                            table_add += "  "

                        if "\n" in k2_values_str:
                            # Block format multiple lines
                            table_add += "| " + f"{k2}: " + "\n"
                            k2_values_str_lines = k2_values_str.split("\n")
                            for j in range(len(k2_values_str_lines)):
                                line = k2_values_str_lines[j]
                                table_add += " " * 7 + "|" + " " * 5 + line
                                if j < len(k2_values_str_lines) - 1:
                                    table_add += "\n"
                        else:
                            if block_format:
                                table_add += "| "
                            else:
                                table_add += "* "
                            table_add += f"{k2}: " + k2_values_str

                        table_add += "\n"
                if current_line != "":
                    table_add += current_line + "\n"
            else:
                table_add += " " * 5 + f"- {values}\n"

            table += table_add

        else:
            lengths = [len(str(v)) for v in values]
            if bullets and max(lengths) > 5:
                table += " " * 5 + "-\n"
                for value in sorted(values):
                    table += " " * 7 + f"- {value}\n"
            else:
                value_str = ", ".join(sorted(values))
                table += " " * 5 + f"- {value_str}\n"

    return table

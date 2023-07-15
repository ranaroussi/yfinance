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
import dateutil as _dateutil
from typing import Dict, Union, List, Optional

import pytz as _tz
import requests as _requests
import re as _re
import pandas as _pd
import numpy as _np
import sys as _sys
import os as _os
import appdirs as _ad
import sqlite3 as _sqlite3
import atexit as _atexit
from functools import lru_cache
import logging

from threading import Lock

from pytz import UnknownTimeZoneError

try:
    import ujson as _json
except ImportError:
    import json as _json

user_agent_headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


# From https://stackoverflow.com/a/59128615
from types import FunctionType
from inspect import getmembers
def attributes(obj):
    disallowed_names = {
      name for name, value in getmembers(type(obj)) 
        if isinstance(value, FunctionType)}
    return {
      name: getattr(obj, name) for name in dir(obj) 
        if name[0] != '_' and name not in disallowed_names and hasattr(obj, name)}


@lru_cache(maxsize=20)
def print_once(msg):
    # 'warnings' module suppression of repeat messages does not work. 
    # This function replicates correct behaviour
    print(msg)


## Logging
# Note: most of this logic is adding indentation with function depth,
#       so that DEBUG log is readable.
class IndentLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        if get_yf_logger().isEnabledFor(logging.DEBUG):
            i = ' ' * self.extra['indent']
            if not isinstance(msg, str):
                msg = str(msg)
            msg = '\n'.join([i + m for m in msg.split('\n')])
        return msg, kwargs

import threading
_indentation_level = threading.local()
class IndentationContext:
    def __init__(self, increment=1):
        self.increment = increment
    def __enter__(self):
        _indentation_level.indent = getattr(_indentation_level, 'indent', 0) + self.increment
    def __exit__(self, exc_type, exc_val, exc_tb):
        _indentation_level.indent -= self.increment

def get_indented_logger(name=None):
    # Never cache the returned value! Will break indentation.
    return IndentLoggerAdapter(logging.getLogger(name), {'indent': getattr(_indentation_level, 'indent', 0)})

def log_indent_decorator(func):
    def wrapper(*args, **kwargs):
        logger = get_indented_logger('yfinance')
        logger.debug(f'Entering {func.__name__}()')

        with IndentationContext():
            result = func(*args, **kwargs)

        logger.debug(f'Exiting {func.__name__}()')
        return result

    return wrapper

class MultiLineFormatter(logging.Formatter):
    # The 'fmt' formatting further down is only applied to first line
    # of log message, specifically the padding after %level%.
    # For multi-line messages, need to manually copy over padding.
    def __init__(self, fmt):
        super().__init__(fmt)
        # Extract amount of padding
        match = _re.search(r'%\(levelname\)-(\d+)s', fmt)
        self.level_length = int(match.group(1)) if match else 0

    def format(self, record):
        original = super().format(record)
        lines = original.split('\n')
        levelname = lines[0].split(' ')[0]
        if len(lines) <= 1:
            return original
        else:
            # Apply padding to all lines below first
            formatted = [lines[0]]
            if self.level_length == 0:
                padding = ' ' * len(levelname)
            else:
                padding = ' ' * self.level_length
            padding += ' '  # +1 for space between level and message
            formatted.extend(padding + line for line in lines[1:])
            return '\n'.join(formatted)

yf_logger = None
yf_log_indented = False
def get_yf_logger():
    global yf_logger
    if yf_logger is None:
        yf_logger = logging.getLogger('yfinance')
    global yf_log_indented
    if yf_log_indented:
        yf_logger = get_indented_logger('yfinance')
    return yf_logger

def setup_debug_formatting():
    global yf_logger
    yf_logger = get_yf_logger()

    if not yf_logger.isEnabledFor(logging.DEBUG):
        yf_logger.warning("logging mode not set to 'DEBUG', so not setting up debug formatting")
        return

    if yf_logger.handlers is None or len(yf_logger.handlers) == 0:
        h = logging.StreamHandler()
        # Ensure different level strings don't interfere with indentation
        formatter = MultiLineFormatter(fmt='%(levelname)-8s %(message)s')
        h.setFormatter(formatter)
        yf_logger.addHandler(h)

    global yf_log_indented
    yf_log_indented = True

def enable_debug_mode():
    get_yf_logger().setLevel(logging.DEBUG)
    setup_debug_formatting()
    
##


def is_isin(string):
    return bool(_re.match("^([A-Z]{2})([A-Z0-9]{9})([0-9]{1})$", string))


def get_all_by_isin(isin, proxy=None, session=None):
    if not (is_isin(isin)):
        raise ValueError("Invalid ISIN number")

    from .base import _BASE_URL_
    session = session or _requests
    url = "{}/v1/finance/search?q={}".format(_BASE_URL_, isin)
    data = session.get(url=url, proxies=proxy, headers=user_agent_headers)
    try:
        data = data.json()
        ticker = data.get('quotes', [{}])[0]
        return {
            'ticker': {
                'symbol': ticker['symbol'],
                'shortname': ticker['shortname'],
                'longname': ticker['longname'],
                'type': ticker['quoteType'],
                'exchange': ticker['exchDisp'],
            },
            'news': data.get('news', [])
        }
    except Exception:
        return {}


def get_ticker_by_isin(isin, proxy=None, session=None):
    data = get_all_by_isin(isin, proxy, session)
    return data.get('ticker', {}).get('symbol', '')


def get_info_by_isin(isin, proxy=None, session=None):
    data = get_all_by_isin(isin, proxy, session)
    return data.get('ticker', {})


def get_news_by_isin(isin, proxy=None, session=None):
    data = get_all_by_isin(isin, proxy, session)
    return data.get('news', {})


def empty_df(index=None):
    if index is None:
        index = []
    empty = _pd.DataFrame(index=index, data={
        'Open': _np.nan, 'High': _np.nan, 'Low': _np.nan,
        'Close': _np.nan, 'Adj Close': _np.nan, 'Volume': _np.nan})
    empty.index.name = 'Date'
    return empty


def empty_earnings_dates_df():
    empty = _pd.DataFrame(
        columns=["Symbol", "Company", "Earnings Date",
                 "EPS Estimate", "Reported EPS", "Surprise(%)"])
    return empty


def build_template(data):
    '''
    build_template returns the details required to rebuild any of the yahoo finance financial statements in the same order as the yahoo finance webpage. The function is built to be used on the "FinancialTemplateStore" json which appears in any one of the three yahoo finance webpages: "/financials", "/cash-flow" and "/balance-sheet".

    Returns:
        - template_annual_order: The order that annual figures should be listed in.
        - template_ttm_order: The order that TTM (Trailing Twelve Month) figures should be listed in.
        - template_order: The order that quarterlies should be in (note that quarterlies have no pre-fix - hence why this is required).
        - level_detail: The level of each individual line item. E.g. for the "/financials" webpage, "Total Revenue" is a level 0 item and is the summation of "Operating Revenue" and "Excise Taxes" which are level 1 items.

    '''
    template_ttm_order = []  # Save the TTM (Trailing Twelve Months) ordering to an object.
    template_annual_order = []  # Save the annual ordering to an object.
    template_order = []  # Save the ordering to an object (this can be utilized for quarterlies)
    level_detail = []  # Record the level of each line item of the income statement ("Operating Revenue" and "Excise Taxes" sum to return "Total Revenue" we need to keep track of this)
    for key in data['template']:
        # Loop through the json to retreive the exact financial order whilst appending to the objects
        template_ttm_order.append('trailing{}'.format(key['key']))
        template_annual_order.append('annual{}'.format(key['key']))
        template_order.append('{}'.format(key['key']))
        level_detail.append(0)
        if 'children' in key:
            for child1 in key['children']:  # Level 1
                template_ttm_order.append('trailing{}'.format(child1['key']))
                template_annual_order.append('annual{}'.format(child1['key']))
                template_order.append('{}'.format(child1['key']))
                level_detail.append(1)
                if 'children' in child1:
                    for child2 in child1['children']:  # Level 2
                        template_ttm_order.append('trailing{}'.format(child2['key']))
                        template_annual_order.append('annual{}'.format(child2['key']))
                        template_order.append('{}'.format(child2['key']))
                        level_detail.append(2)
                        if 'children' in child2:
                            for child3 in child2['children']:  # Level 3
                                template_ttm_order.append('trailing{}'.format(child3['key']))
                                template_annual_order.append('annual{}'.format(child3['key']))
                                template_order.append('{}'.format(child3['key']))
                                level_detail.append(3)
                                if 'children' in child3:
                                    for child4 in child3['children']:  # Level 4
                                        template_ttm_order.append('trailing{}'.format(child4['key']))
                                        template_annual_order.append('annual{}'.format(child4['key']))
                                        template_order.append('{}'.format(child4['key']))
                                        level_detail.append(4)
                                        if 'children' in child4:
                                            for child5 in child4['children']:  # Level 5
                                                template_ttm_order.append('trailing{}'.format(child5['key']))
                                                template_annual_order.append('annual{}'.format(child5['key']))
                                                template_order.append('{}'.format(child5['key']))
                                                level_detail.append(5)
    return template_ttm_order, template_annual_order, template_order, level_detail


def retreive_financial_details(data):
    '''
    retreive_financial_details returns all of the available financial details under the "QuoteTimeSeriesStore" for any of the following three yahoo finance webpages: "/financials", "/cash-flow" and "/balance-sheet".

    Returns:
        - TTM_dicts: A dictionary full of all of the available Trailing Twelve Month figures, this can easily be converted to a pandas dataframe.
        - Annual_dicts: A dictionary full of all of the available Annual figures, this can easily be converted to a pandas dataframe.
    '''
    TTM_dicts = []  # Save a dictionary object to store the TTM financials.
    Annual_dicts = []  # Save a dictionary object to store the Annual financials.
    for key in data['timeSeries']:  # Loop through the time series data to grab the key financial figures.
        try:
            if len(data['timeSeries'][key]) > 0:
                time_series_dict = {}
                time_series_dict['index'] = key
                for each in data['timeSeries'][key]:  # Loop through the years
                    if each == None:
                        continue
                    else:
                        time_series_dict[each['asOfDate']] = each['reportedValue']
                    # time_series_dict["{}".format(each['asOfDate'])] = data['timeSeries'][key][each]['reportedValue']
                if 'trailing' in key:
                    TTM_dicts.append(time_series_dict)
                elif 'annual' in key:
                    Annual_dicts.append(time_series_dict)
        except Exception as e:
            pass
    return TTM_dicts, Annual_dicts


def format_annual_financial_statement(level_detail, annual_dicts, annual_order, ttm_dicts=None, ttm_order=None):
    '''
    format_annual_financial_statement formats any annual financial statement

    Returns:
        - _statement: A fully formatted annual financial statement in pandas dataframe.
    '''
    Annual = _pd.DataFrame.from_dict(annual_dicts).set_index("index")
    Annual = Annual.reindex(annual_order)
    Annual.index = Annual.index.str.replace(r'annual', '')

    # Note: balance sheet is the only financial statement with no ttm detail
    if (ttm_dicts not in [[], None]) and (ttm_order not in [[], None]):
        TTM = _pd.DataFrame.from_dict(ttm_dicts).set_index("index")
        TTM = TTM.reindex(ttm_order)
        # Add 'TTM' prefix to all column names, so if combined we can tell
        # the difference between actuals and TTM (similar to yahoo finance).
        TTM.columns = ['TTM ' + str(col) for col in TTM.columns]
        TTM.index = TTM.index.str.replace(r'trailing', '')
        _statement = Annual.merge(TTM, left_index=True, right_index=True)
    else:
        _statement = Annual

    _statement.index = camel2title(_statement.T.index)
    _statement['level_detail'] = level_detail
    _statement = _statement.set_index([_statement.index, 'level_detail'])
    _statement = _statement[sorted(_statement.columns, reverse=True)]
    _statement = _statement.dropna(how='all')
    return _statement


def format_quarterly_financial_statement(_statement, level_detail, order):
    '''
    format_quarterly_financial_statements formats any quarterly financial statement

    Returns:
        - _statement: A fully formatted quarterly financial statement in pandas dataframe.
    '''
    _statement = _statement.reindex(order)
    _statement.index = camel2title(_statement.T)
    _statement['level_detail'] = level_detail
    _statement = _statement.set_index([_statement.index, 'level_detail'])
    _statement = _statement[sorted(_statement.columns, reverse=True)]
    _statement = _statement.dropna(how='all')
    _statement.columns = _pd.to_datetime(_statement.columns).date
    return _statement


def camel2title(strings: List[str], sep: str = ' ', acronyms: Optional[List[str]] = None) -> List[str]:
    if isinstance(strings, str) or not hasattr(strings, '__iter__'):
        raise TypeError("camel2title() 'strings' argument must be iterable of strings")
    if len(strings) == 0:
        return strings
    if not isinstance(strings[0], str):
        raise TypeError("camel2title() 'strings' argument must be iterable of strings")
    if not isinstance(sep, str) or len(sep) != 1:
        raise ValueError(f"camel2title() 'sep' argument = '{sep}' must be single character")
    if _re.match("[a-zA-Z0-9]", sep):
        raise ValueError(f"camel2title() 'sep' argument = '{sep}' cannot be alpha-numeric")
    if _re.escape(sep) != sep and sep not in {' ', '-'}:
        # Permit some exceptions, I don't understand why they get escaped
        raise ValueError(f"camel2title() 'sep' argument = '{sep}' cannot be special character")

    if acronyms is None:
        pat = "([a-z])([A-Z])"
        rep = rf"\g<1>{sep}\g<2>"
        return [_re.sub(pat, rep, s).title() for s in strings]

    # Handling acronyms requires more care. Assumes Yahoo returns acronym strings upper-case
    if isinstance(acronyms, str) or not hasattr(acronyms, '__iter__') or not isinstance(acronyms[0], str):
        raise TypeError("camel2title() 'acronyms' argument must be iterable of strings")
    for a in acronyms:
        if not _re.match("^[A-Z]+$", a):
            raise ValueError(f"camel2title() 'acronyms' argument must only contain upper-case, but '{a}' detected")

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
    strings = [[j.title() if not j in acronyms else j for j in s] for s in strings]
    strings = [sep.join(s) for s in strings]

    return strings


def snake_case_2_camelCase(s):
    sc = s.split('_')[0] + ''.join(x.title() for x in s.split('_')[1:])
    return sc


def _parse_user_dt(dt, exchange_tz):
    if isinstance(dt, int):
        # Should already be epoch, test with conversion:
        _datetime.datetime.fromtimestamp(dt)
    else:
        # Convert str/date -> datetime, set tzinfo=exchange, get timestamp:
        if isinstance(dt, str):
            dt = _datetime.datetime.strptime(str(dt), '%Y-%m-%d')
        if isinstance(dt, _datetime.date) and not isinstance(dt, _datetime.datetime):
            dt = _datetime.datetime.combine(dt, _datetime.time(0))
        if isinstance(dt, _datetime.datetime) and dt.tzinfo is None:
            # Assume user is referring to exchange's timezone
            dt = _tz.timezone(exchange_tz).localize(dt)
        dt = int(dt.timestamp())
    return dt


def _interval_to_timedelta(interval):
    if interval == "1mo":
        return _dateutil.relativedelta.relativedelta(months=1)
    elif interval == "3mo":
        return _dateutil.relativedelta.relativedelta(months=3)
    elif interval == "1y":
        return _dateutil.relativedelta.relativedelta(years=1)
    elif interval == "1wk":
        return _pd.Timedelta(days=7)
    else: 
        return _pd.Timedelta(interval)


def auto_adjust(data):
    col_order = data.columns
    df = data.copy()
    ratio = (df["Adj Close"] / df["Close"]).to_numpy()
    df["Adj Open"] = df["Open"] * ratio
    df["Adj High"] = df["High"] * ratio
    df["Adj Low"] = df["Low"] * ratio

    df.drop(
        ["Open", "High", "Low", "Close"],
        axis=1, inplace=True)

    df.rename(columns={
        "Adj Open": "Open", "Adj High": "High",
        "Adj Low": "Low", "Adj Close": "Close"
    }, inplace=True)

    return df[[c for c in col_order if c in df.columns]]


def back_adjust(data):
    """ back-adjusted data to mimic true historical prices """

    col_order = data.columns
    df = data.copy()
    ratio = df["Adj Close"] / df["Close"]
    df["Adj Open"] = df["Open"] * ratio
    df["Adj High"] = df["High"] * ratio
    df["Adj Low"] = df["Low"] * ratio

    df.drop(
        ["Open", "High", "Low", "Adj Close"],
        axis=1, inplace=True)

    df.rename(columns={
        "Adj Open": "Open", "Adj High": "High",
        "Adj Low": "Low"
    }, inplace=True)

    return df[[c for c in col_order if c in df.columns]]


def parse_quotes(data):
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

    quotes = _pd.DataFrame({"Open": opens,
                            "High": highs,
                            "Low": lows,
                            "Close": closes,
                            "Adj Close": adjclose,
                            "Volume": volumes})

    quotes.index = _pd.to_datetime(timestamps, unit="s")
    quotes.sort_index(inplace=True)

    return quotes


def parse_actions(data):
    dividends = None
    capital_gains = None
    splits = None

    if "events" in data:
        if "dividends" in data["events"]:
            dividends = _pd.DataFrame(
                data=list(data["events"]["dividends"].values()))
            dividends.set_index("date", inplace=True)
            dividends.index = _pd.to_datetime(dividends.index, unit="s")
            dividends.sort_index(inplace=True)
            dividends.columns = ["Dividends"]

        if "capitalGains" in data["events"]:
            capital_gains = _pd.DataFrame(
                data=list(data["events"]["capitalGains"].values()))
            capital_gains.set_index("date", inplace=True)
            capital_gains.index = _pd.to_datetime(capital_gains.index, unit="s")
            capital_gains.sort_index(inplace=True)
            capital_gains.columns = ["Capital Gains"]

        if "splits" in data["events"]:
            splits = _pd.DataFrame(
                data=list(data["events"]["splits"].values()))
            splits.set_index("date", inplace=True)
            splits.index = _pd.to_datetime(splits.index, unit="s")
            splits.sort_index(inplace=True)
            splits["Stock Splits"] = splits["numerator"] / \
                                     splits["denominator"]
            splits = splits[["Stock Splits"]]

    if dividends is None:
        dividends = _pd.DataFrame(
            columns=["Dividends"], index=_pd.DatetimeIndex([]))
    if capital_gains is None:
        capital_gains = _pd.DataFrame(
            columns=["Capital Gains"], index=_pd.DatetimeIndex([]))
    if splits is None:
        splits = _pd.DataFrame(
            columns=["Stock Splits"], index=_pd.DatetimeIndex([]))

    return dividends, splits, capital_gains


def set_df_tz(df, interval, tz):
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    df.index = df.index.tz_convert(tz)
    return df


def fix_Yahoo_returning_prepost_unrequested(quotes, interval, tradingPeriods):
    # Sometimes Yahoo returns post-market data despite not requesting it.
    # Normally happens on half-day early closes.
    #
    # And sometimes returns pre-market data despite not requesting it.
    # E.g. some London tickers.
    tps_df = tradingPeriods.copy()
    tps_df["_date"] = tps_df.index.date
    quotes["_date"] = quotes.index.date
    idx = quotes.index.copy()
    quotes = quotes.merge(tps_df, how="left")
    quotes.index = idx
    # "end" = end of regular trading hours (including any auction)
    f_drop = quotes.index >= quotes["end"]
    f_drop = f_drop | (quotes.index < quotes["start"])
    if f_drop.any():
        # When printing report, ignore rows that were already NaNs:
        # f_na = quotes[["Open","Close"]].isna().all(axis=1)
        # n_nna = quotes.shape[0] - _np.sum(f_na)
        # n_drop_nna = _np.sum(f_drop & ~f_na)
        # quotes_dropped = quotes[f_drop]
        # if debug and n_drop_nna > 0:
        #     print(f"Dropping {n_drop_nna}/{n_nna} intervals for falling outside regular trading hours")
        quotes = quotes[~f_drop]
    quotes = quotes.drop(["_date", "start", "end"], axis=1)
    return quotes


def fix_Yahoo_returning_live_separate(quotes, interval, tz_exchange):
    # Yahoo bug fix. If market is open today then Yahoo normally returns 
    # todays data as a separate row from rest-of week/month interval in above row. 
    # Seems to depend on what exchange e.g. crypto OK.
    # Fix = merge them together
    n = quotes.shape[0]
    if n > 1:
        dt1 = quotes.index[n - 1]
        dt2 = quotes.index[n - 2]
        if quotes.index.tz is None:
            dt1 = dt1.tz_localize("UTC")
            dt2 = dt2.tz_localize("UTC")
        dt1 = dt1.tz_convert(tz_exchange)
        dt2 = dt2.tz_convert(tz_exchange)
        if interval == "1d":
            # Similar bug in daily data except most data is simply duplicated
            # - exception is volume, *slightly* greater on final row (and matches website)
            if dt1.date() == dt2.date():
                # Last two rows are on same day. Drop second-to-last row
                quotes = quotes.drop(quotes.index[n - 2])
        else:
            if interval == "1wk":
                last_rows_same_interval = dt1.year == dt2.year and dt1.week == dt2.week
            elif interval == "1mo":
                last_rows_same_interval = dt1.month == dt2.month
            elif interval == "3mo":
                last_rows_same_interval = dt1.year == dt2.year and dt1.quarter == dt2.quarter
            else:
                last_rows_same_interval = (dt1-dt2) < _pd.Timedelta(interval)

            if last_rows_same_interval:
                # Last two rows are within same interval
                idx1 = quotes.index[n - 1]
                idx2 = quotes.index[n - 2]
                if idx1 == idx2:
                    # Yahoo returning last interval duplicated, which means
                    # Yahoo is not returning live data (phew!)
                    return quotes
                if _np.isnan(quotes.loc[idx2, "Open"]):
                    quotes.loc[idx2, "Open"] = quotes["Open"][n - 1]
                # Note: nanmax() & nanmin() ignores NaNs, but still need to check not all are NaN to avoid warnings
                if not _np.isnan(quotes["High"][n - 1]):
                    quotes.loc[idx2, "High"] = _np.nanmax([quotes["High"][n - 1], quotes["High"][n - 2]])
                    if "Adj High" in quotes.columns:
                        quotes.loc[idx2, "Adj High"] = _np.nanmax([quotes["Adj High"][n - 1], quotes["Adj High"][n - 2]])

                if not _np.isnan(quotes["Low"][n - 1]):
                    quotes.loc[idx2, "Low"] = _np.nanmin([quotes["Low"][n - 1], quotes["Low"][n - 2]])
                    if "Adj Low" in quotes.columns:
                        quotes.loc[idx2, "Adj Low"] = _np.nanmin([quotes["Adj Low"][n - 1], quotes["Adj Low"][n - 2]])

                quotes.loc[idx2, "Close"] = quotes["Close"][n - 1]
                if "Adj Close" in quotes.columns:
                    quotes.loc[idx2, "Adj Close"] = quotes["Adj Close"][n - 1]
                quotes.loc[idx2, "Volume"] += quotes["Volume"][n - 1]
                quotes = quotes.drop(quotes.index[n - 1])

    return quotes


def safe_merge_dfs(df_main, df_sub, interval):
    if df_sub.shape[0] == 0:
        raise Exception("No data to merge")

    df_sub_backup = df_sub.copy()
    data_cols = [c for c in df_sub.columns if c not in df_main]
    if len(data_cols) > 1:
        raise Exception("Expected 1 data col")
    data_col = data_cols[0]

    df_main = df_main.sort_index()
    intraday = interval.endswith('m') or interval.endswith('s')

    td = _interval_to_timedelta(interval)
    if intraday:
        # On some exchanges the event can occur before market open.
        # Problem when combining with intraday data.
        # Solution = use dates, not datetimes, to map/merge.
        df_main['_date'] = df_main.index.date
        df_sub['_date'] = df_sub.index.date
        indices = _np.searchsorted(_np.append(df_main['_date'], [df_main['_date'].iloc[-1]+td]), df_sub['_date'], side='left')
        df_main = df_main.drop('_date', axis=1)
        df_sub = df_sub.drop('_date', axis=1)
    else:
        indices = _np.searchsorted(_np.append(df_main.index, df_main.index[-1]+td), df_sub.index, side='right')
        indices -= 1  # Convert from [[i-1], [i]) to [[i], [i+1])
        # Numpy.searchsorted does not handle out-of-range well, so handle manually:
        for i in range(len(df_sub.index)):
            dt = df_sub.index[i]
            if dt < df_main.index[0] or dt >= df_main.index[-1]+td:
                # Out-of-range
                indices[i] = -1

    f_outOfRange = indices == -1
    if f_outOfRange.any() and not intraday:
        # If dividend is occuring in next interval after last price row, 
        # add a new row of NaNs
        last_dt = df_main.index[-1]
        next_interval_start_dt = last_dt + td
        if interval == '1d':
            # Allow for weekends & holidays
            next_interval_end_dt = last_dt+7*_pd.Timedelta(days=7)
        else:
            next_interval_end_dt = next_interval_start_dt + td
        for i in _np.where(f_outOfRange)[0]:
            dt = df_sub.index[i]
            if dt >= next_interval_start_dt and dt < next_interval_end_dt:
                new_dt = dt if interval == '1d' else next_interval_start_dt
                get_yf_logger().debug(f"Adding out-of-range {data_col} @ {dt.date()} in new prices row of NaNs")
                df_main.loc[new_dt] = _np.nan

        # Re-calculate indices
        indices = _np.searchsorted(_np.append(df_main.index, df_main.index[-1]+td), df_sub.index, side='right')
        indices -= 1  # Convert from [[i-1], [i]) to [[i], [i+1])
        # Numpy.searchsorted does not handle out-of-range well, so handle manually:
        for i in range(len(df_sub.index)):
            dt = df_sub.index[i]
            if dt < df_main.index[0] or dt >= df_main.index[-1]+td:
                # Out-of-range
                indices[i] = -1

    f_outOfRange = indices == -1
    if f_outOfRange.any():
        if intraday or interval in ['1d', '1wk']:
            raise Exception(f"The following '{data_col}' events are out-of-range, did not expect with interval {interval}: {df_sub.index}")
        get_yf_logger().debug(f'Discarding these {data_col} events:' + '\n' + str(df_sub[f_outOfRange]))
        df_sub = df_sub[~f_outOfRange].copy()
        indices = indices[~f_outOfRange]

    def _reindex_events(df, new_index, data_col_name):
        if len(new_index) == len(set(new_index)):
            # No duplicates, easy
            df.index = new_index
            return df

        df["_NewIndex"] = new_index
        # Duplicates present within periods but can aggregate
        if data_col_name in ["Dividends", "Capital Gains"]:
            # Add
            df = df.groupby("_NewIndex").sum()
            df.index.name = None
        elif data_col_name == "Stock Splits":
            # Product
            df = df.groupby("_NewIndex").prod()
            df.index.name = None
        else:
            raise Exception("New index contains duplicates but unsure how to aggregate for '{}'".format(data_col_name))
        if "_NewIndex" in df.columns:
            df = df.drop("_NewIndex", axis=1)
        return df
    new_index = df_main.index[indices]
    df_sub = _reindex_events(df_sub, new_index, data_col)

    df = df_main.join(df_sub)
    f_na = df[data_col].isna()
    data_lost = sum(~f_na) < df_sub.shape[0]
    if data_lost:
        raise Exception('Data was lost in merge, investigate')

    return df


def fix_Yahoo_dst_issue(df, interval):
    if interval in ["1d", "1w", "1wk"]:
        # These intervals should start at time 00:00. But for some combinations of date and timezone, 
        # Yahoo has time off by few hours (e.g. Brazil 23:00 around Jan-2022). Suspect DST problem.
        # The clue is (a) minutes=0 and (b) hour near 0. 
        # Obviously Yahoo meant 00:00, so ensure this doesn't affect date conversion:
        f_pre_midnight = (df.index.minute == 0) & (df.index.hour.isin([22, 23]))
        dst_error_hours = _np.array([0] * df.shape[0])
        dst_error_hours[f_pre_midnight] = 24 - df.index[f_pre_midnight].hour
        df.index += _pd.TimedeltaIndex(dst_error_hours, 'h')
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
                    md[k] = _pd.to_datetime(md[k], unit='s', utc=True).tz_convert(tz)

        if "currentTradingPeriod" in md:
            for m in ["regular", "pre", "post"]:
                if m in md["currentTradingPeriod"] and isinstance(md["currentTradingPeriod"][m]["start"], int):
                    for t in ["start", "end"]:
                        md["currentTradingPeriod"][m][t] = \
                            _pd.to_datetime(md["currentTradingPeriod"][m][t], unit='s', utc=True).tz_convert(tz)
                    del md["currentTradingPeriod"][m]["gmtoffset"]
                    del md["currentTradingPeriod"][m]["timezone"]

    if "tradingPeriods" in md:
        tps = md["tradingPeriods"]
        if tps == {"pre":[], "post":[]}:
            # Ignore
            pass
        elif isinstance(tps, (list, dict)):
            if isinstance(tps, list):
                # Only regular times
                df = _pd.DataFrame.from_records(_np.hstack(tps))
                df = df.drop(["timezone", "gmtoffset"], axis=1)
                df["start"] = _pd.to_datetime(df["start"], unit='s', utc=True).dt.tz_convert(tz)
                df["end"] = _pd.to_datetime(df["end"], unit='s', utc=True).dt.tz_convert(tz)
            elif isinstance(tps, dict):
                # Includes pre- and post-market
                pre_df = _pd.DataFrame.from_records(_np.hstack(tps["pre"]))
                post_df = _pd.DataFrame.from_records(_np.hstack(tps["post"]))
                regular_df = _pd.DataFrame.from_records(_np.hstack(tps["regular"]))

                pre_df = pre_df.rename(columns={"start":"pre_start", "end":"pre_end"}).drop(["timezone", "gmtoffset"], axis=1)
                post_df = post_df.rename(columns={"start":"post_start", "end":"post_end"}).drop(["timezone", "gmtoffset"], axis=1)
                regular_df = regular_df.drop(["timezone", "gmtoffset"], axis=1)

                cols = ["pre_start", "pre_end", "start", "end", "post_start", "post_end"]
                df = regular_df.join(pre_df).join(post_df)
                for c in cols:
                    df[c] = _pd.to_datetime(df[c], unit='s', utc=True).dt.tz_convert(tz)
                df = df[cols]

            df.index = _pd.to_datetime(df["start"].dt.date)
            df.index = df.index.tz_localize(tz)
            df.index.name = "Date"

            md["tradingPeriods"] = df

    return md

class ProgressBar:
    def __init__(self, iterations, text='completed'):
        self.text = text
        self.iterations = iterations
        self.prog_bar = '[]'
        self.fill_char = '*'
        self.width = 50
        self.__update_amount(0)
        self.elapsed = 1

    def completed(self):
        if self.elapsed > self.iterations:
            self.elapsed = self.iterations
        self.update_iteration(1)
        print('\r' + str(self), end='')
        _sys.stdout.flush()
        print()

    def animate(self, iteration=None):
        if iteration is None:
            self.elapsed += 1
            iteration = self.elapsed
        else:
            self.elapsed += iteration

        print('\r' + str(self), end='')
        _sys.stdout.flush()
        self.update_iteration()

    def update_iteration(self, val=None):
        val = val if val is not None else self.elapsed / float(self.iterations)
        self.__update_amount(val * 100.0)
        self.prog_bar += '  %s of %s %s' % (
            self.elapsed, self.iterations, self.text)

    def __update_amount(self, new_amount):
        percent_done = int(round((new_amount / 100.0) * 100.0))
        all_full = self.width - 2
        num_hashes = int(round((percent_done / 100.0) * all_full))
        self.prog_bar = '[' + self.fill_char * \
                        num_hashes + ' ' * (all_full - num_hashes) + ']'
        pct_place = (len(self.prog_bar) // 2) - len(str(percent_done))
        pct_string = '%d%%' % percent_done
        self.prog_bar = self.prog_bar[0:pct_place] + \
                        (pct_string + self.prog_bar[pct_place + len(pct_string):])

    def __str__(self):
        return str(self.prog_bar)


# ---------------------------------
# TimeZone cache related code
# ---------------------------------

class _KVStore:
    """Simpel Sqlite backed key/value store, key and value are strings. Should be thread safe."""

    def __init__(self, filename):
        self._cache_mutex = Lock()
        with self._cache_mutex:
            self.conn = _sqlite3.connect(filename, timeout=10, check_same_thread=False)
            self.conn.execute('pragma journal_mode=wal')
            try:
                self.conn.execute('create table if not exists "kv" (key TEXT primary key, value TEXT) without rowid')
            except Exception as e:
                if 'near "without": syntax error' in str(e):
                    # "without rowid" requires sqlite 3.8.2. Older versions will raise exception
                    self.conn.execute('create table if not exists "kv" (key TEXT primary key, value TEXT)')
                else:
                    raise
            self.conn.commit()
        _atexit.register(self.close)

    def close(self):
        if self.conn is not None:
            with self._cache_mutex:
                self.conn.close()
                self.conn = None

    def get(self, key: str) -> Union[str, None]:
        """Get value for key if it exists else returns None"""
        try:
            item = self.conn.execute('select value from "kv" where key=?', (key,))
        except _sqlite3.IntegrityError as e:
            self.delete(key)
            return None
        if item:
            return next(item, (None,))[0]

    def set(self, key: str, value: str) -> None:
        if value is None:
            self.delete(key)
        else:
            with self._cache_mutex:
                self.conn.execute('replace into "kv" (key, value) values (?,?)', (key, value))
                self.conn.commit()

    def bulk_set(self, kvdata: Dict[str, str]):
        records = tuple(i for i in kvdata.items())
        with self._cache_mutex:
            self.conn.executemany('replace into "kv" (key, value) values (?,?)', records)
            self.conn.commit()

    def delete(self, key: str):
        with self._cache_mutex:
            self.conn.execute('delete from "kv" where key=?', (key,))
            self.conn.commit()


class _TzCacheException(Exception):
    pass


class _TzCache:
    """Simple sqlite file cache of ticker->timezone"""

    def __init__(self):
        self._setup_cache_folder()
        # Must init db here, where is thread-safe
        try:
            self._tz_db = _KVStore(_os.path.join(self._db_dir, "tkr-tz.db"))
        except _sqlite3.DatabaseError as err:
            raise _TzCacheException("Error creating TzCache folder: '{}' reason: {}"
                                    .format(self._db_dir, err))
        self._migrate_cache_tkr_tz()

    def _setup_cache_folder(self):
        if not _os.path.isdir(self._db_dir):
            try:
                _os.makedirs(self._db_dir)
            except OSError as err:
                raise _TzCacheException("Error creating TzCache folder: '{}' reason: {}"
                                        .format(self._db_dir, err))

        elif not (_os.access(self._db_dir, _os.R_OK) and _os.access(self._db_dir, _os.W_OK)):
            raise _TzCacheException("Cannot read and write in TzCache folder: '{}'"
                                    .format(self._db_dir, ))

    def lookup(self, tkr):
        return self.tz_db.get(tkr)

    def store(self, tkr, tz):
        if tz is None:
            self.tz_db.delete(tkr)
        elif self.tz_db.get(tkr) is not None:
            raise Exception("Tkr {} tz already in cache".format(tkr))
        else:
            self.tz_db.set(tkr, tz)

    @property
    def _db_dir(self):
        global _cache_dir
        return _os.path.join(_cache_dir, "py-yfinance")

    @property
    def tz_db(self):
        return self._tz_db

    def _migrate_cache_tkr_tz(self):
        """Migrate contents from old ticker CSV-cache to SQLite db"""
        old_cache_file_path = _os.path.join(self._db_dir, "tkr-tz.csv")

        if not _os.path.isfile(old_cache_file_path):
            return None
        try:
            df = _pd.read_csv(old_cache_file_path, index_col="Ticker", on_bad_lines="skip")
        except _pd.errors.EmptyDataError:
            _os.remove(old_cache_file_path)
        except TypeError:
            _os.remove(old_cache_file_path)
        else:
            # Discard corrupt data:
            df = df[~df["Tz"].isna().to_numpy()]
            df = df[~(df["Tz"]=='').to_numpy()]
            df = df[~df.index.isna()]
            if not df.empty:
                try:
                    self.tz_db.bulk_set(df.to_dict()['Tz'])
                except Exception as e:
                    # Ignore
                    pass

            _os.remove(old_cache_file_path)


class _TzCacheDummy:
    """Dummy cache to use if tz cache is disabled"""

    def lookup(self, tkr):
        return None

    def store(self, tkr, tz):
        pass

    @property
    def tz_db(self):
        return None


def get_tz_cache():
    """
    Get the timezone cache, initializes it and creates cache folder if needed on first call.
    If folder cannot be created for some reason it will fall back to initialize a
    dummy cache with same interface as real cash.
    """
    # as this can be called from multiple threads, protect it.
    with _cache_init_lock:
        global _tz_cache
        if _tz_cache is None:
            try:
                _tz_cache = _TzCache()
            except _TzCacheException as err:
                get_yf_logger().info("Failed to create TzCache, reason: %s. "
                                 "TzCache will not be used. "
                                 "Tip: You can direct cache to use a different location with 'set_tz_cache_location(mylocation)'",
                                 err)
                _tz_cache = _TzCacheDummy()

        return _tz_cache


_cache_dir = _ad.user_cache_dir()
_cache_init_lock = Lock()
_tz_cache = None


def set_tz_cache_location(cache_dir: str):
    """
    Sets the path to create the "py-yfinance" cache folder in.
    Useful if the default folder returned by "appdir.user_cache_dir()" is not writable.
    Must be called before cache is used (that is, before fetching tickers).
    :param cache_dir: Path to use for caches
    :return: None
    """
    global _cache_dir, _tz_cache
    assert _tz_cache is None, "Time Zone cache already initialized, setting path must be done before cache is created"
    _cache_dir = cache_dir

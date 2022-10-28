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
from typing import Dict, Union

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

from threading import Lock

from pytz import UnknownTimeZoneError

try:
    import ujson as _json
except ImportError:
    import json as _json


user_agent_headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}


def is_isin(string):
    return bool(_re.match("^([A-Z]{2})([A-Z0-9]{9})([0-9]{1})$", string))


def get_all_by_isin(isin, proxy=None, session=None):
    if not(is_isin(isin)):
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


def get_html(url, proxy=None, session=None):
    session = session or _requests
    html = session.get(url=url, proxies=proxy, headers=user_agent_headers).text
    return html


def get_json(url, proxy=None, session=None):
    session = session or _requests
    html = session.get(url=url, proxies=proxy, headers=user_agent_headers).text

    if "QuoteSummaryStore" not in html:
        html = session.get(url=url, proxies=proxy).text
        if "QuoteSummaryStore" not in html:
            return {}

    json_str = html.split('root.App.main =')[1].split(
        '(this)')[0].split(';\n}')[0].strip()
    data = _json.loads(json_str)[
        'context']['dispatcher']['stores']['QuoteSummaryStore']
    # add data about Shares Outstanding for companies' tickers if they are available
    try:
        data['annualBasicAverageShares'] = _json.loads(
            json_str)['context']['dispatcher']['stores'][
                'QuoteTimeSeriesStore']['timeSeries']['annualBasicAverageShares']
    except Exception:
        pass

    # return data
    new_data = _json.dumps(data).replace('{}', 'null')
    new_data = _re.sub(
        r'\{[\'|\"]raw[\'|\"]:(.*?),(.*?)\}', r'\1', new_data)

    return _json.loads(new_data)


def camel2title(o):
    return [_re.sub("([a-z])([A-Z])", r"\g<1> \g<2>", i).title() for i in o]


def _parse_user_dt(dt, exchange_tz):
    if isinstance(dt, int):
        ## Should already be epoch, test with conversion:
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


def auto_adjust(data):
    df = data.copy()
    ratio = df["Close"] / df["Adj Close"]
    df["Adj Open"] = df["Open"] / ratio
    df["Adj High"] = df["High"] / ratio
    df["Adj Low"] = df["Low"] / ratio

    df.drop(
        ["Open", "High", "Low", "Close"],
        axis=1, inplace=True)

    df.rename(columns={
        "Adj Open": "Open", "Adj High": "High",
        "Adj Low": "Low", "Adj Close": "Close"
    }, inplace=True)

    df = df[["Open", "High", "Low", "Close", "Volume"]]
    return df[["Open", "High", "Low", "Close", "Volume"]]


def back_adjust(data):
    """ back-adjusted data to mimic true historical prices """

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

    return df[["Open", "High", "Low", "Close", "Volume"]]


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
    dividends = _pd.DataFrame(
        columns=["Dividends"], index=_pd.DatetimeIndex([]))
    splits = _pd.DataFrame(
        columns=["Stock Splits"], index=_pd.DatetimeIndex([]))

    if "events" in data:
        if "dividends" in data["events"]:
            dividends = _pd.DataFrame(
                data=list(data["events"]["dividends"].values()))
            dividends.set_index("date", inplace=True)
            dividends.index = _pd.to_datetime(dividends.index, unit="s")
            dividends.sort_index(inplace=True)

            dividends.columns = ["Dividends"]

        if "splits" in data["events"]:
            splits = _pd.DataFrame(
                data=list(data["events"]["splits"].values()))
            splits.set_index("date", inplace=True)
            splits.index = _pd.to_datetime(splits.index, unit="s")
            splits.sort_index(inplace=True)
            splits["Stock Splits"] = splits["numerator"] / \
                splits["denominator"]
            splits = splits[["Stock Splits"]]

    return dividends, splits


def set_df_tz(df, interval, tz):
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    df.index = df.index.tz_convert(tz)
    if interval in ["1d","1w","1wk","1mo","3mo"]:
        # If localizing a midnight during DST transition hour when clocks roll back, 
        # meaning clock hits midnight twice, then use the 2nd (ambiguous=True)
        df.index = _pd.to_datetime(df.index.date).tz_localize(tz, ambiguous=True)
    return df


def fix_Yahoo_returning_live_separate(quotes, interval, tz_exchange):
    # Yahoo bug fix. If market is open today then Yahoo normally returns 
    # todays data as a separate row from rest-of week/month interval in above row. 
    # Seems to depend on what exchange e.g. crypto OK.
    # Fix = merge them together
    n = quotes.shape[0]
    if n > 1:
        dt1 = quotes.index[n-1]
        dt2 = quotes.index[n-2]
        if quotes.index.tz is None:
            dt1 = dt1.tz_localize("UTC")
            dt2 = dt2.tz_localize("UTC")
        dt1 = dt1.tz_convert(tz_exchange)
        dt2 = dt2.tz_convert(tz_exchange)
        if interval in ["1wk", "1mo", "3mo"]:
            if interval == "1wk":
                last_rows_same_interval = dt1.year==dt2.year and dt1.week==dt2.week
            elif interval == "1mo":
                last_rows_same_interval = dt1.month==dt2.month
            elif interval == "3mo":
                last_rows_same_interval = dt1.year==dt2.year and dt1.quarter==dt2.quarter
            if last_rows_same_interval:
                # Last two rows are within same interval
                idx1 = quotes.index[n-1]
                idx2 = quotes.index[n-2]
                if _np.isnan(quotes.loc[idx2,"Open"]):
                    quotes.loc[idx2,"Open"] = quotes["Open"][n-1]
                # Note: nanmax() & nanmin() ignores NaNs
                quotes.loc[idx2,"High"] = _np.nanmax([quotes["High"][n-1], quotes["High"][n-2]])
                quotes.loc[idx2,"Low"] = _np.nanmin([quotes["Low"][n-1], quotes["Low"][n-2]])
                quotes.loc[idx2,"Close"] = quotes["Close"][n-1]
                if "Adj High" in quotes.columns:
                    quotes.loc[idx2,"Adj High"] = _np.nanmax([quotes["Adj High"][n-1], quotes["Adj High"][n-2]])
                if "Adj Low" in quotes.columns:
                    quotes.loc[idx2,"Adj Low"] = _np.nanmin([quotes["Adj Low"][n-1], quotes["Adj Low"][n-2]])
                if "Adj Close" in quotes.columns:
                    quotes.loc[idx2,"Adj Close"] = quotes["Adj Close"][n-1]
                quotes.loc[idx2,"Volume"] += quotes["Volume"][n-1]
                quotes = quotes.drop(quotes.index[n-1])

        # Similar bug in daily data except most data is simply duplicated
        # - exception is volume, *slightly* greater on final row (and matches website)
        elif interval=="1d":
            if dt1.date() == dt2.date():
                # Last two rows are on same day. Drop second-to-last row
                quotes = quotes.drop(quotes.index[n-2])

    return quotes


def safe_merge_dfs(df_main, df_sub, interval):
    # Carefully merge 'df_sub' onto 'df_main'
    # If naive merge fails, try again with reindexing df_sub:
    # 1) if interval is weekly or monthly, then try with index set to start of week/month
    # 2) if still failing then manually search through df_main.index to reindex df_sub

    if df_sub.shape[0] == 0:
        raise Exception("No data to merge")
    
    df_sub_backup = df_sub.copy()
    data_cols = [c for c in df_sub.columns if c not in df_main]
    if len(data_cols) > 1:
        raise Exception("Expected 1 data col")
    data_col = data_cols[0]

    def _reindex_events(df, new_index, data_col_name):
        if len(new_index) == len(set(new_index)):
            # No duplicates, easy
            df.index = new_index
            return df

        df["_NewIndex"] = new_index
        # Duplicates present within periods but can aggregate
        if data_col_name == "Dividends":
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
            df = df.drop("_NewIndex",axis=1)
        return df

    df = df_main.join(df_sub)

    f_na = df[data_col].isna()
    data_lost = sum(~f_na) < df_sub.shape[0]
    if not data_lost:
        return df
    # Lost data during join()
    if interval in ["1wk","1mo","3mo"]:
        # Backdate all df_sub.index dates to start of week/month
        if interval == "1wk":
            new_index = _pd.PeriodIndex(df_sub.index, freq='W').to_timestamp()
        elif interval == "1mo":
            new_index = _pd.PeriodIndex(df_sub.index, freq='M').to_timestamp()
        elif interval == "3mo":
            new_index = _pd.PeriodIndex(df_sub.index, freq='Q').to_timestamp()
        new_index = new_index.tz_localize(df.index.tz, ambiguous=True)
        df_sub = _reindex_events(df_sub, new_index, data_col)
        df = df_main.join(df_sub)

    f_na = df[data_col].isna()
    data_lost = sum(~f_na) < df_sub.shape[0]
    if not data_lost:
        return df
    # Lost data during join(). Manually check each df_sub.index date against df_main.index to
    # find matching interval
    df_sub = df_sub_backup.copy()
    new_index = [-1]*df_sub.shape[0]
    for i in range(df_sub.shape[0]):
        dt_sub_i = df_sub.index[i]
        if dt_sub_i in df_main.index:
            new_index[i] = dt_sub_i ; continue
        # Found a bad index date, need to search for near-match in df_main (same week/month)
        fixed = False
        for j in range(df_main.shape[0]-1):
            dt_main_j0 = df_main.index[j]
            dt_main_j1 = df_main.index[j+1]
            if (dt_main_j0 <= dt_sub_i) and (dt_sub_i < dt_main_j1):
                fixed = True
                if interval.endswith('h') or interval.endswith('m'):
                    # Must also be same day
                    fixed = (dt_main_j0.date() == dt_sub_i.date()) and (dt_sub_i.date() == dt_main_j1.date())
                if fixed:
                    dt_sub_i = dt_main_j0 ; break
        if not fixed:
            last_main_dt = df_main.index[df_main.shape[0]-1]
            diff = dt_sub_i - last_main_dt
            if interval == "1mo" and last_main_dt.month == dt_sub_i.month:
                dt_sub_i = last_main_dt ; fixed = True
            elif interval == "3mo" and last_main_dt.year == dt_sub_i.year and last_main_dt.quarter == dt_sub_i.quarter:
                dt_sub_i = last_main_dt ; fixed = True
            elif interval == "1wk":
                if last_main_dt.week == dt_sub_i.week:
                    dt_sub_i = last_main_dt ; fixed = True
                elif (dt_sub_i>=last_main_dt) and (dt_sub_i-last_main_dt < _datetime.timedelta(weeks=1)):
                    # With some specific start dates (e.g. around early Jan), Yahoo
                    # messes up start-of-week, is Saturday not Monday. So check
                    # if same week another way
                    dt_sub_i = last_main_dt ; fixed = True
            elif interval == "1d" and last_main_dt.day == dt_sub_i.day:
                dt_sub_i = last_main_dt ; fixed = True
            elif interval == "1h" and last_main_dt.hour == dt_sub_i.hour:
                dt_sub_i = last_main_dt ; fixed = True
            elif interval.endswith('m') or interval.endswith('h'):
                td = _pd.to_timedelta(interval)
                if (dt_sub_i>=last_main_dt) and (dt_sub_i-last_main_dt < td):
                    dt_sub_i = last_main_dt ; fixed = True
        new_index[i] = dt_sub_i
    df_sub = _reindex_events(df_sub, new_index, data_col)
    df = df_main.join(df_sub)

    f_na = df[data_col].isna()
    data_lost = sum(~f_na) < df_sub.shape[0]
    if data_lost:
        ## Not always possible to match events with trading, e.g. when released pre-market.
        ## So have to append to bottom with nan prices.
        ## But should only be impossible with intra-day price data.
        if interval.endswith('m') or interval.endswith('h'):
            f_missing = ~df_sub.index.isin(df.index)
            df_sub_missing = df_sub[f_missing]
            keys = set(["Adj Open", "Open", "Adj High", "High", "Adj Low", "Low", "Adj Close", "Close"]).intersection(df.columns)
            df_sub_missing[list(keys)] = _np.nan
            df = _pd.concat([df, df_sub_missing], sort=True)
        else:
            raise Exception("Lost data during merge despite all attempts to align data (see above)")

    return df


def fix_Yahoo_dst_issue(df, interval):
    if interval in ["1d","1w","1wk"]:
        # These intervals should start at time 00:00. But for some combinations of date and timezone, 
        # Yahoo has time off by few hours (e.g. Brazil 23:00 around Jan-2022). Suspect DST problem.
        # The clue is (a) minutes=0 and (b) hour near 0. 
        # Obviously Yahoo meant 00:00, so ensure this doesn't affect date conversion:
        f_pre_midnight = (df.index.minute == 0) & (df.index.hour.isin([22,23]))
        dst_error_hours = _np.array([0]*df.shape[0])
        dst_error_hours[f_pre_midnight] = 24-df.index[f_pre_midnight].hour
        df.index += _pd.TimedeltaIndex(dst_error_hours, 'h')
    return df


def is_valid_timezone(tz: str) -> bool:
    try:
        _tz.timezone(tz)
    except UnknownTimeZoneError:
        return False
    return True


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
            self.conn.execute('create table if not exists "kv" (key TEXT primary key, value TEXT) without rowid')
            self.conn.commit()
        _atexit.register(self.close)

    def close(self):
        if self.conn is not None:
            with self._cache_mutex:
                self.conn.close()
                self.conn = None

    def get(self, key: str) -> Union[str, None]:
        """Get value for key if it exists else returns None"""
        item = self.conn.execute('select value from "kv" where key=?', (key,))
        if item:
            return next(item, (None,))[0]

    def set(self, key: str, value: str) -> str:
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
        self._tz_db = None
        self._setup_cache_folder()

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
        # lazy init
        if self._tz_db is None:
            self._tz_db = _KVStore(_os.path.join(self._db_dir, "tkr-tz.db"))
            self._migrate_cache_tkr_tz()

        return self._tz_db

    def _migrate_cache_tkr_tz(self):
        """Migrate contents from old ticker CSV-cache to SQLite db"""
        fp = _os.path.join(self._db_dir, "tkr-tz.csv")
        if not _os.path.isfile(fp):
            return None
        df = _pd.read_csv(fp, index_col="Ticker")
        self.tz_db.bulk_set(df.to_dict()['Tz'])
        _os.remove(fp)


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
                print("Failed to create TzCache, reason: {}".format(err))
                print("TzCache will not be used.")
                print("Tip: You can direct cache to use a different location with 'set_tz_cache_location(mylocation)'")
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

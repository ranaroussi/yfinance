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

import time as _time
import datetime as _datetime
import pytz as _tz
import requests as _requests
import re as _re
import pandas as _pd
import numpy as _np
import sys as _sys
import os as _os
import appdirs as _ad

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


def empty_df(index=[]):
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
    '''
    get_json returns a python dictionary of the store detail for yahoo finance web pages.
    '''
    session = session or _requests
    html = session.get(url=url, proxies=proxy, headers=user_agent_headers).text

    # if "QuoteSummaryStore" not in html:
    #     html = session.get(url=url, proxies=proxy).text
    #     if "QuoteSummaryStore" not in html:
    #         return {}

    json_str = html.split('root.App.main =')[1].split(
        '(this)')[0].split(';\n}')[0].strip()
    # data = _json.loads(json_str)['context']['dispatcher']['stores']['QuoteSummaryStore']
    data = _json.loads(json_str)

    # add data about Shares Outstanding for companies' tickers if they are available
    try:
        data['annualBasicAverageShares'] = _json.loads(
            json_str)['context']['dispatcher']['stores'][
                'QuoteTimeSeriesStore']['timeSeries']['annualBasicAverageShares']
    except Exception:
        pass

	## TODO: Why dumping and parsing again?
    # return data
    new_data = _json.dumps(data).replace('{}', 'null')
    new_data = _re.sub(
        r'\{[\'|\"]raw[\'|\"]:(.*?),(.*?)\}', r'\1', new_data)

    return _json.loads(new_data)

def build_template(data):
    '''
    build_template returns the details required to rebuild any of the yahoo finance financial statements in the same order as the yahoo finance webpage. The function is built to be used on the "FinancialTemplateStore" json which appears in any one of the three yahoo finance webpages: "/financials", "/cash-flow" and "/balance-sheet".
    
    Returns:
        - template_annual_order: The order that annual figures should be listed in.
        - template_ttm_order: The order that TTM (Trailing Twelve Month) figures should be listed in.
        - level_detail: The level of each individual line item. E.g. for the "/financials" webpage, "Total Revenue" is a level 0 item and is the summation of "Operating Revenue" and "Excise Taxes" which are level 1 items.
   
    '''
    template_ttm_order = []   # Save the TTM (Trailing Twelve Months) ordering to an object.
    template_annual_order = []    # Save the annual ordering to an object.
    level_detail = []   #Record the level of each line item of the income statement ("Operating Revenue" and "Excise Taxes" sum to return "Total Revenue" we need to keep track of this)
    for key in data['template']:    # Loop through the json to retreive the exact financial order whilst appending to the objects
        template_ttm_order.append('trailing{}'.format(key['key']))
        template_annual_order.append('annual{}'.format(key['key']))
        level_detail.append(0)
        if 'children' in key:
            for child1 in key['children']:  # Level 1
                template_ttm_order.append('trailing{}'.format(child1['key']))
                template_annual_order.append('annual{}'.format(child1['key']))
                level_detail.append(1)
                if 'children' in child1:
                    for child2 in child1['children']:   # Level 2
                        template_ttm_order.append('trailing{}'.format(child2['key']))
                        template_annual_order.append('annual{}'.format(child2['key']))
                        level_detail.append(2)
                        if 'children' in child2:
                            for child3 in child2['children']:   # Level 3
                                template_ttm_order.append('trailing{}'.format(child3['key']))
                                template_annual_order.append('annual{}'.format(child3['key']))
                                level_detail.append(3)
    return template_ttm_order, template_annual_order, level_detail

def retreive_financial_details(data):
    '''
    retreive_financial_details returns all of the available financial details under the "QuoteTimeSeriesStore" for any of the following three yahoo finance webpages: "/financials", "/cash-flow" and "/balance-sheet".

    Returns:
        - TTM_dicts: A dictionary full of all of the available Trailing Twelve Month figures, this can easily be converted to a pandas dataframe.
        - Annual_dicts: A dictionary full of all of the available Annual figures, this can easily be converted to a pandas dataframe.
    '''
    TTM_dicts = []  # Save a dictionary object to store the TTM financials.
    Annual_dicts = []   # Save a dictionary object to store the Annual financials.
    for key in data['timeSeries']:  # Loop through the time series data to grab the key financial figures.
        try:
            if len(data['timeSeries'][key]) > 0:
                time_series_dict = {}
                time_series_dict['index'] = key
                for each in data['timeSeries'][key]:    # Loop through the years
                    time_series_dict[each['asOfDate']] = each['reportedValue']
                    # time_series_dict["{}".format(each['asOfDate'])] = data['timeSeries'][key][each]['reportedValue']
                if each['periodType'] == 'TTM':
                    TTM_dicts.append(time_series_dict)
                elif each['periodType'] == '12M':
                    Annual_dicts.append(time_series_dict)
        except Exception as e:
            pass
    return TTM_dicts, Annual_dicts


def get_financials_time_series(ticker, name, timescale, ticker_url, proxy=None, session=None):
    acceptable_names = ["financials", "balance-sheet", "cash-flow"]
    if not name in acceptable_names:
        raise Exception("name '{}' must be one of: {}".format(name, acceptable_names))
    acceptable_timestamps = ["annual", "quarterly"]
    if not timescale  in acceptable_timestamps:
        raise Exception("timescale '{}' must be one of: {}".format(timescale, acceptable_timestamps))

    session = session or _requests

    financials_data = get_json(ticker_url+'/'+name, proxy, session)

    # Step 1: get the keys:
    def _finditem1(key, obj):
        values = []
        if isinstance(obj,dict):
            if key in obj.keys():
                values.append(obj[key])
            for k,v in obj.items():
                values += _finditem1(key,v)
        elif isinstance(obj,list):
            for v in obj:
                values += _finditem1(key,v)
        return values
    keys = _finditem1("key",financials_data['context']['dispatcher']['stores']['FinancialTemplateStore'])

    # Step 2: construct url:
    ts_url_base = "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{0}?symbol={0}".format(ticker)
    if len(keys) == 0:
        raise Exception("Fetching keys failed")
    # yr_url = ts_url_base + "&type=" + ",".join(["annual"+k for k in keys])
    # qtr_url = ts_url_base + "&type=" + ",".join(["quarterly"+k for k in keys])
    # url = qtr_url
    url = ts_url_base + "&type=" + ",".join([timescale+k for k in keys])
    # Yahoo returns maximum 4 years or 5 quarters, regardless of start_dt:
    start_dt = _datetime.datetime(2016, 12, 31)
    end = (_datetime.datetime.now() + _datetime.timedelta(days=366))
    url += "&period1={}&period2={}".format(int(start_dt.timestamp()), int(end.timestamp()))

    # Step 3: fetch and reshape data
    json_str = session.get(url=url, proxies=proxy, headers=user_agent_headers).text
    json_data = _json.loads(json_str)
    data_raw = json_data["timeseries"]["result"]
    # data_raw = [v for v in data_raw if len(v) > 1] # Discard keys with no data
    for d in data_raw:
        del d["meta"]

    # Now reshape data into a table:
    # Step 1: get columns and index:
    timestamps = set()
    data_unpacked = {}
    for x in data_raw:
        for k in x.keys():
            if k=="timestamp":
                timestamps.update(x[k])
            else:
                data_unpacked[k] = x[k]
    timestamps = sorted(list(timestamps))
    dates = _pd.to_datetime(timestamps, unit="s")
    df = _pd.DataFrame(columns=dates, index=data_unpacked.keys())
    for k,v in data_unpacked.items():
        if df is None:
            df = _pd.DataFrame(columns=dates, index=[k])
        df.loc[k] = {_pd.Timestamp(x["asOfDate"]):x["reportedValue"]["raw"] for x in v}

    df.index = df.index.str.replace("^"+timescale, "", regex=True)

    df = df[sorted(df.columns, reverse=True)]

    return df


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
            splits = splits["Stock Splits"]

    return dividends, splits


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


# Simple file cache of ticker->timezone:
def get_cache_dirpath():
    return _os.path.join(_ad.user_cache_dir(), "py-yfinance")
def cache_lookup_tkr_tz(tkr):
    fp = _os.path.join(get_cache_dirpath(), "tkr-tz.csv")
    if not _os.path.isfile(fp):
        return None

    df = _pd.read_csv(fp)
    f = df["Ticker"] == tkr
    if sum(f) == 0:
        return None

    return df["Tz"][f].iloc[0]
def cache_store_tkr_tz(tkr,tz):
    df = _pd.DataFrame({"Ticker":[tkr], "Tz":[tz]})

    dp = get_cache_dirpath()
    if not _os.path.isdir(dp):
        _os.makedirs(dp)
    fp = _os.path.join(dp, "tkr-tz.csv")
    if not _os.path.isfile(fp):
        df.to_csv(fp, index=False)
        return

    df_all = _pd.read_csv(fp)
    f = df_all["Ticker"]==tkr
    if sum(f) > 0:
        raise Exception("Tkr {} tz already in cache".format(tkr))

    _pd.concat([df_all,df]).to_csv(fp, index=False)


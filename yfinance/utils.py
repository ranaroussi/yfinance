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

import requests as _requests_lib
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

retry_code_list = [429, 500, 502, 503, 504]
max_retries=6
backoff_factor=0.075
def logging_hook(response, *args, **kwargs):
    # url, code, in_list = None, None, None
    # nonlocal url
    # nonlocal code
    # nonlocal in_list
    res = {}
    try:
        if response.status_code in retry_code_list:
            in_list = True
            print('>>>>>>>>>', 'requests')
            print(dir(response))
            print(vars(response))
            res['url'], res['code'] = response.url, response.status_code
    except AttributeError as e:
        if response.code in retry_code_list:
            in_list = True
            print('>>>>>>>>>', 'urllib')
            print(dir(response))
            print(vars(response))
            res['url'], res['code']  = response.url, response.code
    if 'url' in res or 'code' in res:
        print(f'retrying {res.get("url")} [{res["code"]}]')

retry_strategy = Retry(
    total=max_retries,
    backoff_factor=backoff_factor,
    status_forcelist=retry_code_list,
    method_whitelist=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
_requests = _requests_lib.Session()
_requests.mount("https://", adapter)
_requests.mount("http://", adapter)

_requests.hooks["response"] = [logging_hook]

#moved to bottom of file
# _requests.get = retryException(exceptions=(Exception,))(_requests.get)

from urllib.error import HTTPError
import time
import functools
from requests.exceptions import ChunkedEncodingError
from http.client import IncompleteRead

import re as _re
import pandas as _pd
import numpy as _np
import sys as _sys
import re as _re

try:
    import ujson as _json
except ImportError:
    import json as _json


def empty_df(index=[]):
    empty = _pd.DataFrame(index=index, data={
        'Open': _np.nan, 'High': _np.nan, 'Low': _np.nan,
        'Close': _np.nan, 'Adj Close': _np.nan, 'Volume': _np.nan})
    empty.index.name = 'Date'
    return empty


def get_json(url, proxy=None):
    html = _requests.get(url=url, proxies=proxy).text

    if "QuoteSummaryStore" not in html:
        html = _requests.get(url=url, proxies=proxy).text
        if "QuoteSummaryStore" not in html:
            return {}

    json_str = html.split('root.App.main =')[1].split(
        '(this)')[0].split(';\n}')[0].strip()
    data = _json.loads(json_str)[
        'context']['dispatcher']['stores']['QuoteSummaryStore']

    # return data
    new_data = _json.dumps(data).replace('{}', 'null')
    new_data = _re.sub(
        r'\{[\'|\"]raw[\'|\"]:(.*?),(.*?)\}', r'\1', new_data)

    return _json.loads(new_data)


def camel2title(o):
    return [_re.sub("([a-z])([A-Z])", "\g<1> \g<2>", i).title() for i in o]


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


def parse_actions(data, tz=None):
    dividends = _pd.DataFrame(columns=["Dividends"])
    splits = _pd.DataFrame(columns=["Stock Splits"])

    if "events" in data:
        if "dividends" in data["events"]:
            dividends = _pd.DataFrame(
                data=list(data["events"]["dividends"].values()))
            dividends.set_index("date", inplace=True)
            dividends.index = _pd.to_datetime(dividends.index, unit="s")
            dividends.sort_index(inplace=True)
            if tz is not None:
                dividends.index = dividends.index.tz_localize(tz)

            dividends.columns = ["Dividends"]

        if "splits" in data["events"]:
            splits = _pd.DataFrame(
                data=list(data["events"]["splits"].values()))
            splits.set_index("date", inplace=True)
            splits.index = _pd.to_datetime(splits.index, unit="s")
            splits.sort_index(inplace=True)
            if tz is not None:
                splits.index = splits.index.tz_localize(tz)
            splits["Stock Splits"] = splits["numerator"] / \
                splits["denominator"]
            splits = splits["Stock Splits"]

    return dividends, splits


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



class dotdict(dict):
    def __getattr__(self, key):
        return self[key]

    def tprint(self):
        dotdict.treePrint(self)

    def wrap(self):
        for k,v in self.items():
            if isinstance(v, dict):
                self[k] = dotdict(v).wrap()
        return self

    @classmethod
    def treePrint(cls, D, tablevel=0):
        if isinstance(D, dict):
            for k, v in D.items():
                print('\t'*tablevel + f'{k}: {v if not isinstance(v, dict) else "-"}')
                dotdict.treePrint(v, tablevel+1)

            


def retry(  exceptions=(Exception,),
            total=max_retries, 
            backoff_factor=backoff_factor, 
            exception_predicate=lambda e : True,
            logging_hook=print):
    '''
    decorator for retrying functions after they throw exceptions
    
    Input:
        exceptions: tuple of Exceptions which should be retried after
        total: maximum numer of times to retry
        backoff_factor: {delay} = {backoff_factor} * 2**{num attempts}
        exception_predicate: handle an Exception e iff exception_predicate(e)
        logging_hook: function called on handled exceptions
    '''

    def decorator(  f, 
                    exceptions=exceptions,
                    total=total, 
                    backoff_factor=backoff_factor, 
                    exception_predicate=exception_predicate,
                    logging_hook=logging_hook):

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            for i in range(total):
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    if exception_predicate(e):
                        logging_hook(i, e)
                        delay = backoff_factor * 2**i
                        time.sleep(delay)
                        # print(f'failed retry number: {i} || {e}')
                        error = e
                        continue
                    else:
                        raise e 
            # print('giving up')
            error.msg = 'Too many retries...:' + error.msg
            raise error

        return wrapper

    return decorator

retryHTTP = functools.partial(
                                retry, 
                                exceptions=(HTTPError,),
                                exception_predicate=lambda e: e.code in retry_code_list,
                                logging_hook=logging_hook
                            )

_requests.get = retry(exceptions=(ChunkedEncodingError,IncompleteRead))(_requests.get)




@retry(exceptions=(ValueError,))
def parse_quotes(data, tz=None):
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

    if tz is not None:
        quotes.index = quotes.index.tz_localize(tz)

    return quotes

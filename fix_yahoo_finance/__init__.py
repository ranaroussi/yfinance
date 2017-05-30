#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Yahoo! Finance Fix for Pandas Datareader
# https://github.com/ranaroussi/fix-yahoo-finance
#
# Copyright 2017 Ran Aroussi
#
# Licensed under the GNU Lesser General Public License, v3.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gnu.org/licenses/lgpl-3.0.en.html
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__version__ = "0.0.7"
__author__ = "Ran Aroussi"
__all__ = ['download', 'get_yahoo_crumb', 'parse_ticker_csv']

import datetime
import numpy as np
import pandas as pd
import time
import io
import requests
import re
import warnings

_YAHOO_COOKIE_ = ''
_YAHOO_CRUMB_ = ''
_YAHOO_CHECKED_ = None
_YAHOO_TTL_ = 300



def get_yahoo_crumb(force=False):
    global _YAHOO_COOKIE_, _YAHOO_CRUMB_, _YAHOO_CHECKED_, _YAHOO_TTL_

    # use same cookie for 5 min
    if _YAHOO_CHECKED_ and not force:
        now = datetime.datetime.now()
        delta = (now - _YAHOO_CHECKED_).total_seconds()
        if delta < _YAHOO_TTL_:
            return (_YAHOO_CRUMB_, _YAHOO_COOKIE_)

    res = requests.get('https://finance.yahoo.com/quote/SPY/history')
    _YAHOO_COOKIE_ = res.cookies['B']

    pattern = re.compile('.*"CrumbStore":\{"crumb":"(?P<crumb>[^"]+)"\}')
    for line in res.text.splitlines():
        m = pattern.match(line)
        if m is not None:
            _YAHOO_CRUMB_ = m.groupdict()['crumb']

    # set global params
    _YAHOO_CHECKED_ = datetime.datetime.now()

    return (_YAHOO_CRUMB_, _YAHOO_COOKIE_)


def parse_ticker_csv(csv_str, auto_adjust):
    df = pd.read_csv(csv_str, index_col=0, error_bad_lines=False
                     ).replace('null', np.nan).dropna()

    df.index = pd.to_datetime(df.index)
    df = df.apply(pd.to_numeric)
    df['Volume'] = df['Volume'].fillna(0).astype(int)

    if auto_adjust:
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

        df = df[['Open', 'High', 'Low', 'Close', 'Volume']]

    return df


def download(tickers, start=None, end=None, as_panel=True,
                   group_by='column', auto_adjust=False, *args, **kwargs):

    # format start
    if start is None:
        start = int(time.mktime(time.strptime('1950-01-01', '%Y-%m-%d')))
    elif isinstance(start, datetime.datetime):
        start = int(time.mktime(start.timetuple()))
    else:
        start = int(time.mktime(time.strptime(str(start), '%Y-%m-%d')))

    # format end
    if end is None:
        end = int(time.mktime(datetime.datetime.now().timetuple()))
    elif isinstance(end, datetime.datetime):
        end = int(time.mktime(end.timetuple()))
    else:
        end = int(time.mktime(time.strptime(str(end), '%Y-%m-%d')))

    # iterval
    interval = kwargs["interval"] if "interval" in kwargs else "1d"

    # start downloading
    dfs = {}
    crumb, cookie = get_yahoo_crumb()

    # download tickers
    tickers = tickers if isinstance(tickers, list) else [tickers]
    tickers = [x.upper() for x in tickers]

    for ticker in tickers:
        url_str = "https://query1.finance.yahoo.com/v7/finance/download/%s"
        url_str += "?period1=%s&period2=%s&interval=%s&events=history&crumb=%s"

        tried_once = False
        try:
            url = url_str % (ticker, start, end, interval, crumb)
            print(url)
            hist = io.StringIO(requests.get(url, cookies={'B': cookie}).text)
            dfs[ticker] = parse_ticker_csv(hist, auto_adjust)
        except:
            # something went wrong...
            # try waiting 5 seconds and try one more time using a new cookie/crumb
            if not tried_once:
                time.sleep(5)
                tried_once = True
                crumb, cookie = get_yahoo_crumb(force=True)
                url = url_str % (ticker, start, end, interval, crumb)
                hist = io.StringIO(requests.get(url, cookies={'B': cookie}).text)
                dfs[ticker] = parse_ticker_csv(hist, auto_adjust)

    # create pandl (derecated)
    if as_panel:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)
            data = pd.Panel(dfs)
            if group_by == 'column':
                data = data.swapaxes(0, 2)

    # create multiIndex df
    else:
        data = pd.concat(dfs.values(), axis=1, keys=dfs.keys())
        if group_by == 'column':
            data.columns = data.columns.swaplevel(0, 1)
            data.sort_index(level=0, axis=1, inplace=True)
            if auto_adjust:
                data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
            else:
                data = data[['Open', 'High', 'Low',
                             'Close', 'Adj Close', 'Volume']]

    # return single df if only one ticker
    if len(tickers) == 1:
        data = dfs[tickers[0]]

    return data


# make pandas datareader optional
# otherwise can be called via fix_yahoo_finance.download(...)
try:
    import pandas_datareader
    pandas_datareader.data.get_data_yahoo = download
except:
    pass

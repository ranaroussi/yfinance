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

from __future__ import print_function

__version__ = "0.0.15"
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
import sys
import multitasking

_YAHOO_COOKIE_ = ''
_YAHOO_CRUMB_ = ''
_YAHOO_CHECKED_ = None
_YAHOO_TTL_ = 180


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


_DFS_ = {}
_COMPLETED_ = 0
_PROGRESS_BAR_ = False
_FAILED_ = []


def make_chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def download(tickers, start=None, end=None, as_panel=True,
             group_by='column', auto_adjust=False, progress=True,
             actions=None, threads=1, *args, **kwargs):

    """Download yahoo tickers
    :Parameters:

        tickers : str, list
            List of tickers to download
        start: str
            Download start date string (YYYY-MM-DD) or datetime. Default is 1950-01-01
        end: str
            Download end date string (YYYY-MM-DD) or datetime. Default is today
        as_panel : bool
            Return a multi-index DataFrame or Panel. Default is True (Panel), which is deprecated
        group_by : str
            Group by ticker or 'column' (default)
        auto_adjust: bool
            Adjust all OHLC automatically? Default is False
        actions: str
            Download dividend + stock splits data. Default is None (no actions)
            Options are 'inline' (returns history + actions) and 'only' (actions only)
        threads: int
            How may threads to use? Default is 1 thread
    """

    global _DFS_, _COMPLETED_, _PROGRESS_BAR_, _FAILED_
    _COMPLETED_ = 0
    _FAILED_ = []

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

    # create ticker list
    tickers = tickers if isinstance(tickers, list) else [tickers]
    tickers = [x.upper() for x in tickers]

    # initiate progress bar
    if progress:
        _PROGRESS_BAR_ = ProgressBar(len(tickers), 'downloaded')

    # download using single thread
    if threads is None or threads < 2:
        download_chunk(tickers, start=start, end=end,
                       auto_adjust=auto_adjust, progress=progress,
                       actions=actions, *args, **kwargs)
    # threaded download
    else:
        threads = min([threads, len(tickers)])

        # download in chunks
        chunks = 0
        for chunk in make_chunks(tickers, max([1, len(tickers) // threads])):
            chunks += len(chunk)
            download_thread(chunk, start=start, end=end,
                            auto_adjust=auto_adjust, progress=progress,
                            actions=actions, *args, **kwargs)
        if len(tickers[-chunks:]) > 0:
            download_thread(tickers[-chunks:], start=start, end=end,
                            auto_adjust=auto_adjust, progress=progress,
                            actions=actions, *args, **kwargs)

    # wait for completion
    while _COMPLETED_ < len(tickers):
        time.sleep(0.1)

    # create panel (derecated)
    if as_panel:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)
            data = pd.Panel(_DFS_)
            if group_by == 'column':
                data = data.swapaxes(0, 2)

    # create multiIndex df
    else:
        data = pd.concat(_DFS_.values(), axis=1, keys=_DFS_.keys())
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
        data = _DFS_[tickers[0]]

    if len(_FAILED_) > 0:
        print("\nThe following tickers failed to download:\n",
              ', '.join(_FAILED_))

    return data


def download_one(ticker, start, end, interval, auto_adjust=None, actions=None):

    crumb, cookie = get_yahoo_crumb()

    url_str = "https://query1.finance.yahoo.com/v7/finance/download/%s"
    url_str += "?period1=%s&period2=%s&interval=%s&events=%s&crumb=%s"

    actions = None if '^' in ticker else actions

    if actions:
        url = url_str % (ticker, start, end, interval, 'div', crumb)
        res = requests.get(url, cookies={'B': cookie}).text
        # print(res)
        div = pd.DataFrame(columns=['action', 'value'])
        if "error" not in res:
            div = pd.read_csv(io.StringIO(res),
                              index_col=0, error_bad_lines=False
                              ).replace('null', np.nan).dropna()

            if isinstance(div, pd.DataFrame):
                div.index = pd.to_datetime(div.index)
                div["action"] = "DIVIDEND"
                div = div.rename(columns={'Dividends': 'value'})
                div['value'] = div['value'].astype(float)

        # download Stock Splits data
        url = url_str % (ticker, start, end, interval, 'split', crumb)
        res = requests.get(url, cookies={'B': cookie}).text
        split = pd.DataFrame(columns=['action', 'value'])
        if "error" not in res:
            split = pd.read_csv(io.StringIO(res),
                                index_col=0, error_bad_lines=False
                                ).replace('null', np.nan).dropna()

            if isinstance(split, pd.DataFrame):
                split.index = pd.to_datetime(split.index)
                split["action"] = "SPLIT"
                split = split.rename(columns={'Stock Splits': 'value'})
                if len(split.index) > 0:
                    split['value'] = split.apply(
                        lambda x: 1 / eval(x['value']), axis=1).astype(float)


        if actions == 'only':
            return pd.concat([div, split]).sort_index()

    # download history
    url = url_str % (ticker, start, end, interval, 'history', crumb)
    res = requests.get(url, cookies={'B': cookie}).text
    hist = pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])

    if "error" in res:
        return pd.DataFrame()

    hist = parse_ticker_csv(io.StringIO(res), auto_adjust)

    if actions is None:
        return hist

    hist['Dividends'] = div['value'] if len(div.index) > 0 else np.nan
    hist['Dividends'].fillna(0, inplace=True)
    hist['Stock Splits'] = split['value'] if len(split.index) > 0 else np.nan
    hist['Stock Splits'].fillna(1, inplace=True)

    return hist



@multitasking.task
def download_thread(tickers, start=None, end=None,
                    auto_adjust=False, progress=True,
                    actions=False, *args, **kwargs):
    download_chunk(tickers, start=None, end=None,
                   auto_adjust=False, progress=progress,
                   actions=False, *args, **kwargs)


def download_chunk(tickers, start=None, end=None,
                   auto_adjust=False, progress=True,
                   actions=False, *args, **kwargs):

    global _DFS_, _COMPLETED_, _PROGRESS_BAR_, _FAILED_

    interval = kwargs["interval"] if "interval" in kwargs else "1d"

    # url template
    url_str = "https://query1.finance.yahoo.com/v7/finance/download/%s"
    url_str += "?period1=%s&period2=%s&interval=%s&events=%s&crumb=%s"

    # failed tickers collectors
    round1_failed_tickers = []

    # start downloading
    for ticker in tickers:

        # yahoo crumb/cookie
        crumb, cookie = get_yahoo_crumb()

        tried_once = False
        try:
            hist = download_one(ticker, start, end,
                                interval, auto_adjust, actions)
            if isinstance(hist, pd.DataFrame):
                _DFS_[ticker] = hist
            else:
                round1_failed_tickers.append(ticker)
        except:
            # something went wrong...
            # try one more time using a new cookie/crumb
            if not tried_once:
                tried_once = True
                try:
                    get_yahoo_crumb(force=True)
                    hist = download_one(ticker, start, end,
                                        interval, auto_adjust, actions)
                    if isinstance(hist, pd.DataFrame):
                        _DFS_[ticker] = hist
                        if progress:
                            _PROGRESS_BAR_.animate()
                    else:
                        round1_failed_tickers.append(ticker)
                except:
                    round1_failed_tickers.append(ticker)
        time.sleep(0.001)

    # try failed items again before giving up
    _COMPLETED_ += len(tickers) - len(round1_failed_tickers)

    if len(round1_failed_tickers) > 0:
        get_yahoo_crumb(force=True)
        for ticker in round1_failed_tickers:
            try:
                hist = download_one(ticker, start, end,
                                    interval, auto_adjust, actions)
                if isinstance(hist, pd.DataFrame):
                    _DFS_[ticker] = hist
                    if progress:
                        _PROGRESS_BAR_.animate()
                else:
                    _FAILED_.append(ticker)
            except:
                _FAILED_.append(ticker)
                pass
            time.sleep(0.000001)
        _COMPLETED_ += 1


class ProgressBar:
    def __init__(self, iterations, text='completed'):
        self.text = text
        self.iterations = iterations
        self.prog_bar = '[]'
        self.fill_char = '*'
        self.width = 50
        self.__update_amount(0)
        self.elapsed = 1

    def animate(self, iteration=None):
        if iteration is None:
            self.elapsed += 1
            iteration = self.elapsed
        else:
            self.elapsed += iteration

        print('\r' + str(self), end='')
        sys.stdout.flush()
        self.update_iteration()

    def update_iteration(self):
        self.__update_amount((self.elapsed / float(self.iterations)) * 100.0)
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


# make pandas datareader optional
# otherwise can be called via fix_yahoo_finance.download(...)
try:
    import pandas_datareader
    pandas_datareader.data.get_data_yahoo = download
except:
    pass

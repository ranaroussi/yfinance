#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Yahoo! Finance Fix for Pandas Datareader
# https://github.com/ranaroussi/fix-yahoo-finance
#
# Copyright 2017-2018 Ran Aroussi
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

__version__ = "0.0.21"
__author__ = "Ran Aroussi"
__all__ = ['download', 'get_yahoo_crumb', 'parse_ticker_csv', 'pdr_override']


import datetime as _datetime
import time as _time
import io as _io
import re as _re
import warnings as _warnings
import sys as _sys

import numpy as _np
import pandas as _pd
import requests as _requests
import multitasking as _multitasking


_YAHOO_COOKIE = ''
_YAHOO_CRUMB = ''
_YAHOO_CHECKED = None
_YAHOO_TTL = 180

_DFS = {}
_COMPLETED = 0
_PROGRESS_BAR = False
_FAILED = []


def get_yahoo_crumb(force=False):
    global _YAHOO_COOKIE, _YAHOO_CRUMB, _YAHOO_CHECKED, _YAHOO_TTL

    # use same cookie for 5 min
    if _YAHOO_CHECKED and not force:
        now = _datetime.datetime.now()
        delta = (now - _YAHOO_CHECKED).total_seconds()
        if delta < _YAHOO_TTL:
            return (_YAHOO_CRUMB, _YAHOO_COOKIE)

    res = _requests.get('https://finance.yahoo.com/quote/SPY/history')
    _YAHOO_COOKIE = res.cookies['B']

    pattern = _re.compile('.*"CrumbStore":\{"crumb":"(?P<crumb>[^"]+)"\}')
    for line in res.text.splitlines():
        m = pattern.match(line)
        if m is not None:
            _YAHOO_CRUMB = m.groupdict()['crumb']

    # set global params
    _YAHOO_CHECKED = _datetime.datetime.now()

    return (_YAHOO_CRUMB, _YAHOO_COOKIE)


def parse_ticker_csv(csv_str, auto_adjust):
    df = _pd.read_csv(csv_str, index_col=0, error_bad_lines=False
                     ).replace('null', _np.nan).dropna()

    df.index = _pd.to_datetime(df.index)
    df = df.apply(_pd.to_numeric)
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

    return df.groupby(df.index).first()


def make_chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def download(tickers, start=None, end=None, as_panel=True,
             group_by='column', auto_adjust=False, progress=True,
             actions=None, threads=1, **kwargs):
    """Download yahoo tickers
    :Parameters:

        tickers : str, list
            List of tickers to download
        start: str
            Download start date string (YYYY-MM-DD) or _datetime. Default is 1950-01-01
        end: str
            Download end date string (YYYY-MM-DD) or _datetime. Default is today
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

    global _DFS, _COMPLETED, _PROGRESS_BAR, _FAILED

    _COMPLETED = 0
    _FAILED = []

    # format start
    if start is None:
        start = int(_time.mktime(_time.strptime('1950-01-01', '%Y-%m-%d')))
    elif isinstance(start, _datetime.datetime):
        start = int(_time.mktime(start.timetuple()))
    else:
        start = int(_time.mktime(_time.strptime(str(start), '%Y-%m-%d')))

    # format end
    if end is None:
        end = int(_time.mktime(_datetime.datetime.now().timetuple()))
    elif isinstance(end, _datetime.datetime):
        end = int(_time.mktime(end.timetuple()))
    else:
        end = int(_time.mktime(_time.strptime(str(end), '%Y-%m-%d')))

    # create ticker list
    tickers = tickers if isinstance(tickers, list) else [tickers]
    tickers = [x.upper() for x in tickers]

    # initiate progress bar
    if progress:
        _PROGRESS_BAR = _ProgressBar(len(tickers), 'downloaded')

    # download using single thread
    if threads is None or threads < 2:
        download_chunk(tickers, start=start, end=end,
                       auto_adjust=auto_adjust, progress=progress,
                       actions=actions, **kwargs)
    # threaded download
    else:
        threads = min([threads, len(tickers)])

        # download in chunks
        chunks = 0
        for chunk in make_chunks(tickers, max([1, len(tickers) // threads])):
            chunks += len(chunk)
            download_thread(chunk, start=start, end=end,
                            auto_adjust=auto_adjust, progress=progress,
                            actions=actions, **kwargs)
        if not tickers[-chunks:].empty:
            download_thread(tickers[-chunks:], start=start, end=end,
                            auto_adjust=auto_adjust, progress=progress,
                            actions=actions, **kwargs)

    # wait for completion
    while _COMPLETED < len(tickers):
        _time.sleep(0.1)

    if progress:
        _PROGRESS_BAR.completed()

    # create panel (derecated)
    if as_panel:
        with _warnings.catch_warnings():
            _warnings.filterwarnings("ignore", category=DeprecationWarning)
            data = _pd.Panel(_DFS)
            if group_by == 'column':
                data = data.swapaxes(0, 2)

    # create multiIndex df
    else:
        data = _pd.concat(_DFS.values(), axis=1, keys=_DFS.keys())
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
        data = _DFS[tickers[0]]

    if _FAILED:
        print("\nThe following tickers failed to download:\n",
              ', '.join(_FAILED))

    _DFS = {}
    return data


def download_one(ticker, start, end, interval, auto_adjust=None, actions=None):

    tried_once = False
    crumb, cookie = get_yahoo_crumb()

    url_str = "https://query1.finance.yahoo.com/v7/finance/download/%s"
    url_str += "?period1=%s&period2=%s&interval=%s&events=%s&crumb=%s"

    actions = None if '^' in ticker else actions

    if actions:
        url = url_str % (ticker, start, end, interval, 'div', crumb)
        res = _requests.get(url, cookies={'B': cookie}).text
        # print(res)
        div = _pd.DataFrame(columns=['action', 'value'])
        if "error" not in res:
            div = _pd.read_csv(_io.StringIO(res),
                              index_col=0, error_bad_lines=False
                              ).replace('null', _np.nan).dropna()

            if isinstance(div, _pd.DataFrame):
                div.index = _pd.to_datetime(div.index)
                div["action"] = "DIVIDEND"
                div = div.rename(columns={'Dividends': 'value'})
                div['value'] = div['value'].astype(float)

        # download Stock Splits data
        url = url_str % (ticker, start, end, interval, 'split', crumb)
        res = _requests.get(url, cookies={'B': cookie}).text
        split = _pd.DataFrame(columns=['action', 'value'])
        if "error" not in res:
            split = _pd.read_csv(_io.StringIO(res),
                                index_col=0, error_bad_lines=False
                                ).replace('null', _np.nan).dropna()

            if isinstance(split, _pd.DataFrame):
                split.index = _pd.to_datetime(split.index)
                split["action"] = "SPLIT"
                split = split.rename(columns={'Stock Splits': 'value'})
                if not split.empty:
                    split['value'] = split.apply(
                        lambda x: 1 / eval(x['value']), axis=1).astype(float)

        if actions == 'only':
            return _pd.concat([div, split]).sort_index()

    # download history
    url = url_str % (ticker, start, end, interval, 'history', crumb)
    res = _requests.get(url, cookies={'B': cookie}).text
    hist = _pd.DataFrame(
        columns=['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])

    if "error" in res:
        return _pd.DataFrame()

    hist = parse_ticker_csv(_io.StringIO(res), auto_adjust)

    if not hist.empty:
        if actions is None:
            return hist

        hist['Dividends'] = div['value'] if not div.empty else _np.nan
        hist['Dividends'].fillna(0, inplace=True)
        hist['Stock Splits'] = split['value'] if not split.empty else _np.nan
        hist['Stock Splits'].fillna(1, inplace=True)

        return hist

    # empty len(hist.index) == 0
    if not tried_once:
        tried_once = True
        get_yahoo_crumb(force=True)
        return download_one(ticker, start, end, interval, auto_adjust, actions)


@_multitasking.task
def download_thread(tickers, start=None, end=None,
                    auto_adjust=False, progress=True,
                    actions=False, **kwargs):
    download_chunk(tickers, start=start, end=end,
                   auto_adjust=auto_adjust, progress=progress,
                   actions=actions, **kwargs)


def download_chunk(tickers, start=None, end=None,
                   auto_adjust=False, progress=True,
                   actions=False, **kwargs):

    global _DFS, _COMPLETED, _PROGRESS_BAR, _FAILED

    interval = kwargs["interval"] if "interval" in kwargs else "1d"

    # url template
    url_str = "https://query1.finance.yahoo.com/v7/finance/download/%s"
    url_str += "?period1=%s&period2=%s&interval=%s&events=%s&crumb=%s"

    # failed tickers collectors
    round1_failed_tickers = []

    # start downloading
    for ticker in tickers:

        # yahoo crumb/cookie
        # crumb, cookie = get_yahoo_crumb()
        get_yahoo_crumb()

        tried_once = False
        try:
            hist = download_one(ticker, start, end,
                                interval, auto_adjust, actions)
            if isinstance(hist, _pd.DataFrame):
                _DFS[ticker] = hist
                if progress:
                    _PROGRESS_BAR.animate()
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
                    if isinstance(hist, _pd.DataFrame):
                        _DFS[ticker] = hist
                        if progress:
                            _PROGRESS_BAR.animate()
                    else:
                        round1_failed_tickers.append(ticker)
                except:
                    round1_failed_tickers.append(ticker)
        _time.sleep(0.001)

    # try failed items again before giving up
    _COMPLETED += len(tickers) - len(round1_failed_tickers)

    if round1_failed_tickers:
        get_yahoo_crumb(force=True)
        for ticker in round1_failed_tickers:
            try:
                hist = download_one(ticker, start, end,
                                    interval, auto_adjust, actions)
                if isinstance(hist, _pd.DataFrame):
                    _DFS[ticker] = hist
                    if progress:
                        _PROGRESS_BAR.animate()
                else:
                    _FAILED.append(ticker)
            except:
                _FAILED.append(ticker)
            _time.sleep(0.000001)
        _COMPLETED += 1


class _ProgressBar:
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


# make pandas datareader optional
# otherwise can be called via fix_yahoo_finance.download(...)
def pdr_override():
    try:
        import pandas_datareader
        pandas_datareader.data.get_data_yahoo = download
    except:
        pass

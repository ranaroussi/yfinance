#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Yahoo! Finance Fix for Pandas Datareader
# https://github.com/ranaroussi/yahoo-finance-fix
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

__version__ = "0.0.2"
__author__ = "Ran Aroussi"
__all__ = ['get_data_yahoo']

import datetime
import numpy as np
import os
import pandas as pd
import time
import warnings

from selenium import webdriver

try:
    from pyvirtualdisplay import Display
    virt_display = True
except ImportError:
    virt_display = False


CHROMEDRIVER_PATH = None
def set_chromedriver_path(path_str):
    global CHROMEDRIVER_PATH
    CHROMEDRIVER_PATH = path_str

def get_data_yahoo(tickers, start=None, end=None, as_panel=True,
                   group_by='column', auto_adjust=False, *args, **kwargs):

    global CHROMEDRIVER_PATH
    libdir = os.path.dirname(os.path.realpath(__file__))

    print(CHROMEDRIVER_PATH)
    return

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

    # start browser
    if virt_display:
        display = Display(visible=0, size=(800, 600))
        display.start()

    chromeOptions = webdriver.ChromeOptions()
    prefs = {"download.default_directory": "/tmp"}
    chromeOptions.add_experimental_option("prefs", prefs)

    # add adblock to make page load faster
    try:
        chromeOptions.add_extension(libdir + '/Adblock-Plus_v1.11.crx')
    except:
        pass

    if CHROMEDRIVER_PATH:
        driver = webdriver.Chrome(CHROMEDRIVER_PATH, chrome_options=chromeOptions)
    else:
        driver = webdriver.Chrome(chrome_options=chromeOptions)

    dfs = {}

    # download tickers
    tickers = tickers if isinstance(tickers, list) else [tickers]
    tickers = [x.upper() for x in tickers]

    for ticker in tickers:
        url = "https://finance.yahoo.com/quote/%s/history"
        url += "?period1=%s&period2=%s&interval=%s&filter=history&frequency=%s"
        url = url % (ticker, start, end, interval, interval)

        driver.get(url)

        tries = 0
        while tries < 3:
            tries += 1
            link = driver.find_elements_by_css_selector(
                'a[download="' + ticker + '.csv"]')

            if len(link) == 0:
                time.sleep(.1)
            else:
                tries = 3
                hist = link[0].get_attribute('href')
                driver.get(hist)

                dfs[ticker] = pd.read_csv('/tmp/' + ticker + '.csv', parse_dates=['Date'],
                                          index_col=['Date']).replace('null', np.nan).dropna()
                dfs[ticker] = dfs[ticker].apply(pd.to_numeric)
                dfs[ticker]['Volume'] = dfs[ticker][
                    'Volume'].fillna(0).astype(int)

                if auto_adjust:
                    ratio = dfs[ticker]["Close"] / dfs[ticker]["Adj Close"]
                    dfs[ticker]["Adj Open"] = dfs[ticker]["Open"] / ratio
                    dfs[ticker]["Adj High"] = dfs[ticker]["High"] / ratio
                    dfs[ticker]["Adj Low"] = dfs[ticker]["Low"] / ratio
                    dfs[ticker].drop(
                        ["Open", "High", "Low", "Close"], axis=1, inplace=True)
                    dfs[ticker].rename(columns={
                        "Adj Open": "Open", "Adj High": "High",
                        "Adj Low": "Low", "Adj Close": "Close"
                    }, inplace=True)
                    dfs[ticker] = dfs[ticker][
                        ['Open', 'High', 'Low', 'Close', 'Volume']]

                # os.remove('/tmp/' + ticker + '.csv')

    # close/stop browser
    driver.close()
    driver.quit()

    if virt_display:
        display.stop()

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


import pandas_datareader
pandas_datareader.data.get_data_yahoo = get_data_yahoo

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

from . import Ticker, multi


# from collections import namedtuple as _namedtuple


class Tickers:
    """A class that manages multiple stock tickers and provides methods to fetch their data.

    The `Tickers` class allows you to manage and interact with multiple stock tickers, providing
    methods to retrieve historical market data and news.

    Args:
        tickers (str or list): A string of comma-separated ticker symbols or a list of ticker symbols.
        session (optional): A requests session to be used for network requests.

    Methods:
        history(period, interval, start, end, prepost, actions, auto_adjust, repair,
                proxy, threads, group_by, progress, timeout, **kwargs):
            Fetches historical data for the tickers with specified parameters.

        download(period, interval, start, end, prepost, actions, auto_adjust, repair,
                 proxy, threads, group_by, progress, timeout, **kwargs):
            Downloads historical data for the tickers with specified parameters.

        news():
            Retrieves the latest news for each ticker.

    Returns:
        An instance of the `Tickers` class initialized with the given tickers."""

    def __repr__(self):
        """Return a string representation of the yfinance.Tickers object.

    The representation includes a list of the symbols contained within the 
    Tickers object, formatted as a comma-separated string.

    Returns:
        str: A string in the format "yfinance.Tickers object <symbol1,symbol2,...>"."""
        return f"yfinance.Tickers object <{','.join(self.symbols)}>"

    def __init__(self, tickers, session=None):
        """Initializes the Tickers object with given ticker symbols.

    This constructor takes a list of ticker symbols or a string of 
    comma-separated ticker symbols, and creates a dictionary of Ticker 
    objects. Each ticker symbol is converted to uppercase.

    Args:
        tickers (str or list): A list of ticker symbols or a string of 
            comma-separated ticker symbols.
        session (optional): A session object to be used by each Ticker 
            object. Defaults to None.

    Attributes:
        symbols (list): A list of uppercase ticker symbols.
        tickers (dict): A dictionary where keys are ticker symbols and 
            values are Ticker objects initialized with the given session."""
        tickers = tickers if isinstance(
            tickers, list) else tickers.replace(',', ' ').split()
        self.symbols = [ticker.upper() for ticker in tickers]
        self.tickers = {ticker: Ticker(ticker, session=session) for ticker in self.symbols}

        # self.tickers = _namedtuple(
        #     "Tickers", ticker_objects.keys(), rename=True
        # )(*ticker_objects.values())

    def history(self, period="1mo", interval="1d",
                start=None, end=None, prepost=False,
                actions=True, auto_adjust=True, repair=False,
                proxy=None,
                threads=True, group_by='column', progress=True,
                timeout=10, **kwargs):
        """Fetch historical market data for the specified period and interval.

    Args:
        period (str): The timeframe for which to retrieve data, e.g., '1d', '5d', '1mo', '1y'. Default is '1mo'.
        interval (str): The interval between data points, e.g., '1m', '5m', '1h', '1d'. Default is '1d'.
        start (str or None): The start date for fetching data (format: 'YYYY-MM-DD'). Defaults to None.
        end (str or None): The end date for fetching data (format: 'YYYY-MM-DD'). Defaults to None.
        prepost (bool): Whether to include pre-market and post-market data. Default is False.
        actions (bool): Whether to include corporate actions (such as dividends and splits). Default is True.
        auto_adjust (bool): Whether to adjust data for splits and dividends. Default is True.
        repair (bool): Whether to attempt to repair missing data. Default is False.
        proxy (str or None): Proxy server URL if needed. Defaults to None.
        threads (bool): Whether to use threading for data retrieval. Default is True.
        group_by (str): How to group the retrieved data, either by 'column' or 'ticker'. Default is 'column'.
        progress (bool): Whether to display progress while downloading. Default is True.
        timeout (int): Timeout for network requests, in seconds. Default is 10.
        **kwargs: Additional keyword arguments to pass to the download function.

    Returns:
        pandas.DataFrame: A DataFrame containing the requested historical market data."""

        return self.download(
            period, interval,
            start, end, prepost,
            actions, auto_adjust, repair, 
            proxy,
            threads, group_by, progress,
            timeout, **kwargs)

    def download(self, period="1mo", interval="1d",
                 start=None, end=None, prepost=False,
                 actions=True, auto_adjust=True, repair=False, 
                 proxy=None,
                 threads=True, group_by='column', progress=True,
                 timeout=10, **kwargs):
        """Download historical market data for the specified symbols.

    This method retrieves historical market data for the symbols associated with this instance. The data can be customized by specifying the time period, interval, and other parameters. The retrieved data is stored in each symbol's history attribute.

    Args:
        period (str): The time period for which to retrieve data (e.g., "1mo", "1d").
        interval (str): The frequency of the data points (e.g., "1d", "1h").
        start (str or None): The start date for the data in 'YYYY-MM-DD' format. Defaults to None.
        end (str or None): The end date for the data in 'YYYY-MM-DD' format. Defaults to None.
        prepost (bool): Whether to include pre-market and after-market data. Defaults to False.
        actions (bool): Whether to include corporate actions in the data. Defaults to True.
        auto_adjust (bool): Whether to adjust the data for stock splits and dividends. Defaults to True.
        repair (bool): Whether to attempt to repair corrupted data. Defaults to False.
        proxy (str or None): The proxy server to use for the data request. Defaults to None.
        threads (bool): Whether to use multiple threads for downloading data. Defaults to True.
        group_by (str): How to group the downloaded data ('column' or 'ticker'). Defaults to 'column'.
        progress (bool): Whether to display a progress bar during download. Defaults to True.
        timeout (int): The maximum time to wait for a response, in seconds. Defaults to 10.
        **kwargs: Additional keyword arguments to pass to the download function.

    Returns:
        pandas.DataFrame: A DataFrame containing the downloaded market data, organized according to the 'group_by' parameter."""

        data = multi.download(self.symbols,
                              start=start, end=end,
                              actions=actions,
                              auto_adjust=auto_adjust,
                              repair=repair,
                              period=period,
                              interval=interval,
                              prepost=prepost,
                              proxy=proxy,
                              group_by='ticker',
                              threads=threads,
                              progress=progress,
                              timeout=timeout,
                              **kwargs)

        for symbol in self.symbols:
            self.tickers.get(symbol, {})._history = data[symbol]

        if group_by == 'column':
            data.columns = data.columns.swaplevel(0, 1)
            data.sort_index(level=0, axis=1, inplace=True)

        return data

    def news(self):
        """Fetches and returns the latest news for each stock ticker in the symbols list.

    Iterates over each stock ticker provided in the instance's symbols list, retrieves 
    the latest news items for each ticker, and compiles them into a dictionary.

    Returns:
        dict: A dictionary where each key is a stock ticker symbol and each value is a 
        list of news items related to that ticker."""
        return {ticker: [item for item in Ticker(ticker).news] for ticker in self.symbols}

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

# import time as _time
import datetime as _datetime
import requests as _requests
import pandas as _pd
# import numpy as _np

# import json as _json
# import re as _re
from collections import namedtuple as _namedtuple

from .base import TickerBase


class Ticker(TickerBase):
    ismocked = False
    def __repr__(self):
        return 'yfinance.Ticker object <%s>' % self.ticker

    def _download_options(self, date=None, proxy=None):
        if date is None:
            url = "{}/v7/finance/options/{}".format(
                self._base_url, self.ticker)
        else:
            url = "{}/v7/finance/options/{}?date={}".format(
                self._base_url, self.ticker, date)

        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        r = _requests.get(url=url, proxies=proxy).json()
        if r['optionChain']['result']:
            for exp in r['optionChain']['result'][0]['expirationDates']:
                self._expirations[_datetime.datetime.utcfromtimestamp(
                    exp).strftime('%Y-%m-%d')] = exp
            return r['optionChain']['result'][0]['options'][0]
        return {}

    def _options2df(self, opt, tz=None):
        data = _pd.DataFrame(opt).reindex(columns=[
            'contractSymbol',
            'lastTradeDate',
            'strike',
            'lastPrice',
            'bid',
            'ask',
            'change',
            'percentChange',
            'volume',
            'openInterest',
            'impliedVolatility',
            'inTheMoney',
            'contractSize',
            'currency'])

        data['lastTradeDate'] = _pd.to_datetime(
            data['lastTradeDate'], unit='s')
        if tz is not None:
            data['lastTradeDate'] = data['lastTradeDate'].tz_localize(tz)
        return data

    def option_chain(self, date=None, proxy=None, tz=None):
        if date is None:
            options = self._download_options(proxy=proxy)
        else:
            if not self._expirations:
                self._download_options()
            if date not in self._expirations:
                raise ValueError(
                    "Expiration `%s` cannot be found. "
                    "Available expiration are: [%s]" % (
                        date, ', '.join(self._expirations)))
            date = self._expirations[date]
            options = self._download_options(date, proxy=proxy)

        return _namedtuple('Options', ['calls', 'puts'])(**{
            "calls": self._options2df(options['calls'], tz=tz),
            "puts": self._options2df(options['puts'], tz=tz)
        })

    # ------------------------

    @property
    def isin(self):
        return self.get_isin()

    @property
    def major_holders(self):
        return self.get_major_holders()

    @property
    def institutional_holders(self):
        return self.get_institutional_holders()

    @property
    def mutualfund_holders(self):
        return self.get_mutualfund_holders()

    @property
    def dividends(self):
        return self.get_dividends()

    @property
    def splits(self):
        return self.get_splits()

    @property
    def actions(self):
        return self.get_actions()

    @property
    def info(self):
        return self.get_info()

    @property
    def calendar(self):
        return self.get_calendar()

    @property
    def recommendations(self):
        return self.get_recommendations()

    @property
    def earnings(self):
        return self.get_earnings()

    @property
    def quarterly_earnings(self):
        return self.get_earnings(freq='quarterly')

    @property
    def financials(self):
        """
        A getter that returns a Panda Dataframe with the yearly financials for the past 4 years.
        Ticker.py uses relative imports so use this command to run tests: python -m yfinance.ticker -v
        >>> import os
        >>> import dill
        >>> # Adding mock data to isolate our getter from the Ticker constructor :
        >>> msft = dill.load(open("msft.dill", "rb"))
        >>> # Adding mocked flag to isolate our getter from get_fundamentals() :
        >>> msft.ismocked = True
        >>> msft.financials
                                                    2020-06-30      2019-06-30      2018-06-30     2017-06-30
        Research Development                     19269000000.0   16876000000.0   14726000000.0  13037000000.0
        Effect Of Accounting Charges                      None            None            None           None
        Income Before Tax                        53036000000.0   43688000000.0   36474000000.0  29901000000.0
        Minority Interest                                 None            None            None           None
        Net Income                               44281000000.0   39240000000.0   16571000000.0  25489000000.0
        Selling General Administrative           24709000000.0   23098000000.0   22223000000.0  19942000000.0
        Gross Profit                             96937000000.0   82933000000.0   72007000000.0  62310000000.0
        Ebit                                     52959000000.0   42959000000.0   35058000000.0  29331000000.0
        Operating Income                         52959000000.0   42959000000.0   35058000000.0  29331000000.0
        Other Operating Expenses                          None            None            None           None
        Interest Expense                         -2591000000.0   -2686000000.0   -2733000000.0  -2222000000.0
        Extraordinary Items                               None            None            None           None
        Non Recurring                                     None            None            None           None
        Other Items                                       None            None            None           None
        Income Tax Expense                        8755000000.0    4448000000.0   19903000000.0   4412000000.0
        Total Revenue                           143015000000.0  125843000000.0  110360000000.0  96571000000.0
        Total Operating Expenses                 90056000000.0   82884000000.0   75302000000.0  67240000000.0
        Cost Of Revenue                          46078000000.0   42910000000.0   38353000000.0  34261000000.0
        Total Other Income Expense Net              77000000.0     729000000.0    1416000000.0    570000000.0
        Discontinued Operations                           None            None            None           None
        Net Income From Continuing Ops           44281000000.0   39240000000.0   16571000000.0  25489000000.0
        Net Income Applicable To Common Shares   44281000000.0   39240000000.0   16571000000.0  25489000000.0
        """
        # Run method using mocked data without making a call to the Yahoo Finance api (for testing)
        if self.ismocked == True: return self.get_financials(ismocked = True)
        # Run method normally (for production)
        else: return self.get_financials()

    @property
    def quarterly_financials(self):
        return self.get_financials(freq='quarterly')

    @property
    def balance_sheet(self):
        return self.get_balancesheet()

    @property
    def quarterly_balance_sheet(self):
        return self.get_balancesheet(freq='quarterly')

    @property
    def balancesheet(self):
        return self.get_balancesheet()

    @property
    def quarterly_balancesheet(self):
        return self.get_balancesheet(freq='quarterly')

    @property
    def cashflow(self):
        return self.get_cashflow()

    @property
    def quarterly_cashflow(self):
        return self.get_cashflow(freq='quarterly')

    @property
    def sustainability(self):
        return self.get_sustainability()

    @property
    def options(self):
        if not self._expirations:
            self._download_options()
        return tuple(self._expirations.keys())

if __name__ == "__main__":
    import doctest
    doctest.testmod()
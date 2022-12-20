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
import pandas as _pd

from collections import namedtuple as _namedtuple

from .base import TickerBase


class Ticker(TickerBase):
    def __init__(self, ticker, session=None):
        super(Ticker, self).__init__(ticker, session=session)
        self._expirations = {}

    def __repr__(self):
        return 'yfinance.Ticker object <%s>' % self.ticker

    def _download_options(self, date=None, proxy=None):
        if date is None:
            url = "{}/v7/finance/options/{}".format(
                self._base_url, self.ticker)
        else:
            url = "{}/v7/finance/options/{}?date={}".format(
                self._base_url, self.ticker, date)

        r = self._data.get(url=url, proxy=proxy).json()
        if len(r.get('optionChain', {}).get('result', [])) > 0:
            for exp in r['optionChain']['result'][0]['expirationDates']:
                self._expirations[_datetime.datetime.utcfromtimestamp(
                    exp).strftime('%Y-%m-%d')] = exp
            opt = r['optionChain']['result'][0].get('options', [])
            return opt[0] if len(opt) > 0 else []

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
            data['lastTradeDate'], unit='s', utc=True)
        if tz is not None:
            data['lastTradeDate'] = data['lastTradeDate'].dt.tz_convert(tz)
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
    def major_holders(self) -> _pd.DataFrame:
        return self.get_major_holders()

    @property
    def institutional_holders(self) -> _pd.DataFrame:
        return self.get_institutional_holders()

    @property
    def mutualfund_holders(self) -> _pd.DataFrame:
        return self.get_mutualfund_holders()

    @property
    def dividends(self) -> _pd.Series:
        return self.get_dividends()

    @property
    def capital_gains(self):
        return self.get_capital_gains()

    @property
    def splits(self) -> _pd.Series:
        return self.get_splits()

    @property
    def actions(self) -> _pd.DataFrame:
        return self.get_actions()

    @property
    def shares(self) -> _pd.DataFrame :
        return self.get_shares()

    @property
    def info(self) -> dict:
        return self.get_info()

    @property
    def calendar(self) -> _pd.DataFrame:
        return self.get_calendar()

    @property
    def recommendations(self):
        return self.get_recommendations()

    @property
    def earnings(self) -> _pd.DataFrame:
        return self.get_earnings()

    @property
    def quarterly_earnings(self) -> _pd.DataFrame:
        return self.get_earnings(freq='quarterly')

    @property
    def income_stmt(self) -> _pd.DataFrame:
        return self.get_income_stmt(pretty=True)

    @property
    def quarterly_income_stmt(self) -> _pd.DataFrame:
        return self.get_income_stmt(pretty=True, freq='quarterly')

    @property
    def incomestmt(self) -> _pd.DataFrame:
        return self.income_stmt

    @property
    def quarterly_incomestmt(self) -> _pd.DataFrame:
        return self.quarterly_income_stmt

    @property
    def financials(self) -> _pd.DataFrame:
        return self.income_stmt

    @property
    def quarterly_financials(self) -> _pd.DataFrame:
        return self.quarterly_income_stmt

    @property
    def balance_sheet(self) -> _pd.DataFrame:
        return self.get_balance_sheet(pretty=True)

    @property
    def quarterly_balance_sheet(self) -> _pd.DataFrame:
        return self.get_balance_sheet(pretty=True, freq='quarterly')

    @property
    def balancesheet(self) -> _pd.DataFrame:
        return self.balance_sheet

    @property
    def quarterly_balancesheet(self) -> _pd.DataFrame:
        return self.quarterly_balance_sheet

    @property
    def cash_flow(self) -> _pd.DataFrame:
        return self.get_cash_flow(pretty=True, freq="yearly")

    @property
    def quarterly_cash_flow(self) -> _pd.DataFrame:
        return self.get_cash_flow(pretty=True, freq='quarterly')

    @property
    def cashflow(self) -> _pd.DataFrame:
        return self.cash_flow

    @property
    def quarterly_cashflow(self) -> _pd.DataFrame:
        return self.quarterly_cash_flow

    @property
    def recommendations_summary(self):
        return self.get_recommendations_summary()

    @property
    def analyst_price_target(self) -> _pd.DataFrame:
        return self.get_analyst_price_target()

    @property
    def revenue_forecasts(self) -> _pd.DataFrame:
        return self.get_rev_forecast()

    @property
    def sustainability(self) -> _pd.DataFrame:
        return self.get_sustainability()

    @property
    def options(self) -> tuple:
        if not self._expirations:
            self._download_options()
        return tuple(self._expirations.keys())

    @property
    def news(self):
        return self.get_news()

    @property
    def earnings_trend(self) -> _pd.DataFrame:
        return self.get_earnings_trend()

    @property
    def earnings_dates(self) -> _pd.DataFrame:
        return self.get_earnings_dates()

    @property
    def earnings_forecasts(self) -> _pd.DataFrame:
        return self.get_earnings_forecast()

    @property
    def history_metadata(self) -> dict:
        return self.get_history_metadata()

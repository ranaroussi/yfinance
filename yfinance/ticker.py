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

"""Public ticker object exposing Yahoo Finance data endpoints."""

from __future__ import print_function

from collections import namedtuple as _namedtuple
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

import pandas as _pd

from .base import TickerBase
from .const import _BASE_URL_
from .scrapers.funds import FundsData

_DataFrameOrDict = Union[_pd.DataFrame, Dict[Any, Any]]
_OptionalDataFrameOrDict = Optional[_DataFrameOrDict]


class Ticker(TickerBase):
    """Top-level ticker API facade with convenience properties."""

    if TYPE_CHECKING:
        isin: Optional[str]
        major_holders: Optional[_pd.DataFrame]
        institutional_holders: Optional[_pd.DataFrame]
        mutualfund_holders: Optional[_pd.DataFrame]
        insider_purchases: Optional[_pd.DataFrame]
        insider_transactions: Optional[_pd.DataFrame]
        insider_roster_holders: Optional[_pd.DataFrame]
        dividends: _pd.Series
        capital_gains: _pd.Series
        splits: _pd.Series
        actions: _pd.DataFrame
        shares: _pd.DataFrame
        info: Dict[str, Any]
        fast_info: "FastInfo"
        calendar: Dict[str, Any]
        sec_filings: Dict[str, Any]
        recommendations: _pd.DataFrame
        recommendations_summary: _pd.DataFrame
        upgrades_downgrades: _pd.DataFrame
        earnings: Optional[_pd.DataFrame]
        quarterly_earnings: Optional[_pd.DataFrame]
        income_stmt: _pd.DataFrame
        quarterly_income_stmt: _pd.DataFrame
        ttm_income_stmt: _pd.DataFrame
        incomestmt: _pd.DataFrame
        quarterly_incomestmt: _pd.DataFrame
        ttm_incomestmt: _pd.DataFrame
        financials: _pd.DataFrame
        quarterly_financials: _pd.DataFrame
        ttm_financials: _pd.DataFrame
        balance_sheet: _pd.DataFrame
        quarterly_balance_sheet: _pd.DataFrame
        balancesheet: _pd.DataFrame
        quarterly_balancesheet: _pd.DataFrame
        cash_flow: _pd.DataFrame
        quarterly_cash_flow: _pd.DataFrame
        ttm_cash_flow: _pd.DataFrame
        cashflow: _pd.DataFrame
        quarterly_cashflow: _pd.DataFrame
        ttm_cashflow: _pd.DataFrame
        analyst_price_targets: Dict[str, Any]
        earnings_estimate: _pd.DataFrame
        revenue_estimate: _pd.DataFrame
        earnings_history: _pd.DataFrame
        eps_trend: _pd.DataFrame
        eps_revisions: _pd.DataFrame
        growth_estimates: _pd.DataFrame
        sustainability: _pd.DataFrame

    def __init__(self, ticker, session=None):
        """Initialize a ticker object."""
        super().__init__(ticker, session=session)
        self._expirations = {}
        self._underlying = {}

    def __repr__(self):
        return f'yfinance.Ticker object <{self.ticker}>'

    def _download_options(self, date=None):
        """Download option chain data for the provided expiration date."""
        if date is None:
            url = f"{_BASE_URL_}/v7/finance/options/{self.ticker}"
        else:
            url = f"{_BASE_URL_}/v7/finance/options/{self.ticker}?date={date}"

        r = self._data.get(url=url).json()
        if len(r.get('optionChain', {}).get('result', [])) > 0:
            for exp in r['optionChain']['result'][0]['expirationDates']:
                self._expirations[_pd.Timestamp(exp, unit='s').strftime('%Y-%m-%d')] = exp

            self._underlying = r['optionChain']['result'][0].get('quote', {})

            opt = r['optionChain']['result'][0].get('options', [])

            return {**opt[0], "underlying": self._underlying} if len(opt) > 0 else {}
        return {}

    def _options2df(self, opt, tz=None):
        """Convert Yahoo option rows into a normalized DataFrame."""
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

    def option_chain(self, date=None, tz=None):
        """Return option calls, puts, and underlying quote for an expiration."""
        if date is None:
            options = self._download_options()
        else:
            if not self._expirations:
                self._download_options()
            if date not in self._expirations:
                raise ValueError(
                    f"Expiration `{date}` cannot be found. "
                    f"Available expirations are: [{', '.join(self._expirations)}]")
            date = self._expirations[date]
            options = self._download_options(date)

        if not options:
            return _namedtuple('Options', ['calls', 'puts', 'underlying'])(**{
                "calls": None, "puts": None, "underlying": None
            })

        return _namedtuple('Options', ['calls', 'puts', 'underlying'])(**{
            "calls": self._options2df(options['calls'], tz=tz),
            "puts": self._options2df(options['puts'], tz=tz),
            "underlying": options['underlying']
        })

    @property
    def options(self) -> tuple:
        """Return available option expiration dates."""
        if not self._expirations:
            self._download_options()
        return tuple(self._expirations.keys())

    @property
    def news(self) -> list:
        """Return ticker news articles."""
        return self.get_news()

    @property
    def earnings_dates(self) -> Optional[_pd.DataFrame]:
        """Return upcoming and historical earnings dates."""
        return self.get_earnings_dates()

    @property
    def history_metadata(self) -> dict:
        """Return metadata from the latest historical price query."""
        return self.get_history_metadata()

    @property
    def funds_data(self) -> Optional[FundsData]:
        """Return funds metadata for ETF and mutual-fund tickers."""
        return self.get_funds_data()


def _method_property(method_name, doc, *args, **kwargs):
    """Build a property that calls a getter method on ``Ticker``."""

    def _getter(self):
        method = getattr(self, method_name)
        return method(*args, **kwargs)

    return property(_getter, doc=doc)


def _attr_property(attribute_name, doc):
    """Build a property that proxies another ``Ticker`` attribute."""

    def _getter(self):
        return getattr(self, attribute_name)

    return property(_getter, doc=doc)


if TYPE_CHECKING:
    from .scrapers.quote import FastInfo


Ticker.isin = _method_property("get_isin", "Return the ticker ISIN, if available.")
Ticker.major_holders = _method_property("get_major_holders", "Return major holders data.")
Ticker.institutional_holders = _method_property(
    "get_institutional_holders",
    "Return institutional holders data.",
)
Ticker.mutualfund_holders = _method_property(
    "get_mutualfund_holders",
    "Return mutual fund holders data.",
)
Ticker.insider_purchases = _method_property(
    "get_insider_purchases",
    "Return insider purchase data.",
)
Ticker.insider_transactions = _method_property(
    "get_insider_transactions",
    "Return insider transaction data.",
)
Ticker.insider_roster_holders = _method_property(
    "get_insider_roster_holders",
    "Return insider roster holder data.",
)
Ticker.dividends = _method_property("get_dividends", "Return historical dividends.")
Ticker.capital_gains = _method_property("get_capital_gains", "Return historical capital gains.")
Ticker.splits = _method_property("get_splits", "Return historical stock splits.")
Ticker.actions = _method_property("get_actions", "Return historical corporate actions.")
Ticker.shares = _method_property("get_shares", "Return shares outstanding data.")
Ticker.info = _method_property("get_info", "Return ticker information dictionary.")
Ticker.fast_info = _method_property("get_fast_info", "Return lazily fetched fast info.")
Ticker.calendar = _method_property("get_calendar", "Return calendar events for the ticker.")
Ticker.sec_filings = _method_property("get_sec_filings", "Return SEC filing metadata.")
Ticker.recommendations = _method_property(
    "get_recommendations",
    "Return analyst recommendations.",
)
Ticker.recommendations_summary = _method_property(
    "get_recommendations_summary",
    "Return analyst recommendations summary.",
)
Ticker.upgrades_downgrades = _method_property(
    "get_upgrades_downgrades",
    "Return analyst upgrade and downgrade actions.",
)
Ticker.earnings = _method_property("get_earnings", "Return yearly earnings.")
Ticker.quarterly_earnings = _method_property(
    "get_earnings",
    "Return quarterly earnings.",
    freq="quarterly",
)
Ticker.income_stmt = _method_property(
    "get_income_stmt",
    "Return yearly income statement.",
    pretty=True,
)
Ticker.quarterly_income_stmt = _method_property(
    "get_income_stmt",
    "Return quarterly income statement.",
    pretty=True,
    freq="quarterly",
)
Ticker.ttm_income_stmt = _method_property(
    "get_income_stmt",
    "Return trailing income statement.",
    pretty=True,
    freq="trailing",
)
Ticker.incomestmt = _attr_property("income_stmt", "Alias for ``income_stmt``.")
Ticker.quarterly_incomestmt = _attr_property(
    "quarterly_income_stmt",
    "Alias for ``quarterly_income_stmt``.",
)
Ticker.ttm_incomestmt = _attr_property("ttm_income_stmt", "Alias for ``ttm_income_stmt``.")
Ticker.financials = _attr_property("income_stmt", "Alias for ``income_stmt``.")
Ticker.quarterly_financials = _attr_property(
    "quarterly_income_stmt",
    "Alias for ``quarterly_income_stmt``.",
)
Ticker.ttm_financials = _attr_property("ttm_income_stmt", "Alias for ``ttm_income_stmt``.")
Ticker.balance_sheet = _method_property(
    "get_balance_sheet",
    "Return yearly balance sheet.",
    pretty=True,
)
Ticker.quarterly_balance_sheet = _method_property(
    "get_balance_sheet",
    "Return quarterly balance sheet.",
    pretty=True,
    freq="quarterly",
)
Ticker.balancesheet = _attr_property("balance_sheet", "Alias for ``balance_sheet``.")
Ticker.quarterly_balancesheet = _attr_property(
    "quarterly_balance_sheet",
    "Alias for ``quarterly_balance_sheet``.",
)
Ticker.cash_flow = _method_property(
    "get_cash_flow",
    "Return yearly cash flow statement.",
    pretty=True,
    freq="yearly",
)
Ticker.quarterly_cash_flow = _method_property(
    "get_cash_flow",
    "Return quarterly cash flow statement.",
    pretty=True,
    freq="quarterly",
)
Ticker.ttm_cash_flow = _method_property(
    "get_cash_flow",
    "Return trailing cash flow statement.",
    pretty=True,
    freq="trailing",
)
Ticker.cashflow = _attr_property("cash_flow", "Alias for ``cash_flow``.")
Ticker.quarterly_cashflow = _attr_property(
    "quarterly_cash_flow",
    "Alias for ``quarterly_cash_flow``.",
)
Ticker.ttm_cashflow = _attr_property("ttm_cash_flow", "Alias for ``ttm_cash_flow``.")
Ticker.analyst_price_targets = _method_property(
    "get_analyst_price_targets",
    "Return analyst price target summary.",
)
Ticker.earnings_estimate = _method_property(
    "get_earnings_estimate",
    "Return earnings estimates.",
)
Ticker.revenue_estimate = _method_property(
    "get_revenue_estimate",
    "Return revenue estimates.",
)
Ticker.earnings_history = _method_property(
    "get_earnings_history",
    "Return earnings history.",
)
Ticker.eps_trend = _method_property("get_eps_trend", "Return EPS trend data.")
Ticker.eps_revisions = _method_property("get_eps_revisions", "Return EPS revision data.")
Ticker.growth_estimates = _method_property(
    "get_growth_estimates",
    "Return growth estimates.",
)
Ticker.sustainability = _method_property("get_sustainability", "Return sustainability data.")

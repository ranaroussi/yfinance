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

"""Core ticker implementation used by the public ``Ticker`` wrapper."""

from __future__ import print_function

import json as _json
from io import StringIO
from typing import Optional, Union, cast
from urllib.parse import quote as urlencode

from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from curl_cffi import requests
from frozendict import frozendict

from . import cache, utils
from .config import YF_CONFIG as YfConfig
from .const import _BASE_URL_, _MIC_TO_YAHOO_SUFFIX, _QUERY1_URL_, _ROOT_URL_
from .data import YfData
from .exceptions import YFDataException, YFEarningsDateMissing
from .live import WebSocket
from .scrapers.analysis import Analysis
from .scrapers.fundamentals import Fundamentals
from .scrapers.funds import FundsData
from .scrapers.history import PriceHistory
from .scrapers.holders import Holders
from .scrapers.quote import FastInfo, Quote

_TZ_INFO_FETCH_CTR = {"count": 0}


class TickerBase:
    """Internal base class that provides all data access methods for a ticker."""

    def __init__(self, ticker, session=None):
        """
        Initialize a Yahoo Finance Ticker object.

        Args:
            ticker (str | tuple[str, str]):
                Yahoo Finance symbol (e.g. "AAPL")
                or a tuple of (symbol, MIC) e.g. ('OR','XPAR')
                (MIC = market identifier code)

            session (requests.Session, optional):
                Custom requests session.
        """
        if isinstance(ticker, tuple):
            if len(ticker) != 2:
                raise ValueError("Ticker tuple must be (symbol, mic_code)")
            base_symbol, mic_code = ticker
            # ticker = yahoo_ticker(base_symbol, mic_code)
            if mic_code.startswith('.'):
                mic_code = mic_code[1:]
            if mic_code.upper() not in _MIC_TO_YAHOO_SUFFIX:
                raise ValueError(f"Unknown MIC code: '{mic_code}'")
            sfx = _MIC_TO_YAHOO_SUFFIX[mic_code.upper()]
            if sfx != '':
                ticker = f'{base_symbol}.{sfx}'
            else:
                ticker = base_symbol

        if not isinstance(ticker, str):
            raise ValueError("Ticker symbol must be a string")
        self.ticker: str = ticker.upper()
        self.session = session or requests.Session(impersonate="chrome")
        self._tz = None

        self._isin = None
        self._news = []
        self._shares = None

        self._earnings_dates = {}

        self._earnings = None
        self._financials = None

        # raise an error if user tries to give empty ticker
        if self.ticker == "":
            raise ValueError("Empty ticker name")

        self._data: YfData = YfData(session=session)

        # accept isin as ticker
        if utils.is_isin(self.ticker):
            isin = self.ticker
            c = cache.get_isin_cache()
            resolved_ticker = c.lookup(isin)
            if not resolved_ticker:
                resolved_ticker = utils.get_ticker_by_isin(isin)
            if not resolved_ticker:
                raise ValueError(f"Invalid ISIN number: {isin}")
            self.ticker = resolved_ticker
            c.store(isin, self.ticker)

        # self._price_history = PriceHistory(self._data, self.ticker)
        self._price_history = None  # lazy-load
        self._analysis = Analysis(self._data, self.ticker)
        self._holders = Holders(self._data, self.ticker)
        self._quote = Quote(self._data, self.ticker)
        self._fundamentals = Fundamentals(self._data, self.ticker)
        self._funds_data = None

        self._fast_info = None

        self._message_handler = None
        self.ws = None

    @utils.log_indent_decorator
    def history(self, *args, **kwargs) -> pd.DataFrame:
        """Return price history for the ticker."""
        return self._lazy_load_price_history().history(*args, **kwargs)

    # ------------------------

    def _lazy_load_price_history(self):
        """Instantiate and cache ``PriceHistory`` on first use."""
        if self._price_history is None:
            self._price_history = PriceHistory(
                self._data,
                self.ticker,
                self._get_ticker_tz(timeout=10),
            )
        return self._price_history

    def _get_ticker_tz(self, timeout):
        """Resolve and cache the ticker exchange timezone."""
        if self._tz is not None:
            return self._tz
        c = cache.get_tz_cache()
        tz = c.lookup(self.ticker)

        if tz is not None and (not isinstance(tz, str) or not utils.is_valid_timezone(tz)):
            # Clear from cache and force re-fetch
            c.store(self.ticker, None)
            tz = None

        if tz is None:
            tz = self._fetch_ticker_tz(timeout)
            if tz is None:
                # _fetch_ticker_tz works in 99.999% of cases.
                # For rare fail get from info.
                if _TZ_INFO_FETCH_CTR["count"] < 2:
                    # ... but limit. If _fetch_ticker_tz() always
                    # failing then bigger problem.
                    _TZ_INFO_FETCH_CTR["count"] += 1
                    info = self._quote.info
                    for k in ['exchangeTimezoneName', 'timeZoneFullName']:
                        value = info.get(k)
                        if isinstance(value, str):
                            tz = value
                            break
            if isinstance(tz, str) and utils.is_valid_timezone(tz):
                c.store(self.ticker, tz)
            else:
                tz = None

        self._tz = tz
        return tz

    @utils.log_indent_decorator
    def _fetch_ticker_tz(self, timeout):
        """Fetch exchange timezone directly from Yahoo chart metadata."""
        # Query Yahoo for fast price data just to get returned timezone
        logger = utils.get_yf_logger()

        params = frozendict({"range": "1d", "interval": "1d"})

        # Getting data from json
        url = f"{_BASE_URL_}/v8/finance/chart/{self.ticker}"

        try:
            data = self._data.cache_get(url=url, params=params, timeout=timeout)
            data = data.json()
        except (
            _json.JSONDecodeError,
            requests.exceptions.RequestException,
            AttributeError,
            TypeError,
            ValueError,
        ) as error:
            if not YfConfig.debug.hide_exceptions:
                raise
            logger.error("Failed to get ticker '%s' reason: %s", self.ticker, error)
            return None
        error = data.get('chart', {}).get('error', None)
        if error:
            # explicit error from yahoo API
            logger.debug(
                "Got error from yahoo api for ticker %s, Error: %s",
                self.ticker,
                error,
            )
            return None

        try:
            return data["chart"]["result"][0]["meta"]["exchangeTimezoneName"]
        except (IndexError, KeyError, TypeError) as error:
            if not YfConfig.debug.hide_exceptions:
                raise
            logger.error(
                "Could not get exchangeTimezoneName for ticker '%s' reason: %s",
                self.ticker,
                error,
            )
            logger.debug("Got response: ")
            logger.debug("-------------")
            logger.debug(" %s", data)
            logger.debug("-------------")
        return None

    def get_recommendations(self, as_dict=False):
        """
        Returns a DataFrame with the recommendations
        Columns: period  strongBuy  buy  hold  sell  strongSell
        """
        data = self._quote.recommendations
        if as_dict:
            return data.to_dict()
        return data

    def get_recommendations_summary(self, as_dict=False):
        """Return recommendation table alias."""
        return self.get_recommendations(as_dict=as_dict)

    def get_upgrades_downgrades(self, as_dict=False):
        """
        Returns a DataFrame with the recommendations changes (upgrades/downgrades)
        Index: date of grade
        Columns: firm toGrade fromGrade action
        """
        data = self._quote.upgrades_downgrades
        if as_dict:
            return data.to_dict()
        return data

    def get_calendar(self) -> dict:
        """Return upcoming events, earnings, and dividend dates."""
        return self._quote.calendar

    def get_sec_filings(self) -> dict:
        """Return SEC filings metadata."""
        return self._quote.sec_filings

    def get_major_holders(self, as_dict=False):
        """Return major holders data."""
        data = self._holders.major
        if as_dict:
            return data.to_dict()
        return data

    def get_institutional_holders(self, as_dict=False):
        """Return institutional holders data."""
        data = self._holders.institutional
        if data is not None:
            if as_dict:
                return data.to_dict()
            return data
        return None

    def get_mutualfund_holders(self, as_dict=False):
        """Return mutual fund holders data."""
        data = self._holders.mutualfund
        if data is not None:
            if as_dict:
                return data.to_dict()
            return data
        return None

    def get_insider_purchases(self, as_dict=False):
        """Return insider purchase transactions."""
        data = self._holders.insider_purchases
        if data is not None:
            if as_dict:
                return data.to_dict()
            return data
        return None

    def get_insider_transactions(self, as_dict=False):
        """Return insider transaction history."""
        data = self._holders.insider_transactions
        if data is not None:
            if as_dict:
                return data.to_dict()
            return data
        return None

    def get_insider_roster_holders(self, as_dict=False):
        """Return insider roster holders."""
        data = self._holders.insider_roster
        if data is not None:
            if as_dict:
                return data.to_dict()
            return data
        return None

    def get_info(self) -> dict:
        """Return quote summary information."""
        data = self._quote.info
        return data

    def get_fast_info(self):
        """Return lazily populated fast-info object."""
        if self._fast_info is None:
            self._fast_info = FastInfo(self)
        return self._fast_info

    def get_sustainability(self, as_dict=False):
        """Return sustainability scores and metadata."""
        data = self._quote.sustainability
        if as_dict:
            return data.to_dict()
        return data

    def get_analyst_price_targets(self) -> dict:
        """
        Keys:   current  low  high  mean  median
        """
        data = self._analysis.analyst_price_targets
        return data

    def get_earnings_estimate(self, as_dict=False):
        """
        Index:      0q  +1q  0y  +1y
        Columns:    numberOfAnalysts  avg  low  high  yearAgoEps  growth
        """
        data = self._analysis.earnings_estimate
        return data.to_dict() if as_dict else data

    def get_revenue_estimate(self, as_dict=False):
        """
        Index:      0q  +1q  0y  +1y
        Columns:    numberOfAnalysts  avg  low  high  yearAgoRevenue  growth
        """
        data = self._analysis.revenue_estimate
        return data.to_dict() if as_dict else data

    def get_earnings_history(self, as_dict=False):
        """
        Index:      pd.DatetimeIndex
        Columns:    epsEstimate  epsActual  epsDifference  surprisePercent
        """
        data = self._analysis.earnings_history
        return data.to_dict() if as_dict else data

    def get_eps_trend(self, as_dict=False):
        """
        Index:      0q  +1q  0y  +1y
        Columns:    current  7daysAgo  30daysAgo  60daysAgo  90daysAgo
        """

        data = self._analysis.eps_trend
        return data.to_dict() if as_dict else data

    def get_eps_revisions(self, as_dict=False):
        """
        Index:      0q  +1q  0y  +1y
        Columns:    upLast7days  upLast30days  downLast7days  downLast30days
        """

        data = self._analysis.eps_revisions
        return data.to_dict() if as_dict else data

    def get_growth_estimates(self, as_dict=False):
        """
        Index:      0q  +1q  0y  +1y +5y -5y
        Columns:    stock  industry  sector  index
        """

        data = self._analysis.growth_estimates
        return data.to_dict() if as_dict else data

    def get_earnings(self, as_dict=False, freq="yearly"):
        """
        :Parameters:
            as_dict: bool
                Return table as Python dict
                Default is False
            freq: str
                "yearly" or "quarterly" or "trailing"
                Default is "yearly"
        """

        if self._fundamentals.earnings is None:
            return None
        data = self._fundamentals.earnings[freq]
        if as_dict:
            dict_data = data.to_dict()
            currency = "USD"
            if isinstance(self._earnings, dict):
                maybe_currency = self._earnings.get("financialCurrency")
                if isinstance(maybe_currency, str):
                    currency = maybe_currency
            dict_data['financialCurrency'] = currency
            return dict_data
        return data

    def get_income_stmt(self, as_dict=False, pretty=False, freq="yearly"):
        """
        :Parameters:
            as_dict: bool
                Return table as Python dict
                Default is False
            pretty: bool
                Format row names nicely for readability
                Default is False
            freq: str
                "yearly" or "quarterly" or "trailing"
                Default is "yearly"
        """

        data = self._fundamentals.financials.get_income_time_series(freq=freq)

        if pretty:
            data = data.copy()
            data.index = utils.camel2title(
                data.index.tolist(),
                sep=' ',
                acronyms=["EBIT", "EBITDA", "EPS", "NI"],
            )
        if as_dict:
            return data.to_dict()
        return data

    def get_incomestmt(self, as_dict=False, pretty=False, freq="yearly"):
        """Alias for :meth:`get_income_stmt`."""
        return self.get_income_stmt(as_dict, pretty, freq)

    def get_financials(self, as_dict=False, pretty=False, freq="yearly"):
        """Alias for :meth:`get_income_stmt`."""
        return self.get_income_stmt(as_dict, pretty, freq)

    def get_balance_sheet(self, as_dict=False, pretty=False, freq="yearly"):
        """
        :Parameters:
            as_dict: bool
                Return table as Python dict
                Default is False
            pretty: bool
                Format row names nicely for readability
                Default is False
            freq: str
                "yearly" or "quarterly"
                Default is "yearly"
        """


        data = self._fundamentals.financials.get_balance_sheet_time_series(freq=freq)

        if pretty:
            data = data.copy()
            data.index = utils.camel2title(data.index.tolist(), sep=' ', acronyms=["PPE"])
        if as_dict:
            return data.to_dict()
        return data

    def get_balancesheet(self, as_dict=False, pretty=False, freq="yearly"):
        """Alias for :meth:`get_balance_sheet`."""
        return self.get_balance_sheet(as_dict, pretty, freq)

    def get_cash_flow(
        self,
        as_dict=False,
        pretty=False,
        freq="yearly",
    ) -> Union[pd.DataFrame, dict]:
        """
        :Parameters:
            as_dict: bool
                Return table as Python dict
                Default is False
            pretty: bool
                Format row names nicely for readability
                Default is False
            freq: str
                "yearly" or "quarterly"
                Default is "yearly"
        """


        data = self._fundamentals.financials.get_cash_flow_time_series(freq=freq)

        if pretty:
            data = data.copy()
            data.index = utils.camel2title(data.index.tolist(), sep=' ', acronyms=["PPE"])
        if as_dict:
            return data.to_dict()
        return data

    def get_cashflow(self, as_dict=False, pretty=False, freq="yearly"):
        """Alias for :meth:`get_cash_flow`."""
        return self.get_cash_flow(as_dict, pretty, freq)

    def get_dividends(self, period="max") -> pd.Series:
        """Return dividend history for the requested period."""
        return self._lazy_load_price_history().get_dividends(period=period)

    def get_capital_gains(self, period="max") -> pd.Series:
        """Return capital gains history for the requested period."""
        return self._lazy_load_price_history().get_capital_gains(period=period)

    def get_splits(self, period="max") -> pd.Series:
        """Return stock split history for the requested period."""
        return self._lazy_load_price_history().get_splits(period=period)

    def get_actions(self, period="max") -> pd.DataFrame:
        """Return action history (dividends and splits)."""
        return self._lazy_load_price_history().get_actions(period=period)

    def get_shares(self, as_dict=False) -> Union[pd.DataFrame, dict]:
        """Return yearly shares outstanding data."""
        data = self._fundamentals.shares
        if as_dict:
            return data.to_dict()
        return data

    def _parse_user_datetime(self, value, tz):
        parser = getattr(utils, "_parse_user_dt")
        return parser(value, tz)

    def _resolve_shares_date_bounds(self, start, end, tz, logger):
        end_value = self._parse_user_datetime(end, tz) if end is not None else None
        start_value = self._parse_user_datetime(start, tz) if start is not None else None
        if end_value is None:
            end_value = pd.Timestamp.now("UTC").tz_convert(tz)
        if start_value is None:
            start_value = end_value - pd.Timedelta(days=548)  # 18 months
        if start_value >= end_value:
            logger.error("Start date must be before end")
            return None
        start_value = start_value.floor("D")
        end_value = end_value.ceil("D")
        if pd.isna(start_value) or pd.isna(end_value):
            logger.error("Failed to parse start/end date")
            return None
        return cast(pd.Timestamp, start_value), cast(pd.Timestamp, end_value)

    def _build_shares_url(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> str:
        ts_url_base = (
            "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/"
            f"{self.ticker}?symbol={self.ticker}"
        )
        return (
            f"{ts_url_base}&period1={int(start_date.timestamp())}"
            f"&period2={int(end_date.timestamp())}"
        )

    def _fetch_shares_payload(self, shares_url: str, logger):
        try:
            response = self._data.cache_get(url=shares_url)
            json_data = response.json()
        except (_json.JSONDecodeError, AttributeError, requests.exceptions.RequestException):
            if not YfConfig.debug.hide_exceptions:
                raise
            logger.error("%s: Yahoo web request for share count failed", self.ticker)
            return None

        is_bad_request = json_data.get("finance", {}).get("error", {}).get("code") == "Bad Request"
        if is_bad_request:
            if not YfConfig.debug.hide_exceptions:
                raise requests.exceptions.HTTPError(
                    "Yahoo web request for share count returned 'Bad Request'"
                )
            logger.error("%s: Yahoo web request for share count failed", self.ticker)
            return None
        return json_data

    def _extract_shares_series(self, json_data, tz, logger):
        shares_data = json_data.get("timeseries", {}).get("result", [])
        if not shares_data or "shares_out" not in shares_data[0]:
            return None
        try:
            shares_series = pd.Series(
                shares_data[0]["shares_out"],
                index=pd.to_datetime(shares_data[0]["timestamp"], unit="s"),
            )
        except (KeyError, TypeError, ValueError) as error:
            if not YfConfig.debug.hide_exceptions:
                raise
            logger.error("%s: Failed to parse shares count data: %s", self.ticker, error)
            return None

        if isinstance(shares_series.index, pd.DatetimeIndex):
            dt_index = shares_series.index
        else:
            dt_index = pd.DatetimeIndex(shares_series.index)
        shares_series.index = dt_index.tz_localize(tz)
        return shares_series.sort_index()

    @utils.log_indent_decorator
    def get_shares_full(self, start=None, end=None):
        """Return daily shares outstanding over a date range."""
        logger = utils.get_yf_logger()
        tz = self._get_ticker_tz(timeout=10)
        date_bounds = self._resolve_shares_date_bounds(start, end, tz, logger)
        if date_bounds is None:
            return None

        start_date, end_date = date_bounds
        shares_url = self._build_shares_url(start_date, end_date)
        json_data = self._fetch_shares_payload(shares_url, logger)
        if json_data is None:
            return None
        return self._extract_shares_series(json_data, tz, logger)

    def get_isin(self) -> Optional[str]:
        """Return ISIN for the ticker when available."""
        # *** experimental ***
        if self._isin is not None:
            return self._isin

        ticker = self.ticker.upper()

        if "-" in ticker or "^" in ticker:
            self._isin = '-'
            return self._isin

        q = ticker

        if self._quote.info is None:
            # Don't print error message cause self._quote.info will print one
            return None
        if "shortName" in self._quote.info:
            q = self._quote.info['shortName']

        url = (
            "https://markets.businessinsider.com/ajax/SearchController_Suggest"
            f"?max_results=25&query={urlencode(q)}"
        )
        data = self._data.cache_get(url=url).text

        search_str = f'"{ticker}|'
        if search_str not in data:
            if q.lower() in data.lower():
                search_str = '"|'
                if search_str not in data:
                    self._isin = '-'
                    return self._isin
            else:
                self._isin = '-'
                return self._isin

        self._isin = data.split(search_str)[1].split('"')[0].split('|')[0]
        return self._isin

    def get_news(self, count=10, tab="news") -> list:
        """Allowed options for tab: "news", "all", "press releases"""
        if self._news:
            return self._news

        logger = utils.get_yf_logger()

        tab_queryrefs = {
            "all": "newsAll",
            "news": "latestNews",
            "press releases": "pressRelease",
        }

        query_ref = tab_queryrefs.get(tab.lower())
        if not query_ref:
            valid_tabs = ", ".join(tab_queryrefs.keys())
            raise ValueError(f"Invalid tab name '{tab}'. Choose from: {valid_tabs}")

        url = f"{_ROOT_URL_}/xhr/ncp?queryRef={query_ref}&serviceKey=ncp_fin"
        payload = {
            "serviceConfig": {
                "snippetCount": count,
                "s": [self.ticker]
            }
        }

        data = self._data.post(url, body=payload)
        if data is None or "Will be right back" in data.text:
            raise YFDataException("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***")
        try:
            data = data.json()
        except _json.JSONDecodeError:
            if not YfConfig.debug.hide_exceptions:
                raise
            logger.error(
                "%s: Failed to retrieve the news and received faulty response instead.",
                self.ticker,
            )
            data = {}

        news = data.get("data", {}).get("tickerStream", {}).get("stream", [])

        self._news = [article for article in news if not article.get('ad', [])]
        return self._news

    def get_earnings_dates(self, limit=12, offset=0) -> Optional[pd.DataFrame]:
        """Return upcoming and historical earnings dates."""
        if limit > 100:
            raise ValueError("Yahoo caps limit at 100")

        if self._earnings_dates and limit in self._earnings_dates:
            return self._earnings_dates[limit]

        df = self._get_earnings_dates_using_scrape(limit, offset)
        self._earnings_dates[limit] = df
        return df

    @utils.log_indent_decorator
    def _get_earnings_dates_using_scrape(self, limit=12, offset=0) -> Optional[pd.DataFrame]:
        """
        Uses YfData.cache_get() to scrape earnings data from YahooFinance.
        (https://finance.yahoo.com/calendar/earnings?symbol=INTC)

        Args:
            limit (int): Number of rows to extract (max=100)
            offset (int): if 0, search from future EPS estimates.
                          if 1, search from the most recent EPS.
                          if x, search from x'th recent EPS.

        Returns:
            pd.DataFrame in the following format.

                       EPS Estimate Reported EPS Surprise(%)
            Date
            2025-10-30         2.97            -           -
            2025-07-22         1.73         1.54      -10.88
            2025-05-06         2.63          2.7        2.57
            2025-02-06         2.09         2.42       16.06
            2024-10-31         1.92         1.55      -19.36
            ...                 ...          ...         ...
            2014-07-31         0.61         0.65        7.38
            2014-05-01         0.55         0.68       22.92
            2014-02-13         0.55         0.58        6.36
            2013-10-31         0.51         0.54        6.86
            2013-08-01         0.46          0.5        7.86
        """
        #####################################################
        # Define Constants
        #####################################################
        if 0 < limit <= 25:
            size = 25
        elif 25 < limit <= 50:
            size = 50
        elif 50 < limit <= 100:
            size = 100
        else:
            raise ValueError("Please use limit <= 100")

        # Define the URL
        url = (
            f"https://finance.yahoo.com/calendar/earnings?symbol={self.ticker}"
            f"&offset={offset}&size={size}"
        )
        #####################################################
        # Get data
        #####################################################
        response = self._data.cache_get(url)

        #####################################################
        # Response -> pd.DataFrame
        #####################################################
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")
        # This page should have only one <table>
        table = soup.find("table")
        # If the table is found
        if table:
            # Get the HTML string of the table
            table_html = str(table)

            # Wrap the HTML string in a StringIO object
            html_stringio = StringIO(table_html)

            # Pass the StringIO object to pd.read_html()
            df = pd.read_html(html_stringio, na_values=['-'])[0]

            # Drop redundant columns
            df = df.drop(["Symbol", "Company"], axis=1)

            # Backwards compatibility
            df.rename(columns={'Surprise (%)': 'Surprise(%)'}, inplace=True)

            df = df.dropna(subset="Earnings Date")

            # Parse earnings date
            # - Pandas doesn't like EDT, EST
            df['Earnings Date'] = df['Earnings Date'].str.replace('EDT', 'America/New_York')
            df['Earnings Date'] = df['Earnings Date'].str.replace('EST', 'America/New_York')
            # - separate timezone string (last word)
            date_parts = df['Earnings Date'].str.rsplit(' ', n=1, expand=True)
            df['Earnings Date'] = pd.to_datetime(date_parts[0], format='%B %d, %Y at %I %p')
            df['Earnings Date'] = pd.Series(
                [dt.tz_localize(tz_name) for dt, tz_name in zip(df['Earnings Date'], date_parts[1])]
            )
            df = df.set_index("Earnings Date")

        else:
            err_msg = "No earnings dates found, symbol may be delisted"
            logger = utils.get_yf_logger()
            logger.error("%s: %s", self.ticker, err_msg)
            return None
        return df

    @utils.log_indent_decorator
    def _get_earnings_dates_using_screener(self, limit=12) -> Optional[pd.DataFrame]:
        """
        Get earning dates (future and historic)

        In Summer 2025, Yahoo stopped updating the data at this endpoint.
        So reverting to scraping HTML.

        Args:
            limit (int): max amount of upcoming and recent earnings dates to return.
                Default value 12 should return next 4 quarters and last 8 quarters.
                Increase if more history is needed.
        Returns:
            pd.DataFrame
        """
        logger = utils.get_yf_logger()

        # Fetch data
        url = f"{_QUERY1_URL_}/v1/finance/visualization"
        params = {"lang": "en-US", "region": "US"}
        body = {
            "size": limit,
            "query": {"operator": "eq", "operands": ["ticker", self.ticker]},
            "sortField": "startdatetime",
            "sortType": "DESC",
            "entityIdType": "earnings",
            "includeFields": [
                "startdatetime",
                "timeZoneShortName",
                "epsestimate",
                "epsactual",
                "epssurprisepct",
                "eventtype",
            ],
        }
        response = self._data.post(url, params=params, body=body)
        json_data = response.json()

        # Extract data
        columns = [
            row['label']
            for row in json_data['finance']['result'][0]['documents'][0]['columns']
        ]
        rows = json_data['finance']['result'][0]['documents'][0]['rows']
        df = pd.DataFrame(rows, columns=columns)

        if df.empty:
            _exception = YFEarningsDateMissing(self.ticker)
            err_msg = str(_exception)
            logger.error("%s: %s", self.ticker, err_msg)
            return None

        # Convert eventtype
        # - 1 = earnings call (manually confirmed)
        # - 2 = earnings report
        # - 11 = stockholders meeting (manually confirmed)
        df['Event Type'] = df['Event Type'].replace('^1$', 'Call', regex=True)
        df['Event Type'] = df['Event Type'].replace('^2$', 'Earnings', regex=True)
        df['Event Type'] = df['Event Type'].replace('^11$', 'Meeting', regex=True)

        # Calculate earnings date
        df['Earnings Date'] = pd.to_datetime(df['Event Start Date'])
        tz = self._get_ticker_tz(timeout=30)
        if df['Earnings Date'].dt.tz is None:
            df['Earnings Date'] = df['Earnings Date'].dt.tz_localize(tz)
        else:
            df['Earnings Date'] = df['Earnings Date'].dt.tz_convert(tz)

        # Convert types
        columns_to_update = ['Surprise (%)', 'EPS Estimate', 'Reported EPS']
        df[columns_to_update] = df[columns_to_update].astype('float64').replace(0.0, np.nan)

        # Format the dataframe
        df.drop(['Event Start Date', 'Timezone short name'], axis=1, inplace=True)
        df.set_index('Earnings Date', inplace=True)
        df.rename(columns={'Surprise (%)': 'Surprise(%)'}, inplace=True)  # Compatibility

        self._earnings_dates[limit] = df
        return df

    def get_history_metadata(self) -> dict:
        """Return metadata associated with historical price responses."""
        return self._lazy_load_price_history().get_history_metadata()

    def get_funds_data(self) -> Optional[FundsData]:
        """Return funds metadata for ETF and mutual-fund symbols."""
        if not self._funds_data:
            self._funds_data = FundsData(self._data, self.ticker)

        return self._funds_data

    def live(self, message_handler=None, verbose=True):
        """Open a synchronous live stream for the ticker."""
        self._message_handler = message_handler

        self.ws = WebSocket(verbose=verbose)
        self.ws.subscribe(self.ticker)
        self.ws.listen(self._message_handler)

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

import json as _json
from datetime import datetime, timezone
from io import StringIO
from typing import Optional, Union
from urllib.parse import quote as urlencode
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
from bs4 import BeautifulSoup
from curl_cffi import requests

from . import cache, utils
from .config import YfConfig
from .const import _BASE_URL_, _MIC_TO_YAHOO_SUFFIX, _QUERY1_URL_, _ROOT_URL_
from .data import YfData
from .exceptions import YFDataException, YFEarningsDateMissing, YFRateLimitError
from .live import WebSocket
from .scrapers.analysis import Analysis
from .scrapers.fundamentals import Fundamentals
from .scrapers.funds import FundsData
from .scrapers.history import PriceHistory
from .scrapers.holders import Holders
from .scrapers.quote import FastInfo, Quote

_tz_info_fetch_ctr = 0


class TickerBase:
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
            if mic_code.startswith("."):
                mic_code = mic_code[1:]
            if mic_code.upper() not in _MIC_TO_YAHOO_SUFFIX:
                raise ValueError(f"Unknown MIC code: '{mic_code}'")
            sfx = _MIC_TO_YAHOO_SUFFIX[mic_code.upper()]
            if sfx != "":
                ticker = f"{base_symbol}.{sfx}"
            else:
                ticker = base_symbol

        self.ticker = ticker.upper()
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
            self.ticker = c.lookup(isin)
            if not self.ticker:
                self.ticker = utils.get_ticker_by_isin(isin)
            if self.ticker == "":
                raise ValueError(f"Invalid ISIN number: {isin}")
            if self.ticker:
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
    def history(self, *args, **kwargs) -> pl.DataFrame:
        return self._lazy_load_price_history().history(*args, **kwargs)

    # ------------------------

    def _lazy_load_price_history(self):
        if self._price_history is None:
            self._price_history = PriceHistory(
                self._data, self.ticker, self._get_ticker_tz(timeout=10)
            )
        return self._price_history

    def _get_ticker_tz(self, timeout):
        if self._tz is not None:
            return self._tz
        c = cache.get_tz_cache()
        tz = c.lookup(self.ticker)

        if tz and not utils.is_valid_timezone(tz):
            # Clear from cache and force re-fetch
            c.store(self.ticker, None)
            tz = None

        if tz is None:
            tz = self._fetch_ticker_tz(timeout)
            if tz is None:
                # _fetch_ticker_tz works in 99.999% of cases.
                # For rare fail get from info.
                global _tz_info_fetch_ctr
                if _tz_info_fetch_ctr < 2:
                    # ... but limit. If _fetch_ticker_tz() always
                    # failing then bigger problem.
                    _tz_info_fetch_ctr += 1
                    for k in ["exchangeTimezoneName", "timeZoneFullName"]:
                        if k in self.info:
                            tz = self.info[k]
                            break
            if utils.is_valid_timezone(tz):
                c.store(self.ticker, tz)
            else:
                tz = None

        self._tz = tz
        return tz

    @utils.log_indent_decorator
    def _fetch_ticker_tz(self, timeout):
        # Query Yahoo for fast price data just to get returned timezone
        logger = utils.get_yf_logger()

        params = {"range": "1d", "interval": "1d"}

        # Getting data from json
        url = f"{_BASE_URL_}/v8/finance/chart/{self.ticker}"

        try:
            data = self._data.cache_get(url=url, params=params, timeout=timeout)
            data = data.json()
        except YFRateLimitError:
            # Must propagate this
            raise
        except Exception as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            logger.error(f"Failed to get ticker '{self.ticker}' reason: {e}")
            return None
        else:
            error = data.get("chart", {}).get("error", None)
            if error:
                # explicit error from yahoo API
                logger.debug(
                    f"Got error from yahoo api for ticker {self.ticker}, Error: {error}"
                )
            else:
                try:
                    return data["chart"]["result"][0]["meta"]["exchangeTimezoneName"]
                except Exception as err:
                    if not YfConfig.debug.hide_exceptions:
                        raise
                    logger.error(
                        f"Could not get exchangeTimezoneName for ticker '{self.ticker}' reason: {err}"
                    )
                    logger.debug("Got response: ")
                    logger.debug("-------------")
                    logger.debug(f" {data}")
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
        return self._quote.calendar

    def get_sec_filings(self) -> dict:
        return self._quote.sec_filings

    def get_major_holders(self, as_dict=False):
        data = self._holders.major
        if as_dict:
            return data.to_dict()
        return data

    def get_institutional_holders(self, as_dict=False):
        data = self._holders.institutional
        if data is not None:
            if as_dict:
                return data.to_dict()
            return data

    def get_mutualfund_holders(self, as_dict=False):
        data = self._holders.mutualfund
        if data is not None:
            if as_dict:
                return data.to_dict()
            return data

    def get_insider_purchases(self, as_dict=False):
        data = self._holders.insider_purchases
        if data is not None:
            if as_dict:
                return data.to_dict()
            return data

    def get_insider_transactions(self, as_dict=False):
        data = self._holders.insider_transactions
        if data is not None:
            if as_dict:
                return data.to_dict()
            return data

    def get_insider_roster_holders(self, as_dict=False):
        data = self._holders.insider_roster
        if data is not None:
            if as_dict:
                return data.to_dict()
            return data

    def get_info(self) -> dict:
        data = self._quote.info
        return data

    def get_fast_info(self):
        if self._fast_info is None:
            self._fast_info = FastInfo(self)
        return self._fast_info

    def get_valuation_measures(self):
        data = self._quote.valuation_measures
        return data

    def get_sustainability(self, as_dict=False):
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
            dict_data["financialCurrency"] = (
                "USD"
                if "financialCurrency" not in self._earnings
                else self._earnings["financialCurrency"]
            )
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
            data = data.clone()
            if "metric" in data.columns:
                data = data.with_columns(
                    pl.col("metric").map_elements(
                        lambda x: utils.camel2title(
                            [x], sep=" ", acronyms=["EBIT", "EBITDA", "EPS", "NI"]
                        )[0],
                        return_dtype=pl.Utf8,
                    )
                )
        if as_dict:
            if "metric" in data.columns:
                return {
                    row["metric"]: {k: v for k, v in row.items() if k != "metric"}
                    for row in data.to_dicts()
                }
            return data.to_dicts()
        return data

    def get_incomestmt(self, as_dict=False, pretty=False, freq="yearly"):
        return self.get_income_stmt(as_dict, pretty, freq)

    def get_financials(self, as_dict=False, pretty=False, freq="yearly"):
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
            data = data.clone()
            if "metric" in data.columns:
                data = data.with_columns(
                    pl.col("metric").map_elements(
                        lambda x: utils.camel2title([x], sep=" ", acronyms=["PPE"])[0],
                        return_dtype=pl.Utf8,
                    )
                )
        if as_dict:
            if "metric" in data.columns:
                return {
                    row["metric"]: {k: v for k, v in row.items() if k != "metric"}
                    for row in data.to_dicts()
                }
            return data.to_dicts()
        return data

    def get_balancesheet(self, as_dict=False, pretty=False, freq="yearly"):
        return self.get_balance_sheet(as_dict, pretty, freq)

    def get_cash_flow(
        self, as_dict=False, pretty=False, freq="yearly"
    ) -> Union[pl.DataFrame, dict]:
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
            data = data.clone()
            if "metric" in data.columns:
                data = data.with_columns(
                    pl.col("metric").map_elements(
                        lambda x: utils.camel2title([x], sep=" ", acronyms=["PPE"])[0],
                        return_dtype=pl.Utf8,
                    )
                )
        if as_dict:
            if "metric" in data.columns:
                return {
                    row["metric"]: {k: v for k, v in row.items() if k != "metric"}
                    for row in data.to_dicts()
                }
            return data.to_dicts()
        return data

    def get_cashflow(self, as_dict=False, pretty=False, freq="yearly"):
        return self.get_cash_flow(as_dict, pretty, freq)

    def get_dividends(self, period="max") -> pl.DataFrame:
        return self._lazy_load_price_history().get_dividends(period=period)

    def get_capital_gains(self, period="max") -> pl.DataFrame:
        return self._lazy_load_price_history().get_capital_gains(period=period)

    def get_splits(self, period="max") -> pl.DataFrame:
        return self._lazy_load_price_history().get_splits(period=period)

    def get_actions(self, period="max") -> pl.DataFrame:
        return self._lazy_load_price_history().get_actions(period=period)

    def get_shares(self, as_dict=False) -> Union[pl.DataFrame, dict]:
        data = self._fundamentals.shares
        if as_dict:
            return data.to_dicts()
        return data

    @utils.log_indent_decorator
    def get_shares_full(self, start=None, end=None):
        logger = utils.get_yf_logger()

        # Process dates
        tz = self._get_ticker_tz(timeout=10)
        dt_now = datetime.now(timezone.utc).astimezone(ZoneInfo(tz))
        if start is not None:
            start = utils._parse_user_dt(start, tz)
        if end is not None:
            end = utils._parse_user_dt(end, tz)
        if end is None:
            end = dt_now
        if start is None:
            from datetime import timedelta

            start = end - timedelta(days=548)  # 18 months
        if start >= end:
            logger.error("Start date must be before end")
            return None
        # floor/ceil to day boundary
        import math

        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        import math as _math
        from datetime import timedelta as _td

        end_floored = end.replace(hour=0, minute=0, second=0, microsecond=0)
        if end != end_floored:
            end = end_floored + _td(days=1)
        else:
            end = end_floored

        # Fetch
        ts_url_base = f"https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{self.ticker}?symbol={self.ticker}"
        shares_url = f"{ts_url_base}&period1={int(start.timestamp())}&period2={int(end.timestamp())}"
        try:
            json_data = self._data.cache_get(url=shares_url)
            json_data = json_data.json()
        except (_json.JSONDecodeError, requests.exceptions.RequestException):
            if not YfConfig.debug.hide_exceptions:
                raise
            logger.error(f"{self.ticker}: Yahoo web request for share count failed")
            return None
        try:
            fail = json_data["finance"]["error"]["code"] == "Bad Request"
        except KeyError:
            fail = False
        if fail:
            if not YfConfig.debug.hide_exceptions:
                raise requests.exceptions.HTTPError(
                    "Yahoo web request for share count returned 'Bad Request'"
                )
            logger.error(f"{self.ticker}: Yahoo web request for share count failed")
            return None

        shares_data = json_data["timeseries"]["result"]
        if "shares_out" not in shares_data[0]:
            return None
        try:
            df = pl.DataFrame(
                {
                    "Date": pl.Series(shares_data[0]["timestamp"], dtype=pl.Int64)
                    .cast(pl.Datetime("us", "UTC"))
                    .dt.convert_time_zone(tz),
                    "shares_outstanding": shares_data[0]["shares_out"],
                }
            )
        except Exception as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            logger.error(f"{self.ticker}: Failed to parse shares count data: {e}")
            return None

        df = df.sort("Date")
        return df

    def get_isin(self) -> Optional[str]:
        # *** experimental ***
        if self._isin is not None:
            return self._isin

        ticker = self.ticker.upper()

        if "-" in ticker or "^" in ticker:
            self._isin = "-"
            return self._isin

        q = ticker

        if self._quote.info is None:
            # Don't print error message cause self._quote.info will print one
            return None
        if "shortName" in self._quote.info:
            q = self._quote.info["shortName"]

        url = f"https://markets.businessinsider.com/ajax/SearchController_Suggest?max_results=25&query={urlencode(q)}"
        data = self._data.cache_get(url=url).text

        search_str = f'"{ticker}|'
        if search_str not in data:
            if q.lower() in data.lower():
                search_str = '"|'
                if search_str not in data:
                    self._isin = "-"
                    return self._isin
            else:
                self._isin = "-"
                return self._isin

        self._isin = data.split(search_str)[1].split('"')[0].split("|")[0]
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
            raise ValueError(
                f"Invalid tab name '{tab}'. Choose from: {', '.join(tab_queryrefs.keys())}"
            )

        url = f"{_ROOT_URL_}/xhr/ncp?queryRef={query_ref}&serviceKey=ncp_fin"
        payload = {"serviceConfig": {"snippetCount": count, "s": [self.ticker]}}

        data = self._data.post(url, body=payload)
        if data is None or "Will be right back" in data.text:
            raise YFDataException("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***")
        try:
            data = data.json()
        except _json.JSONDecodeError:
            if not YfConfig.debug.hide_exceptions:
                raise
            logger.error(
                f"{self.ticker}: Failed to retrieve the news and received faulty response instead."
            )
            data = {}

        news = data.get("data", {}).get("tickerStream", {}).get("stream", [])

        self._news = [article for article in news if not article.get("ad", [])]
        return self._news

    def get_earnings_dates(self, limit=12, offset=0) -> Optional[pd.DataFrame]:
        if limit > 100:
            raise ValueError("Yahoo caps limit at 100")

        if self._earnings_dates and limit in self._earnings_dates:
            return self._earnings_dates[limit]

        df = self._get_earnings_dates_using_scrape(limit, offset)
        self._earnings_dates[limit] = df
        return df

    @utils.log_indent_decorator
    def _get_earnings_dates_using_scrape(
        self, limit=12, offset=0
    ) -> Optional[pd.DataFrame]:
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
        if limit > 0 and limit <= 25:
            size = 25
        elif limit > 25 and limit <= 50:
            size = 50
        elif limit > 50 and limit <= 100:
            size = 100
        else:
            raise ValueError("Please use limit <= 100")

        # Define the URL
        url = "https://finance.yahoo.com/calendar/earnings?symbol={}&offset={}&size={}".format(
            self.ticker, offset, size
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

            # Parse HTML table using BeautifulSoup
            table_soup = BeautifulSoup(table_html, "lxml")
            headers = [th.get_text(strip=True) for th in table_soup.find_all("th")]
            rows = []
            tbody = table_soup.find("tbody")
            if tbody:
                for tr in tbody.find_all("tr"):
                    cells = [
                        td.get_text(strip=True) or None for td in tr.find_all("td")
                    ]
                    if cells:
                        rows.append(cells)
            if not rows:
                return None
            df = pl.DataFrame(rows, schema=headers, orient="row")

            # Drop redundant columns
            drop_cols = [c for c in ["Symbol", "Company"] if c in df.columns]
            if drop_cols:
                df = df.drop(drop_cols)

            # Backwards compatibility
            if "Surprise (%)" in df.columns:
                df = df.rename({"Surprise (%)": "Surprise(%)"})

            # Drop rows where Earnings Date is null
            df = df.filter(pl.col("Earnings Date").is_not_null())

            # Parse earnings date
            # Replace timezone abbreviations
            df = df.with_columns(
                pl.col("Earnings Date")
                .str.replace("EDT", "America/New_York")
                .str.replace("EST", "America/New_York")
            )
            # Split into datetime string and tz string (last word)
            df = df.with_columns(
                pl.col("Earnings Date")
                .str.splitn(" ", 7)
                .struct.rename_fields(["p0", "p1", "p2", "p3", "p4", "p5", "tz_str"])
                .alias("parts")
            ).unnest("parts")
            # Reconstruct datetime string without tz
            df = df.with_columns(
                (
                    pl.col("p0")
                    + " "
                    + pl.col("p1")
                    + " "
                    + pl.col("p2")
                    + " "
                    + pl.col("p3")
                    + " "
                    + pl.col("p4")
                    + " "
                    + pl.col("p5")
                ).alias("dt_str")
            ).drop(["p0", "p1", "p2", "p3", "p4", "p5"])

            # Parse datetime and localize per-row timezone
            df = df.with_columns(
                pl.col("dt_str")
                .str.to_datetime(format="%B %d, %Y at %I %p", strict=False)
                .alias("Earnings Date")
            )
            # Apply per-row timezone using ZoneInfo
            tz_list = df["tz_str"].to_list()
            dt_list = df["Earnings Date"].to_list()
            tz_aware = []
            for dt_val, tz_val in zip(dt_list, tz_list):
                if dt_val is not None and tz_val is not None:
                    try:
                        tz_aware.append(dt_val.replace(tzinfo=ZoneInfo(tz_val)))
                    except Exception:
                        tz_aware.append(None)
                else:
                    tz_aware.append(None)
            df = df.with_columns(
                pl.Series("Earnings Date", tz_aware, dtype=pl.Datetime("us", "UTC"))
            ).drop(["dt_str", "tz_str"])

        else:
            err_msg = "No earnings dates found, symbol may be delisted"
            logger = utils.get_yf_logger()
            logger.error(f"{self.ticker}: {err_msg}")
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
            row["label"]
            for row in json_data["finance"]["result"][0]["documents"][0]["columns"]
        ]
        rows = json_data["finance"]["result"][0]["documents"][0]["rows"]
        df = pl.DataFrame(rows, schema=columns, orient="row")

        if df.is_empty():
            _exception = YFEarningsDateMissing(self.ticker)
            err_msg = str(_exception)
            logger.error(f"{self.ticker}: {err_msg}")
            return None

        # Convert eventtype
        # - 1 = earnings call (manually confirmed)
        # - 2 = earnings report
        # - 11 = stockholders meeting (manually confirmed)
        if "Event Type" in df.columns:
            df = df.with_columns(
                pl.col("Event Type")
                .str.replace(r"^1$", "Call")
                .str.replace(r"^2$", "Earnings")
                .str.replace(r"^11$", "Meeting")
            )

        # Calculate earnings date
        tz = self._get_ticker_tz(timeout=30)
        if "Event Start Date" in df.columns:
            df = df.with_columns(
                pl.col("Event Start Date")
                .str.to_datetime(strict=False)
                .alias("Earnings Date")
            )
            # Determine if timezone info is present
            ed_dtype = df["Earnings Date"].dtype
            if hasattr(ed_dtype, "time_zone") and ed_dtype.time_zone:
                df = df.with_columns(pl.col("Earnings Date").dt.convert_time_zone(tz))
            else:
                df = df.with_columns(pl.col("Earnings Date").dt.replace_time_zone(tz))
            drop_cols = [
                c
                for c in ["Event Start Date", "Timezone short name"]
                if c in df.columns
            ]
            if drop_cols:
                df = df.drop(drop_cols)

        # Convert numeric types
        for col in ["Surprise (%)", "EPS Estimate", "Reported EPS"]:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).cast(pl.Float64, strict=False).alias(col)
                )

        # Backwards compatibility rename
        if "Surprise (%)" in df.columns:
            df = df.rename({"Surprise (%)": "Surprise(%)"})

        self._earnings_dates[limit] = df
        return df

    def get_history_metadata(self) -> dict:
        return self._lazy_load_price_history().get_history_metadata()

    def get_funds_data(self) -> Optional[FundsData]:
        if not self._funds_data:
            self._funds_data = FundsData(self._data, self.ticker)

        return self._funds_data

    def live(self, message_handler=None, verbose=True):
        self._message_handler = message_handler

        self.ws = WebSocket(verbose=verbose)
        self.ws.subscribe(self.ticker)
        self.ws.listen(self._message_handler)

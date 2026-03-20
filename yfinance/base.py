"""Core ticker implementation used by the public ``Ticker`` wrapper."""

from __future__ import print_function

import json as _json
from typing import Optional, Union, cast

import pandas as pd
from curl_cffi import requests

from . import cache, utils
from .base_lookup import TickerBaseLookupMixin
from .config import YF_CONFIG as YfConfig
from .const import _MIC_TO_YAHOO_SUFFIX
from .data import YfData
from .live import WebSocket
from .scrapers.analysis import Analysis
from .scrapers.fundamentals import Fundamentals
from .scrapers.funds import FundsData
from .scrapers.history.client import PriceHistory
from .scrapers.holders import Holders
from .scrapers.quote import FastInfo, Quote
from .utils_tz import fetch_ticker_tz, get_ticker_tz

class TickerBase(TickerBaseLookupMixin):
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
        if self._tz is None:
            self._tz = get_ticker_tz(
                self._data,
                self.ticker,
                timeout,
                info_provider=lambda: self._quote.info,
            )
        return self._tz

    @utils.log_indent_decorator
    def _fetch_ticker_tz(self, timeout):
        """Fetch exchange timezone directly from Yahoo chart metadata."""
        return fetch_ticker_tz(self._data, self.ticker, timeout)

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
            if YfConfig.debug.raise_on_error:
                raise
            logger.error("%s: Yahoo web request for share count failed", self.ticker)
            return None

        is_bad_request = json_data.get("finance", {}).get("error", {}).get("code") == "Bad Request"
        if is_bad_request:
            if YfConfig.debug.raise_on_error:
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
            if YfConfig.debug.raise_on_error:
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

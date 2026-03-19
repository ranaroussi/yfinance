"""Quote and fast-info scrapers for Yahoo Finance ticker data."""

from collections.abc import Iterator
import datetime
import json
from typing import TYPE_CHECKING, Any, Optional

import numpy as _np
import pandas as pd

from yfinance.config import YF_CONFIG as YfConfig
from yfinance.const import quote_summary_valid_modules, _QUERY1_URL_
from yfinance.data import YfData
from yfinance.exceptions import YFDataException, YFException
from yfinance.scrapers.utils import fetch_quote_summary, get_raw_json_or_none
from .. import utils


class FastInfo:
    """Contain a small subset of ``info`` fields that can be fetched quickly."""

    def __init__(self, ticker_base_object):
        self._tkr = ticker_base_object
        self._prices_1y = None
        self._prices_1wk_1h_prepost = None
        self._prices_1wk_1h_reg = None
        self._md = None
        self._cache: dict[str, Any] = {
            "currency": None,
            "quote_type": None,
            "exchange": None,
            "timezone": None,
            "shares": None,
            "mcap": None,
            "open": None,
            "day_high": None,
            "day_low": None,
            "last_price": None,
            "last_volume": None,
            "prev_close": None,
            "reg_prev_close": None,
            "50d_day_average": None,
            "200d_day_average": None,
            "year_high": None,
            "year_low": None,
            "year_change": None,
            "10d_avg_vol": None,
            "3mo_avg_vol": None,
            "today_open": None,
            "today_close": None,
            "today_midnight": None,
        }

        # attrs = utils.attributes(self)
        # self.keys = attrs.keys()
        # utils.attributes is calling each method, bad! Have to hardcode
        _properties = ["currency", "quote_type", "exchange", "timezone"]
        _properties += ["shares", "market_cap"]
        _properties += ["last_price", "previous_close", "open", "day_high", "day_low"]
        _properties += ["regular_market_previous_close"]
        _properties += ["last_volume"]
        _properties += [
            "fifty_day_average",
            "two_hundred_day_average",
            "ten_day_average_volume",
            "three_month_average_volume",
        ]
        _properties += ["year_high", "year_low", "year_change"]

        # Because released before fixing key case, need to officially support
        # camel-case but also secretly support snake-case
        base_keys = [k for k in _properties if '_' not in k]

        sc_keys = [k for k in _properties if '_' in k]

        self._sc_to_cc_key = {
            k: utils.snake_case_to_camel_case(k)
            for k in sc_keys
        }
        self._cc_to_sc_key = {v: k for k, v in self._sc_to_cc_key.items()}

        self._public_keys = sorted(base_keys + list(self._sc_to_cc_key.values()))
        self._keys = sorted(self._public_keys + sc_keys)

    # dict imitation:
    def keys(self):
        """Return the supported public fast-info keys."""
        return self._public_keys

    def items(self):
        """Return key-value pairs."""
        return [(k, self[k]) for k in self._public_keys]

    def values(self):
        """Return values for the public keys."""
        return [self[k] for k in self._public_keys]

    def get(self, key, default=None):
        """Return a value for ``key`` or ``default``."""
        if key in self.keys():
            if key in self._cc_to_sc_key:
                key = self._cc_to_sc_key[key]
            return self[key]
        return default

    def __getitem__(self, k):
        if not isinstance(k, str):
            raise KeyError(f"key must be a string not '{type(k)}'")
        if k not in self._keys:
            raise KeyError(f"'{k}' not valid key. Examine 'FastInfo.keys()'")
        if k in self._cc_to_sc_key:
            k = self._cc_to_sc_key[k]
        return getattr(self, k)

    def __contains__(self, k):
        return k in self.keys()

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __str__(self):
        return "lazy-loading dict with keys = " + str(self.keys())

    def __repr__(self):
        return self.__str__()

    def to_json(self, indent=4):
        """Serialize fast info to JSON."""
        return json.dumps(dict(self.items()), indent=indent)

    def _get_1y_prices(self, full_days_only=False, **kwargs):
        """Fetch up to one year of price history."""
        if "fullDaysOnly" in kwargs:
            full_days_only = kwargs.pop("fullDaysOnly")
        if kwargs:
            unknown = ", ".join(sorted(kwargs))
            raise TypeError(f"Unexpected keyword arguments: {unknown}")
        if self._prices_1y is None:
            self._prices_1y = self._tkr.history(period="1y", auto_adjust=False, keepna=True)
            self._md = self._tkr.get_history_metadata()
            try:
                ctp = self._md["currentTradingPeriod"]
                self._cache["today_open"] = pd.to_datetime(
                    ctp["regular"]["start"],
                    unit="s",
                    utc=True,
                ).tz_convert(self.timezone)
                self._cache["today_close"] = pd.to_datetime(
                    ctp["regular"]["end"],
                    unit="s",
                    utc=True,
                ).tz_convert(self.timezone)
                self._cache["today_midnight"] = self._cache["today_close"].ceil("D")
            except (KeyError, TypeError, ValueError):
                self._cache["today_open"] = None
                self._cache["today_close"] = None
                self._cache["today_midnight"] = None
                raise

        if self._prices_1y.empty:
            return self._prices_1y

        dnow = pd.Timestamp.now('UTC').tz_convert(self.timezone).date()
        d1 = dnow
        d0 = (pd.Timestamp(d1) + pd.Timedelta(days=1) - datetime.timedelta(days=365)).date()
        if full_days_only and self._exchange_open_now():
            # Exclude today
            d1 -= datetime.timedelta(days=1)
        return self._prices_1y.loc[str(d0):str(d1)]

    def _get_1wk_1h_prepost_prices(self):
        """Fetch one-hour history including pre/post market."""
        if self._prices_1wk_1h_prepost is None:
            self._prices_1wk_1h_prepost = self._tkr.history(
                period="5d",
                interval="1h",
                auto_adjust=False,
                prepost=True,
            )
        return self._prices_1wk_1h_prepost

    def _get_1wk_1h_reg_prices(self):
        """Fetch one-hour regular-session history."""
        if self._prices_1wk_1h_reg is None:
            self._prices_1wk_1h_reg = self._tkr.history(
                period="5d",
                interval="1h",
                auto_adjust=False,
                prepost=False,
            )
        return self._prices_1wk_1h_reg

    def _get_exchange_metadata(self):
        """Return exchange metadata from history calls."""
        if self._md is not None:
            return self._md

        self._get_1y_prices()
        self._md = self._tkr.get_history_metadata()
        return self._md

    def _exchange_open_now(self):
        """Best-effort check for whether today's session is still open."""
        t = pd.Timestamp.now('UTC')
        self._get_exchange_metadata()

        # if self._cache["today_open"] is None and self._cache["today_close"] is None:
        #     r = False
        # else:
        #     r = self._cache["today_open"] <= t and t < self._cache["today_close"]

        # if self._cache["today_midnight"] is None:
        #     r = False
        # elif self._cache["today_midnight"].date() > t.tz_convert(self.timezone).date():
        #     r = False
        # else:
        #     r = t < self._cache["today_midnight"]

        last_day_cutoff = self._get_1y_prices().index[-1] + datetime.timedelta(days=1)
        last_day_cutoff += datetime.timedelta(minutes=20)
        r = t < last_day_cutoff

        # print("_exchange_open_now() returning", r)
        return r

    @property
    def currency(self):
        """Currency used by the ticker."""
        if self._cache["currency"] is not None:
            return self._cache["currency"]

        md = self._tkr.get_history_metadata()
        self._cache["currency"] = md["currency"]
        return self._cache["currency"]

    @property
    def quote_type(self):
        """Yahoo instrument type for the ticker."""
        if self._cache["quote_type"] is not None:
            return self._cache["quote_type"]

        md = self._tkr.get_history_metadata()
        self._cache["quote_type"] = md["instrumentType"]
        return self._cache["quote_type"]

    @property
    def exchange(self):
        """Exchange name for the ticker."""
        if self._cache["exchange"] is not None:
            return self._cache["exchange"]

        self._cache["exchange"] = self._get_exchange_metadata()["exchangeName"]
        return self._cache["exchange"]

    @property
    def timezone(self):
        """Exchange timezone name."""
        if self._cache["timezone"] is not None:
            return self._cache["timezone"]

        self._cache["timezone"] = self._get_exchange_metadata()["exchangeTimezoneName"]
        return self._cache["timezone"]

    @property
    def shares(self):
        """Most recent shares outstanding value."""
        if self._cache["shares"] is not None:
            return self._cache["shares"]

        shares = self._tkr.get_shares_full(
            start=pd.Timestamp.now("UTC").date() - datetime.timedelta(days=548)
        )
        # if shares is None:
        #     # Requesting 18 months failed, so fallback to shares which should include last year
        #     shares = self._tkr.get_shares()
        if shares is not None:
            if isinstance(shares, pd.DataFrame):
                shares = shares[shares.columns[0]]
            self._cache["shares"] = int(shares.iloc[-1])
        return self._cache["shares"]

    @property
    def last_price(self):
        """Latest close/regular market price."""
        if self._cache["last_price"] is not None:
            return self._cache["last_price"]
        prices = self._get_1y_prices()
        if prices.empty:
            md = self._get_exchange_metadata()
            if "regularMarketPrice" in md:
                self._cache["last_price"] = md["regularMarketPrice"]
        else:
            self._cache["last_price"] = float(prices["Close"].iloc[-1])
            if _np.isnan(self._cache["last_price"]):
                md = self._get_exchange_metadata()
                if "regularMarketPrice" in md:
                    self._cache["last_price"] = md["regularMarketPrice"]
        return self._cache["last_price"]

    @property
    def previous_close(self):
        """Previous close including pre/post market data."""
        if self._cache["prev_close"] is not None:
            return self._cache["prev_close"]
        prices = self._get_1wk_1h_prepost_prices()
        fail = False
        if prices.empty:
            fail = True
        else:
            prices = prices[["Close"]].groupby(prices.index.date).last()
            if prices.shape[0] < 2:
                # Very few symbols have previousClose despite no
                # no trading data e.g. 'QCSTIX'.
                fail = True
            else:
                self._cache["prev_close"] = float(prices["Close"].iloc[-2])
        if fail:
            # Fallback to original info[] if available.
            k = "previousClose"
            self._cache["prev_close"] = self._tkr.get_info().get(k)
        return self._cache["prev_close"]

    @property
    def regular_market_previous_close(self):
        """Previous close for regular market session."""
        if self._cache["reg_prev_close"] is not None:
            return self._cache["reg_prev_close"]
        prices = self._get_1y_prices()
        if prices.shape[0] == 1:
            # Tiny % of tickers don't return daily history before last trading day,
            # so backup option is hourly history:
            prices = self._get_1wk_1h_reg_prices()
            prices = prices[["Close"]].groupby(prices.index.date).last()
        if prices.shape[0] < 2:
            # Very few symbols have regularMarketPreviousClose despite no
            # no trading data. E.g. 'QCSTIX'.
            # So fallback to original info[] if available.
            k = "regularMarketPreviousClose"
            self._cache["reg_prev_close"] = self._tkr.get_info().get(k)
        else:
            self._cache["reg_prev_close"] = float(prices["Close"].iloc[-2])
        return self._cache["reg_prev_close"]

    @property
    def open(self):
        """Latest open price."""
        if self._cache["open"] is not None:
            return self._cache["open"]
        prices = self._get_1y_prices()
        if prices.empty:
            self._cache["open"] = None
        else:
            self._cache["open"] = float(prices["Open"].iloc[-1])
            if _np.isnan(self._cache["open"]):
                self._cache["open"] = None
        return self._cache["open"]

    @property
    def day_high(self):
        """Latest day high price."""
        if self._cache["day_high"] is not None:
            return self._cache["day_high"]
        prices = self._get_1y_prices()
        if prices.empty:
            self._cache["day_high"] = None
        else:
            self._cache["day_high"] = float(prices["High"].iloc[-1])
            if _np.isnan(self._cache["day_high"]):
                self._cache["day_high"] = None
        return self._cache["day_high"]

    @property
    def day_low(self):
        """Latest day low price."""
        if self._cache["day_low"] is not None:
            return self._cache["day_low"]
        prices = self._get_1y_prices()
        if prices.empty:
            self._cache["day_low"] = None
        else:
            self._cache["day_low"] = float(prices["Low"].iloc[-1])
            if _np.isnan(self._cache["day_low"]):
                self._cache["day_low"] = None
        return self._cache["day_low"]

    @property
    def last_volume(self):
        """Latest volume value."""
        if self._cache["last_volume"] is not None:
            return self._cache["last_volume"]
        prices = self._get_1y_prices()
        self._cache["last_volume"] = None if prices.empty else int(prices["Volume"].iloc[-1])
        return self._cache["last_volume"]

    @property
    def fifty_day_average(self):
        """Average close across latest 50 trading days."""
        if self._cache["50d_day_average"] is not None:
            return self._cache["50d_day_average"]

        prices = self._get_1y_prices(full_days_only=True)
        if prices.empty:
            self._cache["50d_day_average"] = None
        else:
            n = prices.shape[0]
            a = n - 50
            b = n
            a = max(a, 0)
            self._cache["50d_day_average"] = float(prices["Close"].iloc[a:b].mean())

        return self._cache["50d_day_average"]

    @property
    def two_hundred_day_average(self):
        """Average close across latest 200 trading days."""
        if self._cache["200d_day_average"] is not None:
            return self._cache["200d_day_average"]

        prices = self._get_1y_prices(full_days_only=True)
        if prices.empty:
            self._cache["200d_day_average"] = None
        else:
            n = prices.shape[0]
            a = n - 200
            b = n
            a = max(a, 0)
            self._cache["200d_day_average"] = float(prices["Close"].iloc[a:b].mean())

        return self._cache["200d_day_average"]

    @property
    def ten_day_average_volume(self):
        """Average volume across latest 10 trading days."""
        if self._cache["10d_avg_vol"] is not None:
            return self._cache["10d_avg_vol"]

        prices = self._get_1y_prices(full_days_only=True)
        if prices.empty:
            self._cache["10d_avg_vol"] = None
        else:
            n = prices.shape[0]
            a = n - 10
            b = n
            a = max(a, 0)
            self._cache["10d_avg_vol"] = int(prices["Volume"].iloc[a:b].mean())

        return self._cache["10d_avg_vol"]

    @property
    def three_month_average_volume(self):
        """Average volume for roughly the latest three months."""
        if self._cache["3mo_avg_vol"] is not None:
            return self._cache["3mo_avg_vol"]

        prices = self._get_1y_prices(full_days_only=True)
        if prices.empty:
            self._cache["3mo_avg_vol"] = None
        else:
            dt1 = prices.index[-1]
            dt0 = dt1 - pd.DateOffset(months=3) + pd.DateOffset(days=1)
            self._cache["3mo_avg_vol"] = int(prices.loc[dt0:dt1, "Volume"].mean())

        return self._cache["3mo_avg_vol"]

    @property
    def year_high(self):
        """Highest traded price across the trailing year."""
        if self._cache["year_high"] is not None:
            return self._cache["year_high"]

        prices = self._get_1y_prices(full_days_only=True)
        if prices.empty:
            prices = self._get_1y_prices(full_days_only=False)
        self._cache["year_high"] = float(prices["High"].max())
        return self._cache["year_high"]

    @property
    def year_low(self):
        """Lowest traded price across the trailing year."""
        if self._cache["year_low"] is not None:
            return self._cache["year_low"]

        prices = self._get_1y_prices(full_days_only=True)
        if prices.empty:
            prices = self._get_1y_prices(full_days_only=False)
        self._cache["year_low"] = float(prices["Low"].min())
        return self._cache["year_low"]

    @property
    def year_change(self):
        """Fractional close-price change across the trailing year."""
        if self._cache["year_change"] is not None:
            return self._cache["year_change"]

        prices = self._get_1y_prices(full_days_only=True)
        if prices.shape[0] >= 2:
            self._cache["year_change"] = (
                prices["Close"].iloc[-1] - prices["Close"].iloc[0]
            ) / prices["Close"].iloc[0]
            self._cache["year_change"] = float(self._cache["year_change"])
        return self._cache["year_change"]

    @property
    def market_cap(self):
        """Estimated market capitalization."""
        if self._cache["mcap"] is not None:
            return self._cache["mcap"]

        try:
            shares = self.shares
        except YFException as error:
            if "Cannot retrieve share count" in str(error):
                shares = None
            else:
                raise

        if shares is None:
            # Very few symbols have marketCap despite no share count.
            # E.g. 'BTC-USD'
            # So fallback to original info[] if available.
            k = "marketCap"
            self._cache["mcap"] = self._tkr.get_info().get(k)
        else:
            last_price = self.last_price
            if last_price is not None:
                self._cache["mcap"] = float(shares * last_price)
        return self._cache["mcap"]

if not TYPE_CHECKING:
    FastInfo.toJSON = FastInfo.to_json


class Quote:
    """Fetcher and parser for quote-summary and quote-response endpoints."""

    def __init__(self, data: YfData, symbol: str):
        self._data = data
        self._symbol = symbol

        self._info = None
        self._retired_info = None
        self._sustainability = None
        self._recommendations = None
        self._upgrades_downgrades = None
        self._calendar = None
        self._sec_filings = None

        self._already_scraped = False
        self._already_fetched = False
        self._already_fetched_complementary = False

    @property
    def info(self) -> dict:
        """Return flattened quote information."""
        if self._info is None:
            self._fetch_info()
            self._fetch_complementary()
        return {} if self._info is None else self._info

    @property
    def sustainability(self) -> pd.DataFrame:
        """Return sustainability scores data."""
        if self._sustainability is None:
            result = self._fetch(modules=['esgScores'])
            if result is None:
                self._sustainability = pd.DataFrame()
            else:
                try:
                    data = result["quoteSummary"]["result"][0]
                except (KeyError, IndexError) as exc:
                    if YfConfig.debug.raise_on_error:
                        raise
                    raise YFDataException(
                        f"Failed to parse json response from Yahoo Finance: {result}"
                    ) from exc
                self._sustainability = pd.DataFrame(data)
        return self._sustainability

    @property
    def recommendations(self) -> pd.DataFrame:
        """Return recommendation trend table."""
        if self._recommendations is None:
            result = self._fetch(modules=['recommendationTrend'])
            if result is None:
                self._recommendations = pd.DataFrame()
            else:
                try:
                    data = result["quoteSummary"]["result"][0]["recommendationTrend"]["trend"]
                except (KeyError, IndexError) as exc:
                    if YfConfig.debug.raise_on_error:
                        raise
                    raise YFDataException(
                        f"Failed to parse json response from Yahoo Finance: {result}"
                    ) from exc
                self._recommendations = pd.DataFrame(data)
        return self._recommendations

    @property
    def upgrades_downgrades(self) -> pd.DataFrame:
        """Return analyst upgrade and downgrade history."""
        if self._upgrades_downgrades is None:
            result = self._fetch(modules=['upgradeDowngradeHistory'])
            if result is None:
                self._upgrades_downgrades = pd.DataFrame()
            else:
                try:
                    data = result["quoteSummary"]["result"][0]["upgradeDowngradeHistory"]["history"]
                    if len(data) == 0:
                        raise YFDataException(
                            f"No upgrade/downgrade history found for {self._symbol}"
                        )
                    df = pd.DataFrame(data)
                    df.rename(
                        columns={
                            "epochGradeDate": "GradeDate",
                            "firm": "Firm",
                            "toGrade": "ToGrade",
                            "fromGrade": "FromGrade",
                            "action": "Action",
                        },
                        inplace=True,
                    )
                    df.set_index('GradeDate', inplace=True)
                    df.index = pd.to_datetime(df.index, unit='s')
                    self._upgrades_downgrades = df
                except (KeyError, IndexError) as exc:
                    if YfConfig.debug.raise_on_error:
                        raise
                    raise YFDataException(
                        f"Failed to parse json response from Yahoo Finance: {result}"
                    ) from exc
        return self._upgrades_downgrades

    @property
    def calendar(self) -> dict:
        """Return dividend and earnings calendar data."""
        if self._calendar is None:
            self._fetch_calendar()
        return {} if self._calendar is None else self._calendar

    @property
    def sec_filings(self) -> dict:
        """Return SEC filing metadata."""
        if self._sec_filings is None:
            f = self._fetch_sec_filings()
            self._sec_filings = {} if f is None else f
        return self._sec_filings

    @staticmethod
    def valid_modules():
        """Return valid quote-summary module names."""
        return quote_summary_valid_modules

    def _fetch(self, modules: list[str]) -> Optional[dict[str, Any]]:
        """Fetch selected quote-summary modules."""
        return fetch_quote_summary(self._data, self._symbol, modules)

    def _fetch_additional_info(self) -> Optional[dict[str, Any]]:
        """Fetch quote-response fields that complement quote-summary modules."""
        params_dict = {"symbols": self._symbol, "formatted": "false"}
        return get_raw_json_or_none(
            self._data,
            f"{_QUERY1_URL_}/v7/finance/quote?",
            params_dict,
        )

    def _extract_query_info_section(
        self,
        result: dict[str, Any],
        section: str,
    ) -> dict[str, Any]:
        """Extract symbol-specific result data from a quote payload section."""
        section_data = result.get(section)
        if not isinstance(section_data, dict):
            return {}

        quote_results = section_data.get("result")
        if not isinstance(quote_results, list) or not quote_results:
            return {}

        first_result = quote_results[0]
        if isinstance(first_result, dict):
            first_result["symbol"] = self._symbol

        query_info = next(
            (
                info
                for info in quote_results
                if isinstance(info, dict) and info.get("symbol") == self._symbol
            ),
            None,
        )
        return query_info if isinstance(query_info, dict) else {}

    @staticmethod
    def _normalize_query_info(query_info: dict[str, Any]) -> dict[str, Any]:
        """Flatten nested dictionaries and normalize maxAge units."""
        processed_info: dict[str, Any] = {}
        for key, value in query_info.items():
            if isinstance(value, dict):
                for nested_key, nested_value in value.items():
                    if nested_value is not None:
                        if nested_key == "maxAge" and nested_value == 1:
                            processed_info[nested_key] = 86400
                        else:
                            processed_info[nested_key] = nested_value
            elif value is not None:
                processed_info[key] = value
        return processed_info

    @classmethod
    def _format_info_value(cls, key, value):
        """Recursively convert Yahoo raw/fmt wrappers into primitive values."""
        if isinstance(value, dict) and "raw" in value and "fmt" in value:
            return value["fmt"] if key in {"regularMarketTime", "postMarketTime"} else value["raw"]
        if isinstance(value, list):
            return [cls._format_info_value(None, item) for item in value]
        if isinstance(value, dict):
            return {
                sub_key: cls._format_info_value(sub_key, sub_value)
                for sub_key, sub_value in value.items()
            }
        if isinstance(value, str):
            return value.replace("\xa0", " ")
        return value

    def _fetch_info(self):
        """Fetch and normalize the main ``info`` payload."""
        if self._already_fetched:
            return
        self._already_fetched = True

        modules = [
            "financialData",
            "quoteType",
            "defaultKeyStatistics",
            "assetProfile",
            "summaryDetail",
        ]
        result = self._fetch(modules=modules)
        additional_info = self._fetch_additional_info()
        if additional_info is not None and result is not None:
            result.update(additional_info)
        else:
            result = additional_info
        if result is None:
            self._info = {}
            return

        query_info: dict[str, Any] = {}
        for section in ("quoteSummary", "quoteResponse"):
            query_info.update(self._extract_query_info_section(result, section))

        normalized_info = self._normalize_query_info(query_info)
        self._info = {
            key: self._format_info_value(key, value)
            for key, value in normalized_info.items()
        }

    def _fetch_complementary(self):
        """Fetch complementary info fields not returned by quote-summary."""
        if self._already_fetched_complementary:
            return
        self._already_fetched_complementary = True

        self._fetch_info()
        if self._info is None:
            return

        keys = {"trailingPegRatio"}
        if keys:
            url = (
                "https://query1.finance.yahoo.com/ws/fundamentals-timeseries"
                f"/v1/finance/timeseries/{self._symbol}?symbol={self._symbol}"
            )
            for k in keys:
                url += "&type=" + k
            start = pd.Timestamp.now("UTC").floor("D") - datetime.timedelta(days=365 // 2)
            start = int(start.timestamp())
            end = pd.Timestamp.now("UTC").ceil("D")
            end = int(end.timestamp())
            url += f"&period1={start}&period2={end}"

            json_str = self._data.cache_get(url=url).text
            json_data = json.loads(json_str)
            json_result = json_data.get("timeseries") or json_data.get("finance")
            if json_result["error"] is not None:
                raise YFException(
                    "Failed to parse json response from Yahoo Finance: "
                    + str(json_result["error"])
                )
            for k in keys:
                keydict = json_result["result"][0]
                if k in keydict:
                    self._info[k] = keydict[k][-1]["reportedValue"]["raw"]
                else:
                    self._info[k] = None

    def _fetch_calendar(self):
        """Fetch earnings and dividend calendar details."""
        result = self._fetch(modules=["calendarEvents"])
        if result is None:
            self._calendar = {}
            return

        try:
            self._calendar = {}
            _events = result["quoteSummary"]["result"][0]["calendarEvents"]
            if "dividendDate" in _events:
                self._calendar["Dividend Date"] = datetime.datetime.fromtimestamp(
                    _events["dividendDate"]
                ).date()
            if "exDividendDate" in _events:
                self._calendar["Ex-Dividend Date"] = datetime.datetime.fromtimestamp(
                    _events["exDividendDate"]
                ).date()

            earnings = _events.get("earnings")
            if earnings is not None:
                self._calendar["Earnings Date"] = [
                    datetime.datetime.fromtimestamp(d).date()
                    for d in earnings.get("earningsDate", [])
                ]
                self._calendar["Earnings High"] = earnings.get("earningsHigh")
                self._calendar["Earnings Low"] = earnings.get("earningsLow")
                self._calendar["Earnings Average"] = earnings.get("earningsAverage")
                self._calendar["Revenue High"] = earnings.get("revenueHigh")
                self._calendar["Revenue Low"] = earnings.get("revenueLow")
                self._calendar["Revenue Average"] = earnings.get("revenueAverage")
        except (KeyError, IndexError) as exc:
            if YfConfig.debug.raise_on_error:
                raise
            raise YFDataException(
                f"Failed to parse json response from Yahoo Finance: {result}"
            ) from exc

    def _fetch_sec_filings(self):
        """Fetch and normalize SEC filing data."""
        result = self._fetch(modules=["secFilings"])
        if result is None:
            return None

        filings = result["quoteSummary"]["result"][0]["secFilings"]["filings"]

        for f in filings:
            if "exhibits" in f:
                f["exhibits"] = {e["type"]: e["url"] for e in f["exhibits"]}
            f["date"] = datetime.datetime.strptime(f["date"], "%Y-%m-%d").date()

        # Experimental: convert to pandas
        # for i in range(len(filings)):
        #     f = filings[i]
        #     if 'exhibits' in f:
        #         for e in f['exhibits']:
        #             f[e['type']] = e['url']
        #         del f['exhibits']
        #     filings[i] = f
        # filings = pd.DataFrame(filings)
        # for c in filings.columns:
        #     if c.startswith('EX-'):
        #         filings[c] = filings[c].astype(str)
        #         filings.loc[filings[c]=='nan', c] = ''
        # filings = filings.drop('epochDate', axis=1)
        # filings = filings.set_index('date')

        return filings

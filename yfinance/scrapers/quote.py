from yfinance._http import HTTPError
import datetime
import json
import numbers
import numpy as _np
import pandas as pd

from yfinance import utils
from yfinance.config import YfConfig
from yfinance.const import quote_summary_valid_modules, _BASE_URL_, _QUERY1_URL_
from yfinance.data import YfData
from yfinance.exceptions import YFDataException, YFException

info_retired_keys_price = {"currentPrice", "dayHigh", "dayLow", "open", "previousClose", "volume", "volume24Hr"}
info_retired_keys_price.update({"regularMarket"+s for s in ["DayHigh", "DayLow", "Open", "PreviousClose", "Price", "Volume"]})
info_retired_keys_price.update({"fiftyTwoWeekLow", "fiftyTwoWeekHigh", "fiftyTwoWeekChange", "52WeekChange", "fiftyDayAverage", "twoHundredDayAverage"})
info_retired_keys_price.update({"averageDailyVolume10Day", "averageVolume10days", "averageVolume"})
info_retired_keys_exchange = {"currency", "exchange", "exchangeTimezoneName", "exchangeTimezoneShortName", "quoteType"}
info_retired_keys_marketCap = {"marketCap"}
info_retired_keys_symbol = {"symbol"}

# Valuation-measure timeseries keys (fundamentals-timeseries API) -> display labels,
# matching the rows historically shown on the Yahoo key-statistics page.
_VALUATION_MEASURE_LABELS = {
    "MarketCap": "Market Cap",
    "EnterpriseValue": "Enterprise Value",
    "PeRatio": "Trailing P/E",
    "ForwardPeRatio": "Forward P/E",
    "PegRatio": "PEG Ratio (5yr expected)",
    "PsRatio": "Price/Sales",
    "PbRatio": "Price/Book",
    "EnterprisesValueRevenueRatio": "Enterprise Value/Revenue",
    "EnterprisesValueEBITDARatio": "Enterprise Value/EBITDA",
}
# Public freq -> fundamentals-timeseries type prefix for the period columns.
_VALUATION_FREQ_PREFIX = {"quarterly": "quarterly", "monthly": "monthly",
                          "yearly": "annual", "trailing": "trailing"}


info_retired_keys = info_retired_keys_price | info_retired_keys_exchange | info_retired_keys_marketCap | info_retired_keys_symbol


_QUOTE_SUMMARY_URL_ = f"{_BASE_URL_}/v10/finance/quoteSummary"


class FastInfo:
    # Contain small subset of info[] items that can be fetched faster elsewhere.
    # Imitates a dict.
    def __init__(self, tickerBaseObject):
        self._tkr = tickerBaseObject

        self._prices_1y = None
        self._prices_1wk_1h_prepost = None
        self._prices_1wk_1h_reg = None
        self._md = None

        self._currency = None
        self._quote_type = None
        self._exchange = None
        self._timezone = None

        self._shares = None
        self._mcap = None

        self._open = None
        self._day_high = None
        self._day_low = None
        self._last_price = None
        self._last_volume = None

        self._prev_close = None

        self._reg_prev_close = None

        self._50d_day_average = None
        self._200d_day_average = None
        self._year_high = None
        self._year_low = None
        self._year_change = None

        self._10d_avg_vol = None
        self._3mo_avg_vol = None

        # attrs = utils.attributes(self)
        # self.keys = attrs.keys()
        # utils.attributes is calling each method, bad! Have to hardcode
        _properties = ["currency", "quote_type", "exchange", "timezone"]
        _properties += ["shares", "market_cap"]
        _properties += ["last_price", "previous_close", "open", "day_high", "day_low"]
        _properties += ["regular_market_previous_close"]
        _properties += ["last_volume"]
        _properties += ["fifty_day_average", "two_hundred_day_average", "ten_day_average_volume", "three_month_average_volume"]
        _properties += ["year_high", "year_low", "year_change"]

        # Because released before fixing key case, need to officially support
        # camel-case but also secretly support snake-case
        base_keys = [k for k in _properties if '_' not in k]

        sc_keys = [k for k in _properties if '_' in k]

        self._sc_to_cc_key = {k: utils.snake_case_2_camelCase(k) for k in sc_keys}
        self._cc_to_sc_key = {v: k for k, v in self._sc_to_cc_key.items()}

        self._public_keys = sorted(base_keys + list(self._sc_to_cc_key.values()))
        self._keys = sorted(self._public_keys + sc_keys)

    # dict imitation:
    def keys(self):
        return self._public_keys

    def items(self):
        return [(k, self[k]) for k in self._public_keys]

    def values(self):
        return [self[k] for k in self._public_keys]

    def get(self, key, default=None):
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

    def __iter__(self):
        return iter(self.keys())

    def __str__(self):
        return "lazy-loading dict with keys = " + str(self.keys())

    def __repr__(self):
        return self.__str__()

    def toJSON(self, indent=4):
        return json.dumps({k: self[k] for k in self.keys()}, indent=indent)

    def _get_1y_prices(self, fullDaysOnly=False):
        if self._prices_1y is None:
            self._prices_1y = self._tkr.history(period="1y", auto_adjust=False, keepna=True)
            self._md = self._tkr.get_history_metadata()
            try:
                ctp = self._md["currentTradingPeriod"]
                self._today_open = pd.to_datetime(ctp["regular"]["start"], unit='s', utc=True).tz_convert(self.timezone)
                self._today_close = pd.to_datetime(ctp["regular"]["end"], unit='s', utc=True).tz_convert(self.timezone)
                self._today_midnight = self._today_close.ceil("D")
            except Exception:
                self._today_open = None
                self._today_close = None
                self._today_midnight = None
                raise

        if self._prices_1y.empty:
            return self._prices_1y

        dnow = pd.Timestamp.now('UTC').tz_convert(self.timezone).date()
        d1 = dnow
        d0 = (d1 + datetime.timedelta(days=1)) - utils._interval_to_timedelta("1y")
        if fullDaysOnly and self._exchange_open_now():
            # Exclude today
            d1 -= utils._interval_to_timedelta("1d")
        return self._prices_1y.loc[str(d0):str(d1)]

    def _get_1wk_1h_prepost_prices(self):
        if self._prices_1wk_1h_prepost is None:
            self._prices_1wk_1h_prepost = self._tkr.history(period="5d", interval="1h", auto_adjust=False, prepost=True)
        return self._prices_1wk_1h_prepost

    def _get_1wk_1h_reg_prices(self):
        if self._prices_1wk_1h_reg is None:
            self._prices_1wk_1h_reg = self._tkr.history(period="5d", interval="1h", auto_adjust=False, prepost=False)
        return self._prices_1wk_1h_reg

    def _get_exchange_metadata(self):
        if self._md is not None:
            return self._md

        self._get_1y_prices()
        self._md = self._tkr.get_history_metadata()
        return self._md

    def _exchange_open_now(self):
        t = pd.Timestamp.now('UTC')
        self._get_exchange_metadata()

        # if self._today_open is None and self._today_close is None:
        #     r = False
        # else:
        #     r = self._today_open <= t and t < self._today_close

        # if self._today_midnight is None:
        #     r = False
        # elif self._today_midnight.date() > t.tz_convert(self.timezone).date():
        #     r = False
        # else:
        #     r = t < self._today_midnight

        last_day_cutoff = self._get_1y_prices().index[-1] + datetime.timedelta(days=1)
        last_day_cutoff += datetime.timedelta(minutes=20)
        r = t < last_day_cutoff

        # print("_exchange_open_now() returning", r)
        return r

    @property
    def currency(self):
        if self._currency is not None:
            return self._currency

        md = self._tkr.get_history_metadata()
        self._currency = md["currency"]
        return self._currency

    @property
    def quote_type(self):
        if self._quote_type is not None:
            return self._quote_type

        md = self._tkr.get_history_metadata()
        self._quote_type = md["instrumentType"]
        return self._quote_type

    @property
    def exchange(self):
        if self._exchange is not None:
            return self._exchange

        self._exchange = self._get_exchange_metadata()["exchangeName"]
        return self._exchange

    @property
    def timezone(self):
        if self._timezone is not None:
            return self._timezone

        self._timezone = self._get_exchange_metadata()["exchangeTimezoneName"]
        return self._timezone

    @property
    def shares(self):
        if self._shares is not None:
            return self._shares

        shares = self._tkr.get_shares_full(start=pd.Timestamp.now('UTC').date()-pd.Timedelta(days=548))
        # if shares is None:
        #     # Requesting 18 months failed, so fallback to shares which should include last year
        #     shares = self._tkr.get_shares()
        if shares is not None:
            if isinstance(shares, pd.DataFrame):
                shares = shares[shares.columns[0]]
            self._shares = int(shares.iloc[-1])
        return self._shares

    @property
    def last_price(self):
        if self._last_price is not None:
            return self._last_price
        prices = self._get_1y_prices()
        if prices.empty:
            md = self._get_exchange_metadata()
            if "regularMarketPrice" in md:
                self._last_price = md["regularMarketPrice"]
        else:
            self._last_price = float(prices["Close"].iloc[-1])
            if _np.isnan(self._last_price):
                md = self._get_exchange_metadata()
                if "regularMarketPrice" in md:
                    self._last_price = md["regularMarketPrice"]
        return self._last_price

    @property
    def previous_close(self):
        if self._prev_close is not None:
            return self._prev_close
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
                self._prev_close = float(prices["Close"].iloc[-2])
        if fail:
            # Fallback to original info[] if available.
            self._tkr.info  # trigger fetch
            k = "previousClose"
            if self._tkr._quote._retired_info is not None and k in self._tkr._quote._retired_info:
                self._prev_close = self._tkr._quote._retired_info[k]
        return self._prev_close

    @property
    def regular_market_previous_close(self):
        if self._reg_prev_close is not None:
            return self._reg_prev_close
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
            self._tkr.info  # trigger fetch
            k = "regularMarketPreviousClose"
            if self._tkr._quote._retired_info is not None and k in self._tkr._quote._retired_info:
                self._reg_prev_close = self._tkr._quote._retired_info[k]
        else:
            self._reg_prev_close = float(prices["Close"].iloc[-2])
        return self._reg_prev_close

    @property
    def open(self):
        if self._open is not None:
            return self._open
        prices = self._get_1y_prices()
        if prices.empty:
            self._open = None
        else:
            self._open = float(prices["Open"].iloc[-1])
            if _np.isnan(self._open):
                self._open = None
        return self._open

    @property
    def day_high(self):
        if self._day_high is not None:
            return self._day_high
        prices = self._get_1y_prices()
        if prices.empty:
            self._day_high = None
        else:
            self._day_high = float(prices["High"].iloc[-1])
            if _np.isnan(self._day_high):
                self._day_high = None
        return self._day_high

    @property
    def day_low(self):
        if self._day_low is not None:
            return self._day_low
        prices = self._get_1y_prices()
        if prices.empty:
            self._day_low = None
        else:
            self._day_low = float(prices["Low"].iloc[-1])
            if _np.isnan(self._day_low):
                self._day_low = None
        return self._day_low

    @property
    def last_volume(self):
        if self._last_volume is not None:
            return self._last_volume
        prices = self._get_1y_prices()
        self._last_volume = None if prices.empty else int(prices["Volume"].iloc[-1])
        return self._last_volume

    @property
    def fifty_day_average(self):
        if self._50d_day_average is not None:
            return self._50d_day_average

        prices = self._get_1y_prices(fullDaysOnly=True)
        if prices.empty:
            self._50d_day_average = None
        else:
            n = prices.shape[0]
            a = n-50
            b = n
            if a < 0:
                a = 0
            self._50d_day_average = float(prices["Close"].iloc[a:b].mean())

        return self._50d_day_average

    @property
    def two_hundred_day_average(self):
        if self._200d_day_average is not None:
            return self._200d_day_average

        prices = self._get_1y_prices(fullDaysOnly=True)
        if prices.empty:
            self._200d_day_average = None
        else:
            n = prices.shape[0]
            a = n-200
            b = n
            if a < 0:
                a = 0

            self._200d_day_average = float(prices["Close"].iloc[a:b].mean())

        return self._200d_day_average

    @property
    def ten_day_average_volume(self):
        if self._10d_avg_vol is not None:
            return self._10d_avg_vol

        prices = self._get_1y_prices(fullDaysOnly=True)
        if prices.empty:
            self._10d_avg_vol = None
        else:
            n = prices.shape[0]
            a = n-10
            b = n
            if a < 0:
                a = 0
            self._10d_avg_vol = int(prices["Volume"].iloc[a:b].mean())

        return self._10d_avg_vol

    @property
    def three_month_average_volume(self):
        if self._3mo_avg_vol is not None:
            return self._3mo_avg_vol

        prices = self._get_1y_prices(fullDaysOnly=True)
        if prices.empty:
            self._3mo_avg_vol = None
        else:
            dt1 = prices.index[-1]
            dt0 = dt1 - utils._interval_to_timedelta("3mo") + utils._interval_to_timedelta("1d")
            self._3mo_avg_vol = int(prices.loc[dt0:dt1, "Volume"].mean())

        return self._3mo_avg_vol

    @property
    def year_high(self):
        if self._year_high is not None:
            return self._year_high

        prices = self._get_1y_prices(fullDaysOnly=True)
        if prices.empty:
            prices = self._get_1y_prices(fullDaysOnly=False)
        self._year_high = float(prices["High"].max())
        return self._year_high

    @property
    def year_low(self):
        if self._year_low is not None:
            return self._year_low

        prices = self._get_1y_prices(fullDaysOnly=True)
        if prices.empty:
            prices = self._get_1y_prices(fullDaysOnly=False)
        self._year_low = float(prices["Low"].min())
        return self._year_low

    @property
    def year_change(self):
        if self._year_change is not None:
            return self._year_change

        prices = self._get_1y_prices(fullDaysOnly=True)
        if prices.shape[0] >= 2:
            self._year_change = (prices["Close"].iloc[-1] - prices["Close"].iloc[0]) / prices["Close"].iloc[0]
            self._year_change = float(self._year_change)
        return self._year_change

    @property
    def market_cap(self):
        if self._mcap is not None:
            return self._mcap

        try:
            shares = self.shares
        except Exception as e:
            if "Cannot retrieve share count" in str(e):
                shares = None
            else:
                raise

        if shares is None:
            # Very few symbols have marketCap despite no share count.
            # E.g. 'BTC-USD'
            # So fallback to original info[] if available.
            self._tkr.info
            k = "marketCap"
            if self._tkr._quote._retired_info is not None and k in self._tkr._quote._retired_info:
                self._mcap = self._tkr._quote._retired_info[k]
        else:
            self._mcap = float(shares * self.last_price)
        return self._mcap


class Quote:
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
        self._valuation_measures = {}  # keyed by freq

        self._already_scraped = False
        self._already_fetched = False
        self._already_fetched_complementary = False

    @property
    def info(self) -> dict:
        if self._info is None:
            self._fetch_info()
            self._fetch_complementary()

        return self._info

    @property
    def sustainability(self) -> pd.DataFrame:
        if self._sustainability is None:
            result = self._fetch(modules=['esgScores'])
            if result is None:
                self._sustainability = pd.DataFrame()
            else:
                try:
                    data = result["quoteSummary"]["result"][0]
                except (KeyError, IndexError):
                    if not YfConfig.debug.hide_exceptions:
                        raise
                    raise YFDataException(f"Failed to parse json response from Yahoo Finance: {result}")
                self._sustainability = pd.DataFrame(data)
        return self._sustainability

    @property
    def recommendations(self) -> pd.DataFrame:
        if self._recommendations is None:
            result = self._fetch(modules=['recommendationTrend'])
            if result is None:
                self._recommendations = pd.DataFrame()
            else:
                try:
                    data = result["quoteSummary"]["result"][0]["recommendationTrend"]["trend"]
                except (KeyError, IndexError):
                    if not YfConfig.debug.hide_exceptions:
                        raise
                    raise YFDataException(f"Failed to parse json response from Yahoo Finance: {result}")
                self._recommendations = pd.DataFrame(data)
        return self._recommendations

    @property
    def upgrades_downgrades(self) -> pd.DataFrame:
        if self._upgrades_downgrades is None:
            result = self._fetch(modules=['upgradeDowngradeHistory'])
            if result is None:
                self._upgrades_downgrades = pd.DataFrame()
            else:
                try:
                    data = result["quoteSummary"]["result"][0]["upgradeDowngradeHistory"]["history"]
                    if len(data) == 0:
                        raise YFDataException(f"No upgrade/downgrade history found for {self._symbol}")
                    df = pd.DataFrame(data)
                    df.rename(columns={"epochGradeDate": "GradeDate", 'firm': 'Firm', 'toGrade': 'ToGrade', 'fromGrade': 'FromGrade', 'action': 'Action'}, inplace=True)
                    df.set_index('GradeDate', inplace=True)
                    df.index = pd.to_datetime(df.index, unit='s')
                    self._upgrades_downgrades = df
                except (KeyError, IndexError):
                    if not YfConfig.debug.hide_exceptions:
                        raise
                    raise YFDataException(f"Failed to parse json response from Yahoo Finance: {result}")
        return self._upgrades_downgrades

    @property
    def calendar(self) -> dict:
        if self._calendar is None:
            self._fetch_calendar()
        return self._calendar

    @property
    def sec_filings(self) -> dict:
        if self._sec_filings is None:
            f = self._fetch_sec_filings()
            self._sec_filings = {} if f is None else f
        return self._sec_filings

    @property
    def valuation_measures(self) -> pd.DataFrame:
        return self.get_valuation_measures()

    def get_valuation_measures(self, freq="quarterly", periods=5) -> pd.DataFrame:
        """Valuation measures (market cap, P/E, P/S, P/B, EV/EBITDA, ...).

        Returns a DataFrame with the 9 valuation measures as rows and a
        ``Current`` column plus period-end date columns (newest first). Values
        are raw numeric measures (floats, with ``NaN`` for missing cells); the
        date column labels remain ``"M/D/YYYY"`` strings.

        Args:
            freq: period columns to return — "quarterly" (default), "monthly",
                "yearly" or "trailing". The "Current" column always reflects the
                latest trailing value.
            periods: cap on the number of period (date) columns returned, newest
                first. Must be an int >= 0 or None. The default of 5 matches the
                column count the old key-statistics page showed. ``periods=0``
                returns only the "Current" column (a 9x1 DataFrame); ``None``
                (or a value larger than the available history) returns every
                available period column. The ``valuation`` property uses this
                default — call the method form to control ``periods``.

        Returns:
            pd.DataFrame: valuation measures, ``Current`` first, sliced to at
                most ``periods`` period columns.
        """
        # Validate `periods` before any fetch so a bad value never hits the network.
        if periods is not None:
            # Accept any integer (incl. numpy ints), but reject bool — it is an
            # int subclass yet a bool column count is almost always a mistake.
            if isinstance(periods, bool) or not isinstance(periods, numbers.Integral):
                raise TypeError(f"periods must be an int >= 0 or None, not {type(periods).__name__}")
            if periods < 0:
                raise ValueError("periods must be >= 0 or None")

        if freq not in self._valuation_measures:
            self._valuation_measures[freq] = self._fetch_valuation_measures(freq)
        df = self._valuation_measures[freq]

        # The full df is cached per-freq; apply the `periods` cap by slicing on
        # return so different `periods` values reuse the one cached fetch. Return
        # a copy (the sliced path already does) so a caller can't mutate the cache.
        if periods is None or df.empty:
            return df.copy()
        date_cols = [c for c in df.columns if c != "Current"]
        return df[["Current"] + date_cols[:periods]]

    @staticmethod
    def valid_modules():
        return quote_summary_valid_modules

    def _fetch(self, modules: list):
        if not isinstance(modules, list):
            raise YFException("Should provide a list of modules, see available modules using `valid_modules`")

        modules = ','.join([m for m in modules if m in quote_summary_valid_modules])
        if len(modules) == 0:
            raise YFException("No valid modules provided, see available modules using `valid_modules`")
        params_dict = {"modules": modules, "corsDomain": "finance.yahoo.com", "formatted": "false", "symbol": self._symbol, "lang": YfConfig.locale.lang, "region": YfConfig.locale.region}
        try:
            result = self._data.get_raw_json(_QUOTE_SUMMARY_URL_ + f"/{self._symbol}", params=params_dict)
        except HTTPError as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            utils.get_yf_logger().error(str(e) + e.response.text)
            return None
        return result

    def _fetch_additional_info(self):
        params_dict = {"symbols": self._symbol, "formatted": "false", "lang": YfConfig.locale.lang, "region": YfConfig.locale.region}
        try:
            result = self._data.get_raw_json(f"{_QUERY1_URL_}/v7/finance/quote?", params=params_dict)
        except HTTPError as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            utils.get_yf_logger().error(str(e) + e.response.text)
            return None
        return result

    def _fetch_info(self):
        if self._already_fetched:
            return
        self._already_fetched = True
        modules = ['financialData', 'quoteType', 'defaultKeyStatistics', 'assetProfile', 'summaryDetail']
        result = self._fetch(modules=modules)
        additional_info = self._fetch_additional_info()

        if result is None:
            result = {}

        if additional_info is not None:
            result.update(additional_info)

        query1_info = {}
        for quote in ["quoteSummary", "quoteResponse"]:
            quote_result = result.get(quote, {}).get("result", [])

            if len(quote_result) > 0:
                quote_result[0]["symbol"] = self._symbol
                query_info = next(
                    (info for info in quote_result if info.get("symbol") == self._symbol),
                    None,
                )
                if query_info:
                    query1_info.update(query_info)

        # Normalize and flatten nested dictionaries while converting maxAge from days (1) to seconds (86400).
        # This handles Yahoo Finance API inconsistency where maxAge is sometimes expressed in days instead of seconds.
        processed_info = {}
        for k, v in query1_info.items():

            # Handle nested dictionary
            if isinstance(v, dict):
                for k1, v1 in v.items():
                    if v1 is not None:
                        processed_info[k1] = 86400 if k1 == "maxAge" and v1 == 1 else v1

            elif v is not None:
                processed_info[k] = v

        query1_info = processed_info

        # recursively format but only because of 'companyOfficers'

        def _format(k, v):
            if isinstance(v, dict) and "raw" in v and "fmt" in v:
                v2 = v["fmt"] if k in {"regularMarketTime", "postMarketTime"} else v["raw"]
            elif isinstance(v, list):
                v2 = [_format(None, x) for x in v]
            elif isinstance(v, dict):
                v2 = {k: _format(k, x) for k, x in v.items()}
            elif isinstance(v, str):
                v2 = v.replace("\xa0", " ")
            else:
                v2 = v
            return v2

        self._info = {k: _format(k, v) for k, v in query1_info.items()}

    def _fetch_valuation_measures(self, freq="quarterly"):
        # Valuation measures come from the fundamentals-timeseries API (the same
        # source as the income/balance-sheet/cash-flow statements) instead of
        # scraping the key-statistics web page, which was fragile (it returned an
        # empty table whenever Yahoo changed the page layout). The returned shape
        # matches the previous scrape: measures as the index, a 'Current' column
        # plus period-end date columns (newest first). Values are the raw numeric
        # measures (floats, with NaN for missing cells) rather than the old
        # display-formatted strings (e.g. '3.76T', '32.39'). ``freq``
        # ('quarterly' / 'monthly' / 'yearly' / 'trailing') selects the period
        # columns; 'Current' always comes from the trailing series.
        prefix = _VALUATION_FREQ_PREFIX.get(freq)
        if prefix is None:
            raise ValueError(f"freq must be one of {list(_VALUATION_FREQ_PREFIX)}, not '{freq}'")
        keys = list(_VALUATION_MEASURE_LABELS.keys())
        # Always also fetch the 'trailing' series for the 'Current' column.
        prefixes = sorted({prefix, "trailing"})
        types = ",".join(f"{p}{k}" for k in keys for p in prefixes)
        period1 = int(datetime.datetime(2016, 12, 31).timestamp())
        period2 = int(pd.Timestamp.now("UTC").ceil("D").timestamp())
        url = f"{_BASE_URL_}/ws/fundamentals-timeseries/v1/finance/timeseries/{self._symbol}"
        params = {"symbol": self._symbol, "type": types, "period1": period1, "period2": period2}
        try:
            # cache_get (not get_raw_json) to match scrapers/fundamentals.py and
            # benefit from response caching for the same timeseries endpoint.
            response = self._data.cache_get(url, params=params)
            data = json.loads(response.text)
        except Exception as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            utils.get_yf_logger().error(f"Failed to fetch valuation measures: {e}")
            return pd.DataFrame()

        try:
            result = (data.get("timeseries") or {}).get("result") or []
            period = {}      # label -> {Timestamp: raw value}  (the requested freq)
            trailing = {}    # label -> {Timestamp: raw value}
            for item in result:
                for type_name, points in item.items():
                    if type_name in ("meta", "timestamp") or not isinstance(points, list):
                        continue
                    if prefix != "trailing" and type_name.startswith(prefix):
                        base, target = type_name[len(prefix):], period
                    elif type_name.startswith("trailing"):
                        base, target = type_name[len("trailing"):], trailing
                    else:
                        continue
                    label = _VALUATION_MEASURE_LABELS.get(base)
                    if label is None:
                        continue
                    for point in points:
                        if not point:
                            continue
                        as_of = point.get("asOfDate")
                        value = (point.get("reportedValue") or {}).get("raw")
                        if as_of is not None and value is not None:
                            ts = pd.Timestamp(as_of).normalize()
                            target.setdefault(label, {})[ts] = value
            if prefix == "trailing":
                period = trailing
            if not period and not trailing:
                return pd.DataFrame()

            # 'Current' column = each measure's most recent trailing value.
            current = {label: series[max(series)] for label, series in trailing.items() if series}

            # Every period the API returns, newest first (no artificial cap).
            dates = sorted({d for series in period.values() for d in series}, reverse=True)
            date_cols = [f"{d.month}/{d.day}/{d.year}" for d in dates]
            # Emit every measure as a row, even those with no data — the
            # key-statistics page always listed all measures, so dropping them
            # would lose rows the scrape kept (e.g. PEG Ratio / EV-EBITDA for
            # BRK-B). Missing cells become NaN (the parse only stored non-None
            # floats, so .get(..., nan) yields the raw value or NaN).
            rows = {}
            for label in _VALUATION_MEASURE_LABELS.values():
                row = {"Current": current.get(label, _np.nan)}
                series = period.get(label, {})
                for d, col in zip(dates, date_cols):
                    row[col] = series.get(d, _np.nan)
                rows[label] = row
            df = pd.DataFrame.from_dict(rows, orient="index")
            df = df.reindex(list(_VALUATION_MEASURE_LABELS.values()))
            df = df[["Current"] + date_cols]
            df.index.name = None
            return df
        except Exception as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            utils.get_yf_logger().error(f"Failed to parse valuation measures: {e}")
            return pd.DataFrame()

    def _fetch_complementary(self):
        if self._already_fetched_complementary:
            return
        self._already_fetched_complementary = True

        self._fetch_info()
        if self._info is None:
            return

        # Complementary key-statistics. For now just want 'trailing PEG ratio'
        keys = {"trailingPegRatio"}
        if keys:
            # Simplified the original scrape code for key-statistics. Very expensive for fetching
            # just one value, best if scraping most/all:
            #
            # p = _re.compile(r'root\.App\.main = (.*);')
            # url = 'https://finance.yahoo.com/quote/{}/key-statistics?p={}'.format(self._ticker.ticker, self._ticker.ticker)
            # try:
            #     r = session.get(url)
            #     data = _json.loads(p.findall(r.text)[0])
            #     key_stats = data['context']['dispatcher']['stores']['QuoteTimeSeriesStore']["timeSeries"]
            #     for k in keys:
            #         if k not in key_stats or len(key_stats[k])==0:
            #             # Yahoo website prints N/A, indicates Yahoo lacks necessary data to calculate
            #             v = None
            #         else:
            #             # Select most recent (last) raw value in list:
            #             v = key_stats[k][-1]["reportedValue"]["raw"]
            #         self._info[k] = v
            # except Exception:
            #     raise
            #     pass
            #
            # For just one/few variable is faster to query directly:
            url = f"https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{self._symbol}?symbol={self._symbol}"
            for k in keys:
                url += "&type=" + k
            # Request 6 months of data
            start = pd.Timestamp.now('UTC').floor("D") - datetime.timedelta(days=365 // 2)
            start = int(start.timestamp())
            end = pd.Timestamp.now('UTC').ceil("D")
            end = int(end.timestamp())
            url += f"&period1={start}&period2={end}"

            json_str = self._data.cache_get(url=url).text
            json_data = json.loads(json_str)
            json_result = json_data.get("timeseries") or json_data.get("finance") or {}
            if json_result.get("error") is not None:
                raise YFException("Failed to parse json response from Yahoo Finance: " + str(json_result.get("error")))
            result = json_result.get("result") or []
            keydict = result[0] if result else {}
            for k in keys:
                if k in keydict and keydict[k]:
                    self._info[k] = keydict[k][-1].get("reportedValue", {}).get("raw")
                else:
                    self._info[k] = None

    def _fetch_calendar(self):
        # secFilings return too old data, so not requesting it for now
        result = self._fetch(modules=['calendarEvents'])
        if result is None:
            self._calendar = {}
            return

        try:
            self._calendar = dict()
            _events = result["quoteSummary"]["result"][0]["calendarEvents"]
            if 'dividendDate' in _events:
                self._calendar['Dividend Date'] = datetime.datetime.fromtimestamp(_events['dividendDate']).date()
            if 'exDividendDate' in _events:
                self._calendar['Ex-Dividend Date'] = datetime.datetime.fromtimestamp(_events['exDividendDate']).date()
            # splits = _events.get('splitDate')  # need to check later, i will add code for this if found data
            earnings = _events.get('earnings')
            if earnings is not None:
                self._calendar['Earnings Date'] = [datetime.datetime.fromtimestamp(d).date() for d in earnings.get('earningsDate', [])]
                self._calendar['Earnings High'] = earnings.get('earningsHigh', None)
                self._calendar['Earnings Low'] = earnings.get('earningsLow', None)
                self._calendar['Earnings Average'] = earnings.get('earningsAverage', None)
                self._calendar['Revenue High'] = earnings.get('revenueHigh', None)
                self._calendar['Revenue Low'] = earnings.get('revenueLow', None)
                self._calendar['Revenue Average'] = earnings.get('revenueAverage', None)
        except (KeyError, IndexError):
            if not YfConfig.debug.hide_exceptions:
                raise
            raise YFDataException(f"Failed to parse json response from Yahoo Finance: {result}")


    def _fetch_sec_filings(self):
        result = self._fetch(modules=['secFilings'])
        if result is None:
            return None

        filings = result["quoteSummary"]["result"][0]["secFilings"]["filings"]

        # Improve structure
        for f in filings:
            if 'exhibits' in f:
                f['exhibits'] = {e['type']:e['url'] for e in f['exhibits']}
            f['date'] = datetime.datetime.strptime(f['date'], '%Y-%m-%d').date()

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

import datetime
import logging
import json
import warnings

import pandas as pd
import numpy as _np

from yfinance import utils
from yfinance.data import TickerData
from yfinance.exceptions import YFNotImplementedError

info_retired_keys_price = {"currentPrice", "dayHigh", "dayLow", "open", "previousClose", "volume", "volume24Hr"}
info_retired_keys_price.update({"regularMarket"+s for s in ["DayHigh", "DayLow", "Open", "PreviousClose", "Price", "Volume"]})
info_retired_keys_price.update({"fiftyTwoWeekLow", "fiftyTwoWeekHigh", "fiftyTwoWeekChange", "52WeekChange", "fiftyDayAverage", "twoHundredDayAverage"})
info_retired_keys_price.update({"averageDailyVolume10Day", "averageVolume10days", "averageVolume"})
info_retired_keys_exchange = {"currency", "exchange", "exchangeTimezoneName", "exchangeTimezoneShortName", "quoteType"}
info_retired_keys_marketCap = {"marketCap"}
info_retired_keys_symbol = {"symbol"}
info_retired_keys = info_retired_keys_price | info_retired_keys_exchange | info_retired_keys_marketCap | info_retired_keys_symbol


_BASIC_URL_ = "https://query2.finance.yahoo.com/v6/finance/quoteSummary"

from collections.abc import MutableMapping
class InfoDictWrapper(MutableMapping):
    """ Simple wrapper around info dict, intercepting 'gets' to 
    print how-to-migrate messages for specific keys. Requires
    override dict API"""

    def __init__(self, info):
        self.info = info

    def keys(self):
        return self.info.keys()

    def __str__(self):
        return self.info.__str__()

    def __repr__(self):
        return self.info.__repr__()

    def __contains__(self, k):
        return k in self.info.keys()

    def __getitem__(self, k):
        if k in info_retired_keys_price:
            warnings.warn(f"Price data removed from info (key='{k}'). Use Ticker.fast_info or history() instead", DeprecationWarning)
            return None
        elif k in info_retired_keys_exchange:
            warnings.warn(f"Exchange data removed from info (key='{k}'). Use Ticker.fast_info or Ticker.get_history_metadata() instead", DeprecationWarning)
            return None
        elif k in info_retired_keys_marketCap:
            warnings.warn(f"Market cap removed from info (key='{k}'). Use Ticker.fast_info instead", DeprecationWarning)
            return None
        elif k in info_retired_keys_symbol:
            warnings.warn(f"Symbol removed from info (key='{k}'). You know this already", DeprecationWarning)
            return None
        return self.info[self._keytransform(k)]

    def __setitem__(self, k, value):
        self.info[self._keytransform(k)] = value

    def __delitem__(self, k):
        del self.info[self._keytransform(k)]

    def __iter__(self):
        return iter(self.info)
    
    def __len__(self):
        return len(self.info)

    def _keytransform(self, k):
        return k


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
        base_keys = [k for k in _properties if not '_' in k]

        sc_keys = [k for k in _properties if '_' in k]

        self._sc_to_cc_key = {k:utils.snake_case_2_camelCase(k) for k in sc_keys}
        self._cc_to_sc_key = {v:k for k,v in self._sc_to_cc_key.items()}
 
        self._public_keys = sorted(base_keys + list(self._sc_to_cc_key.values()))
        self._keys = sorted(self._public_keys + sc_keys)

    # dict imitation:
    def keys(self):
        return self._public_keys
    def items(self):
        return [(k,self[k]) for k in self._public_keys]
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
            raise KeyError(f"key must be a string")
        if not k in self._keys:
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
        d = {k:self[k] for k in self.keys()}
        return _json.dumps({k:self[k] for k in self.keys()}, indent=indent)

    def _get_1y_prices(self, fullDaysOnly=False):
        if self._prices_1y is None:
            # Temporarily disable error printing
            logging.disable(logging.CRITICAL)
            self._prices_1y = self._tkr.history(period="380d", auto_adjust=False, keepna=True)
            logging.disable(logging.NOTSET)
            self._md = self._tkr.get_history_metadata()
            try:
                ctp = self._md["currentTradingPeriod"]
                self._today_open = pd.to_datetime(ctp["regular"]["start"], unit='s', utc=True).tz_convert(self.timezone)
                self._today_close = pd.to_datetime(ctp["regular"]["end"], unit='s', utc=True).tz_convert(self.timezone)
                self._today_midnight = self._today_close.ceil("D")
            except:
                self._today_open = None
                self._today_close = None
                self._today_midnight = None
                raise

        if self._prices_1y.empty:
            return self._prices_1y

        dnow = pd.Timestamp.utcnow().tz_convert(self.timezone).date()
        d1 = dnow
        d0 = (d1 + datetime.timedelta(days=1)) - utils._interval_to_timedelta("1y")
        if fullDaysOnly and self._exchange_open_now():
            # Exclude today
            d1 -= utils._interval_to_timedelta("1d")
        return self._prices_1y.loc[str(d0):str(d1)]

    def _get_1wk_1h_prepost_prices(self):
        if self._prices_1wk_1h_prepost is None:
            # Temporarily disable error printing
            logging.disable(logging.CRITICAL)
            self._prices_1wk_1h_prepost = self._tkr.history(period="1wk", interval="1h", auto_adjust=False, prepost=True)
            logging.disable(logging.NOTSET)
        return self._prices_1wk_1h_prepost

    def _get_1wk_1h_reg_prices(self):
        if self._prices_1wk_1h_reg is None:
            # Temporarily disable error printing
            logging.disable(logging.CRITICAL)
            self._prices_1wk_1h_reg = self._tkr.history(period="1wk", interval="1h", auto_adjust=False, prepost=False)
            logging.disable(logging.NOTSET)
        return self._prices_1wk_1h_reg

    def _get_exchange_metadata(self):
        if self._md is not None:
            return self._md

        self._get_1y_prices()
        self._md = self._tkr.get_history_metadata()
        return self._md

    def _exchange_open_now(self):
        t = pd.Timestamp.utcnow()
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

        if self._tkr._history_metadata is None:
            self._get_1y_prices()
        md = self._tkr.get_history_metadata()
        self._currency = md["currency"]
        return self._currency

    @property
    def quote_type(self):
        if self._quote_type is not None:
            return self._quote_type

        if self._tkr._history_metadata is None:
            self._get_1y_prices()
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

        shares = self._tkr.get_shares_full(start=pd.Timestamp.utcnow().date()-pd.Timedelta(days=548))
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
            elif "failed to decrypt Yahoo" in str(e):
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

    def __init__(self, data: TickerData, proxy=None):
        self._data = data
        self.proxy = proxy

        self._info = None
        self._retired_info = None
        self._sustainability = None
        self._recommendations = None
        self._calendar = None

        self._already_scraped = False
        self._already_fetched = False
        self._already_fetched_complementary = False

    @property
    def info(self) -> dict:
        if self._info is None:
            self._fetch(self.proxy)
            self._fetch_complementary(self.proxy)

        return self._info

    @property
    def sustainability(self) -> pd.DataFrame:
        if self._sustainability is None:
            raise YFNotImplementedError('sustainability')
        return self._sustainability

    @property
    def recommendations(self) -> pd.DataFrame:
        if self._recommendations is None:
            raise YFNotImplementedError('recommendations')
        return self._recommendations

    @property
    def calendar(self) -> pd.DataFrame:
        if self._calendar is None:
            raise YFNotImplementedError('calendar')
        return self._calendar

    def _fetch(self, proxy):
        if self._already_fetched:
            return
        self._already_fetched = True
        modules = ['financialData', 'quoteType', 'defaultKeyStatistics', 'assetProfile', 'summaryDetail']
        params_dict = {}
        params_dict["modules"] = modules
        params_dict["ssl"] = "true"
        result = self._data.get_raw_json(
            _BASIC_URL_ + f"/{self._data.ticker}", params=params_dict, proxy=proxy
        )
        result["quoteSummary"]["result"][0]["symbol"] = self._data.ticker
        query1_info = next(
            (info for info in result.get("quoteSummary", {}).get("result", []) if info["symbol"] == self._data.ticker),
            None,
        )
        # Most keys that appear in multiple dicts have same value. Except 'maxAge' because
        # Yahoo not consistent with days vs seconds. Fix it here:
        for k in query1_info:
            if "maxAge" in query1_info[k] and query1_info[k]["maxAge"] == 1:
                query1_info[k]["maxAge"] = 86400
        query1_info = {
            k1: v1 
            for k, v in query1_info.items() 
            if isinstance(v, dict) 
            for k1, v1 in v.items() 
            if v1
        }
        # recursively format but only because of 'companyOfficers'
        def _format(k, v):
            if isinstance(v, dict) and "raw" in v and "fmt" in v:
                v2 = v["fmt"] if k in {"regularMarketTime", "postMarketTime"} else v["raw"]
            elif isinstance(v, list):
                v2 = [_format(None, x) for x in v]
            elif isinstance(v, dict):
                v2 = {k:_format(k, x) for k, x in v.items()}
            elif isinstance(v, str):
                v2 = v.replace("\xa0", " ")
            else:
                v2 = v
            return v2
        for k, v in query1_info.items():
            query1_info[k] = _format(k, v)
        self._info = query1_info

    def _fetch_complementary(self, proxy):
        if self._already_fetched_complementary:
            return
        self._already_fetched_complementary = True

        # self._scrape(proxy)  # decrypt broken
        self._fetch(proxy)
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
            #     r = session.get(url, headers=utils.user_agent_headers)
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
            url = "https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{}?symbol={}".format(
                self._data.ticker, self._data.ticker)
            for k in keys:
                url += "&type=" + k
            # Request 6 months of data
            start = pd.Timestamp.utcnow().floor("D") - datetime.timedelta(days=365 // 2)
            start = int(start.timestamp())
            end = pd.Timestamp.utcnow().ceil("D")
            end = int(end.timestamp())
            url += f"&period1={start}&period2={end}"

            json_str = self._data.cache_get(url=url, proxy=proxy).text
            json_data = json.loads(json_str)
            try:
                key_stats = json_data["timeseries"]["result"][0]
                if k not in key_stats:
                    # Yahoo website prints N/A, indicates Yahoo lacks necessary data to calculate
                    v = None
                else:
                    # Select most recent (last) raw value in list:
                    v = key_stats[k][-1]["reportedValue"]["raw"]
            except Exception:
                v = None
            self._info[k] = v

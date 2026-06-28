from yfinance._http import HTTPError
import datetime
import json
import numpy as _np
import pandas as pd
from bs4 import BeautifulSoup

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
info_retired_keys = info_retired_keys_price | info_retired_keys_exchange | info_retired_keys_marketCap | info_retired_keys_symbol


_QUOTE_SUMMARY_URL_ = f"{_BASE_URL_}/v10/finance/quoteSummary"


class FastInfo:
    # Contain small subset of info[] items that can be fetched faster elsewhere.
    # Imitates a dict.
    def __init__(self, tickerBaseObject):
        self._tkr = tickerBaseObject
        self._prices = {k: None for k in (
            'prices_1y', 'prices_1wk_1h_prepost', 'prices_1wk_1h_reg', 'md',
            'today_open', 'today_close', 'today_midnight',
        )}
        self._market = {k: None for k in (
            'currency', 'quote_type', 'exchange', 'timezone', 'shares', 'mcap',
            'open', 'day_high', 'day_low', 'last_price', 'last_volume', 'prev_close',
            'reg_prev_close', 'fifty_day_average', 'two_hundred_day_average',
            'year_high', 'year_low', 'year_change', 'ten_day_average_volume',
            'three_month_average_volume',
        )}

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

        sc_to_cc = {k: utils.snake_case_2_camelCase(k) for k in sc_keys}
        cc_to_sc = {v: k for k, v in sc_to_cc.items()}
        public_keys = sorted(base_keys + list(sc_to_cc.values()))
        keys = sorted(public_keys + sc_keys)
        self._keymaps = {'sc_to_cc_key': sc_to_cc, 'cc_to_sc_key': cc_to_sc, 'public_keys': public_keys, 'keys': keys}

    # dict imitation:
    def keys(self):
        return self._keymaps['public_keys']

    def items(self):
        return [(k, self[k]) for k in self._keymaps['public_keys']]

    def values(self):
        return [self[k] for k in self._keymaps['public_keys']]

    def get(self, key, default=None):
        if key in self.keys():
            if key in self._keymaps['cc_to_sc_key']:
                key = self._keymaps['cc_to_sc_key'][key]
            return self[key]
        return default

    def __getitem__(self, k):
        if not isinstance(k, str):
            raise KeyError(f"key must be a string not '{type(k)}'")
        if k not in self._keymaps['keys']:
            raise KeyError(f"'{k}' not valid key. Examine 'FastInfo.keys()'")
        if k in self._keymaps['cc_to_sc_key']:
            k = self._keymaps['cc_to_sc_key'][k]
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
        if self._prices['prices_1y'] is None:
            self._prices['prices_1y'] = self._tkr.history(period="1y", auto_adjust=False, keepna=True)
            self._prices['md'] = self._tkr.get_history_metadata()
            try:
                ctp = self._prices['md']["currentTradingPeriod"]
                self._prices['today_open'] = pd.to_datetime(ctp["regular"]["start"], unit='s', utc=True).tz_convert(self.timezone)
                self._prices['today_close'] = pd.to_datetime(ctp["regular"]["end"], unit='s', utc=True).tz_convert(self.timezone)
                self._prices['today_midnight'] = self._prices['today_close'].ceil("D")
            except Exception:
                self._prices['today_open'] = None
                self._prices['today_close'] = None
                self._prices['today_midnight'] = None
                raise

        if self._prices['prices_1y'].empty:
            return self._prices['prices_1y']

        dnow = pd.Timestamp.now('UTC').tz_convert(self.timezone).date()
        d1 = dnow
        d0 = (d1 + datetime.timedelta(days=1)) - utils._interval_to_timedelta("1y")
        if fullDaysOnly and self._exchange_open_now():
            # Exclude today
            d1 -= utils._interval_to_timedelta("1d")
        return self._prices['prices_1y'].loc[str(d0):str(d1)]

    def _get_1wk_1h_prepost_prices(self):
        if self._prices['prices_1wk_1h_prepost'] is None:
            self._prices['prices_1wk_1h_prepost'] = self._tkr.history(period="5d", interval="1h", auto_adjust=False, prepost=True)
        return self._prices['prices_1wk_1h_prepost']

    def _get_1wk_1h_reg_prices(self):
        if self._prices['prices_1wk_1h_reg'] is None:
            self._prices['prices_1wk_1h_reg'] = self._tkr.history(period="5d", interval="1h", auto_adjust=False, prepost=False)
        return self._prices['prices_1wk_1h_reg']

    def _get_exchange_metadata(self):
        if self._prices['md'] is not None:
            return self._prices['md']

        self._get_1y_prices()
        self._prices['md'] = self._tkr.get_history_metadata()
        return self._prices['md']

    def _exchange_open_now(self):
        t = pd.Timestamp.now('UTC')
        self._get_exchange_metadata()

        # if self._prices['today_open'] is None and self._prices['today_close'] is None:
        #     r = False
        # else:
        #     r = self._prices['today_open'] <= t and t < self._prices['today_close']

        # if self._prices['today_midnight'] is None:
        #     r = False
        # elif self._prices['today_midnight'].date() > t.tz_convert(self.timezone).date():
        #     r = False
        # else:
        #     r = t < self._prices['today_midnight']

        last_day_cutoff = self._get_1y_prices().index[-1] + datetime.timedelta(days=1)
        last_day_cutoff += datetime.timedelta(minutes=20)
        r = t < last_day_cutoff

        # print("_exchange_open_now() returning", r)
        return r

    @property
    def currency(self):
        if self._market['currency'] is not None:
            return self._market['currency']

        md = self._tkr.get_history_metadata()
        self._market['currency'] = md["currency"]
        return self._market['currency']

    @property
    def quote_type(self):
        if self._market['quote_type'] is not None:
            return self._market['quote_type']

        md = self._tkr.get_history_metadata()
        self._market['quote_type'] = md["instrumentType"]
        return self._market['quote_type']

    @property
    def exchange(self):
        if self._market['exchange'] is not None:
            return self._market['exchange']

        self._market['exchange'] = self._get_exchange_metadata()["exchangeName"]
        return self._market['exchange']

    @property
    def timezone(self):
        if self._market['timezone'] is not None:
            return self._market['timezone']

        self._market['timezone'] = self._get_exchange_metadata()["exchangeTimezoneName"]
        return self._market['timezone']

    @property
    def shares(self):
        if self._market['shares'] is not None:
            return self._market['shares']

        shares = self._tkr.get_shares_full(start=pd.Timestamp.now('UTC').date()-pd.Timedelta(days=548))
        # if shares is None:
        #     # Requesting 18 months failed, so fallback to shares which should include last year
        #     shares = self._tkr.get_shares()
        if shares is not None:
            if isinstance(shares, pd.DataFrame):
                shares = shares[shares.columns[0]]
            self._market['shares'] = int(shares.iloc[-1])
        return self._market['shares']

    @property
    def last_price(self):
        if self._market['last_price'] is not None:
            return self._market['last_price']
        prices = self._get_1y_prices()
        if prices.empty:
            md = self._get_exchange_metadata()
            if "regularMarketPrice" in md:
                self._market['last_price'] = md["regularMarketPrice"]
        else:
            self._market['last_price'] = float(prices["Close"].iloc[-1])
            if _np.isnan(self._market['last_price']):
                md = self._get_exchange_metadata()
                if "regularMarketPrice" in md:
                    self._market['last_price'] = md["regularMarketPrice"]
        return self._market['last_price']

    @property
    def previous_close(self):
        if self._market['prev_close'] is not None:
            return self._market['prev_close']
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
                self._market['prev_close'] = float(prices["Close"].iloc[-2])
        if fail:
            # Fallback to original info[] if available.
            self._tkr.info  # trigger fetch
            k = "previousClose"
            if self._tkr._quote._cache['retired_info'] is not None and k in self._tkr._quote._cache['retired_info']:
                self._market['prev_close'] = self._tkr._quote._cache['retired_info'][k]
        return self._market['prev_close']

    @property
    def regular_market_previous_close(self):
        if self._market['reg_prev_close'] is not None:
            return self._market['reg_prev_close']
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
            if self._tkr._quote._cache['retired_info'] is not None and k in self._tkr._quote._cache['retired_info']:
                self._market['reg_prev_close'] = self._tkr._quote._cache['retired_info'][k]
        else:
            self._market['reg_prev_close'] = float(prices["Close"].iloc[-2])
        return self._market['reg_prev_close']

    @property
    def open(self):
        if self._market['open'] is not None:
            return self._market['open']
        prices = self._get_1y_prices()
        if prices.empty:
            self._market['open'] = None
        else:
            self._market['open'] = float(prices["Open"].iloc[-1])
            if _np.isnan(self._market['open']):
                self._market['open'] = None
        return self._market['open']

    @property
    def day_high(self):
        if self._market['day_high'] is not None:
            return self._market['day_high']
        prices = self._get_1y_prices()
        if prices.empty:
            self._market['day_high'] = None
        else:
            self._market['day_high'] = float(prices["High"].iloc[-1])
            if _np.isnan(self._market['day_high']):
                self._market['day_high'] = None
        return self._market['day_high']

    @property
    def day_low(self):
        if self._market['day_low'] is not None:
            return self._market['day_low']
        prices = self._get_1y_prices()
        if prices.empty:
            self._market['day_low'] = None
        else:
            self._market['day_low'] = float(prices["Low"].iloc[-1])
            if _np.isnan(self._market['day_low']):
                self._market['day_low'] = None
        return self._market['day_low']

    @property
    def last_volume(self):
        if self._market['last_volume'] is not None:
            return self._market['last_volume']
        prices = self._get_1y_prices()
        self._market['last_volume'] = None if prices.empty else int(prices["Volume"].iloc[-1])
        return self._market['last_volume']

    @property
    def fifty_day_average(self):
        if self._market['fifty_day_average'] is not None:
            return self._market['fifty_day_average']

        prices = self._get_1y_prices(fullDaysOnly=True)
        if prices.empty:
            self._market['fifty_day_average'] = None
        else:
            n = prices.shape[0]
            a = n-50
            b = n
            if a < 0:
                a = 0
            self._market['fifty_day_average'] = float(prices["Close"].iloc[a:b].mean())

        return self._market['fifty_day_average']

    @property
    def two_hundred_day_average(self):
        if self._market['two_hundred_day_average'] is not None:
            return self._market['two_hundred_day_average']

        prices = self._get_1y_prices(fullDaysOnly=True)
        if prices.empty:
            self._market['two_hundred_day_average'] = None
        else:
            n = prices.shape[0]
            a = n-200
            b = n
            if a < 0:
                a = 0

            self._market['two_hundred_day_average'] = float(prices["Close"].iloc[a:b].mean())

        return self._market['two_hundred_day_average']

    @property
    def ten_day_average_volume(self):
        if self._market['ten_day_average_volume'] is not None:
            return self._market['ten_day_average_volume']

        prices = self._get_1y_prices(fullDaysOnly=True)
        if prices.empty:
            self._market['ten_day_average_volume'] = None
        else:
            n = prices.shape[0]
            a = n-10
            b = n
            if a < 0:
                a = 0
            self._market['ten_day_average_volume'] = int(prices["Volume"].iloc[a:b].mean())

        return self._market['ten_day_average_volume']

    @property
    def three_month_average_volume(self):
        if self._market['three_month_average_volume'] is not None:
            return self._market['three_month_average_volume']

        prices = self._get_1y_prices(fullDaysOnly=True)
        if prices.empty:
            self._market['three_month_average_volume'] = None
        else:
            dt1 = prices.index[-1]
            dt0 = dt1 - utils._interval_to_timedelta("3mo") + utils._interval_to_timedelta("1d")
            self._market['three_month_average_volume'] = int(prices.loc[dt0:dt1, "Volume"].mean())

        return self._market['three_month_average_volume']

    @property
    def year_high(self):
        if self._market['year_high'] is not None:
            return self._market['year_high']

        prices = self._get_1y_prices(fullDaysOnly=True)
        if prices.empty:
            prices = self._get_1y_prices(fullDaysOnly=False)
        self._market['year_high'] = float(prices["High"].max())
        return self._market['year_high']

    @property
    def year_low(self):
        if self._market['year_low'] is not None:
            return self._market['year_low']

        prices = self._get_1y_prices(fullDaysOnly=True)
        if prices.empty:
            prices = self._get_1y_prices(fullDaysOnly=False)
        self._market['year_low'] = float(prices["Low"].min())
        return self._market['year_low']

    @property
    def year_change(self):
        if self._market['year_change'] is not None:
            return self._market['year_change']

        prices = self._get_1y_prices(fullDaysOnly=True)
        if prices.shape[0] >= 2:
            self._market['year_change'] = (prices["Close"].iloc[-1] - prices["Close"].iloc[0]) / prices["Close"].iloc[0]
            self._market['year_change'] = float(self._market['year_change'])
        return self._market['year_change']

    @property
    def market_cap(self):
        if self._market['mcap'] is not None:
            return self._market['mcap']

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
            if self._tkr._quote._cache['retired_info'] is not None and k in self._tkr._quote._cache['retired_info']:
                self._market['mcap'] = self._tkr._quote._cache['retired_info'][k]
        else:
            self._market['mcap'] = float(shares * self.last_price)
        return self._market['mcap']


class Quote:
    def __init__(self, data: YfData, symbol: str):
        self._data = data
        self._symbol = symbol
        self._cache = {k: None if k not in ('already_scraped', 'already_fetched', 'already_fetched_complementary') else False for k in (
            'info', 'retired_info', 'sustainability', 'recommendations',
            'upgrades_downgrades', 'calendar', 'sec_filings', 'valuation_measures',
            'already_scraped', 'already_fetched', 'already_fetched_complementary',
        )}

    @property
    def info(self) -> dict:
        if self._cache['info'] is None:
            self._fetch_info()
            self._fetch_complementary()

        return self._cache['info']

    @property
    def sustainability(self) -> pd.DataFrame:
        if self._cache['sustainability'] is None:
            result = self._fetch(modules=['esgScores'])
            if result is None:
                self._cache['sustainability'] = pd.DataFrame()
            else:
                try:
                    data = result["quoteSummary"]["result"][0]
                except (KeyError, IndexError):
                    if not YfConfig.debug.hide_exceptions:
                        raise
                    raise YFDataException(f"Failed to parse json response from Yahoo Finance: {result}")
                self._cache['sustainability'] = pd.DataFrame(data)
        return self._cache['sustainability']

    @property
    def recommendations(self) -> pd.DataFrame:
        if self._cache['recommendations'] is None:
            result = self._fetch(modules=['recommendationTrend'])
            if result is None:
                self._cache['recommendations'] = pd.DataFrame()
            else:
                try:
                    data = result["quoteSummary"]["result"][0]["recommendationTrend"]["trend"]
                except (KeyError, IndexError):
                    if not YfConfig.debug.hide_exceptions:
                        raise
                    raise YFDataException(f"Failed to parse json response from Yahoo Finance: {result}")
                self._cache['recommendations'] = pd.DataFrame(data)
        return self._cache['recommendations']

    @property
    def upgrades_downgrades(self) -> pd.DataFrame:
        if self._cache['upgrades_downgrades'] is None:
            result = self._fetch(modules=['upgradeDowngradeHistory'])
            if result is None:
                self._cache['upgrades_downgrades'] = pd.DataFrame()
            else:
                try:
                    data = result["quoteSummary"]["result"][0]["upgradeDowngradeHistory"]["history"]
                    if len(data) == 0:
                        raise YFDataException(f"No upgrade/downgrade history found for {self._symbol}")
                    df = pd.DataFrame(data)
                    df.rename(columns={"epochGradeDate": "GradeDate", 'firm': 'Firm', 'toGrade': 'ToGrade', 'fromGrade': 'FromGrade', 'action': 'Action'}, inplace=True)
                    df.set_index('GradeDate', inplace=True)
                    df.index = pd.to_datetime(df.index, unit='s')
                    self._cache['upgrades_downgrades'] = df
                except (KeyError, IndexError):
                    if not YfConfig.debug.hide_exceptions:
                        raise
                    raise YFDataException(f"Failed to parse json response from Yahoo Finance: {result}")
        return self._cache['upgrades_downgrades']

    @property
    def calendar(self) -> dict:
        if self._cache['calendar'] is None:
            self._fetch_calendar()
        return self._cache['calendar']

    @property
    def sec_filings(self) -> dict:
        if self._cache['sec_filings'] is None:
            f = self._fetch_sec_filings()
            self._cache['sec_filings'] = {} if f is None else f
        return self._cache['sec_filings']

    @property
    def valuation_measures(self) -> pd.DataFrame:
        if self._cache['valuation_measures'] is None:
            self._fetch_valuation_measures()
        return self._cache['valuation_measures']

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
        if self._cache['already_fetched']:
            return
        self._cache['already_fetched'] = True
        modules = ['financialData', 'quoteType', 'defaultKeyStatistics', 'assetProfile', 'summaryDetail']
        result = self._fetch(modules=modules)
        additional_info = self._fetch_additional_info()
        if additional_info is not None and result is not None:
            result.update(additional_info)
        else:
            result = additional_info

        query1_info = {}
        for quote in ["quoteSummary", "quoteResponse"]:
            if quote in result and len(result[quote]["result"]) > 0:
                result[quote]["result"][0]["symbol"] = self._symbol
                query_info = next(
                    (info for info in result.get(quote, {}).get("result", [])
                    if info["symbol"] == self._symbol),
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

        self._cache['info'] = {k: _format(k, v) for k, v in query1_info.items()}

    def _fetch_valuation_measures(self):
        url = f"https://finance.yahoo.com/quote/{self._symbol}/key-statistics"
        try:
            response = self._data.cache_get(url=url)
        except Exception as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            utils.get_yf_logger().error(f"Failed to fetch key-statistics page: {e}")
            self._cache['valuation_measures'] = pd.DataFrame()
            return

        try:
            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table")
            if table is None:
                self._cache['valuation_measures'] = pd.DataFrame()
                return

            headers = [th.get_text(strip=True) for th in table.find("tr").find_all(["th", "td"])]
            rows = []
            for tr in table.find_all("tr")[1:]:
                cells = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
                rows.append(cells)

            df = pd.DataFrame(rows, columns=headers)
            df = df.set_index(df.columns[0])
            df.index.name = None
            self._cache['valuation_measures'] = df
        except Exception as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            utils.get_yf_logger().error(f"Failed to parse key-statistics page: {e}")
            self._cache['valuation_measures'] = pd.DataFrame()

    def _fetch_complementary(self):
        if self._cache['already_fetched_complementary']:
            return
        self._cache['already_fetched_complementary'] = True

        self._fetch_info()
        if self._cache['info'] is None:
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
            #         self._cache['info'][k] = v
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
            json_result = json_data.get("timeseries") or json_data.get("finance")
            if json_result["error"] is not None:
                raise YFException("Failed to parse json response from Yahoo Finance: " + str(json_result["error"]))
            for k in keys:
                keydict = json_result["result"][0]
                if k in keydict:
                    self._cache['info'][k] = keydict[k][-1]["reportedValue"]["raw"]
                else:
                    self.info[k] = None

    def _fetch_calendar(self):
        # secFilings return too old data, so not requesting it for now
        result = self._fetch(modules=['calendarEvents'])
        if result is None:
            self._cache['calendar'] = {}
            return

        try:
            self._cache['calendar'] = dict()
            _events = result["quoteSummary"]["result"][0]["calendarEvents"]
            if 'dividendDate' in _events:
                self._cache['calendar']['Dividend Date'] = datetime.datetime.fromtimestamp(_events['dividendDate']).date()
            if 'exDividendDate' in _events:
                self._cache['calendar']['Ex-Dividend Date'] = datetime.datetime.fromtimestamp(_events['exDividendDate']).date()
            # splits = _events.get('splitDate')  # need to check later, i will add code for this if found data
            earnings = _events.get('earnings')
            if earnings is not None:
                self._cache['calendar']['Earnings Date'] = [datetime.datetime.fromtimestamp(d).date() for d in earnings.get('earningsDate', [])]
                self._cache['calendar']['Earnings High'] = earnings.get('earningsHigh', None)
                self._cache['calendar']['Earnings Low'] = earnings.get('earningsLow', None)
                self._cache['calendar']['Earnings Average'] = earnings.get('earningsAverage', None)
                self._cache['calendar']['Revenue High'] = earnings.get('revenueHigh', None)
                self._cache['calendar']['Revenue Low'] = earnings.get('revenueLow', None)
                self._cache['calendar']['Revenue Average'] = earnings.get('revenueAverage', None)
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

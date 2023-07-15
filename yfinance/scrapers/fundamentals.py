import datetime
import logging
import json

import pandas as pd
import numpy as np

from yfinance import utils, const
from yfinance.data import TickerData
from yfinance.exceptions import YFinanceException, YFNotImplementedError

class Fundamentals:

    def __init__(self, data: TickerData, proxy=None):
        self._data = data
        self.proxy = proxy

        self._earnings = None
        self._financials = None
        self._shares = None

        self._financials_data = None
        self._fin_data_quote = None
        self._basics_already_scraped = False
        self._financials = Financials(data)

    @property
    def financials(self) -> "Financials":
        return self._financials

    @property
    def earnings(self) -> dict:
        if self._earnings is None:
            raise YFNotImplementedError('earnings')
        return self._earnings

    @property
    def shares(self) -> pd.DataFrame:
        if self._shares is None:
            raise YFNotImplementedError('shares')
        return self._shares


class Financials:
    def __init__(self, data: TickerData):
        self._data = data
        self._income_time_series = {}
        self._balance_sheet_time_series = {}
        self._cash_flow_time_series = {}

    def get_income_time_series(self, freq="yearly", proxy=None) -> pd.DataFrame:
        res = self._income_time_series
        if freq not in res:
            res[freq] = self._fetch_time_series("income", freq, proxy=None)
        return res[freq]

    def get_balance_sheet_time_series(self, freq="yearly", proxy=None) -> pd.DataFrame:
        res = self._balance_sheet_time_series
        if freq not in res:
            res[freq] = self._fetch_time_series("balance-sheet", freq, proxy=None)
        return res[freq]

    def get_cash_flow_time_series(self, freq="yearly", proxy=None) -> pd.DataFrame:
        res = self._cash_flow_time_series
        if freq not in res:
            res[freq] = self._fetch_time_series("cash-flow", freq, proxy=None)
        return res[freq]

    @utils.log_indent_decorator
    def _fetch_time_series(self, name, timescale, proxy=None):
        # Fetching time series preferred over scraping 'QuoteSummaryStore',
        # because it matches what Yahoo shows. But for some tickers returns nothing, 
        # despite 'QuoteSummaryStore' containing valid data.

        allowed_names = ["income", "balance-sheet", "cash-flow"]
        allowed_timescales = ["yearly", "quarterly"]

        if name not in allowed_names:
            raise ValueError("Illegal argument: name must be one of: {}".format(allowed_names))
        if timescale not in allowed_timescales:
            raise ValueError("Illegal argument: timescale must be one of: {}".format(allowed_names))

        try:
            statement = self._create_financials_table(name, timescale, proxy)

            if statement is not None:
                return statement
        except YFinanceException as e:
            utils.get_yf_logger().error("%s: Failed to create %s financials table for reason: %r", self._data.ticker, name, e)
        return pd.DataFrame()

    def _create_financials_table(self, name, timescale, proxy):
        if name == "income":
            # Yahoo stores the 'income' table internally under 'financials' key
            name = "financials"

        keys = const.fundamentals_keys[name]

        try:
            return self.get_financials_time_series(timescale, keys, proxy)
        except Exception as e:
            pass

    def get_financials_time_series(self, timescale, keys: list, proxy=None) -> pd.DataFrame:
        timescale_translation = {"yearly": "annual", "quarterly": "quarterly"}
        timescale = timescale_translation[timescale]

        # Step 2: construct url:
        ts_url_base = \
            "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{0}?symbol={0}" \
                .format(self._data.ticker)

        url = ts_url_base + "&type=" + ",".join([timescale + k for k in keys])
        # Yahoo returns maximum 4 years or 5 quarters, regardless of start_dt:
        start_dt = datetime.datetime(2016, 12, 31)
        end = pd.Timestamp.utcnow().ceil("D")
        url += "&period1={}&period2={}".format(int(start_dt.timestamp()), int(end.timestamp()))

        # Step 3: fetch and reshape data
        json_str = self._data.cache_get(url=url, proxy=proxy).text
        json_data = json.loads(json_str)
        data_raw = json_data["timeseries"]["result"]
        # data_raw = [v for v in data_raw if len(v) > 1] # Discard keys with no data
        for d in data_raw:
            del d["meta"]

        # Now reshape data into a table:
        # Step 1: get columns and index:
        timestamps = set()
        data_unpacked = {}
        for x in data_raw:
            for k in x.keys():
                if k == "timestamp":
                    timestamps.update(x[k])
                else:
                    data_unpacked[k] = x[k]
        timestamps = sorted(list(timestamps))
        dates = pd.to_datetime(timestamps, unit="s")
        df = pd.DataFrame(columns=dates, index=list(data_unpacked.keys()))
        for k, v in data_unpacked.items():
            if df is None:
                df = pd.DataFrame(columns=dates, index=[k])
            df.loc[k] = {pd.Timestamp(x["asOfDate"]): x["reportedValue"]["raw"] for x in v}

        df.index = df.index.str.replace("^" + timescale, "", regex=True)

        # Reorder table to match order on Yahoo website
        df = df.reindex([k for k in keys if k in df.index])
        df = df[sorted(df.columns, reverse=True)]

        return df

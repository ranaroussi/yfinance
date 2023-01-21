import datetime as _dt
import re as _re

import pandas as _pd

from yfinance import utils
from yfinance.data import TickerData

from pprint import pprint

class KeyStats:

    def __init__(self, data: TickerData, proxy=None):
        self._data = data
        self.proxy = proxy

        self._stats = None
        self._valuations = None

        self._already_scraped = False

    @property
    def stats(self) -> dict:
        if self._stats is None:
            self._scrape(self.proxy)
        return self._stats

    @property
    def valuations(self) -> dict:
        if self._valuations is None:
            self._scrape(self.proxy)
        return self._valuations

    def _scrape(self, proxy):
        if self._already_scraped:
            return
        self._already_scraped = True


        data = self._data.get_json_data_stores('key-statistics', proxy)


        self._stats = data['QuoteSummaryStore']
        del self._stats["defaultKeyStatistics"]  # available in Ticker.info
        del self._stats["financialData"]  # available in Ticker.info
        exchange_tz = self._stats["quoteType"]["exchangeTimezoneName"]
        try:
            c = "calendarEvents"
            for k in ["dividendDate", "exDividendDate"]:
                self._stats[c][k] = _pd.to_datetime(self._stats[c][k], unit='s', utc=True)
                if self._stats[c][k].time() == _dt.time(0):
                    # Probably not UTC but meant to be in exchange timezone
                    self._stats[c][k] = self._stats[c][k].tz_convert(None).tz_localize(exchange_tz)
        except:
            pass


        ts = data['QuoteTimeSeriesStore']["timeSeries"]
        trailing_series = []
        year_series = []
        for k in ts:
            if len(ts[k]) == 0:
                # Yahoo website prints N/A, indicates Yahoo lacks necessary data to calculate
                continue

            if len(ts[k]) == 1:
                date = _pd.to_datetime(ts[k][0]["asOfDate"])

                v = ts[k][0]["reportedValue"]
                if isinstance(v, dict):
                    v = v["raw"]

                k = _re.sub("^trailing", "", k)
                trailing_series.append(_pd.Series([v], index=[date], name=k))

            else:
                if k == "timestamp":
                    continue

                dates = [d["asOfDate"] for d in ts[k]]
                dates = _pd.to_datetime(dates)

                has_raw = isinstance(ts[k][0]["reportedValue"], dict) and "raw" in ts[k][0]["reportedValue"]
                if has_raw:
                    values = [d["reportedValue"]["raw"] for d in ts[k]]
                else:
                    values = [d["reportedValue"] for d in ts[k]]

                k = _re.sub("^quarterly", "", k)
                year_series.append(_pd.Series(values, index=dates, name=k))

        year_table = _pd.concat(year_series, axis=1)
        trailing_table = _pd.concat(trailing_series, axis=1)
        table = _pd.concat([year_table, trailing_table], axis=0)
        table = table.T
        table = table[table.columns.sort_values(ascending=False)]

        self._valuations = table

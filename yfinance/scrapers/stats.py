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

        self._already_scraped = False

    @property
    def stats(self) -> dict:
        if self._stats is None:
            self._scrape(self.proxy)

        return self._stats

    def _scrape(self, proxy):
        if self._already_scraped:
            return
        self._already_scraped = True

        data = self._data.get_json_data_stores('key-statistics', proxy)
        key_stats = data['QuoteTimeSeriesStore']["timeSeries"]

        stats_trailing_series = []
        # stats_year_index = None
        stats_year_series = []
        for k in key_stats:
            if len(key_stats[k]) == 0:
                # Yahoo website prints N/A, indicates Yahoo lacks necessary data to calculate
                continue

            if len(key_stats[k]) == 1:
                date = _pd.to_datetime(key_stats[k][0]["asOfDate"])

                v = key_stats[k][0]["reportedValue"]
                if isinstance(v, dict):
                    v = v["raw"]

                k = _re.sub("^trailing", "", k)
                stats_trailing_series.append(_pd.Series([v], index=[date], name=k))

            else:
                if k == "timestamp":
                    # stats_year_index = _pd.to_datetime(key_stats[k], unit='s')
                    continue

                dates = [d["asOfDate"] for d in key_stats[k]]
                dates = _pd.to_datetime(dates)

                has_raw = isinstance(key_stats[k][0]["reportedValue"], dict) and "raw" in key_stats[k][0]["reportedValue"]
                if has_raw:
                    values = [d["reportedValue"]["raw"] for d in key_stats[k]]
                else:
                    values = [d["reportedValue"] for d in key_stats[k]]

                k = _re.sub("^quarterly", "", k)
                stats_year_series.append(_pd.Series(values, index=dates, name=k))

        stats_year_table = _pd.concat(stats_year_series, axis=1)
        stats_trailing_table = _pd.concat(stats_trailing_series, axis=1)
        stats_table = _pd.concat([stats_year_table, stats_trailing_table], axis=0)
        stats_table = stats_table.T
        stats_table = stats_table[stats_table.columns.sort_values(ascending=False)]

        self._stats = stats_table

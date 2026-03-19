"""Fundamentals and financial statement time-series scrapers."""

import datetime
import json
import warnings
from typing import Any, Optional

import pandas as pd

from yfinance.config import YF_CONFIG as YfConfig
from yfinance.data import YfData
from yfinance.exceptions import YFException, YFNotImplementedError
from .. import const, utils


class Fundamentals:
    """Entry point for fundamentals-related ticker datasets."""

    def __init__(self, data: YfData, symbol: str):
        """Initialize fundamentals scraper."""
        self._data = data
        self._symbol = symbol
        self._financials: "Financials" = Financials(data, symbol)

    @property
    def financials(self) -> "Financials":
        """Return financial statements helper."""
        return self._financials

    @property
    def earnings(self) -> None:
        """Deprecated earnings property retained for backward compatibility."""
        warnings.warn(
            (
                "'Ticker.earnings' is deprecated as not available via API. "
                'Look for "Net Income" in Ticker.income_stmt.'
            ),
            DeprecationWarning,
        )

    @property
    def shares(self) -> pd.DataFrame:
        """Shares are not currently implemented on this scraper."""
        raise YFNotImplementedError("shares")


class Financials:
    """Fetch and cache income, balance-sheet, and cash-flow time series."""

    def __init__(self, data: YfData, symbol: str):
        """Initialize financial statement caches."""
        self._data = data
        self._symbol = symbol
        self._income_time_series: dict[str, pd.DataFrame] = {}
        self._balance_sheet_time_series: dict[str, pd.DataFrame] = {}
        self._cash_flow_time_series: dict[str, pd.DataFrame] = {}

    def get_income_time_series(self, freq: str = "yearly") -> pd.DataFrame:
        """Return cached or fetched income statement time series."""
        res = self._income_time_series
        if freq not in res:
            res[freq] = self._fetch_time_series("income", freq)
        return res[freq]

    def get_balance_sheet_time_series(self, freq: str = "yearly") -> pd.DataFrame:
        """Return cached or fetched balance-sheet time series."""
        res = self._balance_sheet_time_series
        if freq not in res:
            res[freq] = self._fetch_time_series("balance-sheet", freq)
        return res[freq]

    def get_cash_flow_time_series(self, freq: str = "yearly") -> pd.DataFrame:
        """Return cached or fetched cash-flow time series."""
        res = self._cash_flow_time_series
        if freq not in res:
            res[freq] = self._fetch_time_series("cash-flow", freq)
        return res[freq]

    @utils.log_indent_decorator
    def _fetch_time_series(self, name: str, timescale: str) -> pd.DataFrame:
        """Fetch one financial statement time series from Yahoo's timeseries API."""
        allowed_names = ["income", "balance-sheet", "cash-flow"]
        allowed_timescales = ["yearly", "quarterly", "trailing"]

        if name not in allowed_names:
            raise ValueError(f"Illegal argument: name must be one of: {allowed_names}")
        if timescale not in allowed_timescales:
            raise ValueError(
                f"Illegal argument: timescale must be one of: {allowed_timescales}"
            )
        if timescale == "trailing" and name not in ("income", "cash-flow"):
            raise ValueError(
                "Illegal argument: frequency 'trailing' only available for cash-flow "
                "or income data."
            )

        try:
            statement = self._create_financials_table(name, timescale)
            if statement is not None:
                return statement
        except YFException as err:
            if not YfConfig.debug.hide_exceptions:
                raise
            utils.get_yf_logger().error(
                "%s: Failed to create %s financials table for reason: %s",
                self._symbol,
                name,
                err,
            )
        return pd.DataFrame()

    def _create_financials_table(
        self,
        name: str,
        timescale: str,
    ) -> Optional[pd.DataFrame]:
        """Create a financial statement DataFrame for one name/timescale."""
        if name == "income":
            # Yahoo stores the income statement under the "financials" key.
            name = "financials"

        keys = const.fundamentals_keys[name]

        try:
            return self._get_financials_time_series(timescale, keys)
        except (KeyError, TypeError, ValueError, IndexError, json.JSONDecodeError):
            if not YfConfig.debug.hide_exceptions:
                raise
            return None

    def _translate_timescale(self, timescale: str) -> str:
        mapping = {
            "yearly": "annual",
            "quarterly": "quarterly",
            "trailing": "trailing",
        }
        return mapping[timescale]

    def _build_financials_url(self, timescale: str, keys: list[str]) -> str:
        base_url = (
            "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/"
            f"v1/finance/timeseries/{self._symbol}?symbol={self._symbol}"
        )
        statement_type = ",".join(timescale + key for key in keys)
        start_dt = datetime.datetime(2016, 12, 31)
        end = pd.Timestamp.now("UTC").ceil("D")
        return (
            f"{base_url}&type={statement_type}"
            f"&period1={int(start_dt.timestamp())}"
            f"&period2={int(end.timestamp())}"
        )

    def _fetch_timeseries_result(self, url: str) -> list[dict[str, Any]]:
        json_str = self._data.cache_get(url=url).text
        json_data = json.loads(json_str)
        data_raw = json_data["timeseries"]["result"]
        for item in data_raw:
            item.pop("meta", None)
        return data_raw

    @staticmethod
    def _unpack_timeseries_data(
        data_raw: list[dict[str, Any]],
    ) -> tuple[list[int], dict[str, Any]]:
        timestamps = set()
        unpacked: dict[str, Any] = {}
        for item in data_raw:
            for key, value in item.items():
                if key == "timestamp":
                    timestamps.update(value)
                else:
                    unpacked[key] = value
        return sorted(timestamps), unpacked

    @staticmethod
    def _build_table(unpacked: dict[str, Any], timestamps: list[int]) -> pd.DataFrame:
        dates = pd.to_datetime(timestamps, unit="s")
        df = pd.DataFrame(columns=dates, index=list(unpacked.keys()))
        for key, values in unpacked.items():
            df.loc[key] = {
                pd.Timestamp(point["asOfDate"]): point["reportedValue"]["raw"]
                for point in values
            }
        return df

    @staticmethod
    def _coerce_float_columns(df: pd.DataFrame) -> pd.DataFrame:
        for column in df.columns:
            df[column] = df[column].astype("float")
        return df

    def _get_financials_time_series(self, timescale: str, keys: list[str]) -> pd.DataFrame:
        """Fetch and reshape one financial statement time series table."""
        translated_timescale = self._translate_timescale(timescale)
        url = self._build_financials_url(translated_timescale, keys)

        data_raw = self._fetch_timeseries_result(url)
        timestamps, unpacked = self._unpack_timeseries_data(data_raw)
        table = self._build_table(unpacked, timestamps)

        table.index = table.index.str.replace(f"^{translated_timescale}", "", regex=True)
        table = self._coerce_float_columns(table)

        table = table.reindex([key for key in keys if key in table.index])
        table = table[sorted(table.columns, reverse=True)]

        if translated_timescale == "trailing":
            table = table.iloc[:, [0]]

        return pd.DataFrame(table)

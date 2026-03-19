"""Historical price scraper and repair logic for Yahoo Finance chart data."""

import datetime as _datetime
import time as _time
from typing import Any, Optional, cast

from curl_cffi import requests
import dateutil as _dateutil
from dateutil.relativedelta import relativedelta as _relativedelta
import numpy as np
import pandas as pd

from yfinance import utils
from yfinance.config import YF_CONFIG as YfConfig
from yfinance.const import _PRICE_COLNAMES_
from .capital_gains import repair_capital_gains
from .dividend_repair import fix_bad_div_adjust
from .fetch import fetch_history
from .helpers import (
    _PriceChangeRepairSettings,
    _parse_history_request,
)
from .price_repair import fix_prices_sudden_change
from .reconstruct import reconstruct_intervals_batch
from .repair_workflows import fix_unit_random_mixups, fix_zeroes
from .split_repair import fix_bad_stock_splits


class PriceHistory:
    """Fetch, normalize, and optionally repair Yahoo Finance price history."""

    def __init__(self, data, ticker, tz, session=None):
        self._data = data
        self.ticker = ticker.upper()
        self.tz = tz
        self.session = session or requests.Session(impersonate="chrome")

        self._history_cache: dict[tuple[str, str], pd.DataFrame] = {}
        self._history_metadata: dict[str, Any] = {}
        self._history_metadata_formatted = False

        # Limit recursion depth when repairing prices
        self._reconstruct_start_interval: Optional[str] = None

    @utils.log_indent_decorator
    def history(self, *args, **kwargs) -> pd.DataFrame:
        """
        :Parameters:
            period : str
              | Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
              | Default: 1mo
              | Can combine with start/end e.g. end = start + period
            interval : str
              | Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
              | Intraday data cannot extend last 60 days
            start : str
              | Download start date string (YYYY-MM-DD) or _datetime, inclusive.
              | Default: 99 years ago
              | E.g. for start="2020-01-01", first data point = "2020-01-01"
            end : str
              | Download end date string (YYYY-MM-DD) or _datetime, exclusive.
              | Default: now
              | E.g. for end="2023-01-01", last data point = "2022-12-31"
            prepost : bool
              | Include Pre and Post market data in results?
              | Default: False
            auto_adjust : bool
              | Adjust all OHLC automatically?
              | Default: True
            back_adjust : bool
              | Back-adjusted data to mimic true historical prices
            repair : bool
              | Fixes price errors in Yahoo data: 100x, missing, bad dividend adjust.
              | Default: False
              | Full details at: :doc:`../advanced/price_repair`.
            keepna : bool
              | Keep NaN rows returned by Yahoo?
              | Default: False
            rounding : bool
              | Optional: Round values to 2 decimal places?
              | Default: False = use precision suggested by Yahoo!
            timeout : None or float
              | Optional: timeout fetches after N seconds
              | Default: 10 seconds
            raise_errors : bool
                If True, then raise errors as Exceptions instead of logging.
        """
        request = _parse_history_request("history", args, kwargs)
        return fetch_history(self, request)

    def _get_history_cache(self, period="max", interval="1d") -> pd.DataFrame:
        cache_key = (interval, period)
        if cache_key in self._history_cache:
            return self._history_cache[cache_key]

        df = self.history(period=period, interval=interval, prepost=True)
        self._history_cache[cache_key] = df
        return df

    def get_history_metadata(self) -> dict[str, Any]:
        """Return cached Yahoo chart metadata, fetching trading periods when needed."""
        if "tradingPeriods" not in self._history_metadata:
            # Request intraday data, because then Yahoo returns exchange schedule (tradingPeriods).
            self._get_history_cache(period="5d", interval="1h")

        if not self._history_metadata_formatted:
            formatted_md = utils.format_history_metadata(self._history_metadata)
            if isinstance(formatted_md, dict):
                self._history_metadata = cast(dict[str, Any], formatted_md)
            self._history_metadata_formatted = True

        return self._history_metadata

    def set_history_metadata(
        self,
        metadata: dict[str, Any],
        formatted: Optional[bool] = None,
    ) -> None:
        """Replace cached Yahoo chart metadata, optionally updating format state."""
        self._history_metadata = metadata
        if formatted is not None:
            self._history_metadata_formatted = formatted

    def clear_history_metadata(self) -> None:
        """Reset cached Yahoo chart metadata to an empty mapping."""
        self._history_metadata = {}
        self._history_metadata_formatted = False

    def get_history_metadata_value(self, key: str, default: Any = None) -> Any:
        """Read one cached history metadata value."""
        return self._history_metadata.get(key, default)

    def get_data_client(self):
        """Return the underlying Yahoo data client used for requests."""
        return self._data

    def set_history_metadata_value(self, key: str, value: Any) -> None:
        """Update one cached history metadata value."""
        self._history_metadata[key] = value

    def get_reconstruct_start_interval(self) -> Optional[str]:
        """Return the current repair recursion guard interval."""
        return self._reconstruct_start_interval

    def set_reconstruct_start_interval(self, interval: Optional[str]) -> None:
        """Update the repair recursion guard interval."""
        self._reconstruct_start_interval = interval

    def clear_reconstruct_start_interval(self, interval: Optional[str] = None) -> None:
        """Clear the repair recursion guard interval when it matches the requested value."""
        if interval is None or self._reconstruct_start_interval == interval:
            self._reconstruct_start_interval = None

    def reconstruct_intervals_batch(self, df, interval, prepost, tag=-1.0):
        """Public wrapper for interval reconstruction helpers."""
        return self._reconstruct_intervals_batch(df, interval, prepost, tag)

    def resample_history(self, df, df_interval, target_interval, period=None) -> pd.DataFrame:
        """Public wrapper around history resampling for helper modules."""
        return self._resample(df, df_interval, target_interval, period)

    def standardise_currency(self, df, currency):
        """Public wrapper for currency normalization helpers."""
        return self._standardise_currency(df, currency)

    def convert_dividends_currency(self, dividends, fx, repair=False):
        """Public wrapper for dividend FX normalization helpers."""
        return self._dividends_convert_fx(dividends, fx, repair)

    def repair_unit_mixups(self, df, interval, tz_exchange, prepost):
        """Public wrapper for unit-mixup repair helpers."""
        return self._fix_unit_mixups(df, interval, tz_exchange, prepost)

    def repair_zero_price_rows(self, df, interval, tz_exchange, prepost):
        """Public wrapper for zero-price repair helpers."""
        return self._fix_zeroes(df, interval, tz_exchange, prepost)

    def repair_bad_div_adjust(self, df, interval, currency):
        """Public wrapper for dividend-adjustment repair helpers."""
        return self._fix_bad_div_adjust(df, interval, currency)

    def repair_bad_stock_splits(self, df, interval, tz_exchange):
        """Public wrapper for stock-split repair helpers."""
        return self._fix_bad_stock_splits(df, interval, tz_exchange)

    def repair_capital_gains(self, df):
        """Public wrapper for capital-gains repair helpers."""
        return self._repair_capital_gains(df)

    def repair_prices_sudden_change(self, df, settings: _PriceChangeRepairSettings):
        """Public wrapper for sudden price-change repair helpers."""
        return self._fix_prices_sudden_change(df, settings)

    def get_dividends(self, period="max") -> pd.Series:
        """Return non-zero dividend events for the requested period."""
        df = self._get_history_cache(period=period)
        if "Dividends" in df.columns:
            dividends = cast(pd.Series, df["Dividends"])
            return cast(pd.Series, dividends[dividends != 0])
        return pd.Series()

    def get_capital_gains(self, period="max") -> pd.Series:
        """Return non-zero capital-gains events for the requested period."""
        df = self._get_history_cache(period=period)
        if "Capital Gains" in df.columns:
            capital_gains = cast(pd.Series, df["Capital Gains"])
            return cast(pd.Series, capital_gains[capital_gains != 0])
        return pd.Series()

    def get_splits(self, period="max") -> pd.Series:
        """Return non-zero stock-split events for the requested period."""
        df = self._get_history_cache(period=period)
        if "Stock Splits" in df.columns:
            splits = cast(pd.Series, df["Stock Splits"])
            return cast(pd.Series, splits[splits != 0])
        return pd.Series()

    def get_actions(self, period="max") -> pd.DataFrame:
        """Return the combined actions dataframe for the requested period."""
        df = self._get_history_cache(period=period)

        action_columns = []
        if "Dividends" in df.columns:
            action_columns.append("Dividends")
        if "Stock Splits" in df.columns:
            action_columns.append("Stock Splits")
        if "Capital Gains" in df.columns:
            action_columns.append("Capital Gains")

        if action_columns:
            actions = cast(pd.DataFrame, df[action_columns])
            return cast(pd.DataFrame, actions[actions != 0].dropna(how="all").fillna(0))
        return pd.DataFrame(index=df.index.copy())

    def _resample_period_and_origin(self, target_interval, period, df_tz):
        origin = "epoch"
        if target_interval == "1wk":
            if period == "ytd":
                year_start = pd.Timestamp(f"{_datetime.datetime.now().year}-01-01")
                return "7D", year_start.tz_localize(df_tz)
            return "W-MON", origin
        if target_interval == "5d":
            if period == "ytd":
                year_start = pd.Timestamp(f"{_datetime.datetime.now().year}-01-01")
                return "5D", year_start.tz_localize(df_tz)
            return "5D", origin
        if target_interval == "1mo":
            return "MS", origin
        if target_interval == "3mo":
            align_month = (
                "JAN"
                if period == "ytd"
                else _datetime.datetime.now().strftime("%b").upper()
            )
            return f"QS-{align_month}", origin
        raise ValueError(f"Not implemented resampling to interval '{target_interval}'")

    @staticmethod
    def _resample_map(df) -> dict[str, str]:
        resample_map = {
            "Open": "first",
            "Low": "min",
            "High": "max",
            "Close": "last",
            "Volume": "sum",
            "Dividends": "sum",
            "Stock Splits": "prod",
        }
        if "Repaired?" in df.columns:
            resample_map["Repaired?"] = "any"
        if "Adj Close" in df.columns:
            resample_map["Adj Close"] = resample_map["Close"]
        if "Capital Gains" in df.columns:
            resample_map["Capital Gains"] = "sum"
        return resample_map

    def _resample(self, df, df_interval, target_interval, period=None) -> pd.DataFrame:
        # resample
        if df_interval == target_interval:
            return df
        offset = None
        df_index = pd.DatetimeIndex(df.index)
        df_tz = df_index.tz
        resample_period, origin = self._resample_period_and_origin(target_interval, period, df_tz)
        resample_map = self._resample_map(df)
        df.loc[df["Stock Splits"] == 0.0, "Stock Splits"] = 1.0
        if origin != "epoch":
            df2 = df.resample(resample_period, label="left", closed="left", origin=origin).agg(
                resample_map
            )
        else:
            df2 = df.resample(resample_period, label="left", closed="left", offset=offset).agg(
                resample_map
            )
        df2.loc[df2["Stock Splits"] == 1.0, "Stock Splits"] = 0.0
        return df2

    @utils.log_indent_decorator
    def _reconstruct_intervals_batch(self, df, interval, prepost, tag=-1.0):
        return reconstruct_intervals_batch(self, df, interval, prepost, tag=tag)

    @staticmethod
    def _subunit_currency_details(currency):
        mapping = {
            "GBp": ("GBP", 0.01),
            "ZAc": ("ZAR", 0.01),
            "ILA": ("ILS", 0.01),
        }
        return mapping.get(currency)

    def _latest_traded_row(self, df):
        f_volume = df["Volume"] > 0
        if not f_volume.any():
            return None
        return df.iloc[np.where(f_volume)[0][-1]]

    def _prices_are_in_subunits(self, last_row, multiplier: float) -> bool:
        if last_row is None:
            return False
        if last_row.name <= (pd.Timestamp.now("UTC") - _datetime.timedelta(days=30)):
            return True
        try:
            ratio = self._history_metadata["regularMarketPrice"] / last_row["Close"]
            return abs((ratio * multiplier) - 1) >= 0.1
        except (KeyError, TypeError, ValueError, ZeroDivisionError):
            if not YfConfig.debug.hide_exceptions:
                raise
            return True

    @staticmethod
    def _should_scale_dividends(df) -> bool:
        f_div = df["Dividends"] != 0.0
        if not f_div.any():
            return False
        divs = df[["Close", "Dividends"]].copy()
        divs["Close"] = divs["Close"].ffill().shift(1, fill_value=divs["Close"].iloc[0])
        divs = divs[f_div]
        div_pcts = (divs["Dividends"] / divs["Close"]).to_numpy()
        return len(div_pcts) > 0 and np.average(div_pcts) > 1

    def _standardise_currency(self, df, currency):
        details = self._subunit_currency_details(currency)
        if details is None:
            return df, currency
        currency2, multiplier = details
        last_row = self._latest_traded_row(df)
        if last_row is None:
            return df, currency
        if self._prices_are_in_subunits(last_row, multiplier):
            for c in _PRICE_COLNAMES_:
                df[c] *= multiplier
        self._history_metadata["currency"] = currency2

        if self._should_scale_dividends(df):
            df["Dividends"] *= multiplier

        return df, currency2

    def _dividends_convert_fx(self, dividends, fx, repair=False):
        bad_div_currencies = [c for c in dividends["currency"].unique() if c != fx]
        major_currencies = ["USD", "JPY", "EUR", "CNY", "GBP", "CAD"]
        for c in bad_div_currencies:
            fx2_tkr = None
            if c == "USD":
                # Simple convert from USD to target FX
                fx_tkr = f"{fx}=X"
                reverse = False
            elif fx == "USD":
                # Use same USD FX but reversed
                fx_tkr = f"{fx}=X"
                reverse = True
            elif c in major_currencies and fx in major_currencies:
                # Simple convert
                fx_tkr = f"{c}{fx}=X"
                reverse = False
            else:
                # No guarantee that Yahoo has direct FX conversion, so
                # convert via USD
                # - step 1: -> USD
                fx_tkr = f"{c}=X"
                reverse = True
                # - step 2: USD -> FX
                fx2_tkr = f"{fx}=X"

            fx_dat = PriceHistory(self._data, fx_tkr, self.session)
            fx_rate = fx_dat.history(period="1mo", repair=repair)["Close"].iloc[-1]
            if reverse:
                fx_rate = 1 / fx_rate
            dividends.loc[dividends["currency"] == c, "Dividends"] *= fx_rate
            if fx2_tkr is not None:
                fx2_dat = PriceHistory(self._data, fx2_tkr, self.session)
                fx2_rate = fx2_dat.history(period="1mo", repair=repair)["Close"].iloc[-1]
                dividends.loc[dividends["currency"] == c, "Dividends"] *= fx2_rate

        dividends["currency"] = fx
        return dividends

    @utils.log_indent_decorator
    def _fix_unit_mixups(self, df, interval, tz_exchange, prepost):
        if df.empty:
            return df
        df2 = self._fix_unit_switch(df, interval, tz_exchange)
        df3 = self._fix_unit_random_mixups(df2, interval, tz_exchange, prepost)
        return df3

    @utils.log_indent_decorator
    def _fix_unit_random_mixups(self, df, interval, tz_exchange, prepost):
        return fix_unit_random_mixups(self, df, interval, tz_exchange, prepost)

    @utils.log_indent_decorator
    def _fix_unit_switch(self, df, interval, tz_exchange):
        # Sometimes Yahoo returns few prices in cents/pence instead of $/£
        # I.e. 100x bigger
        # 2 ways this manifests:
        # - random 100x errors spread throughout table
        # - a sudden switch between $<->cents at some date
        # This function fixes the second.
        # Eventually Yahoo fixes but could take them 2 weeks.

        if self._history_metadata["currency"] == "KWF":
            # Kuwaiti Dinar divided into 1000 not 100
            n = 1000
        else:
            n = 100
        return self._fix_prices_sudden_change(
            df,
            _PriceChangeRepairSettings(
                interval=interval,
                tz_exchange=tz_exchange,
                change=float(n),
                correct_dividend=True,
            ),
        )

    @utils.log_indent_decorator
    def _fix_zeroes(self, df, interval, tz_exchange, prepost):
        return fix_zeroes(self, df, interval, tz_exchange, prepost)

    @utils.log_indent_decorator
    def _repair_capital_gains(self, df):
        return repair_capital_gains(self, df)

    @utils.log_indent_decorator
    def _fix_bad_div_adjust(self, df, interval, _currency):
        return fix_bad_div_adjust(self, df, interval, _currency)

    @utils.log_indent_decorator
    def _fix_bad_stock_splits(self, df, interval, tz_exchange):
        return fix_bad_stock_splits(self, df, interval, tz_exchange)

    @utils.log_indent_decorator
    def _fix_prices_sudden_change(self, df, settings: _PriceChangeRepairSettings):
        return fix_prices_sudden_change(self, df, settings)

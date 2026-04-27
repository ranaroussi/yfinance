import datetime
import json
import re
import warnings
from datetime import timedelta, timezone

import polars as pl

from yfinance import const, utils
from yfinance.config import YfConfig
from yfinance.data import YfData
from yfinance.exceptions import YFException, YFNotImplementedError


class Fundamentals:
    def __init__(self, data: YfData, symbol: str):
        self._data = data
        self._symbol = symbol

        self._earnings = None
        self._financials = None
        self._shares = None

        self._financials_data = None
        self._fin_data_quote = None
        self._basics_already_scraped = False
        self._financials = Financials(data, symbol)

    @property
    def financials(self) -> "Financials":
        return self._financials

    @property
    def earnings(self) -> dict:
        warnings.warn(
            "'Ticker.earnings' is deprecated as not available via API. Look for \"Net Income\" in Ticker.income_stmt.",
            DeprecationWarning,
        )
        return None

    @property
    def shares(self) -> pl.DataFrame:
        if self._shares is None:
            raise YFNotImplementedError("shares")
        return self._shares


class Financials:
    def __init__(self, data: YfData, symbol: str):
        self._data = data
        self._symbol = symbol
        self._income_time_series = {}
        self._balance_sheet_time_series = {}
        self._cash_flow_time_series = {}

    def get_income_time_series(self, freq="yearly") -> pl.DataFrame:
        res = self._income_time_series
        if freq not in res:
            res[freq] = self._fetch_time_series("income", freq)
        return res[freq]

    def get_balance_sheet_time_series(self, freq="yearly") -> pl.DataFrame:
        res = self._balance_sheet_time_series
        if freq not in res:
            res[freq] = self._fetch_time_series("balance-sheet", freq)
        return res[freq]

    def get_cash_flow_time_series(self, freq="yearly") -> pl.DataFrame:
        res = self._cash_flow_time_series
        if freq not in res:
            res[freq] = self._fetch_time_series("cash-flow", freq)
        return res[freq]

    @utils.log_indent_decorator
    def _fetch_time_series(self, name, timescale):
        # Fetching time series preferred over scraping 'QuoteSummaryStore',
        # because it matches what Yahoo shows. But for some tickers returns nothing,
        # despite 'QuoteSummaryStore' containing valid data.

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
                "Illegal argument: frequency 'trailing'"
                + " only available for cash-flow or income data."
            )

        try:
            statement = self._create_financials_table(name, timescale)

            if statement is not None:
                return statement
        except YFException as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            utils.get_yf_logger().error(
                f"{self._symbol}: Failed to create {name} financials table for reason: {e}"
            )
        return pl.DataFrame()

    def _create_financials_table(self, name, timescale):
        if name == "income":
            # Yahoo stores the 'income' table internally under 'financials' key
            name = "financials"

        keys = const.fundamentals_keys[name]

        try:
            return self._get_financials_time_series(timescale, keys)
        except Exception:
            if not YfConfig.debug.hide_exceptions:
                raise
            pass

    def _get_financials_time_series(self, timescale, keys: list) -> pl.DataFrame:
        timescale_translation = {
            "yearly": "annual",
            "quarterly": "quarterly",
            "trailing": "trailing",
        }
        timescale = timescale_translation[timescale]

        # Step 2: construct url:
        ts_url_base = f"https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{self._symbol}?symbol={self._symbol}"
        url = ts_url_base + "&type=" + ",".join([timescale + k for k in keys])
        # Yahoo returns maximum 4 years or 5 quarters, regardless of start_dt:
        start_dt = datetime.datetime(2016, 12, 31)
        end_dt = datetime.datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + timedelta(days=1)
        url += f"&period1={int(start_dt.timestamp())}&period2={int(end_dt.timestamp())}"

        # Step 3: fetch and reshape data
        json_str = self._data.cache_get(url=url).text
        json_data = json.loads(json_str)
        data_raw = json_data["timeseries"]["result"]
        for d in data_raw:
            del d["meta"]

        # Unpack the raw data
        data_unpacked = {}
        for x in data_raw:
            for k in x.keys():
                if k != "timestamp":
                    data_unpacked[k] = x[k]

        # Build long-form rows
        rows = []
        for k, v in data_unpacked.items():
            metric = re.sub(f"^{timescale}", "", k)
            for entry in v:
                raw_val = None
                if entry.get("reportedValue") is not None:
                    try:
                        raw_val = float(entry["reportedValue"]["raw"])
                    except (TypeError, KeyError, ValueError):
                        raw_val = None
                rows.append(
                    {
                        "metric": metric,
                        "date": entry[
                            "asOfDate"
                        ],  # string like "2023-12-31" or "2023-12-31T00:00:00.000Z"
                        "value": raw_val,
                    }
                )

        if not rows:
            return pl.DataFrame()

        # Parse date strings — try ISO with timezone first, then plain date
        long_df = pl.DataFrame(rows).with_columns(
            pl.coalesce(
                [
                    pl.col("date").str.to_datetime(
                        format="%Y-%m-%dT%H:%M:%S%z", strict=False
                    ),
                    pl.col("date").str.to_datetime(
                        format="%Y-%m-%dT%H:%M:%S%.3f%z", strict=False
                    ),
                    pl.col("date").str.to_datetime(format="%Y-%m-%d", strict=False),
                ]
            ).alias("date")
        )

        # Keep only requested metrics in order
        existing_metrics = long_df["metric"].unique().to_list()
        valid_metrics = [
            re.sub(f"^{timescale}", "", k)
            for k in keys
            if re.sub(f"^{timescale}", "", k) in existing_metrics
        ]
        long_df = long_df.filter(pl.col("metric").is_in(valid_metrics))

        if long_df.is_empty():
            return pl.DataFrame()

        # Pivot to wide form: rows=metric, cols=date
        wide_df = long_df.pivot(
            on="date", index="metric", values="value", aggregate_function="first"
        )

        # Sort date columns descending (most recent first), keep metric col first
        date_cols = sorted([c for c in wide_df.columns if c != "metric"], reverse=True)
        wide_df = wide_df.select(["metric"] + date_cols)

        # Reorder rows to match order on Yahoo website
        metric_order = {m: i for i, m in enumerate(valid_metrics)}
        wide_df = wide_df.sort(
            pl.col("metric").map_elements(
                lambda m: metric_order.get(m, 9999), return_dtype=pl.Int32
            )
        )

        # Trailing 12 months: return only the first date column (plus metric)
        if timescale == "trailing" and len(wide_df.columns) > 2:
            wide_df = wide_df.select([wide_df.columns[0], wide_df.columns[1]])

        return wide_df

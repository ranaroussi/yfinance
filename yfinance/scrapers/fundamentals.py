import datetime
import json

import pandas as pd

from yfinance import utils, const
from yfinance.config import YfConfig
from yfinance.const import _BASE_URL_
from yfinance.data import YfData
from yfinance.exceptions import YFException, YFNotImplementedError


_QUOTE_SUMMARY_URL_ = f"{_BASE_URL_}/v10/finance/quoteSummary/"


class Fundamentals:

    def __init__(self, data: YfData, symbol: str):
        self._data = data
        self._symbol = symbol

        self._earnings = None
        self._eps_history = None
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
        """Revenue + earnings + profit margin from the Yahoo ``earnings``
        quoteSummary module. Mirrors the chart shown on the quote page.

        Returns a dict with keys ``yearly`` and ``quarterly`` mapping to
        DataFrames (columns: Revenue, Earnings, Profit Margin), plus
        ``financialCurrency``."""
        if self._earnings is None:
            self._fetch_earnings()
        return self._earnings

    @property
    def eps_history(self) -> pd.DataFrame:
        """Quarterly EPS history (actual vs estimate, surprise %, period end
        and reported date) from the Yahoo ``earnings`` quoteSummary module.
        The earnings-call beat/miss chart on the quote page."""
        if self._eps_history is None:
            self._fetch_earnings()
        return self._eps_history

    def _fetch_earnings(self) -> None:
        """Single-shot fetch of the ``earnings`` module: populates both
        ``self._earnings`` (revenue + earnings) and ``self._eps_history``
        (EPS chart) so callers don't pay two HTTP calls."""
        try:
            resp = self._data.get(
                url=_QUOTE_SUMMARY_URL_ + self._symbol,
                params={"modules": "earnings"},
            )
            payload = resp.json()
            block = (payload.get("quoteSummary") or {}).get("result") or []
            block = block[0].get("earnings") if block else None
        except Exception as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            utils.get_yf_logger().error(f"{self._symbol}: failed fetching earnings module: {e}")
            block = None

        currency = (block or {}).get("financialCurrency", "USD")
        self._earnings = {
            "yearly": self._parse_financials_chart((block or {}).get("financialsChart", {}).get("yearly", [])),
            "quarterly": self._parse_financials_chart((block or {}).get("financialsChart", {}).get("quarterly", [])),
            "financialCurrency": currency,
        }
        self._eps_history = self._parse_earnings_chart((block or {}).get("earningsChart", {}).get("quarterly", []))

    @staticmethod
    def _parse_financials_chart(rows: list) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=["Revenue", "Earnings", "Profit Margin"])
        df = pd.DataFrame([
            {
                "date": row.get("date"),
                "Revenue": (row.get("revenue") or {}).get("raw"),
                "Earnings": (row.get("earnings") or {}).get("raw"),
                "Profit Margin": (row.get("profitMargin") or {}).get("raw"),
            }
            for row in rows
        ])
        return df.set_index("date")

    @staticmethod
    def _parse_earnings_chart(rows: list) -> pd.DataFrame:
        if not rows:
            return pd.DataFrame(columns=["EPS Actual", "EPS Estimate", "Surprise %", "Period End", "Reported Date"])
        df = pd.DataFrame([
            {
                "date": row.get("date"),
                "EPS Actual": (row.get("actual") or {}).get("raw"),
                "EPS Estimate": (row.get("estimate") or {}).get("raw"),
                "Surprise %": (float(row["surprisePct"]) if row.get("surprisePct") not in (None, "") else None),
                "Period End": pd.to_datetime((row.get("periodEndDate") or {}).get("raw"), unit="s", errors="coerce"),
                "Reported Date": pd.to_datetime((row.get("reportedDate") or {}).get("raw"), unit="s", errors="coerce"),
            }
            for row in rows
        ])
        return df.set_index("date")

    @property
    def shares(self) -> pd.DataFrame:
        if self._shares is None:
            raise YFNotImplementedError('shares')
        return self._shares


class Financials:
    def __init__(self, data: YfData, symbol: str):
        self._data = data
        self._symbol = symbol
        self._income_time_series = {}
        self._balance_sheet_time_series = {}
        self._cash_flow_time_series = {}

    def get_income_time_series(self, freq="yearly") -> pd.DataFrame:
        res = self._income_time_series
        if freq not in res:
            res[freq] = self._fetch_time_series("income", freq)
        return res[freq]

    def get_balance_sheet_time_series(self, freq="yearly") -> pd.DataFrame:
        res = self._balance_sheet_time_series
        if freq not in res:
            res[freq] = self._fetch_time_series("balance-sheet", freq)
        return res[freq]

    def get_cash_flow_time_series(self, freq="yearly") -> pd.DataFrame:
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
            raise ValueError(f"Illegal argument: timescale must be one of: {allowed_timescales}")
        if timescale == "trailing" and name not in ('income', 'cash-flow'):
            raise ValueError("Illegal argument: frequency 'trailing'" +
                             " only available for cash-flow or income data.")

        try:
            statement = self._create_financials_table(name, timescale)

            if statement is not None:
                return statement
        except YFException as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            utils.get_yf_logger().error(f"{self._symbol}: Failed to create {name} financials table for reason: {e}")
        return pd.DataFrame()

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

    def _get_financials_time_series(self, timescale, keys: list) -> pd.DataFrame:
        timescale_translation = {"yearly": "annual", "quarterly": "quarterly", "trailing": "trailing"}
        timescale = timescale_translation[timescale]

        # Step 2: construct url:
        ts_url_base = f"https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{self._symbol}?symbol={self._symbol}"
        url = ts_url_base + "&type=" + ",".join([timescale + k for k in keys])
        # Yahoo returns maximum 4 years or 5 quarters, regardless of start_dt:
        start_dt = datetime.datetime(2016, 12, 31)
        end = pd.Timestamp.now('UTC').ceil("D")
        url += f"&period1={int(start_dt.timestamp())}&period2={int(end.timestamp())}"

        # Step 3: fetch and reshape data
        json_str = self._data.cache_get(url=url).text
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

        # Ensure float type, not object
        for d in df.columns:
            df[d] = df[d].astype('float')

        # Reorder table to match order on Yahoo website
        df = df.reindex([k for k in keys if k in df.index])
        df = df[sorted(df.columns, reverse=True)]

        # Trailing 12 months return only the first column.
        if (timescale == "trailing"):
            df = df.iloc[:, [0]]

        return df

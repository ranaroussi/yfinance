"""Analysis scraper for analyst and earnings-trend data."""

from typing import Any, Dict, List, Optional

import pandas as pd

from yfinance.config import YF_CONFIG as YfConfig
from yfinance.data import YfData
from yfinance.scrapers.utils import fetch_quote_summary


class Analysis:
    """Fetch and cache ticker analysis datasets from quoteSummary."""

    def __init__(self, data: YfData, symbol: str):
        """Initialize analysis scraper state."""
        self._data = data
        self._symbol = symbol
        self._cache: Dict[str, Any] = {
            "earnings_trend": None,
            "analyst_price_targets": None,
            "earnings_estimate": None,
            "revenue_estimate": None,
            "earnings_history": None,
            "eps_trend": None,
            "eps_revisions": None,
            "growth_estimates": None,
        }

    def _earnings_trend(self) -> List[Dict[str, Any]]:
        trend = self._cache["earnings_trend"]
        if trend is None:
            self._fetch_earnings_trend()
            trend = self._cache["earnings_trend"]
        return trend or []

    def _get_periodic_df(self, key: str) -> pd.DataFrame:
        """Build a period-indexed DataFrame for one earningsTrend section."""
        data: list[dict[str, Any]] = []
        for item in self._earnings_trend()[:4]:
            period = item.get("period")
            if period is None:
                continue

            row: dict[str, Any] = {"period": period}
            values = item.get(key, {})
            if not isinstance(values, dict):
                continue

            for field, value in values.items():
                if not isinstance(value, dict) or len(value) == 0:
                    continue
                row[field] = value.get("raw")
            data.append(row)

        if len(data) == 0:
            return pd.DataFrame()
        return pd.DataFrame(data).set_index("period")

    def _cached_periodic_df(self, cache_key: str, source_key: str) -> pd.DataFrame:
        cached = self._cache[cache_key]
        if cached is not None:
            return cached
        self._cache[cache_key] = self._get_periodic_df(source_key)
        return self._cache[cache_key]

    @property
    def earnings_estimate(self) -> pd.DataFrame:
        """Return earnings estimate table by period."""
        return self._cached_periodic_df("earnings_estimate", "earningsEstimate")

    @property
    def revenue_estimate(self) -> pd.DataFrame:
        """Return revenue estimate table by period."""
        return self._cached_periodic_df("revenue_estimate", "revenueEstimate")

    @property
    def eps_trend(self) -> pd.DataFrame:
        """Return EPS trend table by period."""
        return self._cached_periodic_df("eps_trend", "epsTrend")

    @property
    def eps_revisions(self) -> pd.DataFrame:
        """Return EPS revisions table by period."""
        return self._cached_periodic_df("eps_revisions", "epsRevisions")

    @property
    def analyst_price_targets(self) -> dict:
        """Return analyst price targets."""
        cached = self._cache["analyst_price_targets"]
        if cached is not None:
            return cached

        try:
            data = self._fetch(["financialData"])
            if data is None:
                raise KeyError("Missing quoteSummary data")
            financial_data = data["quoteSummary"]["result"][0]["financialData"]
        except (TypeError, KeyError, IndexError):
            if YfConfig.debug.raise_on_error:
                raise
            self._cache["analyst_price_targets"] = {}
            return self._cache["analyst_price_targets"]

        result = {}
        for key, value in financial_data.items():
            if key.startswith("target"):
                new_key = key.replace("target", "").lower().replace("price", "").strip()
                result[new_key] = value
            elif key == "currentPrice":
                result["current"] = value

        self._cache["analyst_price_targets"] = result
        return self._cache["analyst_price_targets"]

    @property
    def earnings_history(self) -> pd.DataFrame:
        """Return historical reported-vs-estimate earnings data."""
        cached = self._cache["earnings_history"]
        if cached is not None:
            return cached

        try:
            data = self._fetch(["earningsHistory"])
            if data is None:
                raise KeyError("Missing quoteSummary data")
            history = data["quoteSummary"]["result"][0]["earningsHistory"]["history"]
        except (TypeError, KeyError, IndexError):
            if YfConfig.debug.raise_on_error:
                raise
            self._cache["earnings_history"] = pd.DataFrame()
            return self._cache["earnings_history"]

        rows = []
        for item in history:
            row = {"quarter": item.get("quarter", {}).get("fmt")}
            for key, value in item.items():
                if key == "quarter":
                    continue
                if not isinstance(value, dict) or len(value) == 0:
                    continue
                row[key] = value.get("raw")
            rows.append(row)

        if len(history) == 0:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        if "quarter" in df.columns:
            df["quarter"] = pd.to_datetime(df["quarter"], format="%Y-%m-%d")
            df.set_index("quarter", inplace=True)

        self._cache["earnings_history"] = df
        return self._cache["earnings_history"]

    def _growth_rows_from_stock(self) -> list[dict[str, Any]]:
        rows = []
        for item in self._earnings_trend():
            period = item.get("period")
            if period is None:
                continue
            rows.append(
                {
                    "period": period,
                    "stockTrend": item.get("growth", {}).get("raw"),
                }
            )
        return rows

    @staticmethod
    def _upsert_growth_row(
        rows: list[dict[str, Any]],
        period: Any,
        trend_name: str,
        growth: Any,
    ) -> None:
        existing_row = next((row for row in rows if row["period"] == period), None)
        if existing_row is not None:
            existing_row[trend_name] = growth
            return
        rows.append({"period": period, trend_name: growth})

    @property
    def growth_estimates(self) -> pd.DataFrame:
        """Return stock/industry/sector/index growth estimates by period."""
        cached = self._cache["growth_estimates"]
        if cached is not None:
            return cached

        try:
            trends_response = self._fetch(["industryTrend", "sectorTrend", "indexTrend"])
            if trends_response is None:
                raise KeyError("Missing quoteSummary data")
            trends = trends_response["quoteSummary"]["result"][0]
        except (TypeError, KeyError, IndexError):
            if YfConfig.debug.raise_on_error:
                raise
            self._cache["growth_estimates"] = pd.DataFrame()
            return self._cache["growth_estimates"]

        rows = self._growth_rows_from_stock()
        for trend_name, trend_info in trends.items():
            estimates = trend_info.get("estimates")
            if not estimates:
                continue
            for estimate in estimates:
                self._upsert_growth_row(
                    rows=rows,
                    period=estimate.get("period"),
                    trend_name=trend_name,
                    growth=estimate.get("growth"),
                )

        if len(rows) == 0:
            return pd.DataFrame()

        self._cache["growth_estimates"] = (
            pd.DataFrame(rows).set_index("period").dropna(how="all")
        )
        return self._cache["growth_estimates"]

    def _fetch(self, modules: List[str]) -> Optional[Dict[str, Any]]:
        """Fetch quoteSummary JSON for a set of valid modules."""
        return fetch_quote_summary(self._data, self._symbol, modules)

    def _fetch_earnings_trend(self) -> None:
        """Populate cached earningsTrend payload."""
        try:
            data = self._fetch(["earningsTrend"])
            if data is None:
                raise KeyError("Missing quoteSummary data")
            trend = data["quoteSummary"]["result"][0]["earningsTrend"]["trend"]
            self._cache["earnings_trend"] = trend
        except (TypeError, KeyError, IndexError):
            if YfConfig.debug.raise_on_error:
                raise
            self._cache["earnings_trend"] = []

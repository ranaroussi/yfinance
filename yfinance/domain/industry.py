"""Industry domain model and parsing helpers."""

from typing import Any, Dict, List, Optional

import pandas as _pd

from .. import utils
from ..config import YF_CONFIG as YfConfig

from .domain import Domain, _QUERY_URL_

_PARSE_ERROR_TYPES = (KeyError, TypeError, ValueError)


class Industry(Domain):
    """Represents an industry within a sector."""

    def __init__(self, key, session=None, region: str = "US"):
        """Initialize an industry by API key."""
        super().__init__(key, session, region)
        self._query_url = f"{_QUERY_URL_}/industries/{self._key}"

        self._sector_key: Optional[str] = None
        self._sector_name: Optional[str] = None
        self._top_performing_companies: Optional[_pd.DataFrame] = None
        self._top_growth_companies: Optional[_pd.DataFrame] = None

    def __repr__(self):
        """Return a concise representation of the industry."""
        return f"yfinance.Industry object <{self._key}>"

    @property
    def sector_key(self) -> str:
        """Return the parent sector key for this industry."""
        self._ensure_fetched(self._sector_key)
        if self._sector_key is None:
            raise ValueError(f"Failed to retrieve sector key for industry '{self._key}'")
        return self._sector_key

    @property
    def sector_name(self) -> str:
        """Return the parent sector name for this industry."""
        self._ensure_fetched(self._sector_name)
        if self._sector_name is None:
            raise ValueError(
                f"Failed to retrieve sector name for industry '{self._key}'"
            )
        return self._sector_name

    @property
    def top_performing_companies(self) -> Optional[_pd.DataFrame]:
        """Return top performing companies in the industry."""
        self._ensure_fetched(self._top_performing_companies)
        return self._top_performing_companies

    @property
    def top_growth_companies(self) -> Optional[_pd.DataFrame]:
        """Return top growth companies in the industry."""
        self._ensure_fetched(self._top_growth_companies)
        return self._top_growth_companies

    def _parse_top_performing_companies(
        self,
        top_performing_companies: List[Dict[str, Any]],
    ) -> Optional[_pd.DataFrame]:
        """Parse top-performing companies payload to DataFrame."""
        companies_column = ["symbol", "name", "ytd return", "last price", "target price"]
        companies_values = [
            (
                company.get("symbol"),
                company.get("name"),
                company.get("ytdReturn", {}).get("raw"),
                company.get("lastPrice", {}).get("raw"),
                company.get("targetPrice", {}).get("raw"),
            )
            for company in top_performing_companies
        ]

        if not companies_values:
            return None

        return _pd.DataFrame(companies_values, columns=companies_column).set_index("symbol")

    def _parse_top_growth_companies(
        self,
        top_growth_companies: List[Dict[str, Any]],
    ) -> Optional[_pd.DataFrame]:
        """Parse top-growth companies payload to DataFrame."""
        companies_column = ["symbol", "name", "ytd return", "growth estimate"]
        companies_values = [
            (
                company.get("symbol"),
                company.get("name"),
                company.get("ytdReturn", {}).get("raw"),
                company.get("growthEstimate", {}).get("raw"),
            )
            for company in top_growth_companies
        ]

        if not companies_values:
            return None

        return _pd.DataFrame(companies_values, columns=companies_column).set_index("symbol")

    def _fetch_and_parse(self) -> None:
        """Fetch and parse industry data from Yahoo Finance."""
        result = None

        try:
            result, data = self._fetch_common_data(self._query_url)
            self._sector_key = data.get("sectorKey")
            self._sector_name = data.get("sectorName")
            self._top_performing_companies = self._parse_top_performing_companies(
                data.get("topPerformingCompanies") or []
            )
            self._top_growth_companies = self._parse_top_growth_companies(
                data.get("topGrowthCompanies") or []
            )
        except _PARSE_ERROR_TYPES as err:
            if YfConfig.debug.raise_on_error:
                raise
            self._log_fetch_error(utils.get_yf_logger(), "industry", err, result)

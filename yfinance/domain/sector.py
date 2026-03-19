"""Sector domain model and parsing helpers."""

from typing import Any, Dict, List, Optional

import pandas as _pd

from ..config import YF_CONFIG as YfConfig
from ..const import SECTOR_INDUSTY_MAPPING_LC
from ..utils import dynamic_docstring, generate_list_table_from_dict, get_yf_logger

from .domain import Domain, _QUERY_URL_

_PARSE_ERROR_TYPES = (KeyError, TypeError, ValueError)


class Sector(Domain):
    """Represents a financial market sector."""

    def __init__(self, key, session=None):
        """Initialize a sector by API key."""
        super().__init__(key, session)
        self._query_url: str = f"{_QUERY_URL_}/sectors/{self._key}"
        self._top_etfs: Optional[Dict[str, str]] = None
        self._top_mutual_funds: Optional[Dict[str, str]] = None
        self._industries: Optional[_pd.DataFrame] = None

    def __repr__(self):
        """Return a concise representation of the sector."""
        return f"yfinance.Sector object <{self._key}>"

    @property
    def top_etfs(self) -> Dict[str, str]:
        """Return top ETF symbols and names for the sector."""
        self._ensure_fetched(self._top_etfs)
        if self._top_etfs is None:
            return {}
        return self._top_etfs

    @property
    def top_mutual_funds(self) -> Dict[str, str]:
        """Return top mutual fund symbols and names for the sector."""
        self._ensure_fetched(self._top_mutual_funds)
        if self._top_mutual_funds is None:
            return {}
        return self._top_mutual_funds

    @dynamic_docstring(
        {"sector_industry": generate_list_table_from_dict(SECTOR_INDUSTY_MAPPING_LC, bullets=True)}
    )
    @property
    def industries(self) -> _pd.DataFrame:
        """Return the industries within the sector."""
        self._ensure_fetched(self._industries)
        if self._industries is None:
            raise ValueError(f"Failed to retrieve industries for sector '{self._key}'")
        return self._industries

    def _parse_top_etfs(self, top_etfs: List[Dict[str, Any]]) -> Dict[str, str]:
        """Parse top ETF data from API payload."""
        parsed: Dict[str, str] = {}
        for item in top_etfs:
            symbol = item.get("symbol")
            name = item.get("name")
            if isinstance(symbol, str) and isinstance(name, str):
                parsed[symbol] = name
        return parsed

    def _parse_top_mutual_funds(
        self,
        top_mutual_funds: List[Dict[str, Any]],
    ) -> Dict[str, str]:
        """Parse top mutual fund data from API payload."""
        parsed: Dict[str, str] = {}
        for item in top_mutual_funds:
            symbol = item.get("symbol")
            name = item.get("name")
            if isinstance(symbol, str) and isinstance(name, str):
                parsed[symbol] = name
        return parsed

    def _parse_industries(self, industries: List[Dict[str, Any]]) -> _pd.DataFrame:
        """Parse industries payload into a DataFrame."""
        industries_column = ["key", "name", "symbol", "market weight"]
        industries_values = [
            (
                industry.get("key"),
                industry.get("name"),
                industry.get("symbol"),
                industry.get("marketWeight", {}).get("raw"),
            )
            for industry in industries
            if industry.get("name") != "All Industries"
        ]
        return _pd.DataFrame(industries_values, columns=industries_column).set_index("key")

    def _fetch_and_parse(self) -> None:
        """Fetch and parse sector data from Yahoo Finance."""
        result = None

        try:
            result, data = self._fetch_common_data(self._query_url)
            self._top_etfs = self._parse_top_etfs(data.get("topETFs") or [])
            self._top_mutual_funds = self._parse_top_mutual_funds(
                data.get("topMutualFunds") or []
            )
            self._industries = self._parse_industries(data.get("industries") or [])

        except _PARSE_ERROR_TYPES as err:
            if not YfConfig.debug.hide_exceptions:
                raise
            self._log_fetch_error(get_yf_logger(), "sector", err, result)

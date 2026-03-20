"""Base abstractions for Yahoo Finance domain entities."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

import pandas as _pd

from ..const import _QUERY1_URL_
from ..data import YfData
from ..ticker import Ticker

_QUERY_URL_ = f"{_QUERY1_URL_}/v1/finance"


class Domain(ABC):
    """Abstract base class for sector and industry domain entities."""

    def __init__(self, key: str, session=None, region: str = "US"):
        """Initialize a domain entity with key and optional HTTP session."""
        self._key: str = key
        self._region: str = region
        self._data: YfData = YfData(session=session)

        self._name: Optional[str] = None
        self._symbol: Optional[str] = None
        self._overview: Optional[Dict[str, Any]] = None
        self._top_companies: Optional[_pd.DataFrame] = None
        self._research_reports: Optional[List[Dict[str, str]]] = None

    @property
    def key(self) -> str:
        """Return the unique key identifying the domain."""
        return self._key

    @property
    def name(self) -> str:
        """Return the domain display name."""
        self._ensure_fetched(self._name)
        if self._name is None:
            raise ValueError(f"Failed to retrieve name for domain '{self._key}'")
        return self._name

    @property
    def symbol(self) -> str:
        """Return the representative symbol for the domain."""
        self._ensure_fetched(self._symbol)
        if self._symbol is None:
            raise ValueError(f"Failed to retrieve symbol for domain '{self._key}'")
        return self._symbol

    @property
    def ticker(self) -> Ticker:
        """Return the domain symbol as a :class:`Ticker` object."""
        return Ticker(self.symbol)

    @property
    def overview(self) -> Dict[str, Any]:
        """Return overview information for the domain."""
        self._ensure_fetched(self._overview)
        if self._overview is None:
            raise ValueError(f"Failed to retrieve overview for domain '{self._key}'")
        return self._overview

    @property
    def top_companies(self) -> Optional[_pd.DataFrame]:
        """Return top companies table for the domain."""
        self._ensure_fetched(self._top_companies)
        return self._top_companies

    @property
    def research_reports(self) -> List[Dict[str, str]]:
        """Return associated research reports."""
        self._ensure_fetched(self._research_reports)
        if self._research_reports is None:
            return []
        return self._research_reports

    def _fetch(self, query_url: str) -> Dict[str, Any]:
        """Fetch raw JSON for a domain endpoint."""
        params_dict = {
            "formatted": "true",
            "withReturns": "true",
            "lang": "en-US",
            "region": self._region,
        }
        result = self._data.get_raw_json(query_url, params=params_dict)
        return result

    def _parse_and_assign_common(self, data: Dict[str, Any]) -> None:
        """Parse and cache common fields shared by sector and industry."""
        self._name = data.get("name")
        self._symbol = data.get("symbol")
        self._overview = self._parse_overview(data.get("overview", {}))
        self._top_companies = self._parse_top_companies(data.get("topCompanies", []))
        self._research_reports = data.get("researchReports") or []

    def _parse_overview(self, overview: Dict[str, Any]) -> Dict[str, Any]:
        """Parse overview payload into a flatter dict."""
        return {
            "companies_count": overview.get("companiesCount"),
            "market_cap": overview.get("marketCap", {}).get("raw"),
            "message_board_id": overview.get("messageBoardId"),
            "description": overview.get("description"),
            "industries_count": overview.get("industriesCount"),
            "market_weight": overview.get("marketWeight", {}).get("raw"),
            "employee_count": overview.get("employeeCount", {}).get("raw"),
        }

    def _parse_top_companies(
        self,
        top_companies: List[Dict[str, Any]],
    ) -> Optional[_pd.DataFrame]:
        """Parse top-companies payload into a DataFrame."""
        top_companies_column = ["symbol", "name", "rating", "market weight"]
        top_companies_values = [
            (
                company.get("symbol"),
                company.get("name"),
                company.get("rating"),
                company.get("marketWeight", {}).get("raw"),
            )
            for company in top_companies
        ]

        if not top_companies_values:
            return None

        return _pd.DataFrame(
            top_companies_values,
            columns=top_companies_column,
        ).set_index("symbol")

    def _fetch_common_data(self, query_url: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Fetch a domain payload and parse common fields."""
        result = self._fetch(query_url)
        data = result["data"]
        self._parse_and_assign_common(data)
        return result, data

    def _log_fetch_error(
        self,
        logger,
        domain_type: str,
        error: Exception,
        result: Optional[Dict[str, Any]],
    ) -> None:
        """Log a failed domain payload fetch in a consistent format."""
        logger.error(
            "Failed to get %s data for '%s' reason: %s",
            domain_type,
            self._key,
            error,
        )
        logger.debug("Got response:")
        logger.debug("-------------")
        logger.debug("%s", result)
        logger.debug("-------------")

    @abstractmethod
    def _fetch_and_parse(self) -> None:
        """Fetch and parse domain-specific data."""
        raise NotImplementedError(
            "_fetch_and_parse() needs to be implemented by children classes"
        )

    def _ensure_fetched(self, attribute) -> None:
        """Fetch domain data on first access for lazy-loaded properties."""
        if attribute is None:
            self._fetch_and_parse()

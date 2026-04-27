from __future__ import print_function

from typing import Dict, Optional

import polars as _pl

from .. import utils
from ..config import YfConfig
from ..data import YfData
from .domain import _QUERY_URL_, Domain


class Industry(Domain):
    """
    Represents an industry within a sector.
    """

    def __init__(self, key, session=None):
        """
        Args:
            key (str): The key identifier for the industry.
            session (optional): The session to use for requests.
        """
        YfData(session=session)
        super(Industry, self).__init__(key, session)
        self._query_url = f"{_QUERY_URL_}/industries/{self._key}"

        self._sector_key = None
        self._sector_name = None
        self._top_performing_companies = None
        self._top_growth_companies = None

    def __repr__(self):
        """
        Returns a string representation of the Industry instance.

        Returns:
            str: String representation of the Industry instance.
        """
        return f"yfinance.Industry object <{self._key}>"

    @property
    def sector_key(self) -> str:
        """
        Returns the sector key of the industry.

        Returns:
            str: The sector key.
        """
        self._ensure_fetched(self._sector_key)
        return self._sector_key

    @property
    def sector_name(self) -> str:
        """
        Returns the sector name of the industry.

        Returns:
            str: The sector name.
        """
        self._ensure_fetched(self._sector_name)
        return self._sector_name

    @property
    def top_performing_companies(self) -> Optional[_pl.DataFrame]:
        """
        Returns the top performing companies in the industry.

        Returns:
            Optional[pl.DataFrame]: DataFrame containing top performing companies.
        """
        self._ensure_fetched(self._top_performing_companies)
        return self._top_performing_companies

    @property
    def top_growth_companies(self) -> Optional[_pl.DataFrame]:
        """
        Returns the top growth companies in the industry.

        Returns:
            Optional[pl.DataFrame]: DataFrame containing top growth companies.
        """
        self._ensure_fetched(self._top_growth_companies)
        return self._top_growth_companies

    def _parse_top_performing_companies(
        self, top_performing_companies: Dict
    ) -> Optional[_pl.DataFrame]:
        """
        Parses the top performing companies data.

        Args:
            top_performing_companies (Dict): Dictionary containing top performing companies data.

        Returns:
            Optional[pl.DataFrame]: DataFrame containing parsed top performing companies data.
        """
        compnaies_values = [
            (
                c.get("symbol", None),
                c.get("name", None),
                c.get("ytdReturn", {}).get("raw", None),
                c.get("lastPrice", {}).get("raw", None),
                c.get("targetPrice", {}).get("raw", None),
            )
            for c in top_performing_companies
        ]

        if not compnaies_values:
            return None

        symbols, names, ytd_returns, last_prices, target_prices = zip(*compnaies_values)
        return _pl.DataFrame(
            {
                "symbol": list(symbols),
                "name": list(names),
                "ytd return": list(ytd_returns),
                "last price": list(last_prices),
                "target price": list(target_prices),
            }
        )

    def _parse_top_growth_companies(
        self, top_growth_companies: Dict
    ) -> Optional[_pl.DataFrame]:
        """
        Parses the top growth companies data.

        Args:
            top_growth_companies (Dict): Dictionary containing top growth companies data.

        Returns:
            Optional[pl.DataFrame]: DataFrame containing parsed top growth companies data.
        """
        compnaies_values = [
            (
                c.get("symbol", None),
                c.get("name", None),
                c.get("ytdReturn", {}).get("raw", None),
                c.get("growthEstimate", {}).get("raw", None),
            )
            for c in top_growth_companies
        ]

        if not compnaies_values:
            return None

        symbols, names, ytd_returns, growth_estimates = zip(*compnaies_values)
        return _pl.DataFrame(
            {
                "symbol": list(symbols),
                "name": list(names),
                "ytd return": list(ytd_returns),
                "growth estimate": list(growth_estimates),
            }
        )

    def _fetch_and_parse(self) -> None:
        """
        Fetches and parses the industry data.
        """
        result = None

        try:
            result = self._fetch(self._query_url)
            data = result["data"]
            self._parse_and_assign_common(data)

            self._sector_key = data.get("sectorKey")
            self._sector_name = data.get("sectorName")
            self._top_performing_companies = self._parse_top_performing_companies(
                data.get("topPerformingCompanies")
            )
            self._top_growth_companies = self._parse_top_growth_companies(
                data.get("topGrowthCompanies")
            )

            return result
        except Exception as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            logger = utils.get_yf_logger()
            logger.error(f"Failed to get industry data for '{self._key}' reason: {e}")
            logger.debug("Got response: ")
            logger.debug("-------------")
            logger.debug(f" {result}")
            logger.debug("-------------")

"""Scraper for ETF and mutual-fund-specific quote summary data."""

from typing import Any, Dict, Optional

import pandas as pd

from yfinance.http import log_response_payload
from yfinance.config import YF_CONFIG as YfConfig
from yfinance.const import _QUOTE_SUMMARY_URL_
from yfinance.data import YfData
from yfinance.exceptions import YFDataException
from .. import utils

class FundsData:
    """
    ETF and Mutual Funds Data
    Queried Modules: quoteType, summaryProfile, fundProfile, topHoldings

    Notes:
    - fundPerformance module is not implemented as better data is queryable using history
    """

    def __init__(self, data: YfData, symbol: str):
        """
        Args:
            data (YfData): The YfData object for fetching data.
            symbol (str): The symbol of the fund.
        """
        self._data = data
        self._symbol = symbol

        # quoteType
        self._quote_type: Optional[str] = None

        # summaryProfile
        self._description: Optional[str] = None

        # fundProfile
        self._fund_overview: Optional[Dict[str, Optional[str]]] = None
        self._fund_operations: Optional[pd.DataFrame] = None

        # topHoldings
        self._asset_classes: Optional[Dict[str, float]] = None
        self._top_holdings: Optional[pd.DataFrame] = None
        self._equity_holdings: Optional[pd.DataFrame] = None
        self._bond_holdings: Optional[pd.DataFrame] = None
        self._bond_ratings: Optional[Dict[str, float]] = None
        self._sector_weightings: Optional[Dict[str, float]] = None

    def quote_type(self) -> str:
        """
        Returns the quote type of the fund.

        Returns:
            str: The quote type.
        """
        if self._quote_type is None:
            self._fetch_and_parse()
        return self._quote_type or ""

    @property
    def description(self) -> str:
        """
        Returns the description of the fund.

        Returns:
            str: The description.
        """
        if self._description is None:
            self._fetch_and_parse()
        return self._description or ""

    @property
    def fund_overview(self) -> Dict[str, Optional[str]]:
        """
        Returns the fund overview.

        Returns:
            Dict[str, Optional[str]]: The fund overview.
        """
        if self._fund_overview is None:
            self._fetch_and_parse()
        return self._fund_overview or {}

    @property
    def fund_operations(self) -> pd.DataFrame:
        """
        Returns the fund operations.

        Returns:
            pd.DataFrame: The fund operations.
        """
        if self._fund_operations is None:
            self._fetch_and_parse()
        return self._fund_operations if self._fund_operations is not None else pd.DataFrame()

    @property
    def asset_classes(self) -> Dict[str, float]:
        """
        Returns the asset classes of the fund.

        Returns:
            Dict[str, float]: The asset classes.
        """
        if self._asset_classes is None:
            self._fetch_and_parse()
        return self._asset_classes or {}

    @property
    def top_holdings(self) -> pd.DataFrame:
        """
        Returns the top holdings of the fund.

        Returns:
            pd.DataFrame: The top holdings.
        """
        if self._top_holdings is None:
            self._fetch_and_parse()
        return self._top_holdings if self._top_holdings is not None else pd.DataFrame()

    @property
    def equity_holdings(self) -> pd.DataFrame:
        """
        Returns the equity holdings of the fund.

        Returns:
            pd.DataFrame: The equity holdings.
        """
        if self._equity_holdings is None:
            self._fetch_and_parse()
        return self._equity_holdings if self._equity_holdings is not None else pd.DataFrame()

    @property
    def bond_holdings(self) -> pd.DataFrame:
        """
        Returns the bond holdings of the fund.

        Returns:
            pd.DataFrame: The bond holdings.
        """
        if self._bond_holdings is None:
            self._fetch_and_parse()
        return self._bond_holdings if self._bond_holdings is not None else pd.DataFrame()

    @property
    def bond_ratings(self) -> Dict[str, float]:
        """
        Returns the bond ratings of the fund.

        Returns:
            Dict[str, float]: The bond ratings.
        """
        if self._bond_ratings is None:
            self._fetch_and_parse()
        return self._bond_ratings or {}

    @property
    def sector_weightings(self) -> Dict[str, float]:
        """
        Returns the sector weightings of the fund.

        Returns:
            Dict[str, float]: The sector weightings.
        """
        if self._sector_weightings is None:
            self._fetch_and_parse()
        return self._sector_weightings or {}

    def _fetch(self):
        """
        Fetches the raw JSON data from the API.

        Returns:
            dict: The raw JSON data.
        """
        modules = ",".join(["quoteType", "summaryProfile", "topHoldings", "fundProfile"])
        params_dict = {
            "modules": modules,
            "corsDomain": "finance.yahoo.com",
            "symbol": self._symbol,
            "formatted": "false",
        }
        result = self._data.get_raw_json(
            f"{_QUOTE_SUMMARY_URL_}/{self._symbol}",
            params=params_dict,
        )
        return result

    def _fetch_and_parse(self) -> None:
        """
        Fetches and parses the data from the API.
        """
        result = self._fetch()
        data: Dict[str, Any] = {}
        try:
            data = result["quoteSummary"]["result"][0]
            self._quote_type = data["quoteType"]["quoteType"]
            if self._quote_type not in ("ETF", "MUTUALFUND"):
                raise YFDataException(f"{self._symbol}: No Fund data found.")

            self._parse_description(data["summaryProfile"])
            self._parse_top_holdings(data.get("topHoldings", {}))
            self._parse_fund_profile(data.get("fundProfile", {}))
        except KeyError as exc:
            if YfConfig.debug.raise_on_error:
                raise
            raise YFDataException(f"{self._symbol}: No Fund data found.") from exc
        except (TypeError, ValueError, IndexError, AttributeError) as error:
            if YfConfig.debug.raise_on_error:
                raise
            logger = utils.get_yf_logger()
            logger.error(
                "Failed to get fund data for '%s' reason: %s",
                self._symbol,
                error,
            )
            log_response_payload(logger, data)

    @staticmethod
    def _parse_raw_values(data, default=None):
        """
        Parses raw values from the data.

        Args:
            data: The data to parse.
            default: The default value if data is not a dictionary.

        Returns:
            The parsed value or the default value.
        """
        if not isinstance(data, dict):
            return data

        return data.get("raw", default)

    @staticmethod
    def _to_float(value: Any) -> float:
        if value is None:
            return float("nan")
        try:
            return float(value)
        except (TypeError, ValueError):
            return float("nan")

    def _parse_description(self, data) -> None:
        """
        Parses the description from the data.

        Args:
            data: The data to parse.
        """
        self._description = data.get("longBusinessSummary", "")

    def _parse_top_holdings(self, data) -> None:
        """
        Parses the top holdings from the data.

        Args:
            data: The data to parse.
        """
        # asset classes
        self._asset_classes = {
            "cashPosition": self._to_float(self._parse_raw_values(data.get("cashPosition"))),
            "stockPosition": self._to_float(self._parse_raw_values(data.get("stockPosition"))),
            "bondPosition": self._to_float(self._parse_raw_values(data.get("bondPosition"))),
            "preferredPosition": self._to_float(
                self._parse_raw_values(data.get("preferredPosition"))
            ),
            "convertiblePosition": self._to_float(
                self._parse_raw_values(data.get("convertiblePosition"))
            ),
            "otherPosition": self._to_float(self._parse_raw_values(data.get("otherPosition"))),
        }

        # top holdings
        _holdings = data.get("holdings", [])
        _symbol, _name, _holding_percent = [], [], []

        for item in _holdings:
            _symbol.append(item["symbol"])
            _name.append(item["holdingName"])
            _holding_percent.append(item["holdingPercent"])

        self._top_holdings = pd.DataFrame(
            {
                "Symbol": _symbol,
                "Name": _name,
                "Holding Percent": _holding_percent,
            }
        ).set_index("Symbol")

        # equity holdings
        _equity_holdings = data.get("equityHoldings", {})
        self._equity_holdings = pd.DataFrame(
            {
                "Average": [
                    "Price/Earnings",
                    "Price/Book",
                    "Price/Sales",
                    "Price/Cashflow",
                    "Median Market Cap",
                    "3 Year Earnings Growth",
                ],
                self._symbol: [
                    self._parse_raw_values(_equity_holdings.get("priceToEarnings", pd.NA)),
                    self._parse_raw_values(_equity_holdings.get("priceToBook", pd.NA)),
                    self._parse_raw_values(_equity_holdings.get("priceToSales", pd.NA)),
                    self._parse_raw_values(_equity_holdings.get("priceToCashflow", pd.NA)),
                    self._parse_raw_values(_equity_holdings.get("medianMarketCap", pd.NA)),
                    self._parse_raw_values(
                        _equity_holdings.get("threeYearEarningsGrowth", pd.NA)
                    ),
                ],
                "Category Average": [
                    self._parse_raw_values(_equity_holdings.get("priceToEarningsCat", pd.NA)),
                    self._parse_raw_values(_equity_holdings.get("priceToBookCat", pd.NA)),
                    self._parse_raw_values(_equity_holdings.get("priceToSalesCat", pd.NA)),
                    self._parse_raw_values(_equity_holdings.get("priceToCashflowCat", pd.NA)),
                    self._parse_raw_values(_equity_holdings.get("medianMarketCapCat", pd.NA)),
                    self._parse_raw_values(
                        _equity_holdings.get("threeYearEarningsGrowthCat", pd.NA)
                    ),
                ],
            }
        ).set_index("Average")

        # bond holdings
        _bond_holdings = data.get("bondHoldings", {})
        self._bond_holdings = pd.DataFrame(
            {
                "Average": ["Duration", "Maturity", "Credit Quality"],
                self._symbol: [
                    self._parse_raw_values(_bond_holdings.get("duration", pd.NA)),
                    self._parse_raw_values(_bond_holdings.get("maturity", pd.NA)),
                    self._parse_raw_values(_bond_holdings.get("creditQuality", pd.NA)),
                ],
                "Category Average": [
                    self._parse_raw_values(_bond_holdings.get("durationCat", pd.NA)),
                    self._parse_raw_values(_bond_holdings.get("maturityCat", pd.NA)),
                    self._parse_raw_values(_bond_holdings.get("creditQualityCat", pd.NA)),
                ],
            }
        ).set_index("Average")

        # bond ratings
        self._bond_ratings = {
            str(key): self._to_float(value)
            for d in data.get("bondRatings", [])
            if isinstance(d, dict)
            for key, value in d.items()
        }

        # sector weightings
        self._sector_weightings = {
            str(key): self._to_float(value)
            for d in data.get("sectorWeightings", [])
            if isinstance(d, dict)
            for key, value in d.items()
        }

    def _parse_fund_profile(self, data):
        """
        Parses the fund profile from the data.

        Args:
            data: The data to parse.
        """
        self._fund_overview = {
            "categoryName": data.get("categoryName"),
            "family": data.get("family"),
            "legalType": data.get("legalType"),
        }

        _fund_operations = data.get("feesExpensesInvestment", {})
        _fund_operations_cat = data.get("feesExpensesInvestmentCat", {})

        self._fund_operations = pd.DataFrame(
            {
                "Attributes": [
                    "Annual Report Expense Ratio",
                    "Annual Holdings Turnover",
                    "Total Net Assets",
                ],
                self._symbol: [
                    self._parse_raw_values(_fund_operations.get("annualReportExpenseRatio", pd.NA)),
                    self._parse_raw_values(_fund_operations.get("annualHoldingsTurnover", pd.NA)),
                    self._parse_raw_values(_fund_operations.get("totalNetAssets", pd.NA)),
                ],
                "Category Average": [
                    self._parse_raw_values(
                        _fund_operations_cat.get("annualReportExpenseRatio", pd.NA)
                    ),
                    self._parse_raw_values(
                        _fund_operations_cat.get("annualHoldingsTurnover", pd.NA)
                    ),
                    self._parse_raw_values(_fund_operations_cat.get("totalNetAssets", pd.NA)),
                ],
            }
        ).set_index("Attributes")

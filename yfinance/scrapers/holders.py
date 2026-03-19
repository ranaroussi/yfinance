"""Holders and insider-activity scraper helpers."""

from typing import Any, cast

import pandas as pd

from yfinance.config import YF_CONFIG as YfConfig
from yfinance.const import holders_quote_summary_modules
from yfinance.data import YfData
from yfinance.exceptions import YFDataException
from yfinance.scrapers.utils import fetch_quote_summary


class Holders:
    """Load and parse holder-related quoteSummary modules for one ticker."""

    _SCRAPE_URL_ = "https://finance.yahoo.com/quote"
    _TABLE_KEYS = (
        "major",
        "major_direct_holders",
        "institutional",
        "mutualfund",
        "insider_transactions",
        "insider_purchases",
        "insider_roster",
    )

    def __init__(self, data: YfData, symbol: str):
        """Initialize holders scraper state."""
        self._data = data
        self._symbol = symbol
        self._tables: dict[str, pd.DataFrame | None] = {
            key: None for key in self._TABLE_KEYS
        }

    def _get_table(self, key: str) -> pd.DataFrame:
        if self._tables[key] is None:
            self._fetch_and_parse()
        table = self._tables.get(key)
        if table is None:
            return pd.DataFrame()
        return table

    def _set_all_tables_empty(self) -> None:
        for key in self._TABLE_KEYS:
            self._tables[key] = pd.DataFrame()

    @property
    def major(self) -> pd.DataFrame:
        """Return major holders breakdown table."""
        return self._get_table("major")

    @property
    def institutional(self) -> pd.DataFrame:
        """Return institutional ownership table."""
        return self._get_table("institutional")

    @property
    def mutualfund(self) -> pd.DataFrame:
        """Return mutual-fund ownership table."""
        return self._get_table("mutualfund")

    @property
    def insider_transactions(self) -> pd.DataFrame:
        """Return insider transactions table."""
        return self._get_table("insider_transactions")

    @property
    def insider_purchases(self) -> pd.DataFrame:
        """Return net insider purchase activity table."""
        return self._get_table("insider_purchases")

    @property
    def insider_roster(self) -> pd.DataFrame:
        """Return insider roster table."""
        return self._get_table("insider_roster")

    def _fetch(self) -> dict[str, Any]:
        result = fetch_quote_summary(
            self._data,
            self._symbol,
            list(holders_quote_summary_modules),
        )
        if result is None:
            raise YFDataException("Failed to fetch holders json data.")
        return result

    def _fetch_and_parse(self) -> None:
        try:
            result = self._fetch()
        except YFDataException:
            if YfConfig.debug.raise_on_error:
                raise
            self._set_all_tables_empty()
            return

        try:
            data = result["quoteSummary"]["result"][0]
            self._parse_institution_ownership(data.get("institutionOwnership", {}))
            self._parse_fund_ownership(data.get("fundOwnership", {}))
            # self._parse_major_direct_holders(data.get("majorDirectHolders", {}))
            self._parse_major_holders_breakdown(data.get("majorHoldersBreakdown", {}))
            self._parse_insider_transactions(data.get("insiderTransactions", {}))
            self._parse_insider_holders(data.get("insiderHolders", {}))
            self._parse_net_share_purchase_activity(
                data.get("netSharePurchaseActivity", {})
            )
        except (KeyError, IndexError) as exc:
            if YfConfig.debug.raise_on_error:
                raise
            raise YFDataException("Failed to parse holders json data.") from exc

    @staticmethod
    def _parse_raw_values(data: Any) -> Any:
        """Extract Yahoo raw values where present."""
        if isinstance(data, dict) and "raw" in data:
            return data["raw"]
        return data

    def _parse_ownership(self, data: dict[str, Any], key: str) -> pd.DataFrame:
        holders = data.get("ownershipList", [])
        for owner in holders:
            for owner_key, value in owner.items():
                owner[owner_key] = self._parse_raw_values(value)
            owner.pop("maxAge", None)

        df = pd.DataFrame(holders)
        if not df.empty:
            df["reportDate"] = pd.to_datetime(df["reportDate"], unit="s")
            df.rename(
                columns={
                    "reportDate": "Date Reported",
                    "organization": "Holder",
                    "position": "Shares",
                    "value": "Value",
                },
                inplace=True,
            )
        self._tables[key] = df
        return df

    def _parse_institution_ownership(self, data: dict[str, Any]) -> None:
        """Parse institution ownership records."""
        self._parse_ownership(data, "institutional")

    def _parse_fund_ownership(self, data: dict[str, Any]) -> None:
        """Parse fund ownership records."""
        self._parse_ownership(data, "mutualfund")

    def _parse_major_direct_holders(self, data: dict[str, Any]) -> None:
        """Parse major direct holders records."""
        holders = data.get("holders", [])
        for owner in holders:
            for owner_key, value in owner.items():
                owner[owner_key] = self._parse_raw_values(value)
            owner.pop("maxAge", None)

        df = pd.DataFrame(holders)
        if not df.empty:
            df["reportDate"] = pd.to_datetime(df["reportDate"], unit="s")
            df.rename(
                columns={
                    "reportDate": "Date Reported",
                    "organization": "Holder",
                    "positionDirect": "Shares",
                    "valueDirect": "Value",
                },
                inplace=True,
            )
        self._tables["major_direct_holders"] = df

    def _parse_major_holders_breakdown(self, data: dict[str, Any]) -> None:
        """Parse major holders breakdown table."""
        data.pop("maxAge", None)
        df = pd.DataFrame.from_dict(data, orient="index")
        if not df.empty:
            df.columns.name = "Breakdown"
            first_col = cast(str, df.columns[0])
            df.rename(columns={first_col: "Value"}, inplace=True)
        self._tables["major"] = df

    def _parse_insider_transactions(self, data: dict[str, Any]) -> None:
        """Parse insider transactions table."""
        holders = data.get("transactions", [])
        for owner in holders:
            for owner_key, value in owner.items():
                owner[owner_key] = self._parse_raw_values(value)
            owner.pop("maxAge", None)

        df = pd.DataFrame(holders)
        if not df.empty:
            df["startDate"] = pd.to_datetime(df["startDate"], unit="s")
            df.rename(
                columns={
                    "startDate": "Start Date",
                    "filerName": "Insider",
                    "filerRelation": "Position",
                    "filerUrl": "URL",
                    "moneyText": "Transaction",
                    "transactionText": "Text",
                    "shares": "Shares",
                    "value": "Value",
                    "ownership": "Ownership",
                },
                inplace=True,
            )
        self._tables["insider_transactions"] = df

    def _parse_insider_holders(self, data: dict[str, Any]) -> None:
        """Parse insider holders roster table."""
        holders = data.get("holders", [])
        for owner in holders:
            for owner_key, value in owner.items():
                owner[owner_key] = self._parse_raw_values(value)
            owner.pop("maxAge", None)

        df = pd.DataFrame(holders)
        if not df.empty:
            if "positionDirectDate" in df:
                df["positionDirectDate"] = pd.to_datetime(
                    df["positionDirectDate"],
                    unit="s",
                )
            if "latestTransDate" in df:
                df["latestTransDate"] = pd.to_datetime(df["latestTransDate"], unit="s")

            df.rename(
                columns={
                    "name": "Name",
                    "relation": "Position",
                    "url": "URL",
                    "transactionDescription": "Most Recent Transaction",
                    "latestTransDate": "Latest Transaction Date",
                    "positionDirectDate": "Position Direct Date",
                    "positionDirect": "Shares Owned Directly",
                    "positionIndirectDate": "Position Indirect Date",
                    "positionIndirect": "Shares Owned Indirectly",
                },
                inplace=True,
            )

            df["Name"] = df["Name"].astype(str)
            df["Position"] = df["Position"].astype(str)
            df["URL"] = df["URL"].astype(str)
            df["Most Recent Transaction"] = df["Most Recent Transaction"].astype(str)

        self._tables["insider_roster"] = df

    def _parse_net_share_purchase_activity(self, data: dict[str, Any]) -> None:
        """Parse aggregate net share purchase activity table."""
        period = data.get("period", "")
        df = pd.DataFrame(
            {
                f"Insider Purchases Last {period}": [
                    "Purchases",
                    "Sales",
                    "Net Shares Purchased (Sold)",
                    "Total Insider Shares Held",
                    "% Net Shares Purchased (Sold)",
                    "% Buy Shares",
                    "% Sell Shares",
                ],
                "Shares": [
                    data.get("buyInfoShares"),
                    data.get("sellInfoShares"),
                    data.get("netInfoShares"),
                    data.get("totalInsiderShares"),
                    data.get("netPercentInsiderShares"),
                    data.get("buyPercentInsiderShares"),
                    data.get("sellPercentInsiderShares"),
                ],
                "Trans": [
                    data.get("buyInfoCount"),
                    data.get("sellInfoCount"),
                    data.get("netInfoCount"),
                    pd.NA,
                    pd.NA,
                    pd.NA,
                    pd.NA,
                ],
            }
        ).convert_dtypes()
        self._tables["insider_purchases"] = df

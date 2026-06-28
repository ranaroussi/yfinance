from yfinance._http import HTTPError
import pandas as pd

from yfinance import utils
from yfinance.config import YfConfig
from yfinance.const import _BASE_URL_
from yfinance.data import YfData
from yfinance.exceptions import YFDataException

_QUOTE_SUMMARY_URL_ = f"{_BASE_URL_}/v10/finance/quoteSummary"


class _HoldersCache:
    __slots__ = (
        'major', 'major_direct_holders', 'institutional', 'mutualfund',
        'insider_transactions', 'insider_purchases', 'insider_roster',
    )

    def __init__(self):
        self.major = None
        self.major_direct_holders = None
        self.institutional = None
        self.mutualfund = None
        self.insider_transactions = None
        self.insider_purchases = None
        self.insider_roster = None


class Holders:
    _SCRAPE_URL_ = 'https://finance.yahoo.com/quote'

    def __init__(self, data: YfData, symbol: str):
        self._data = data
        self._symbol = symbol
        self._cache = _HoldersCache()

    @property
    def major(self) -> pd.DataFrame:
        if self._cache.major is None:
            self._fetch_and_parse()
        return self._cache.major

    @property
    def institutional(self) -> pd.DataFrame:
        if self._cache.institutional is None:
            self._fetch_and_parse()
        return self._cache.institutional

    @property
    def mutualfund(self) -> pd.DataFrame:
        if self._cache.mutualfund is None:
            self._fetch_and_parse()
        return self._cache.mutualfund

    @property
    def insider_transactions(self) -> pd.DataFrame:
        if self._cache.insider_transactions is None:
            self._fetch_and_parse()
        return self._cache.insider_transactions

    @property
    def insider_purchases(self) -> pd.DataFrame:
        if self._cache.insider_purchases is None:
            self._fetch_and_parse()
        return self._cache.insider_purchases

    @property
    def insider_roster(self) -> pd.DataFrame:
        if self._cache.insider_roster is None:
            self._fetch_and_parse()
        return self._cache.insider_roster

    def _fetch(self):
        modules = ','.join(
            ["institutionOwnership", "fundOwnership", "majorDirectHolders", "majorHoldersBreakdown", "insiderTransactions", "insiderHolders", "netSharePurchaseActivity"])
        params_dict = {"modules": modules, "corsDomain": "finance.yahoo.com", "formatted": "false"}
        result = self._data.get_raw_json(f"{_QUOTE_SUMMARY_URL_}/{self._symbol}", params=params_dict)
        return result

    def _fetch_and_parse(self):
        try:
            result = self._fetch()
        except HTTPError as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            utils.get_yf_logger().error(str(e) + e.response.text)

            self._cache.major = pd.DataFrame()
            self._cache.major_direct_holders = pd.DataFrame()
            self._cache.institutional = pd.DataFrame()
            self._cache.mutualfund = pd.DataFrame()
            self._cache.insider_transactions = pd.DataFrame()
            self._cache.insider_purchases = pd.DataFrame()
            self._cache.insider_roster = pd.DataFrame()

            return

        try:
            data = result["quoteSummary"]["result"][0]
            # parse "institutionOwnership", "fundOwnership", "majorDirectHolders", "majorHoldersBreakdown", "insiderTransactions", "insiderHolders", "netSharePurchaseActivity"
            self._parse_institution_ownership(data.get("institutionOwnership", {}))
            self._parse_fund_ownership(data.get("fundOwnership", {}))
            # self._parse_major_direct_holders(data.get("majorDirectHolders", {}))  # need more data to investigate
            self._parse_major_holders_breakdown(data.get("majorHoldersBreakdown", {}))
            self._parse_insider_transactions(data.get("insiderTransactions", {}))
            self._parse_insider_holders(data.get("insiderHolders", {}))
            self._parse_net_share_purchase_activity(data.get("netSharePurchaseActivity", {}))
        except (KeyError, IndexError):
            if not YfConfig.debug.hide_exceptions:
                raise
            raise YFDataException("Failed to parse holders json data.")

    @staticmethod
    def _parse_raw_values(data):
        if isinstance(data, dict) and "raw" in data:
            return data["raw"]
        return data

    def _parse_institution_ownership(self, data):
        holders = data.get("ownershipList", {})
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        df = pd.DataFrame(holders)
        if not df.empty:
            df["reportDate"] = pd.to_datetime(df["reportDate"], unit="s")
            df.rename(columns={"reportDate": "Date Reported", "organization": "Holder", "position": "Shares", "value": "Value"}, inplace=True)  # "pctHeld": "% Out"
        self._cache.institutional = df

    def _parse_fund_ownership(self, data):
        holders = data.get("ownershipList", {})
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        df = pd.DataFrame(holders)
        if not df.empty:
            df["reportDate"] = pd.to_datetime(df["reportDate"], unit="s")
            df.rename(columns={"reportDate": "Date Reported", "organization": "Holder", "position": "Shares", "value": "Value"}, inplace=True)
        self._cache.mutualfund = df

    def _parse_major_direct_holders(self, data):
        holders = data.get("holders", {})
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        df = pd.DataFrame(holders)
        if not df.empty:
            df["reportDate"] = pd.to_datetime(df["reportDate"], unit="s")
            df.rename(columns={"reportDate": "Date Reported", "organization": "Holder", "positionDirect": "Shares", "valueDirect": "Value"}, inplace=True)
        self._cache.major_direct_holders = df

    def _parse_major_holders_breakdown(self, data):
        if "maxAge" in data:
            del data["maxAge"]
        df = pd.DataFrame.from_dict(data, orient="index")
        if not df.empty:
            df.columns.name = "Breakdown"
            df.rename(columns={df.columns[0]: 'Value'}, inplace=True)
        self._cache.major = df

    def _parse_insider_transactions(self, data):
        holders = data.get("transactions", {})
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        df = pd.DataFrame(holders)
        if not df.empty:
            df["startDate"] = pd.to_datetime(df["startDate"], unit="s")
            df.rename(columns={
                "startDate": "Start Date",
                "filerName": "Insider",
                "filerRelation": "Position",
                "filerUrl": "URL",
                "moneyText": "Transaction",
                "transactionText": "Text",
                "shares": "Shares",
                "value": "Value",
                "ownership": "Ownership"  # ownership flag, direct or institutional
            }, inplace=True)
        self._cache.insider_transactions = df

    def _parse_insider_holders(self, data):
        holders = data.get("holders", {})
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        df = pd.DataFrame(holders)
        if not df.empty:
            if "positionDirectDate" in df:
                df["positionDirectDate"] = pd.to_datetime(df["positionDirectDate"], unit="s")
            if "latestTransDate" in df:
                df["latestTransDate"] = pd.to_datetime(df["latestTransDate"], unit="s")

            df.rename(columns={
                "name": "Name",
                "relation": "Position",
                "url": "URL",
                "transactionDescription": "Most Recent Transaction",
                "latestTransDate": "Latest Transaction Date",
                "positionDirectDate": "Position Direct Date",
                "positionDirect": "Shares Owned Directly",
                "positionIndirectDate": "Position Indirect Date",
                "positionIndirect": "Shares Owned Indirectly"
            }, inplace=True)

            df["Name"] = df["Name"].astype(str)
            df["Position"] = df["Position"].astype(str)
            df["URL"] = df["URL"].astype(str)
            df["Most Recent Transaction"] = df["Most Recent Transaction"].astype(str)

        self._cache.insider_roster = df

    def _parse_net_share_purchase_activity(self, data):
        df = pd.DataFrame(
            {
                "Insider Purchases Last " + data.get("period", ""): [
                    "Purchases",
                    "Sales",
                    "Net Shares Purchased (Sold)",
                    "Total Insider Shares Held",
                    "% Net Shares Purchased (Sold)",
                    "% Buy Shares",
                    "% Sell Shares"
                ],
                "Shares": [
                    data.get('buyInfoShares'),
                    data.get('sellInfoShares'),
                    data.get('netInfoShares'),
                    data.get('totalInsiderShares'),
                    data.get('netPercentInsiderShares'),
                    data.get('buyPercentInsiderShares'),
                    data.get('sellPercentInsiderShares')
                ],
                "Trans": [
                    data.get('buyInfoCount'),
                    data.get('sellInfoCount'),
                    data.get('netInfoCount'),
                    pd.NA,
                    pd.NA,
                    pd.NA,
                    pd.NA
                ]
            }
        ).convert_dtypes()
        self._cache.insider_purchases = df

# from io import StringIO

import pandas as pd
import requests

from yfinance import utils
from yfinance.data import YfData
from yfinance.const import _BASE_URL_
from yfinance.exceptions import YFinanceDataException

_QUOTE_SUMMARY_URL_ = f"{_BASE_URL_}/v10/finance/quoteSummary/"


class Holders:
    _SCRAPE_URL_ = 'https://finance.yahoo.com/quote'

    def __init__(self, data: YfData, symbol: str, proxy=None):
        self._data = data
        self._symbol = symbol
        self.proxy = proxy

        self._major = None
        self._major_direct_holders = None
        self._institutional = None
        self._mutualfund = None

        self._insider_transactions = None
        self._insider_purchases = None
        self._insider_roster = None

    @property
    def major(self) -> pd.DataFrame:
        if self._major is None:
            # self._scrape(self.proxy)
            self._fetch_and_parse()
        return self._major

    @property
    def institutional(self) -> pd.DataFrame:
        if self._institutional is None:
            # self._scrape(self.proxy)
            self._fetch_and_parse()
        return self._institutional

    @property
    def mutualfund(self) -> pd.DataFrame:
        if self._mutualfund is None:
            # self._scrape(self.proxy)
            self._fetch_and_parse()
        return self._mutualfund

    @property
    def insider_transactions(self) -> pd.DataFrame:
        if self._insider_transactions is None:
            # self._scrape_insider_transactions(self.proxy)
            self._fetch_and_parse()
        return self._insider_transactions

    @property
    def insider_purchases(self) -> pd.DataFrame:
        if self._insider_purchases is None:
            # self._scrape_insider_transactions(self.proxy)
            self._fetch_and_parse()
        return self._insider_purchases

    @property
    def insider_roster(self) -> pd.DataFrame:
        if self._insider_roster is None:
            # self._scrape_insider_ros(self.proxy)
            self._fetch_and_parse()
        return self._insider_roster

    def _fetch(self, proxy):
        modules = ','.join(
            ["institutionOwnership", "fundOwnership", "majorDirectHolders", "majorHoldersBreakdown", "insiderTransactions", "insiderHolders", "netSharePurchaseActivity"])
        params_dict = {"modules": modules, "corsDomain": "finance.yahoo.com", "formatted": "false"}
        result = self._data.get_raw_json(f"{_QUOTE_SUMMARY_URL_}/{self._symbol}", user_agent_headers=self._data.user_agent_headers, params=params_dict, proxy=proxy)
        return result

    def _fetch_and_parse(self):
        try:
            result = self._fetch(self.proxy)
        except requests.exceptions.HTTPError as e:
            utils.get_yf_logger().error(str(e))

            self._major = pd.DataFrame()
            self._major_direct_holders = pd.DataFrame()
            self._institutional = pd.DataFrame()
            self._mutualfund = pd.DataFrame()
            self._insider_transactions = pd.DataFrame()
            self._insider_purchases = pd.DataFrame()
            self._insider_roster = pd.DataFrame()

            return

        try:
            data = result["quoteSummary"]["result"][0]
            # parse "institutionOwnership", "fundOwnership", "majorDirectHolders", "majorHoldersBreakdown", "insiderTransactions", "insiderHolders", "netSharePurchaseActivity"
            self._parse_institution_ownership(data["institutionOwnership"])
            self._parse_fund_ownership(data["fundOwnership"])
            # self._parse_major_direct_holders(data["majorDirectHolders"])  # need more data to investigate
            self._parse_major_holders_breakdown(data["majorHoldersBreakdown"])
            self._parse_insider_transactions(data["insiderTransactions"])
            self._parse_insider_holders(data["insiderHolders"])
            self._parse_net_share_purchase_activity(data["netSharePurchaseActivity"])
        except (KeyError, IndexError):
            raise YFinanceDataException("Failed to parse holders json data.")

    @staticmethod
    def _parse_raw_values(data):
        if isinstance(data, dict) and "raw" in data:
            return data["raw"]
        return data

    def _parse_institution_ownership(self, data):
        holders = data["ownershipList"]
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        df = pd.DataFrame(holders)
        if not df.empty:
            df["reportDate"] = pd.to_datetime(df["reportDate"], unit="s")
            df.rename(columns={"reportDate": "Date Reported", "organization": "Holder", "position": "Shares", "value": "Value"}, inplace=True)  # "pctHeld": "% Out"
        self._institutional = df

    def _parse_fund_ownership(self, data):
        holders = data["ownershipList"]
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        df = pd.DataFrame(holders)
        if not df.empty:
            df["reportDate"] = pd.to_datetime(df["reportDate"], unit="s")
            df.rename(columns={"reportDate": "Date Reported", "organization": "Holder", "position": "Shares", "value": "Value"}, inplace=True)
        self._mutualfund = df

    def _parse_major_direct_holders(self, data):
        holders = data["holders"]
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        df = pd.DataFrame(holders)
        if not df.empty:
            df["reportDate"] = pd.to_datetime(df["reportDate"], unit="s")
            df.rename(columns={"reportDate": "Date Reported", "organization": "Holder", "positionDirect": "Shares", "valueDirect": "Value"}, inplace=True)
        self._major_direct_holders = df

    def _parse_major_holders_breakdown(self, data):
        if "maxAge" in data:
            del data["maxAge"]
        df = pd.DataFrame.from_dict(data, orient="index")
        if not df.empty:
            df.columns.name = "Breakdown"
            df.rename(columns={df.columns[0]: 'Value'}, inplace=True)
        self._major = df

    def _parse_insider_transactions(self, data):
        holders = data["transactions"]
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
        self._insider_transactions = df

    def _parse_insider_holders(self, data):
        holders = data["holders"]
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        df = pd.DataFrame(holders)
        if not df.empty:
            df["positionDirectDate"] = pd.to_datetime(df["positionDirectDate"], unit="s")
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

        self._insider_roster = df

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
        self._insider_purchases = df

    

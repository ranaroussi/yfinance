from io import StringIO

import pandas as pd

from yfinance.data import YfData
from yfinance.const import _BASE_URL_
from yfinance.exceptions import YFNotImplementedError, YFinanceDataException, YFinanceException

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
        modules = ','.join(["institutionOwnership", "fundOwnership", "majorDirectHolders", "majorHoldersBreakdown", "insiderTransactions", "insiderHolders", "netSharePurchaseActivity"])
        params_dict = {"modules": modules, "corsDomain": "finance.yahoo.com", "symbol": self._symbol, "formatted": "false"}
        result = self._data.get_raw_json(_QUOTE_SUMMARY_URL_, user_agent_headers=self._data.user_agent_headers, params=params_dict, proxy=proxy)
        return result

    def _fetch_and_parse(self):
        result = self._fetch(self.proxy)
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
            df["positionIndirectDate"] = pd.to_datetime(df["positionIndirectDate"], unit="s")
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
        self._insider_roster = df

    def _parse_net_share_purchase_activity(self, data):
        if "maxAge" in data:
            del data["maxAge"]
        df = pd.DataFrame.from_dict(data, orient="index")
        if not df.empty:
            df.columns.name = "Activity"
            df.rename(columns={df.columns[0]: 'Value'}, inplace=True)
        self._insider_purchases = df

    """
    def _scrape(self, proxy):
        ticker_url = f"{self._SCRAPE_URL_}/{self._symbol}"
        try:
            resp = self._data.cache_get(ticker_url + '/holders', proxy=proxy)
            holders = pd.read_html(StringIO(resp.text))
        except Exception:
            holders = []

        if len(holders) >= 3:
            self._major = holders[0]
            self._institutional = holders[1]
            self._mutualfund = holders[2]
        elif len(holders) >= 2:
            self._major = holders[0]
            self._institutional = holders[1]
        elif len(holders) >= 1:
            self._major = holders[0]

        if self._institutional is not None:
            if 'Date Reported' in self._institutional:
                self._institutional['Date Reported'] = pd.to_datetime(
                    self._institutional['Date Reported'])
            if '% Out' in self._institutional:
                self._institutional['% Out'] = self._institutional[
                                                   '% Out'].str.replace('%', '').astype(float) / 100

        if self._mutualfund is not None:
            if 'Date Reported' in self._mutualfund:
                self._mutualfund['Date Reported'] = pd.to_datetime(
                    self._mutualfund['Date Reported'])
            if '% Out' in self._mutualfund:
                self._mutualfund['% Out'] = self._mutualfund[
                                                '% Out'].str.replace('%', '').astype(float) / 100
    
    def _scrape_insider_transactions(self, proxy):
        ticker_url = f"{self._SCRAPE_URL_}/{self._symbol}"
        resp = self._data.cache_get(ticker_url + '/insider-transactions', proxy=proxy)

        if "Will be right back" in resp.text:
                raise RuntimeError("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\n"
                                   "Our engineers are working quickly to resolve "
                                   "the issue. Thank you for your patience.")
        
        try:
            insider_transactions = pd.read_html(StringIO(resp.text))
        except ValueError:
            insider_transactions = []
        
        if len(insider_transactions) >= 3:
            self._insider_purchases = insider_transactions[0]
            self._insider_transactions = insider_transactions[2]
        elif len(insider_transactions) >= 2:
            self._insider_purchases = insider_transactions[0]
        elif len(insider_transactions) >= 1:
            self._insider_transactions = insider_transactions[0]

        if self._insider_transactions is not None:
            holders = self._insider_transactions

            # add positions column
            def split_insider_title(input_string):
                import re
                parts = input_string.split(' ')

                for i, part in enumerate(parts):
                    if not re.match(r'^[A-Z]+\.*-*[A-Z]*$', part):
                        name_part = ' '.join(parts[:i])
                        title_part = ' '.join(parts[i:])
                        return [name_part.strip(), title_part.strip()]

                return [input_string]
            holders.loc[:, ['Insider', 'Position']] = holders['Insider']\
                .apply(split_insider_title).apply(lambda x: pd.Series(x, index=['Insider', 'Position']))
            
            holders = holders[['Insider', 'Position'] + holders.columns\
                              .difference(['Insider', 'Position']).tolist()]

            # add N/A for no information
            holders.fillna('N/A', inplace=True)
            holders = holders.reset_index(drop=True)
            
            if 'Date' in holders:
                holders['Date'] = pd.to_datetime(holders['Date'])

            if 'Shares' in holders:
                holders['Shares'] = holders['Shares'].astype(int)

            self._insider_transactions = holders

        if self._insider_purchases is not None:
            holders = self._insider_purchases
            
            holders.fillna('N/A', inplace=True) 
            holders = holders.reset_index(drop=True)

            if 'Shares' in holders:
                def convert_shares(value):
                    import re
                    if re.match(r'^\d+(\.?\d*)?[BbMmKk%]$', value):
                        return value  # Leave values like '40.9B', '7.30%', etc. unchanged
                    
                    elif pd.notna(pd.to_numeric(value, errors='coerce')):
                        return int(value)  # Convert to integer if possible
                    
                    else:
                        return value 
                    
                holders['Shares'] = holders['Shares'].apply(convert_shares)
            self._insider_purchases = holders


    def _scrape_insider_ros(self, proxy):
        ticker_url = f"{self._SCRAPE_URL_}/{self._symbol}"
        resp = self._data.cache_get(ticker_url + '/insider-roster', proxy=proxy)
        
        if "Will be right back" in resp.text:
                raise RuntimeError("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\n"
                                   "Our engineers are working quickly to resolve "
                                   "the issue. Thank you for your patience.")

        try:    
            insider_roster = pd.read_html(StringIO(resp.text))
        except ValueError:
            insider_roster = []

        if len(insider_roster) >= 1:
            self._insider_roster = insider_roster[0]

        if self._insider_roster is not None:
            holders = self._insider_roster

            holders = holders[:-1]  # Remove the last row

            def split_name_title(input_string):
                import re
                parts = input_string.split(' ')

                for i, part in enumerate(parts):
                    if not re.match(r'^[A-Z]+\.*-*[A-Z]*$', part):
                        name_part = ' '.join(parts[:i])
                        title_part = ' '.join(parts[i:])
                        return [name_part.strip(), title_part.strip()]

                return [input_string]
            holders.loc[:, ['Individual or Entity', 'Position']] = holders['Individual or Entity']\
                .apply(split_name_title).apply(lambda x: pd.Series(x, index=['Individual or Entity', 'Position']))
            
            holders = holders[['Individual or Entity', 'Position'] + holders.columns\
                              .difference(['Individual or Entity', 'Position']).tolist()]

            # add N/A for no information
            holders.fillna('N/A', inplace=True)
            holders = holders.reset_index(drop=True)

            self._insider_roster = holders
    """

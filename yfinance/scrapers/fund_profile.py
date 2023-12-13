import pandas as pd

from yfinance.data import YfData
from yfinance.const import _BASE_URL_
from yfinance.exceptions import YFinanceDataException

_QUOTE_SUMMARY_URL_ = f"{_BASE_URL_}/v10/finance/quoteSummary/"


class FundProfile:
    def __init__(self, data: YfData, symbol: str, proxy=None):
        self._data = data
        self._symbol = symbol
        self.proxy = proxy

        self._description = None
        self._top_holdings = None
        self._equity_holdings = None
        self._bond_holdings = None
        self._bond_ratings = None
        self._sector_weightings = None
        self._profile = None
        self._perfomance = None

    @property
    def description(self) -> str:
        if self._description is None:
            self._fetch_and_parse()
        return self._description

    @property
    def top_holdings(self) -> pd.DataFrame:
        if self._top_holdings is None:
            self._fetch_and_parse()
        return self._top_holdings

    @property
    def equity_holdings(self) -> pd.DataFrame:
        if self._equity_holdings is None:
            self._fetch_and_parse()
        return self._equity_holdings

    @property
    def bond_holdings(self) -> pd.DataFrame:
        if self._bond_holdings is None:
            self._fetch_and_parse()
        return self._bond_holdings

    @property
    def bond_ratings(self) -> pd.DataFrame:
        if self._bond_ratings is None:
            self._fetch_and_parse()
        return self._bond_ratings

    @property
    def sector_weightings(self) -> pd.DataFrame:
        if self._sector_weightings is None:
            self._fetch_and_parse()
        return self._sector_weightings

    @property
    def profile(self) -> pd.DataFrame:
        if self._profile is None:
            self._fetch_and_parse()
        return self._profile

    @property
    def perfomance(self) -> pd.DataFrame:
        if self._perfomance is None:
            self._fetch_and_parse()
        return self._perfomance

    def _fetch(self, proxy):
        modules = ','.join(["quoteType", "summaryProfile", "topHoldings", "fundPerformance", "fundProfile"])
        params_dict = {"modules": modules, "corsDomain": "finance.yahoo.com", "symbol": self._symbol, "formatted": "false"}
        result = self._data.get_raw_json(_QUOTE_SUMMARY_URL_, user_agent_headers=self._data.user_agent_headers, params=params_dict, proxy=proxy)
        return result

    def _fetch_and_parse(self):
        result = self._fetch(self.proxy)
        try:
            data = result["quoteSummary"]["result"][0]
            # check quote type
            try:
                quote_type = data["quoteType"]["quoteType"]
                if quote_type != "ETF":
                    raise YFinanceDataException("Only ETFs are supported.")
            except KeyError:
                raise YFinanceDataException("Failed to parse quote type. No ETF data found.")
            # parse "summaryProfile", "topHoldings", "fundPerformance", "fundProfile",
            self._parse_description(data["summaryProfile"])
            self._parse_top_holdings(data["topHoldings"])
            self._parse_fund_performance(data["fundPerformance"])
            self._parse_fund_profile(data["fundProfile"])
        except (KeyError, IndexError):
            raise YFinanceDataException("Failed to parse fund json data.")

    @staticmethod
    def _parse_raw_values(data):
        if isinstance(data, dict) and "raw" in data:
            return data["raw"]
        return data

    def _parse_description(self, data):
        self._description = data.get("longBusinessSummary", "")

    def _parse_top_holdings(self, data):
        if "maxAge" in data:
            del data["maxAge"]
        df = pd.DataFrame.from_dict(data, orient="index")
        if not df.empty:
            df.columns.name = "Symbol"
            df.rename(columns={df.columns[0]: 'Name'}, inplace=True)
        self._top_holdings = df

    def _parse_fund_performance(self, data):
        if "maxAge" in data:
            del data["maxAge"]
        df = pd.DataFrame.from_dict(data, orient="index")
        if not df.empty:
            df.columns.name = "Performance"
            df.rename(columns={df.columns[0]: 'Value'}, inplace=True)
        self._perfomance = df

    def _parse_fund_profile(self, data):
        if "maxAge" in data:
            del data["maxAge"]
        df = pd.DataFrame.from_dict(data, orient="index")
        if not df.empty:
            df.columns.name = "Profile"
            df.rename(columns={df.columns[0]: 'Value'}, inplace=True)
        self._profile = df

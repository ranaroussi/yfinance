import curl_cffi
import pandas as pd

from yfinance import utils
from yfinance.config import YfConfig
from yfinance.const import _BASE_URL_
from yfinance.data import YfData
from yfinance.exceptions import YFDataException

_QUOTE_SUMMARY_URL_ = f"{_BASE_URL_}/v10/finance/quoteSummary"

class Holders:
    _SCRAPE_URL_ = 'https://finance.yahoo.com/quote'

    def __init__(self, data: YfData, symbol: str):
        self._data = data
        self._symbol = symbol

        self._major = None
        self._major_direct_holders = None
        self._institutional = None
        self._mutualfund = None

        self._insider_transactions = None
        self._insider_purchases = None
        self._insider_roster = None

    @property
    def major(self):
        if self._major is None:
            self._fetch_and_parse()
        return self._major

    @property
    def institutional(self):
        if self._institutional is None:
            self._fetch_and_parse()
        return self._institutional

    @property
    def mutualfund(self):
        if self._mutualfund is None:
            self._fetch_and_parse()
        return self._mutualfund

    @property
    def insider_transactions(self):
        if self._insider_transactions is None:
            self._fetch_and_parse()
        return self._insider_transactions

    @property
    def insider_purchases(self):
        if self._insider_purchases is None:
            self._fetch_and_parse()
        return self._insider_purchases

    @property
    def insider_roster(self):
        if self._insider_roster is None:
            self._fetch_and_parse()
        return self._insider_roster

    def _fetch(self):
        modules = ','.join(
            ["institutionOwnership", "fundOwnership", "majorDirectHolders", "majorHoldersBreakdown", "insiderTransactions", "insiderHolders", "netSharePurchaseActivity"])
        params_dict = {"modules": modules, "corsDomain": "finance.yahoo.com", "formatted": "false"}
        result = self._data.get_raw_json(f"{_QUOTE_SUMMARY_URL_}/{self._symbol}", params=params_dict)
        return result

    def _fetch_and_parse(self):
        try:
            result = self._fetch()
        except curl_cffi.requests.exceptions.HTTPError as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            utils.get_yf_logger().error(str(e) + e.response.text)

            empty = utils.empty_backend_df
            self._major = empty()
            self._major_direct_holders = empty()
            self._institutional = empty()
            self._mutualfund = empty()
            self._insider_transactions = empty()
            self._insider_purchases = empty()
            self._insider_roster = empty()

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
        self._institutional = utils.df_from_records(
            holders,
            datetime_unix_cols=["reportDate"],
            rename={"reportDate": "Date Reported", "organization": "Holder", "position": "Shares", "value": "Value"},
        )

    def _parse_fund_ownership(self, data):
        holders = data.get("ownershipList", {})
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        self._mutualfund = utils.df_from_records(
            holders,
            datetime_unix_cols=["reportDate"],
            rename={"reportDate": "Date Reported", "organization": "Holder", "position": "Shares", "value": "Value"},
        )

    def _parse_major_direct_holders(self, data):
        holders = data.get("holders", {})
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        self._major_direct_holders = utils.df_from_records(
            holders,
            datetime_unix_cols=["reportDate"],
            rename={"reportDate": "Date Reported", "organization": "Holder", "positionDirect": "Shares", "valueDirect": "Value"},
        )

    def _parse_major_holders_breakdown(self, data):
        if "maxAge" in data:
            del data["maxAge"]
        if utils.current_backend() == 'polars':
            import polars as pl
            if not data:
                self._major = pl.DataFrame()
                return
            self._major = pl.DataFrame(
                {"Breakdown": list(data.keys()), "Value": list(data.values())},
                strict=False,
            )
            return
        df = pd.DataFrame.from_dict(data, orient="index")
        if not df.empty:
            df.columns.name = "Breakdown"
            df.rename(columns={df.columns[0]: 'Value'}, inplace=True)
        self._major = df

    def _parse_insider_transactions(self, data):
        holders = data.get("transactions", {})
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        self._insider_transactions = utils.df_from_records(
            holders,
            datetime_unix_cols=["startDate"],
            rename={
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
        )

    def _parse_insider_holders(self, data):
        holders = data.get("holders", {})
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        self._insider_roster = utils.df_from_records(
            holders,
            datetime_unix_cols=["positionDirectDate", "latestTransDate"],
            rename={
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
            str_cols=["Name", "Position", "URL", "Most Recent Transaction"],
        )

    def _parse_net_share_purchase_activity(self, data):
        label_col = "Insider Purchases Last " + data.get("period", "")
        labels = [
            "Purchases", "Sales", "Net Shares Purchased (Sold)",
            "Total Insider Shares Held",
            "% Net Shares Purchased (Sold)", "% Buy Shares", "% Sell Shares",
        ]
        shares = [data.get('buyInfoShares'), data.get('sellInfoShares'),
                  data.get('netInfoShares'), data.get('totalInsiderShares'),
                  data.get('netPercentInsiderShares'),
                  data.get('buyPercentInsiderShares'),
                  data.get('sellPercentInsiderShares')]
        if utils.current_backend() == 'polars':
            import polars as pl
            trans = [data.get('buyInfoCount'), data.get('sellInfoCount'),
                     data.get('netInfoCount'), None, None, None, None]
            self._insider_purchases = pl.DataFrame(
                {label_col: labels, "Shares": shares, "Trans": trans},
                strict=False,
            )
            return
        trans_pd = [data.get('buyInfoCount'), data.get('sellInfoCount'),
                    data.get('netInfoCount'), pd.NA, pd.NA, pd.NA, pd.NA]
        self._insider_purchases = pd.DataFrame(
            {label_col: labels, "Shares": shares, "Trans": trans_pd}
        ).convert_dtypes()

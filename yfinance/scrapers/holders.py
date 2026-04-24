import curl_cffi
import polars as pl

from yfinance import utils
from yfinance.config import YfConfig
from yfinance.const import _BASE_URL_
from yfinance.data import YfData
from yfinance.exceptions import YFDataException

_QUOTE_SUMMARY_URL_ = f"{_BASE_URL_}/v10/finance/quoteSummary"


class Holders:
    _SCRAPE_URL_ = "https://finance.yahoo.com/quote"

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
    def major(self) -> pl.DataFrame:
        if self._major is None:
            self._fetch_and_parse()
        return self._major

    @property
    def institutional(self) -> pl.DataFrame:
        if self._institutional is None:
            self._fetch_and_parse()
        return self._institutional

    @property
    def mutualfund(self) -> pl.DataFrame:
        if self._mutualfund is None:
            self._fetch_and_parse()
        return self._mutualfund

    @property
    def insider_transactions(self) -> pl.DataFrame:
        if self._insider_transactions is None:
            self._fetch_and_parse()
        return self._insider_transactions

    @property
    def insider_purchases(self) -> pl.DataFrame:
        if self._insider_purchases is None:
            self._fetch_and_parse()
        return self._insider_purchases

    @property
    def insider_roster(self) -> pl.DataFrame:
        if self._insider_roster is None:
            self._fetch_and_parse()
        return self._insider_roster

    def _fetch(self):
        modules = ",".join(
            [
                "institutionOwnership",
                "fundOwnership",
                "majorDirectHolders",
                "majorHoldersBreakdown",
                "insiderTransactions",
                "insiderHolders",
                "netSharePurchaseActivity",
            ]
        )
        params_dict = {
            "modules": modules,
            "corsDomain": "finance.yahoo.com",
            "formatted": "false",
        }
        result = self._data.get_raw_json(
            f"{_QUOTE_SUMMARY_URL_}/{self._symbol}", params=params_dict
        )
        return result

    def _fetch_and_parse(self):
        try:
            result = self._fetch()
        except curl_cffi.requests.exceptions.HTTPError as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            utils.get_yf_logger().error(str(e) + e.response.text)

            self._major = pl.DataFrame()
            self._major_direct_holders = pl.DataFrame()
            self._institutional = pl.DataFrame()
            self._mutualfund = pl.DataFrame()
            self._insider_transactions = pl.DataFrame()
            self._insider_purchases = pl.DataFrame()
            self._insider_roster = pl.DataFrame()

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
            self._parse_net_share_purchase_activity(
                data.get("netSharePurchaseActivity", {})
            )
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
        df = pl.DataFrame(holders) if holders else pl.DataFrame()
        if df.height > 0:
            df = df.with_columns(
                pl.col("reportDate")
                .cast(pl.Int64)
                .mul(1_000_000)
                .cast(pl.Datetime("us", "UTC"))
                .alias("reportDate")
            )
            df = df.rename(
                {
                    "reportDate": "Date Reported",
                    "organization": "Holder",
                    "position": "Shares",
                    "value": "Value",
                }
            )
        self._institutional = df

    def _parse_fund_ownership(self, data):
        holders = data.get("ownershipList", {})
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        df = pl.DataFrame(holders) if holders else pl.DataFrame()
        if df.height > 0:
            df = df.with_columns(
                pl.col("reportDate")
                .cast(pl.Int64)
                .mul(1_000_000)
                .cast(pl.Datetime("us", "UTC"))
                .alias("reportDate")
            )
            df = df.rename(
                {
                    "reportDate": "Date Reported",
                    "organization": "Holder",
                    "position": "Shares",
                    "value": "Value",
                }
            )
        self._mutualfund = df

    def _parse_major_direct_holders(self, data):
        holders = data.get("holders", {})
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        df = pl.DataFrame(holders) if holders else pl.DataFrame()
        if df.height > 0:
            df = df.with_columns(
                pl.col("reportDate")
                .cast(pl.Int64)
                .mul(1_000_000)
                .cast(pl.Datetime("us", "UTC"))
                .alias("reportDate")
            )
            df = df.rename(
                {
                    "reportDate": "Date Reported",
                    "organization": "Holder",
                    "positionDirect": "Shares",
                    "valueDirect": "Value",
                }
            )
        self._major_direct_holders = df

    def _parse_major_holders_breakdown(self, data):
        if "maxAge" in data:
            del data["maxAge"]
        if data:
            df = pl.DataFrame(
                {"Breakdown": list(data.keys()), "Value": list(data.values())}
            )
        else:
            df = pl.DataFrame()
        self._major = df

    def _parse_insider_transactions(self, data):
        holders = data.get("transactions", {})
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        df = pl.DataFrame(holders) if holders else pl.DataFrame()
        if df.height > 0:
            df = df.with_columns(
                pl.col("startDate")
                .cast(pl.Int64)
                .mul(1_000_000)
                .cast(pl.Datetime("us", "UTC"))
                .alias("startDate")
            )
            df = df.rename(
                {
                    "startDate": "Start Date",
                    "filerName": "Insider",
                    "filerRelation": "Position",
                    "filerUrl": "URL",
                    "moneyText": "Transaction",
                    "transactionText": "Text",
                    "shares": "Shares",
                    "value": "Value",
                    "ownership": "Ownership",  # ownership flag, direct or institutional
                }
            )
        self._insider_transactions = df

    def _parse_insider_holders(self, data):
        holders = data.get("holders", {})
        for owner in holders:
            for k, v in owner.items():
                owner[k] = self._parse_raw_values(v)
            del owner["maxAge"]
        df = pl.DataFrame(holders) if holders else pl.DataFrame()
        if df.height > 0:
            date_conversions = []
            if "positionDirectDate" in df.columns:
                date_conversions.append(
                    pl.col("positionDirectDate")
                    .cast(pl.Int64)
                    .mul(1_000_000)
                    .cast(pl.Datetime("us", "UTC"))
                    .alias("positionDirectDate")
                )
            if "latestTransDate" in df.columns:
                date_conversions.append(
                    pl.col("latestTransDate")
                    .cast(pl.Int64)
                    .mul(1_000_000)
                    .cast(pl.Datetime("us", "UTC"))
                    .alias("latestTransDate")
                )
            if date_conversions:
                df = df.with_columns(date_conversions)

            _rename_map = {
                "name": "Name",
                "relation": "Position",
                "url": "URL",
                "transactionDescription": "Most Recent Transaction",
                "latestTransDate": "Latest Transaction Date",
                "positionDirectDate": "Position Direct Date",
                "positionDirect": "Shares Owned Directly",
                "positionIndirectDate": "Position Indirect Date",
                "positionIndirect": "Shares Owned Indirectly",
            }
            df = df.rename({k: v for k, v in _rename_map.items() if k in df.columns})

            df = df.with_columns(
                [
                    pl.col("Name").cast(pl.Utf8),
                    pl.col("Position").cast(pl.Utf8),
                    pl.col("URL").cast(pl.Utf8),
                    pl.col("Most Recent Transaction").cast(pl.Utf8),
                ]
            )

        self._insider_roster = df

    def _parse_net_share_purchase_activity(self, data):
        df = pl.DataFrame(
            {
                "Insider Purchases Last " + data.get("period", ""): pl.Series(
                    [
                        "Purchases",
                        "Sales",
                        "Net Shares Purchased (Sold)",
                        "Total Insider Shares Held",
                        "% Net Shares Purchased (Sold)",
                        "% Buy Shares",
                        "% Sell Shares",
                    ],
                    dtype=pl.Utf8,
                ),
                "Shares": pl.Series(
                    [
                        data.get("buyInfoShares"),
                        data.get("sellInfoShares"),
                        data.get("netInfoShares"),
                        data.get("totalInsiderShares"),
                        data.get("netPercentInsiderShares"),
                        data.get("buyPercentInsiderShares"),
                        data.get("sellPercentInsiderShares"),
                    ],
                    dtype=pl.Float64,
                ),
                "Trans": pl.Series(
                    [
                        data.get("buyInfoCount"),
                        data.get("sellInfoCount"),
                        data.get("netInfoCount"),
                        None,
                        None,
                        None,
                        None,
                    ],
                    dtype=pl.Int64,
                ),
            }
        )
        self._insider_purchases = df

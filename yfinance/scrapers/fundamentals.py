import datetime
import json

import pandas as pd

from yfinance import utils
from yfinance.data import TickerData
from yfinance.exceptions import YFianceDataException, YFianceException


class Fundamentals:

    def __init__(self, data: TickerData, proxy=None):
        self._data = data
        self.proxy = proxy

        self._earnings = None
        self._financials = None
        self._shares = None

        self._financials_data = None
        self._fin_data_quote = None
        self._basics_already_scraped = False
        self._financials = Fiancials(data)

    @property
    def financials(self) -> "Fiancials":
        return self._financials

    @property
    def earnings(self) -> dict:
        if self._earnings is None:
            self._scrape_earnings(self.proxy)
        return self._earnings

    @property
    def shares(self) -> pd.DataFrame:
        if self._shares is None:
            self._scrape_shares(self.proxy)
        return self._shares

    def _scrape_basics(self, proxy):
        if self._basics_already_scraped:
            return
        self._basics_already_scraped = True

        self._financials_data = self._data.get_json_data_stores('financials', proxy)
        try:
            self._fin_data_quote = self._financials_data['QuoteSummaryStore']
        except KeyError:
            err_msg = "No financials data found, symbol may be delisted"
            print('- %s: %s' % (self._data.ticker, err_msg))
            return None

    def _scrape_earnings(self, proxy):
        self._scrape_basics(proxy)
        # earnings
        self._earnings = {"yearly": pd.DataFrame(), "quarterly": pd.DataFrame()}
        if self._fin_data_quote is None:
            return
        if isinstance(self._fin_data_quote.get('earnings'), dict):
            try:
                earnings = self._fin_data_quote['earnings']['financialsChart']
                earnings['financialCurrency'] = self._fin_data_quote['earnings'].get('financialCurrency', 'USD')
                self._earnings['financialCurrency'] = earnings['financialCurrency']
                df = pd.DataFrame(earnings['yearly']).set_index('date')
                df.columns = utils.camel2title(df.columns)
                df.index.name = 'Year'
                self._earnings['yearly'] = df

                df = pd.DataFrame(earnings['quarterly']).set_index('date')
                df.columns = utils.camel2title(df.columns)
                df.index.name = 'Quarter'
                self._earnings['quarterly'] = df
            except Exception:
                pass

    def _scrape_shares(self, proxy):
        self._scrape_basics(proxy)
        # shares outstanding
        try:
            # keep only years with non None data
            available_shares = [shares_data for shares_data in
                                self._financials_data['QuoteTimeSeriesStore']['timeSeries']['annualBasicAverageShares']
                                if
                                shares_data]
            shares = pd.DataFrame(available_shares)
            shares['Year'] = shares['asOfDate'].agg(lambda x: int(x[:4]))
            shares.set_index('Year', inplace=True)
            shares.drop(columns=['dataId', 'asOfDate',
                                 'periodType', 'currencyCode'], inplace=True)
            shares.rename(
                columns={'reportedValue': "BasicShares"}, inplace=True)
            self._shares = shares
        except Exception:
            pass


class Fiancials:
    def __init__(self, data: TickerData):
        self._data = data
        self._income = {}
        self._balance_sheet = {}
        self._cash_flow = {}

    def get_income(self, freq="yearly", proxy=None) -> pd.DataFrame:
        res = self._income
        if freq not in res:
            res[freq] = self._scrape("income", freq, proxy=None)
        return res[freq]

    def get_balance_sheet(self, freq="yearly", proxy=None) -> pd.DataFrame:
        res = self._balance_sheet
        if freq not in res:
            res[freq] = self._scrape("balance-sheet", freq, proxy=None)
        return res[freq]

    def get_cash_flow(self, freq="yearly", proxy=None) -> pd.DataFrame:
        res = self._cash_flow
        if freq not in res:
            res[freq] = self._scrape("cash-flow", freq, proxy=None)
        return res[freq]

    def _scrape(self, name, timescale, proxy=None):
        allowed_names = ["income", "balance-sheet", "cash-flow"]
        allowed_timescales = ["yearly", "quarterly"]

        if name not in allowed_names:
            raise ValueError("Illegal argument: name must be one of: {}".format(allowed_names))
        if timescale not in allowed_timescales:
            raise ValueError("Illegal argument: timescale must be one of: {}".format(allowed_names))

        try:
            statement = self._create_financials_table(name, timescale, proxy)
            if statement is not None:
                return statement
        except YFianceException as e:
            print("Failed to create financials table for {} reason: {}".format(name, repr(e)))
        return pd.DataFrame()

    def _create_financials_table(self, name, timescale, proxy):
        if name == "income":
            # Yahoo stores the 'income' table internally under 'financials' key
            name = "financials"

        keys = self._get_datastore_keys(name, proxy)

        try:
            # Developers note: TTM and template stuff allows for reproducing the nested structure
            # visible on Yahoo website. But more work needed to make it user-friendly! Ideally
            # return a tree data structure instead of Pandas MultiIndex
            # So until this is implemented, just return simple tables
            return self.get_financials_time_series(timescale, keys, proxy)

        except Exception as e:
            pass

    def _get_datastore_keys(self, sub_page, proxy) -> list:
        data_stores = self._data.get_json_data_stores(sub_page, proxy)

        # Step 1: get the keys:
        def _finditem1(key, obj):
            values = []
            if isinstance(obj, dict):
                if key in obj.keys():
                    values.append(obj[key])
                for k, v in obj.items():
                    values += _finditem1(key, v)
            elif isinstance(obj, list):
                for v in obj:
                    values += _finditem1(key, v)
            return values

        try:
            keys = _finditem1("key", data_stores['FinancialTemplateStore'])
        except KeyError as e:
            raise YFianceDataException("Parsing FinancialTemplateStore failed, reason: {}".format(repr(e)))

        if not keys:
            raise YFianceDataException("No keys in FinancialTemplateStore")
        return keys

    def get_financials_time_series(self, timescale, keys: list, proxy=None) -> pd.DataFrame:
        timescale_translation = {"yearly": "annual", "quarterly": "quarterly"}
        timescale = timescale_translation[timescale]

        # Step 2: construct url:
        ts_url_base = \
            "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{0}?symbol={0}" \
                .format(self._data.ticker)

        url = ts_url_base + "&type=" + ",".join([timescale + k for k in keys])
        # Yahoo returns maximum 4 years or 5 quarters, regardless of start_dt:
        start_dt = datetime.datetime(2016, 12, 31)
        end = (datetime.datetime.now() + datetime.timedelta(days=366))
        url += "&period1={}&period2={}".format(int(start_dt.timestamp()), int(end.timestamp()))

        # Step 3: fetch and reshape data
        json_str = self._data.get(url=url, proxy=proxy).text
        json_data = json.loads(json_str)
        data_raw = json_data["timeseries"]["result"]
        # data_raw = [v for v in data_raw if len(v) > 1] # Discard keys with no data
        for d in data_raw:
            del d["meta"]

        # Now reshape data into a table:
        # Step 1: get columns and index:
        timestamps = set()
        data_unpacked = {}
        for x in data_raw:
            for k in x.keys():
                if k == "timestamp":
                    timestamps.update(x[k])
                else:
                    data_unpacked[k] = x[k]
        timestamps = sorted(list(timestamps))
        dates = pd.to_datetime(timestamps, unit="s")
        df = pd.DataFrame(columns=dates, index=list(data_unpacked.keys()))
        for k, v in data_unpacked.items():
            if df is None:
                df = pd.DataFrame(columns=dates, index=[k])
            df.loc[k] = {pd.Timestamp(x["asOfDate"]): x["reportedValue"]["raw"] for x in v}

        df.index = df.index.str.replace("^" + timescale, "", regex=True)

        # Reorder table to match order on Yahoo website
        df = df.reindex([k for k in keys if k in df.index])
        df = df[sorted(df.columns, reverse=True)]

        df.index = utils.camel2title(df.index)

        return df

#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Yahoo! Finance market data downloader (+fix for Pandas Datareader)
# https://github.com/ranaroussi/yfinance
#
# Copyright 2017-2019 Ran Aroussi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

__version__ = "0.1.45"
__author__ = "Ran Aroussi"
__all__ = ['download', 'Ticker', 'Tickers', 'pdr_override']

import time as _time
import datetime as _datetime
import requests as _requests
# import json as _json
from collections import namedtuple as _namedtuple
import multitasking as _multitasking
import pandas as _pd
import numpy as _np
import sys as _sys
import re as _re

_DFS = {}
_PROGRESS_BAR = None


def genTickers(tickers):
    tickers = tickers if isinstance(
        tickers, list) else tickers.replace(',', ' ').split()
    tickers = [ticker.upper() for ticker in tickers]
    ticker_objects = {}

    for ticker in tickers:
        ticker_objects[ticker] = Ticker(ticker)
    return _namedtuple("Tickers", ticker_objects.keys()
                       )(*ticker_objects.values())


class Tickers():

    def __repr__(self):
        return 'yfinance.Tickers object <%s>' % ",".join(self.symbols)

    def __init__(self, tickers):
        tickers = tickers if isinstance(
            tickers, list) else tickers.replace(',', ' ').split()
        self.symbols = [ticker.upper() for ticker in tickers]
        ticker_objects = {}

        for ticker in self.symbols:
            ticker_objects[ticker] = Ticker(ticker)

        self.tickers = _namedtuple(
            "Tickers", ticker_objects.keys(), rename=True
        )(*ticker_objects.values())

    def download(self, period="1mo", interval="1d",
                 start=None, end=None, prepost=False,
                 actions=True, auto_adjust=True, proxy=None,
                 threads=True, group_by='column', progress=True,
                 **kwargs):

        data = download(self.symbols,
                        start=start, end=end, actions=actions,
                        auto_adjust=auto_adjust,
                        period=period,
                        interval=interval,
                        prepost=prepost,
                        proxy=proxy,
                        group_by='ticker',
                        threads=threads,
                        progress=progress,
                        **kwargs)

        for symbol in self.symbols:
            getattr(self.tickers, symbol)._history = data[symbol]

        if group_by == 'column':
            data.columns = data.columns.swaplevel(0, 1)
            data.sort_index(level=0, axis=1, inplace=True)

        return data


class Ticker():

    def __repr__(self):
        return 'yfinance.Ticker object <%s>' % self.ticker

    def __init__(self, ticker):
        self.ticker = ticker.upper()
        self._history = None
        self._base_url = 'https://query1.finance.yahoo.com'

        self._financials = None
        self._balance_sheet = None
        self._cashflow = None
        self._all_financials = None
        self._sustainability = None
        self._scrape_url = 'https://finance.yahoo.com/quote'
        self._expirations = {}

    @property
    def info(self):
        """ retreive metadata and currenct price data """
        url = "{}/v7/finance/quote?symbols={}".format(
            self._base_url, self.ticker)
        r = _requests.get(url=url).json()["quoteResponse"]["result"]
        if r:
            return r[0]
        return {}

    def _download_options(self, date=None, proxy=None):
        if date is None:
            url = "{}/v7/finance/options/{}".format(
                self._base_url, self.ticker)
        else:
            url = "{}/v7/finance/options/{}?date={}".format(
                self._base_url, self.ticker, date)

        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        r = _requests.get(url=url, proxies=proxy).json()
        if r['optionChain']['result']:
            for exp in r['optionChain']['result'][0]['expirationDates']:
                self._expirations[_datetime.datetime.fromtimestamp(
                    exp).strftime('%Y-%m-%d')] = exp
            return r['optionChain']['result'][0]['options'][0]
        return {}

    @property
    def options(self):
        if not self._expirations:
            self._download_options()
        return tuple(self._expirations.keys())

    def _options2df(self, opt):
        return _pd.DataFrame(opt).reindex(columns=[
            'contractSymbol',
            'lastTradeDate',
            'strike',
            'lastPrice',
            'bid',
            'ask',
            'change',
            'percentChange',
            'volume',
            'openInterest',
            'impliedVolatility',
            'inTheMoney',
            'contractSize',
            'currency'])

    def option_chain(self, date=None, proxy=None):
        if date is None:
            options = self._download_options(proxy=proxy)
        else:
            if not self._expirations:
                self._download_options()
            date = self._expirations[date]
            options = self._download_options(date, proxy=proxy)

        return _namedtuple('Options', ['calls', 'puts'])(**{
            "calls": self._options2df(options['calls']),
            "puts": self._options2df(options['puts'])
        })

    @staticmethod
    def _auto_adjust(data):
        df = data.copy()
        ratio = df["Close"] / df["Adj Close"]
        df["Adj Open"] = df["Open"] / ratio
        df["Adj High"] = df["High"] / ratio
        df["Adj Low"] = df["Low"] / ratio

        df.drop(
            ["Open", "High", "Low", "Close"],
            axis=1, inplace=True)

        df.rename(columns={
            "Adj Open": "Open", "Adj High": "High",
            "Adj Low": "Low", "Adj Close": "Close"
        }, inplace=True)

        df = df[["Open", "High", "Low", "Close", "Volume"]]
        return df

    @staticmethod
    def _parse_quotes(data):
        timestamps = data["timestamp"]
        ohlc = data["indicators"]["quote"][0]
        volumes = ohlc["volume"]
        opens = ohlc["open"]
        closes = ohlc["close"]
        lows = ohlc["low"]
        highs = ohlc["high"]

        adjclose = closes
        if "adjclose" in data["indicators"]:
            adjclose = data["indicators"]["adjclose"][0]["adjclose"]

        quotes = _pd.DataFrame({"Open": opens,
                                "High": highs,
                                "Low": lows,
                                "Close": closes,
                                "Adj Close": adjclose,
                                "Volume": volumes})

        quotes.index = _pd.to_datetime(timestamps, unit="s")
        quotes.sort_index(inplace=True)
        return quotes

    @staticmethod
    def _parse_actions(data):
        dividends = _pd.DataFrame(columns=["Dividends"])
        splits = _pd.DataFrame(columns=["Stock Splits"])

        if "events" in data:
            if "dividends" in data["events"]:
                dividends = _pd.DataFrame(
                    data=list(data["events"]["dividends"].values()))
                dividends.set_index("date", inplace=True)
                dividends.index = _pd.to_datetime(dividends.index, unit="s")
                dividends.sort_index(inplace=True)
                dividends.columns = ["Dividends"]

            if "splits" in data["events"]:
                splits = _pd.DataFrame(
                    data=list(data["events"]["splits"].values()))
                splits.set_index("date", inplace=True)
                splits.index = _pd.to_datetime(
                    splits.index, unit="s")
                splits.sort_index(inplace=True)
                splits["Stock Splits"] = splits["numerator"] / \
                    splits["denominator"]
                splits = splits["Stock Splits"]

        return dividends, splits

    def get_dividends(self, proxy=None):
        if self._history is None:
            self.history(period="max", proxy=proxy)
        dividends = self._history["Dividends"]
        return dividends[dividends != 0]

    def get_splits(self, proxy=None):
        if self._history is None:
            self.history(period="max", proxy=proxy)
        splits = self._history["Stock Splits"]
        return splits[splits != 0]

    def get_actions(self, proxy=None):
        if self._history is None:
            self.history(period="max", proxy=proxy)
        actions = self._history[["Dividends", "Stock Splits"]]
        return actions[actions != 0].dropna(how='all').fillna(0)

    # ------------------------

    def history(self, period="1mo", interval="1d",
                start=None, end=None, prepost=False, actions=True,
                auto_adjust=True, proxy=None, rounding=False):
        """
        :Parameters:
            period : str
                Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
                Either Use period parameter or use start and end
            interval : str
                Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
                Intraday data cannot extend last 60 days
            start: str
                Download start date string (YYYY-MM-DD) or _datetime.
                Default is 1900-01-01
            end: str
                Download end date string (YYYY-MM-DD) or _datetime.
                Default is now
            prepost : bool
                Include Pre and Post market data in results?
                Default is False
            auto_adjust: bool
                Adjust all OHLC automatically? Default is True
            proxy: str
                Optional. Proxy server URL scheme. Default is None
            rounding: bool
                Optional. Whether to round the retrieved values to the precision suggested by Yahoo.
        """

        if start or period is None or period.lower() == "max":
            if start is None:
                start = -2208988800
            elif isinstance(start, _datetime.datetime):
                start = int(_time.mktime(start.timetuple()))
            else:
                start = int(_time.mktime(
                    _time.strptime(str(start), '%Y-%m-%d')))
            if end is None:
                end = int(_time.time())
            elif isinstance(end, _datetime.datetime):
                end = int(_time.mktime(end.timetuple()))
            else:
                end = int(_time.mktime(_time.strptime(str(end), '%Y-%m-%d')))

            params = {"period1": start, "period2": end}
        else:
            period = period.lower()
            params = {"range": period}

        params["interval"] = interval.lower()
        params["includePrePost"] = prepost
        params["events"] = "div,splits"

        # 1) fix weired bug with Yahoo! - returning 60m for 30m bars
        if params["interval"] == "30m":
            params["interval"] = "15m"

        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        # Getting data from json
        url = "{}/v8/finance/chart/{}".format(self._base_url, self.ticker)
        data = _requests.get(url=url, params=params, proxies=proxy)
        if "Will be right back" in data.text:
            raise RuntimeError("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\n"
                               "Our engineers are working quickly to resolve "
                               "the issue. Thank you for your patience.")
        data = data.json()

        # Work with errors
        err_msg = "No data found for this date range, symbol may be delisted"
        if "chart" in data and data["chart"]["error"]:
            err_msg = data["chart"]["error"]["description"]
            _DFS[self.ticker] = _emptydf()
            raise ValueError(self.ticker, err_msg)

        elif "chart" not in data or data["chart"]["result"] is None or \
                not data["chart"]["result"]:
            _DFS[self.ticker] = _emptydf()
            raise ValueError(self.ticker, err_msg)

        # parse quotes
        try:
            quotes = self._parse_quotes(data["chart"]["result"][0])
        except Exception:
            _DFS[self.ticker] = _emptydf()
            raise ValueError(self.ticker, err_msg)

        # 2) fix weired bug with Yahoo! - returning 60m for 30m bars
        if interval.lower() == "30m":
            quotes2 = quotes.resample('30T')
            quotes = _pd.DataFrame(index=quotes2.last().index, data={
                'Open': quotes2['Open'].first(),
                'High': quotes2['High'].max(),
                'Low': quotes2['Low'].min(),
                'Close': quotes2['Close'].last(),
                'Adj Close': quotes2['Adj Close'].last(),
                'Volume': quotes2['Volume'].sum()
            })
            try:
                quotes['Dividends'] = quotes2['Dividends'].max()
            except Exception:
                pass
            try:
                quotes['Stock Splits'] = quotes2['Dividends'].max()
            except Exception:
                pass

        if auto_adjust:
            quotes = self._auto_adjust(quotes)

        if rounding:
            quotes = _np.round(quotes, data[
                "chart"]["result"][0]["meta"]["priceHint"])
        quotes['Volume'] = quotes['Volume'].fillna(0).astype(_np.int64)

        quotes.dropna(inplace=True)

        # actions
        dividends, splits = self._parse_actions(data["chart"]["result"][0])

        # combine
        df = _pd.concat([quotes, dividends, splits], axis=1, sort=True)
        df["Dividends"].fillna(0, inplace=True)
        df["Stock Splits"].fillna(0, inplace=True)

        # index eod/intraday
        df.index = df.index.tz_localize("UTC").tz_convert(
            data["chart"]["result"][0]["meta"]["exchangeTimezoneName"])

        if params["interval"][-1] == "m":
            df.index.name = "Datetime"
        else:
            df.index = _pd.to_datetime(df.index.date)
            df.index.name = "Date"

        self._history = df.copy()

        if not actions:
            df.drop(columns=["Dividends", "Stock Splits"], inplace=True)

        return df

    # ------------------------

    def _get_fundamentals(self, kind, proxy=None):
        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        """
        url = '%s/%s' % (self._scrape_url, self.ticker)
        html = _requests.get(url=url, proxies=proxy).text
        json_str = html.split('root.App.main =')[1].split(
            '(this)')[0].split(';\n}')[0].strip()
        data = _json.loads(json_str)[
            'context']['dispatcher']['stores']['QuoteSummaryStore']

        new_data = ''
        data_parts = _json.dumps(data).replace('{}', 'null').split('{"raw": ')
        for x in range(len(data_parts)):
            if "fmt" in data_parts[x]:
                p = data_parts[x].split(', "fmt":', maxsplit=1)
                new_data += p[0] + p[1].split('}', maxsplit=1)[1]
            else:
                new_data += data_parts[x]
        data = _json.loads(new_data)

        return data
        """

        url = '%s/%s/%s' % (self._scrape_url, self.ticker, kind)
        try:
            data = _pd.read_html(_requests.get(url=url, proxies=proxy).text)[0]
        except ValueError:
            return _pd.DataFrame()

        if kind == 'sustainability':
            data['Significant Involvement'] = data[
                'Significant Involvement'] != 'No'
            return data

        data.columns = [''] + list(data[:1].values[0][1:])
        data.set_index('', inplace=True)

        if len(data.columns) < 2:
            return _pd.DataFrame()

        for col in data.columns:
            data[col] = _np.where(data[col] == '-', _np.nan, data[col])
        idx = data[data[data.columns[0]] == data[data.columns[1]]].index
        data.loc[idx] = '-'
        return data[1:]
    
    #demjson.decode() and jsonnet.evaluate_snippet() can parse raw javascript string
    def _get_financial_translator(self, vendor, proxy=None):
        """
        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}
        #vendor = 'https://s.yimg.com/uc/finance/dd-site/js/vendor.d859a2b02e2b0845735f.min.js'
        req = _requests.get(url=vendor, proxies=proxy)
        r = req.text
        res = _re.search('t\.p\+\"\"\+\((.*?)\[e\]\|\|e\)\+\"\.\"\+(.*?)\[e\]\+\"\.min\.js\";', r)
        ks = json.loads(_re.sub(r'([\{\s,])(\w+)(:)', r'\1"\2"\3', res[1]))
        vs = json.loads(_re.sub(r'([\{\s,])(\w+)(:)', r'\1"\2"\3', res[2]))
        fncneUrl = vs[list(ks.keys())[list(ks.values()).index('Quote.financials')]]
        url = 'https://s.yimg.com/uc/finance/dd-site/js/Quote.financials.' + fncneUrl + '.min.js'
        req = _requests.get(url=url, proxies=proxy)
        strs = req.text.split('e.exports=')
        objs = [json.loads(_re.sub(':!0', ':true', _re.sub(r'([\{\s,])(\w+)(:)', r'\1"\2"\3', re.sub('}(}\);|,\d+:function\(e,t\){)', '', strs[n])))) for n in range(6, 9)]
        """
        #valid financial translator as of 2019/9/24
        ft = {'incomeStatement': [['endDate', 'REVENUE', True], ['totalRevenue', 'TOTAL_REVENUE', False], ['costOfRevenue', 'COST_OF_REVENUE', False], ['grossProfit', 'GROSS_PROFIT', False], ['', 'OPERATING_EXPENSES', False], ['researchDevelopment', 'RESEARCH_DEVELOPMENT', False], ['sellingGeneralAdministrative', 'SELLING_GEN_ADMIN', False], ['nonRecurring', 'NON_RECURRING', False], ['otherOperatingExpenses', 'OTHERS', False], ['totalOperatingExpenses', 'TOTAL_OPERATING_EX', False], ['operatingIncome', 'OPERATING_INCOME_LOSS', False], ['', 'INCOME_FROM_CONTINUING_OPS', False], ['totalOtherIncomeExpenseNet', 'TOTAL_OTHER_INCOME_EXPENSES_NET', False], ['ebit', 'EARNINGS_BEFORE_INTEREST_TAX', False], ['interestExpense', 'INTEREST_EXPENSE', False], ['incomeBeforeTax', 'INCOME_BEFORE_TAX', False], ['incomeTaxExpense', 'INCOME_TAX_EXPENSE', False], ['minorityInterest', 'MINORITY_INTEREST', False], ['netIncomeFromContinuingOps', 'NET_INCOME_FROM_CONTINUING_OPS', False], ['', 'NON_RECURRING_EVENTS', False], ['discontinuedOperations', 'DISCONTINUED_OPS', False], ['extraordinaryItems', 'EXTRAORDINARY_ITEMS', False], ['effectOfAccountingCharges', 'EFFECT_OF_ACCOUNTING_CHANGES', False], ['otherItems', 'OTHER_ITEMS', False], ['', 'NET_INCOME_TITLE', False], ['netIncome', 'NET_INCOME', False], ['preferredStock', 'PREFERRED_STOCK_OTHER_ADJ', False], ['netIncomeApplicableToCommonShares', 'NET_INCOME_APPLICABLE_TO_COMMON_SHARES', False]], 'balanceSheet': [['endDate', 'PERIOD_ENDING', True], ['', 'CURRENT_ASSETS', False], ['cash', 'CASH_AND_CASH_EQUIVALENTS', False], ['shortTermInvestments', 'SHORT_TERM_INVESTMENTS', False], ['netReceivables', 'NET_RECEIVABLES', False], ['inventory', 'INVENTORY', False], ['otherCurrentAssets', 'OTHER_CURRENT_ASSETS', False], ['totalCurrentAssets', 'TOTAL_CURRENT_ASSETS', False], ['longTermInvestments', 'LONG_TERM_INVESTMENTS', False], ['propertyPlantEquipment', 'PROPERTY_PLANT_AND_EQUIPMENT', False], ['goodWill', 'GOODWILL', False], ['intangibleAssets', 'INTANGIBLE_ASSETS', False], ['accumulatedAmortization', 'ACCUMULATED_AMORTIZATION', False], ['otherAssets', 'OTHER_ASSETS', False], ['deferredLongTermAssetCharges', 'DEFERRED_LONG_TERM_ASSET_CHARGES', False], ['totalAssets', 'TOTAL_ASSETS', False], ['', 'CURRENT_LIABILITIES', False], ['accountsPayable', 'ACCOUNTS_PAYABLE', False], ['shortLongTermDebt', 'SHORT_CURRENT_LONG_TERM_DEBT', False], ['otherCurrentLiab', 'OTHER_CURRENT_LIABILITIES', False], ['totalCurrentLiabilities', 'TOTAL_CURRENT_LIABILITIES', False], ['longTermDebt', 'LONG_TERM_DEBT', False], ['otherLiab', 'OTHER_LIABILITIES', False], ['deferredLongTermLiab', 'DEFERRED_LONG_TERM_LIABILITY_CHARGES', False], ['minorityInterest', 'MINORITY_INTEREST', False], ['negativeGoodWill', 'NEGATIVE_GOODWILL', False], ['totalLiab', 'TOTAL_LIABILITIES', False], ['', 'STOCKHOLDERS_EQUITY', False], ['stockOptionWarrants', 'MISC_STOCKS_OPTIONS_WARRANTS', False], ['redeemablePreferredStock', 'REDEEMABLE_PREFERRED_STOCK', False], ['redeemablePreferredStock', 'PREFERRED_STOCK', False], ['commonStock', 'COMMON_STOCK', False], ['retainedEarnings', 'RETAINED_EARNINGS', False], ['treasuryStock', 'TREASURY_STOCK', False], ['capitalSurplus', 'CAPITAL_SURPLUS', False], ['otherStockholderEquity', 'OTHER_STOCKHOLDER_EQUITY', False], ['totalStockholderEquity', 'TOTAL_STOCKHOLDER_EQUITY', False], ['netTangibleAssets', 'NET_TANGIBLE_ASSETS', False]], 'cashflowStatement': [['endDate', 'PERIOD_ENDING', True], ['netIncome', 'NET_INCOME', False], ['', 'OPERATING_ACTIVITIES_CASHFLOWS_PROVIDED', False], ['depreciation', 'DEPRECIATION', False], ['changeToNetincome', 'ADJUSTMENT_TO_NET_INCOME', False], ['changeToAccountReceivables', 'CHANGES_IN_ACCOUNTS_RECEIVABLES', False], ['changeToLiabilities', 'CHANGES_IN_LIABILITIES', False], ['changeToInventory', 'CHANGES_IN_INVENTORIES', False], ['changeToOperatingActivities', 'CHANGES_IN_OTHER_OPERATING_ACT', False], ['totalCashFromOperatingActivities', 'TOTAL_CASH_FLOW_FROM_OP_ACT', False], ['', 'INVESTING_ACTIVITIES_CASHFLOWS_PROVIDED', False], ['capitalExpenditures', 'CAPITAL_EX', False], ['investments', 'INVESTMENTS', False], ['otherCashflowsFromInvestingActivities', 'OTHER_CASHFLOWS_FROM_INVESTING_ACT', False], ['totalCashflowsFromInvestingActivities', 'TOTAL_CASH_FLOW_FROM_INVEST_ACT', False], ['', 'FINANCING_ACTIVITIES_CASHFLOWS_PROVIDED', False], ['dividendsPaid', 'DIVIDENDS_PAID', False], ['salePurchaseOfStock', 'SALE_PURCHASE_OF_STOCK', False], ['netBorrowings', 'NET_BORROWINGS', False], ['otherCashflowsFromFinancingActivities', 'OTHER_CASHFLOWS_FROM_FINANCING_ACT', False], ['totalCashFromFinancingActivities', 'TOTAL_CASH_FLOW_FROM_FIN_ACT', False], ['effectOfExchangeRate', 'EFFECT_OF_EXCHANGE_RATE_CHANGES', False], ['changeInCash', 'CHANGE_IN_CASH_AND_EQ', False]]}
        return ([('incomeStatementHistory','incomeStatementHistory','incomeStatement'),
            ('cashflowStatementHistory','cashflowStatements','cashflowStatement'),
            ('balanceSheetHistory','balanceSheetStatements','balanceSheet'),
            ('incomeStatementHistoryQuarterly','incomeStatementHistory','incomeStatement'),
            ('cashflowStatementHistoryQuarterly','cashflowStatements','cashflowStatement'),
            ('balanceSheetHistoryQuarterly','balanceSheetStatements','balanceSheet')],
            ft)
    
    def _get_all_financials(self, proxy=None):
        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}
        url = '%s/%s/%s' % (self._scrape_url, self.ticker, 'financials')
        my_headers = { 'User-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.18362' }
        req = _requests.get(url=url, headers = my_headers, proxies=proxy)
        parseTag = 'root.App.main = '
        r = req.text
        idx = r.find(parseTag)
        j = json.loads(r[idx:].split('\n')[0][len(parseTag):][:-1])
        #dct = j['context']['dispatcher']['stores']['StreamDataStore']['quoteData'][StockName] #contains Ticker.info but less complete and with a UUID
        #inf = {x:(dct[x]['raw'] if type(dct[x]) is dict else dct[x]) for x in dct}
        q = j['context']['dispatcher']['stores']['QuoteSummaryStore']
        (fncls, ft) = self._get_financial_translator(re.search('https://s\.yimg\.com/uc/finance/dd-site/js/vendor\..*?\.min\.js', req.text)[0], proxy)
        strings = j['context']['dispatcher']['stores']['LangStore']['baseLangs']['td-app-finance']
        dfs = []
        for (nm, sbnm, knm) in fncls:
            print((nm, sbnm, knm))
            df = [[strings[x[1]] if not x[2] else ''] + [((q[nm][sbnm][n][x[0]]['raw'] if not x[2] else datetime.fromtimestamp(q[nm][sbnm][n][x[0]]['raw'])) if 'raw' in q[nm][sbnm][n][x[0]] else '') if x[0] in q[nm][sbnm][n] else '-' for n in range(len(q[nm][sbnm]))] for x in ft[knm]]
            df = pd.DataFrame(df[1:], None, df[0])
            df.set_index('', inplace=True)
            for col in df.columns: df[col] = np.where(df[col] == '', np.nan, df[col])
            dfs.append(df)
        return dfs    
    
    def get_financials(self, proxy=None):
        if self._financials is None:
            self._financials = self._get_fundamentals(
                'financials', proxy)
        return self._financials

    def get_balance_sheet(self, proxy=None):
        if self._balance_sheet is None:
            self._balance_sheet = self._get_fundamentals(
                'balance-sheet', proxy)
        return self._balance_sheet

    def get_cashflow(self, proxy=None):
        if self._cashflow is None:
            self._cashflow = self._get_fundamentals(
                'cash-flow', proxy)
        return self._cashflow
    
    def get_all_financials(self, proxy=None):
        if self._all_financials is None:
            self._all_financials = self._get_all_financials(proxy)
            self._financials = self._all_financials[0]
            self._cashflow = self._all_financials[1]
            self._balance_sheet = self._all_financials[2]
        return self._all_financials

    def get_sustainability(self, proxy=None):
        if self._sustainability is None:
            self._sustainability = self._get_fundamentals(
                'sustainability', proxy)
        return self._sustainability

    # ------------------------

    @property
    def dividends(self):
        return self.get_dividends()

    @property
    def splits(self):
        return self.get_splits()

    @property
    def actions(self):
        return self.get_actions()

    @property
    def financials(self):
        return self.get_financials()

    @property
    def balance_sheet(self):
        return self.get_balance_sheet()

    @property
    def balancesheet(self):
        return self.get_balance_sheet()

    @property
    def cashflow(self):
        return self.get_cashflow()
    
    @property
    def all_financials(self):
        return self.get_all_financials()

    @property
    def sustainability(self):
        return self.get_sustainability()


@_multitasking.task
def _download_one_threaded(ticker, start=None, end=None, auto_adjust=False,
                           actions=False, progress=True, period="max",
                           interval="1d", prepost=False, proxy=None, rounding=True):

    global _PROGRESS_BAR, _DFS
    data = _download_one(ticker, start, end, auto_adjust, actions,
                         period, interval, prepost, proxy, rounding)
    _DFS[ticker.upper()] = data
    if progress:
        _PROGRESS_BAR.animate()


def _download_one(ticker, start=None, end=None, auto_adjust=False,
                  actions=False, period="max", interval="1d",
                  prepost=False, proxy=None, rounding=True):

    return Ticker(ticker).history(period=period, interval=interval,
                                  start=start, end=end, prepost=prepost,
                                  actions=actions, auto_adjust=auto_adjust,
                                  proxy=proxy, rounding=rounding)


def download(tickers, start=None, end=None, actions=False, threads=True,
             group_by='column', auto_adjust=False, progress=True,
             period="max", interval="1d", prepost=False, proxy=None,
             rounding=True,
             **kwargs):
    """Download yahoo tickers
    :Parameters:
        tickers : str, list
            List of tickers to download
        period : str
            Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
            Either Use period parameter or use start and end
        interval : str
            Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
            Intraday data cannot extend last 60 days
        start: str
            Download start date string (YYYY-MM-DD) or _datetime.
            Default is 1900-01-01
        end: str
            Download end date string (YYYY-MM-DD) or _datetime.
            Default is now
        group_by : str
            Group by 'ticker' or 'column' (default)
        prepost : bool
            Include Pre and Post market data in results?
            Default is False
        auto_adjust: bool
            Adjust all OHLC automatically? Default is False
        actions: bool
            Download dividend + stock splits data. Default is False
        threads: bool / int
            How many threads to use for mass downloading. Default is True
        proxy: str
            Optional. Proxy server URL scheme. Default is None
        rounding: bool
            Optional. Whether to round the retrieved values to the precision suggested by Yahoo.
    """
    global _PROGRESS_BAR, _DFS

    # create ticker list
    tickers = tickers if isinstance(
        tickers, list) else tickers.replace(',', ' ').split()
    tickers = list(set([ticker.upper() for ticker in tickers]))

    if progress:
        _PROGRESS_BAR = _ProgressBar(len(tickers), 'downloaded')

    # reset _DFS
    _DFS = {}

    # download using threads
    if threads:
        if threads is True:
            threads = min([len(tickers), _multitasking.cpu_count() * 2])
        _multitasking.set_max_threads(threads)
        for i, ticker in enumerate(tickers):
            _download_one_threaded(ticker, period=period, interval=interval,
                                   start=start, end=end, prepost=prepost,
                                   actions=actions, auto_adjust=auto_adjust,
                                   progress=(progress and i > 0), proxy=proxy,
                                   rounding=rounding)
        while len(_DFS) < len(tickers):
            _time.sleep(0.01)

    # download synchronously
    else:
        for i, ticker in enumerate(tickers):
            data = _download_one(ticker, period=period, interval=interval,
                                 start=start, end=end, prepost=prepost,
                                 actions=actions, auto_adjust=auto_adjust,
                                 rounding=rounding)
            _DFS[ticker.upper()] = data
            if progress:
                _PROGRESS_BAR.animate()

    if progress:
        _PROGRESS_BAR.completed()

    if len(tickers) == 1:
        return _DFS[tickers[0]]

    try:
        data = _pd.concat(_DFS.values(), axis=1, keys=_DFS.keys())
    except Exception:
        _realign_dfs()
        data = _pd.concat(_DFS.values(), axis=1, keys=_DFS.keys())

    if group_by == 'column':
        data.columns = data.columns.swaplevel(0, 1)
        data.sort_index(level=0, axis=1, inplace=True)

    return data


def _realign_dfs():
    idx_len = 0
    idx = None

    for df in _DFS.values():
        if len(df) > idx_len:
            idx_len = len(df)
            idx = df.index

    for key in _DFS.keys():
        try:
            _DFS[key] = _pd.DataFrame(
                index=idx, data=_DFS[key]).drop_duplicates()
        except Exception:
            _DFS[key] = _pd.concat(
                [_emptydf(idx), _DFS[key].dropna()], axis=0, sort=True)

        # remove duplicate index
        _DFS[key] = _DFS[key].loc[~_DFS[key].index.duplicated(keep='last')]


def _emptydf(index=[]):
    empty = _pd.DataFrame(index=index, data={
        'Open': _np.nan, 'High': _np.nan, 'Low': _np.nan,
        'Close': _np.nan, 'Adj Close': _np.nan, 'Volume': _np.nan})
    empty.index.name = 'Date'
    return empty


# make pandas datareader optional
# otherwise can be called via fix_yahoo_finance.download(...)
def pdr_override():
    try:
        import pandas_datareader
        pandas_datareader.data.get_data_yahoo = download
        pandas_datareader.data.get_data_yahoo_actions = download
        pandas_datareader.data.DataReader = download
    except Exception:
        pass


class _ProgressBar:
    def __init__(self, iterations, text='completed'):
        self.text = text
        self.iterations = iterations
        self.prog_bar = '[]'
        self.fill_char = '*'
        self.width = 50
        self.__update_amount(0)
        self.elapsed = 1

    def completed(self):
        if self.elapsed > self.iterations:
            self.elapsed = self.iterations
        self.update_iteration(1)
        print('\r' + str(self), end='')
        _sys.stdout.flush()
        print()

    def animate(self, iteration=None):
        if iteration is None:
            self.elapsed += 1
            iteration = self.elapsed
        else:
            self.elapsed += iteration

        print('\r' + str(self), end='')
        _sys.stdout.flush()
        self.update_iteration()

    def update_iteration(self, val=None):
        val = val if val is not None else self.elapsed / float(self.iterations)
        self.__update_amount(val * 100.0)
        self.prog_bar += '  %s of %s %s' % (
            self.elapsed, self.iterations, self.text)

    def __update_amount(self, new_amount):
        percent_done = int(round((new_amount / 100.0) * 100.0))
        all_full = self.width - 2
        num_hashes = int(round((percent_done / 100.0) * all_full))
        self.prog_bar = '[' + self.fill_char * \
            num_hashes + ' ' * (all_full - num_hashes) + ']'
        pct_place = (len(self.prog_bar) // 2) - len(str(percent_done))
        pct_string = '%d%%' % percent_done
        self.prog_bar = self.prog_bar[0:pct_place] + \
            (pct_string + self.prog_bar[pct_place + len(pct_string):])

    def __str__(self):
        return str(self.prog_bar)

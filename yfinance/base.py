#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# yfinance - market data downloader
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

import time as _time
import datetime as _datetime
import pytz as _tz
import requests as _requests
import pandas as _pd
import numpy as _np
import re as _re
from pprint import pprint

try:
    from urllib.parse import quote as urlencode
except ImportError:
    from urllib import quote as urlencode

from . import utils

import json as _json
# import re as _re
# import sys as _sys

from . import shared

_BASE_URL_ = 'https://query2.finance.yahoo.com'
_SCRAPE_URL_ = 'https://finance.yahoo.com/quote'
_ROOT_URL_ = 'https://finance.yahoo.com'

_DEV = False
# _DEV = True


class TickerBase():
    def __init__(self, ticker, session=None):
        self.ticker = ticker.upper()
        self.session = session
        self._history = None
        self._base_url = _BASE_URL_
        self._scrape_url = _SCRAPE_URL_
        self._tz = None

        self._fundamentals = False
        self._info = None
        self._analysis = None
        self._sustainability = None
        self._recommendations = None
        self._major_holders = None
        self._institutional_holders = None
        self._mutualfund_holders = None
        self._isin = None
        self._news = []
        self._shares = None

        self._calendar = None
        self._expirations = {}
        self._earnings_dates = None
        self._earnings_history = None

        self._earnings = None
        self._income_stmt = None
        self._balance_sheet = None
        self._cashflow = None

        # accept isin as ticker
        if utils.is_isin(self.ticker):
            self.ticker = utils.get_ticker_by_isin(self.ticker, None, session)

    def stats(self, proxy=None):
        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        if self._fundamentals:
            return

        ticker_url = "{}/{}".format(self._scrape_url, self.ticker)

        # get info and sustainability
        data = utils.get_json(ticker_url, proxy, self.session)
        return data

    def history(self, period="1mo", interval="1d",
                start=None, end=None, prepost=False, actions=True,
                auto_adjust=True, back_adjust=False, keepna=False,
                proxy=None, rounding=False, timeout=None, **kwargs):
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
            back_adjust: bool
                Back-adjusted data to mimic true historical prices
            keepna: bool
                Keep NaN rows returned by Yahoo?
                Default is False
            proxy: str
                Optional. Proxy server URL scheme. Default is None
            rounding: bool
                Round values to 2 decimal places?
                Optional. Default is False = precision suggested by Yahoo!
            timeout: None or float
                If not None stops waiting for a response after given number of
                seconds. (Can also be a fraction of a second e.g. 0.01)
                Default is None.
            **kwargs: dict
                debug: bool
                    Optional. If passed as False, will suppress
                    error message printing to console.
        """

        # Work with errors
        debug_mode = True
        if "debug" in kwargs and isinstance(kwargs["debug"], bool):
            debug_mode = kwargs["debug"]

        err_msg = "No data found for this date range, symbol may be delisted"

        if start or period is None or period.lower() == "max":
            # Check can get TZ. Fail => probably delisted
            try:
                tz = self._get_ticker_tz()
            except KeyError as e:
                if "exchangeTimezoneName" in str(e):
                    shared._DFS[self.ticker] = utils.empty_df()
                    shared._ERRORS[self.ticker] = err_msg
                    if "many" not in kwargs and debug_mode:
                        print('- %s: %s' % (self.ticker, err_msg))
                    return utils.empty_df()
                else:
                    raise

            if end is None:
                end = int(_time.time())
            else:
                end = utils._parse_user_dt(end, tz)
            if start is None:
                if interval == "1m":
                    start = end - 604800  # Subtract 7 days
                else:
                    start = -631159200
            else:
                start = utils._parse_user_dt(start, tz)
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

        session = self.session or _requests
        data = None

        try:
            data = session.get(
                url=url,
                params=params,
                proxies=proxy,
                headers=utils.user_agent_headers,
                timeout=timeout
            )
            if "Will be right back" in data.text or data is None:
                raise RuntimeError("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\n"
                                   "Our engineers are working quickly to resolve "
                                   "the issue. Thank you for your patience.")

            data = data.json()
        except Exception:
            pass

        if data is None or not type(data) is dict or 'status_code' in data.keys():
            shared._DFS[self.ticker] = utils.empty_df()
            shared._ERRORS[self.ticker] = err_msg
            if "many" not in kwargs and debug_mode:
                print('- %s: %s' % (self.ticker, err_msg))
            return utils.empty_df()

        if "chart" in data and data["chart"]["error"]:
            err_msg = data["chart"]["error"]["description"]
            shared._DFS[self.ticker] = utils.empty_df()
            shared._ERRORS[self.ticker] = err_msg
            if "many" not in kwargs and debug_mode:
                print('- %s: %s' % (self.ticker, err_msg))
            return shared._DFS[self.ticker]

        elif "chart" not in data or data["chart"]["result"] is None or \
                not data["chart"]["result"]:
            shared._DFS[self.ticker] = utils.empty_df()
            shared._ERRORS[self.ticker] = err_msg
            if "many" not in kwargs and debug_mode:
                print('- %s: %s' % (self.ticker, err_msg))
            return shared._DFS[self.ticker]

        # parse quotes
        try:
            quotes = utils.parse_quotes(data["chart"]["result"][0])
            # Yahoo bug fix - it often appends latest price even if after end date
            if end and not quotes.empty:
                endDt = _pd.to_datetime(_datetime.datetime.utcfromtimestamp(end))
                if quotes.index[quotes.shape[0]-1] >= endDt:
                    quotes = quotes.iloc[0:quotes.shape[0]-1]
        except Exception:
            shared._DFS[self.ticker] = utils.empty_df()
            shared._ERRORS[self.ticker] = err_msg
            if "many" not in kwargs and debug_mode:
                print('- %s: %s' % (self.ticker, err_msg))
            return shared._DFS[self.ticker]

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

        try:
            if auto_adjust:
                quotes = utils.auto_adjust(quotes)
            elif back_adjust:
                quotes = utils.back_adjust(quotes)
        except Exception as e:
            if auto_adjust:
                err_msg = "auto_adjust failed with %s" % e
            else:
                err_msg = "back_adjust failed with %s" % e
            shared._DFS[self.ticker] = utils.empty_df()
            shared._ERRORS[self.ticker] = err_msg
            if "many" not in kwargs and debug_mode:
                print('- %s: %s' % (self.ticker, err_msg))

        if rounding:
            quotes = _np.round(quotes, data[
                "chart"]["result"][0]["meta"]["priceHint"])
        quotes['Volume'] = quotes['Volume'].fillna(0).astype(_np.int64)

        if not keepna:
            quotes.dropna(inplace=True)

        # actions
        dividends, splits = utils.parse_actions(data["chart"]["result"][0])

        tz_exchange = data["chart"]["result"][0]["meta"]["exchangeTimezoneName"]

        # combine
        df = _pd.concat([quotes, dividends, splits], axis=1, sort=True)
        df["Dividends"].fillna(0, inplace=True)
        df["Stock Splits"].fillna(0, inplace=True)

        # index eod/intraday
        df.index = df.index.tz_localize("UTC").tz_convert(tz_exchange)

        df = utils.fix_Yahoo_dst_issue(df, params["interval"])
            
        if params["interval"][-1] == "m":
            df.index.name = "Datetime"
        elif params["interval"] == "1h":
            pass
        else:
            df.index = _pd.to_datetime(df.index.date).tz_localize(tz_exchange)
            df.index.name = "Date"

        # duplicates and missing rows cleanup
        df.dropna(how='all', inplace=True)
        df = df[~df.index.duplicated(keep='first')]

        self._history = df.copy()

        if not actions:
            df.drop(columns=["Dividends", "Stock Splits"], inplace=True)

        return df

    # ------------------------

    def _get_ticker_tz(self):
        if not self._tz is None:
            return self._tz

        tkr_tz = utils.cache_lookup_tkr_tz(self.ticker)
        if tkr_tz is None:
            tkr_tz = self.info["exchangeTimezoneName"]
            # info fetch is relatively slow so cache timezone
            utils.cache_store_tkr_tz(self.ticker, tkr_tz)

        self._tz = tkr_tz
        return tkr_tz

    def _get_info(self, proxy=None):
        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        if (self._info is None) or (self._sustainability is None) or (self._recommendations is None):
            ## Need to fetch
            pass
        else:
            return

        ticker_url = "{}/{}".format(self._scrape_url, self.ticker)

        # get info and sustainability
        data = utils.get_json(ticker_url, proxy, self.session)
        data = data['context']['dispatcher']['stores']['QuoteSummaryStore']

        # sustainability
        d = {}
        try:
            if isinstance(data.get('esgScores'), dict):
                for item in data['esgScores']:
                    if not isinstance(data['esgScores'][item], (dict, list)):
                        d[item] = data['esgScores'][item]

                s = _pd.DataFrame(index=[0], data=d)[-1:].T
                s.columns = ['Value']
                s.index.name = '%.f-%.f' % (
                    s[s.index == 'ratingYear']['Value'].values[0],
                    s[s.index == 'ratingMonth']['Value'].values[0])

                self._sustainability = s[~s.index.isin(
                    ['maxAge', 'ratingYear', 'ratingMonth'])]
        except Exception:
            pass

        # info (be nice to python 2)
        self._info = {}
        try:
            items = ['summaryProfile', 'financialData', 'quoteType',
                     'defaultKeyStatistics', 'assetProfile', 'summaryDetail']
            for item in items:
                if isinstance(data.get(item), dict):
                    self._info.update(data[item])
        except Exception:
            pass

        # For ETFs, provide this valuable data: the top holdings of the ETF
        try:
            if 'topHoldings' in data:
                self._info.update(data['topHoldings'])
        except Exception:
            pass

        try:
            if not isinstance(data.get('summaryDetail'), dict):
                # For some reason summaryDetail did not give any results. The price dict usually has most of the same info
                self._info.update(data.get('price', {}))
        except Exception:
            pass

        try:
            # self._info['regularMarketPrice'] = self._info['regularMarketOpen']
            self._info['regularMarketPrice'] = data.get('price', {}).get(
                'regularMarketPrice', self._info.get('regularMarketOpen', None))
        except Exception:
            pass

        try:
            self._info['preMarketPrice'] = data.get('price', {}).get(
                'preMarketPrice', self._info.get('preMarketPrice', None))
        except Exception:
            pass

        self._info['logo_url'] = ""
        try:
            domain = self._info['website'].split(
                '://')[1].split('/')[0].replace('www.', '')
            self._info['logo_url'] = 'https://logo.clearbit.com/%s' % domain
        except Exception:
            pass

        # events
        try:
            cal = _pd.DataFrame(
                data['calendarEvents']['earnings'])
            cal['earningsDate'] = _pd.to_datetime(
                cal['earningsDate'], unit='s')
            self._calendar = cal.T
            self._calendar.index = utils.camel2title(self._calendar.index)
            self._calendar.columns = ['Value']
        except Exception:
            pass

        # analyst recommendations
        try:
            rec = _pd.DataFrame(
                data['upgradeDowngradeHistory']['history'])
            rec['earningsDate'] = _pd.to_datetime(
                rec['epochGradeDate'], unit='s')
            rec.set_index('earningsDate', inplace=True)
            rec.index.name = 'Date'
            rec.columns = utils.camel2title(rec.columns)
            self._recommendations = rec[[
                'Firm', 'To Grade', 'From Grade', 'Action']].sort_index()
        except Exception:
            pass

    def _get_fundamentals(self, proxy=None):
        def cleanup(data):
            '''
            The cleanup function is used for parsing yahoo finance json financial statement data into a pandas dataframe format.
            '''
            df = _pd.DataFrame(data).drop(columns=['maxAge'])
            for col in df.columns:
                df[col] = _np.where(
                    df[col].astype(str) == '-', _np.nan, df[col])

            df.set_index('endDate', inplace=True)
            try:
                df.index = _pd.to_datetime(df.index, unit='s')
            except ValueError:
                df.index = _pd.to_datetime(df.index)
            df = df.T
            df.columns.name = ''
            df.index.name = 'Breakdown'

            # rename incorrect yahoo key
            df.rename(index={'treasuryStock': 'Gains Losses Not Affecting Retained Earnings'}, inplace=True)

            df.index = utils.camel2title(df.index)
            return df

        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        if self._fundamentals:
            return

        ticker_url = "{}/{}".format(self._scrape_url, self.ticker)

        # holders
        try:
            resp = utils.get_html(ticker_url + '/holders', proxy, self.session)
            holders = _pd.read_html(resp)
        except Exception:
            holders = []

        if len(holders) >= 3:
            self._major_holders = holders[0]
            self._institutional_holders = holders[1]
            self._mutualfund_holders = holders[2]
        elif len(holders) >= 2:
            self._major_holders = holders[0]
            self._institutional_holders = holders[1]
        elif len(holders) >= 1:
            self._major_holders = holders[0]

        # self._major_holders = holders[0]
        # self._institutional_holders = holders[1]

        if self._institutional_holders is not None:
            if 'Date Reported' in self._institutional_holders:
                self._institutional_holders['Date Reported'] = _pd.to_datetime(
                    self._institutional_holders['Date Reported'])
            if '% Out' in self._institutional_holders:
                self._institutional_holders['% Out'] = self._institutional_holders[
                    '% Out'].str.replace('%', '').astype(float) / 100

        if self._mutualfund_holders is not None:
            if 'Date Reported' in self._mutualfund_holders:
                self._mutualfund_holders['Date Reported'] = _pd.to_datetime(
                    self._mutualfund_holders['Date Reported'])
            if '% Out' in self._mutualfund_holders:
                self._mutualfund_holders['% Out'] = self._mutualfund_holders[
                    '% Out'].str.replace('%', '').astype(float) / 100

        self._get_info(proxy)

        # get fundamentals
        financials_data = utils.get_json(ticker_url + '/financials', proxy, self.session)
        data = financials_data['context']['dispatcher']['stores']['QuoteSummaryStore']

        # generic patterns
        self._earnings = {"yearly": utils.empty_df(), "quarterly": utils.empty_df()}
        self._cashflow = {"yearly": utils.empty_df(), "quarterly": utils.empty_df()}
        self._balance_sheet = {"yearly": utils.empty_df(), "quarterly": utils.empty_df()}
        self._income = {"yearly": utils.empty_df(), "quarterly": utils.empty_df()}
        for key in (
            (self._cashflow, 'cashflowStatement', 'cashflowStatements'),
            (self._balance_sheet, 'balanceSheet', 'balanceSheetStatements'),
            (self._income, 'incomeStatement', 'incomeStatementHistory')
        ):
            item = key[1] + 'History'
            if isinstance(data.get(item), dict):
                try:
                    # key[0]['yearly'] = cleanup(data[item][key[2]])
                    key[0]['yearly_legacy'] = cleanup(data[item][key[2]])
                except Exception:
                    pass

            item = key[1] + 'HistoryQuarterly'
            if isinstance(data.get(item), dict):
                try:
                    key[0]['quarterly'] = cleanup(data[item][key[2]])
                except Exception:
                    pass

        # earnings
        if isinstance(data.get('earnings'), dict):
            try:
                earnings = data['earnings']['financialsChart']
                earnings['financialCurrency'] = 'USD' if 'financialCurrency' not in data['earnings'] else data['earnings']['financialCurrency']
                self._earnings['financialCurrency'] = earnings['financialCurrency']
                df = _pd.DataFrame(earnings['yearly']).set_index('date')
                df.columns = utils.camel2title(df.columns)
                df.index.name = 'Year'
                self._earnings['yearly'] = df

                df = _pd.DataFrame(earnings['quarterly']).set_index('date')
                df.columns = utils.camel2title(df.columns)
                df.index.name = 'Quarter'
                self._earnings['quarterly'] = df
            except Exception:
                pass

        if _DEV:
            # Fetching using get_financials_time_series() missing TTM column so cannot completely replace git-shogg's method
            annual_income_stmt_ts = utils.get_financials_time_series(self.ticker, "financials", "annual", ticker_url, proxy, self.session)
            annual_balance_sheet_ts = utils.get_financials_time_series(self.ticker, "balance-sheet", "annual", ticker_url, proxy, self.session)
            annual_cashflow_ts = utils.get_financials_time_series(self.ticker, "cash-flow", "annual", ticker_url, proxy, self.session)

            qtr_income_stmt_ts = utils.get_financials_time_series(self.ticker, "financials", "quarterly", ticker_url, proxy, self.session)
            qtr_balance_sheet_ts = utils.get_financials_time_series(self.ticker, "balance-sheet", "quarterly", ticker_url, proxy, self.session)
            qtr_cashflow_ts = utils.get_financials_time_series(self.ticker, "cash-flow", "quarterly", ticker_url, proxy, self.session)

        #------------------ Income Statement ------------------ 
        # try:
        # Grab the financial template store. This details the order in which the financials should be presented.
        data = financials_data['context']['dispatcher']['stores']['FinancialTemplateStore']
        financials_template_ttm_order, financials_template_annual_order, financials_level_detail = utils.build_template(data)

        # Grab the raw financial details (this can be later combined with the financial template store detail to correctly order and present the data).
        data = financials_data['context']['dispatcher']['stores']['QuoteTimeSeriesStore']
        TTM_dicts, Annual_dicts = utils.retreive_financial_details(data)

        TTM = _pd.DataFrame.from_dict(TTM_dicts).set_index("index")
        Annual = _pd.DataFrame.from_dict(Annual_dicts).set_index("index")
        # Combine the raw financial details and the template
        TTM = TTM.reindex(financials_template_ttm_order)
        Annual = Annual.reindex(financials_template_annual_order)

        # Add 'TTM' prefix to all column names, so if combined we can tell the difference between actuals and TTM (similar to yahoo finance).
        TTM.columns = ['TTM ' + str(col) for col in TTM.columns]
        TTM.index = TTM.index.str.replace(r'trailing', '')
        Annual.index = Annual.index.str.replace(r'annual','')
        _income_statement = Annual.merge(TTM, left_index=True, right_index=True)

        if _DEV:
            annual_scraped_keys = _income_statement.dropna(how="all").index.values

            # First, compare not-nan rows against get_financials_time_series(), are same rows returned?
            annual_ts_keys = annual_income_stmt_ts.index.values
            missing_from_ts = set(annual_scraped_keys)-set(annual_ts_keys)
            missing_from_scraped = set(annual_ts_keys)-set(annual_scraped_keys)
            if len(missing_from_ts)>0:
                print("") ; print("")
                print("WARNING: These scraped keys missing from time-series annual income-stmt:")
                print(_income_statement[_income_statement.index.isin(missing_from_ts)][sorted(_income_statement.columns, reverse=True)])
            if len(missing_from_scraped)>0:
                print("") ; print("")
                print("WARNING: These time-series keys missing from scraped annual income-stmt. Normally they are present in Yahoo.com so why missing here?")
                print(annual_income_stmt_ts[annual_income_stmt_ts.index.isin(missing_from_scraped)])

            # Next, compare time-series quarterly against scraped annual to see if same MultiIndex can be used
            qtr_ts_keys = qtr_income_stmt_ts.index.values
            missing_from_annual = set(qtr_ts_keys)-set(annual_scraped_keys)
            # Remove what already know is missing
            missing_from_annual -= missing_from_scraped
            if len(missing_from_annual) > 0:
                print("WARNING: These keys in time-series quarterly income-stmt are MISSING from scraped annual, complicates table construction")
                print(qtr_income_stmt_ts[qtr_income_stmt_ts.index.isin(missing_from_annual)])
            else:
                print("GREAT: Should be easy to fix quarterly income-stmt, just copy MultiIndex from annual (once you fix missing keys)")

        _income_statement.index = utils.camel2title(_income_statement.T)
        _income_statement['level_detail'] = financials_level_detail 
        _income_statement = _income_statement.set_index([_income_statement.index,'level_detail'])
        _income_statement = _income_statement[sorted(_income_statement.columns, reverse=True)]
        _income_statement = _income_statement.dropna(how='all')
        self._income["yearly"] = _income_statement
        # except Exception as e:
        #     self._income["yearly"] = _pd.DataFrame()

        #------------------ Balance Sheet ------------------ 
        # try:
        balance_sheet_data = utils.get_json(ticker_url+'/balance-sheet', proxy, self.session)
        data = balance_sheet_data['context']['dispatcher']['stores']['FinancialTemplateStore']
        balance_sheet_template_ttm_order, balance_sheet_template_annual_order, balance_sheet_level_detail = utils.build_template(data)

        data = balance_sheet_data['context']['dispatcher']['stores']['QuoteTimeSeriesStore']
        TTM_dicts, Annual_dicts = utils.retreive_financial_details(data)

        Annual = _pd.DataFrame.from_dict(Annual_dicts).set_index("index")
        Annual = Annual.reindex(balance_sheet_template_annual_order)
        Annual.index = Annual.index.str.replace(r'annual','')

        if _DEV:
            annual_scraped_keys = Annual.dropna(how="all").index.values

            # First, compare not-nan rows against get_financials_time_series(), are same rows returned?
            annual_ts_keys = annual_balance_sheet_ts.index.values
            missing_from_ts = set(annual_scraped_keys)-set(annual_ts_keys)
            missing_from_scraped = set(annual_ts_keys)-set(annual_scraped_keys)
            if len(missing_from_ts)>0:
                print("") ; print("")
                print("WARNING: These scraped keys missing from time-series annual balance-sheet:")
                print(Annual[Annual.index.isin(missing_from_ts)][sorted(Annual.columns, reverse=True)])
            if len(missing_from_scraped)>0:
                print("") ; print("")
                print("WARNING: These time-series keys missing from scraped annual balance-sheet. Normally they are present in Yahoo.com so why missing here?")
                print(annual_balance_sheet_ts[annual_balance_sheet_ts.index.isin(missing_from_scraped)])

            # Next, compare time-series quarterly against scraped annual to see if same MultiIndex can be used
            qtr_ts_keys = qtr_balance_sheet_ts.index.values
            missing_from_annual = set(qtr_ts_keys)-set(annual_scraped_keys)
            # Remove what already know is missing
            missing_from_annual -= missing_from_scraped
            if len(missing_from_annual) > 0:
                print("") ; print("")
                print("WARNING: These keys in time-series quarterly balance-sheet are MISSING from scraped annual, complicates table construction")
                print(qtr_balance_sheet_ts[qtr_balance_sheet_ts.index.isin(missing_from_annual)])
            else:
                print("") ; print("")
                print("GREAT: Should be easy to fix quarterly balance-sheet, just copy MultiIndex from annual (once you fix missing keys)")

        Annual.index = utils.camel2title(Annual.T)
        _balance_sheet = Annual
        _balance_sheet['level_detail'] = balance_sheet_level_detail 
        _balance_sheet = _balance_sheet.set_index([_balance_sheet.index,'level_detail'])
        _balance_sheet = _balance_sheet[sorted(_balance_sheet.columns, reverse=True)]
        self._balance_sheet["yearly"] = _balance_sheet.dropna(how='all')
        # except Exception as e:
        #     self._balance_sheet["yearly"] = _pd.DataFrame()

        #------------------ Cash Flow Statement ------------------ 
        # try:
        cash_flow_data = utils.get_json(ticker_url+'/cash-flow', proxy, self.session)
        data = cash_flow_data['context']['dispatcher']['stores']['FinancialTemplateStore']   # Grab the financial template store. This  details the order in which the financials should be presented.
        cash_flow_template_ttm_order, cash_flow_template_annual_order, cash_flow_level_detail = utils.build_template(data)

        data = cash_flow_data['context']['dispatcher']['stores']['QuoteTimeSeriesStore'] # Grab the raw financial details (this can be later combined with the financial template store detail to correctly order and present the data).
        TTM_dicts, Annual_dicts = utils.retreive_financial_details(data)

        TTM = _pd.DataFrame.from_dict(TTM_dicts).set_index("index")
        Annual = _pd.DataFrame.from_dict(Annual_dicts).set_index("index")
        # Combine the raw financial details and the template
        TTM = TTM.reindex(cash_flow_template_ttm_order)
        Annual = Annual.reindex(cash_flow_template_annual_order)
        TTM.columns = ['TTM ' + str(col) for col in TTM.columns] # Add 'TTM' prefix to all column names, so if combined we can tell the difference between actuals and TTM (similar to yahoo finance).
        TTM.index = TTM.index.str.replace(r'trailing', '')
        Annual.index = Annual.index.str.replace(r'annual','')
        _cash_flow_stmt = Annual.merge(TTM, left_index=True, right_index=True)

        if _DEV:
            annual_scraped_keys = _cash_flow_stmt.dropna(how="all").index.values

            # First, compare not-nan rows against get_financials_time_series(), are same rows returned?
            annual_ts_keys = annual_cashflow_ts.index.values
            missing_from_ts = set(annual_scraped_keys)-set(annual_ts_keys)
            missing_from_scraped = set(annual_ts_keys)-set(annual_scraped_keys)
            if len(missing_from_ts)>0:
                print("") ; print("")
                print("WARNING: These scraped keys missing from time-series annual cash-flow:")
                print(_cash_flow_stmt[_cash_flow_stmt.index.isin(missing_from_ts)][sorted(_cash_flow_stmt.columns, reverse=True)])
            if len(missing_from_scraped)>0:
                print("") ; print("")
                print("WARNING: These time-series keys missing from scraped annual cash-flow. Normally they are present in Yahoo.com so why missing here?")
                print(annual_cashflow_ts[annual_cashflow_ts.index.isin(missing_from_scraped)])

            # Next, compare time-series quarterly against scraped annual to see if same MultiIndex can be used
            qtr_ts_keys = qtr_cashflow_ts.index.values
            missing_from_annual = set(qtr_ts_keys)-set(annual_scraped_keys)
            # Remove what already know is missing
            missing_from_annual -= missing_from_scraped
            if len(missing_from_annual) > 0:
                print("") ; print("")
                print("WARNING: These keys in time-series quarterly cash-flow are MISSING from scraped annual, complicates table construction")
                print(qtr_cashflow_ts[qtr_cashflow_ts.index.isin(missing_from_annual)])
            else:
                print("") ; print("")
                print("GREAT: Should be easy to fix quarterly cash-flow, just copy MultiIndex from annual (once you fix missing keys)")

        _cash_flow_stmt.index = utils.camel2title(_cash_flow_stmt.T)
        _cash_flow_stmt['level_detail'] = cash_flow_level_detail 
        _cash_flow_stmt = _cash_flow_stmt.set_index([_cash_flow_stmt.index,'level_detail'])
        _cash_flow_stmt = _cash_flow_stmt[sorted(_cash_flow_stmt.columns, reverse=True)]
        self._cashflow["yearly"] = _cash_flow_stmt.dropna(how='all')
        # except Exception as e:
        #     self._cashflow["yearly"] = _pd.DataFrame()

        #------------------ Analysis Data/Analyst Forecasts ------------------
        try:
            analysis_data = utils.get_json(ticker_url+'/analysis',proxy,self.session)
            analysis_data = analysis_data['context']['dispatcher']['stores']['QuoteSummaryStore']        
        except Exception as e:
            analysis_data = {}
        try:
            self._analyst_trend_details = _pd.DataFrame(analysis_data['recommendationTrend']['trend'])
        except Exception as e:
            self._analyst_trend_details = _pd.DataFrame()
        try:
            self._analyst_price_target = _pd.DataFrame(analysis_data['financialData'], index=[0])[['targetLowPrice','currentPrice','targetMeanPrice','targetHighPrice','numberOfAnalystOpinions']].T
        except Exception as e:
            self._analyst_price_target = _pd.DataFrame()
        earnings_estimate = []
        revenue_estimate = []
        if len(self._analyst_trend_details) != 0:
            for key in analysis_data['earningsTrend']['trend']:
                try:
                    earnings_dict = key['earningsEstimate']
                    earnings_dict['period'] = key['period']
                    earnings_dict['endDate'] = key['endDate']
                    earnings_estimate.append(earnings_dict)

                    revenue_dict = key['revenueEstimate']
                    revenue_dict['period'] = key['period']
                    revenue_dict['endDate'] = key['endDate']
                    revenue_estimate.append(revenue_dict)
                except Exception as e:
                    pass
            self._rev_est = _pd.DataFrame(revenue_estimate)
            self._eps_est = _pd.DataFrame(earnings_estimate)
        else:
            self._rev_est = _pd.DataFrame()
            self._eps_est = _pd.DataFrame()

        # shares outstanding
        try:
            # keep only years with non None data
            available_shares = [shares_data for shares_data in data['annualBasicAverageShares'] if shares_data]
            shares = _pd.DataFrame(available_shares)
            shares['Year'] = shares['asOfDate'].agg(lambda x: int(x[:4]))
            shares.set_index('Year', inplace=True)
            shares.drop(columns=['dataId', 'asOfDate',
                        'periodType', 'currencyCode'], inplace=True)
            shares.rename(
                columns={'reportedValue': "BasicShares"}, inplace=True)
            self._shares = shares
        except Exception:
            pass

        # Analysis
        data = utils.get_json(ticker_url + '/analysis', proxy, self.session)

        if isinstance(data.get('earningsTrend'), dict):
            try:
                analysis = _pd.DataFrame(data['earningsTrend']['trend'])
                analysis['endDate'] = _pd.to_datetime(analysis['endDate'])
                analysis.set_index('period', inplace=True)
                analysis.index = analysis.index.str.upper()
                analysis.index.name = 'Period'
                analysis.columns = utils.camel2title(analysis.columns)

                dict_cols = []

                for idx, row in analysis.iterrows():
                    for colname, colval in row.items():
                        if isinstance(colval, dict):
                            dict_cols.append(colname)
                            for k, v in colval.items():
                                new_colname = colname + ' ' + \
                                    utils.camel2title([k])[0]
                                analysis.loc[idx, new_colname] = v

                self._analysis = analysis[[
                    c for c in analysis.columns if c not in dict_cols]]
            except Exception:
                pass

        # Complementary key-statistics (currently fetching the important trailingPegRatio which is the value shown in the website)
        res = {}
        try:
            my_headers = {'user-agent': 'curl/7.55.1', 'accept': 'application/json', 'content-type': 'application/json',
                          'referer': 'https://finance.yahoo.com/', 'cache-control': 'no-cache', 'connection': 'close'}
            p = _re.compile(r'root\.App\.main = (.*);')
            r = _requests.session().get('https://finance.yahoo.com/quote/{}/key-statistics?p={}'.format(self.ticker,
                                                                                                        self.ticker), headers=my_headers)
            q_results = {}
            my_qs_keys = ['pegRatio']  # QuoteSummaryStore
            # , 'quarterlyPegRatio']  # QuoteTimeSeriesStore
            my_ts_keys = ['trailingPegRatio']

            # Complementary key-statistics
            data = _json.loads(p.findall(r.text)[0])
            key_stats = data['context']['dispatcher']['stores']['QuoteTimeSeriesStore']
            q_results.setdefault(self.ticker, [])
            for i in my_ts_keys:
                # j=0
                try:
                    # res = {i: key_stats['timeSeries'][i][1]['reportedValue']['raw']}
                    # We need to loop over multiple items, if they exist: 0,1,2,..
                    zzz = key_stats['timeSeries'][i]
                    for j in range(len(zzz)):
                        if key_stats['timeSeries'][i][j]:
                            res = {i: key_stats['timeSeries']
                                   [i][j]['reportedValue']['raw']}
                            q_results[self.ticker].append(res)

                # print(res)
                # q_results[ticker].append(res)
                except:
                    q_results[ticker].append({i: np.nan})

            res = {'Company': ticker}
            q_results[ticker].append(res)
        except Exception:
            pass

        if 'trailingPegRatio' in res:
            self._info['trailingPegRatio'] = res['trailingPegRatio']

        self._fundamentals = True

    def get_recommendations(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_info(proxy)
        data = self._recommendations
        if as_dict:
            return data.to_dict()
        return data

    def get_calendar(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_info(proxy)
        data = self._calendar
        if as_dict:
            return data.to_dict()
        return data

    def get_major_holders(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_fundamentals(proxy=proxy)
        data = self._major_holders
        if as_dict:
            return data.to_dict()
        return data

    def get_institutional_holders(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_fundamentals(proxy=proxy)
        data = self._institutional_holders
        if data is not None:
            if as_dict:
                return data.to_dict()
            return data

    def get_mutualfund_holders(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_fundamentals(proxy=proxy)
        data = self._mutualfund_holders
        if data is not None:
            if as_dict:
                return data.to_dict()
            return data

    def get_info(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_info(proxy)
        data = self._info
        if as_dict:
            return data.to_dict()
        return data

    def get_sustainability(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_info(proxy)
        data = self._sustainability
        if as_dict:
            return data.to_dict()
        return data

    def get_current_recommendations(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_fundamentals(proxy=proxy)
        data = self._analyst_trend_details
        if as_dict:
            return data.to_dict()
        return data

    def get_analyst_price_target(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_fundamentals(proxy=proxy)
        data = self._analyst_price_target
        if as_dict:
            return data.to_dict()
        return data

    def get_rev_forecast(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_fundamentals(proxy=proxy)
        data = self._rev_est
        if as_dict:
            return data.to_dict()
        return data

    def get_earnings_forecast(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_fundamentals(proxy=proxy)
        data = self._eps_est
        if as_dict:
            return data.to_dict()
        return data

    def get_earnings(self, proxy=None, as_dict=False, freq="yearly"):
        self._get_fundamentals(proxy=proxy)
        data = self._earnings[freq]
        if as_dict:
            dict_data = data.to_dict()
            dict_data['financialCurrency'] = 'USD' if 'financialCurrency' not in self._earnings else self._earnings['financialCurrency']
            return dict_data
        return data

    def get_analysis(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_fundamentals(proxy=proxy)
        data = self._analysis
        if as_dict:
            return data.to_dict()
        return data

    def get_income_stmt(self, proxy=None, as_dict=False, freq="yearly"):
        self._get_fundamentals(proxy=proxy)
        data = self._income[freq]
        if as_dict:
            return data.to_dict()
        return data
    def get_income_stmt_legacy(self, proxy=None, as_dict=False, freq="yearly"):
        self._get_fundamentals(proxy=proxy)
        data = self._income[freq+"_legacy"]
        if as_dict:
            return data.to_dict()
        return data

    def get_balance_sheet(self, proxy=None, as_dict=False, freq="yearly"):
        self._get_fundamentals(proxy=proxy)
        data = self._balance_sheet[freq]
        if as_dict:
            return data.to_dict()
        return data
    def get_balance_sheet_legacy(self, proxy=None, as_dict=False, freq="yearly"):
        self._get_fundamentals(proxy=proxy)
        data = self._balance_sheet[freq+"_legacy"]
        if as_dict:
            return data.to_dict()
        return data

    def get_cashflow(self, proxy=None, as_dict=False, freq="yearly"):
        self._get_fundamentals(proxy=proxy)
        data = self._cashflow[freq]
        if as_dict:
            return data.to_dict()
        return data
    def get_cashflow_legacy(self, proxy=None, as_dict=False, freq="yearly"):
        self._get_fundamentals(proxy=proxy)
        data = self._cashflow[freq+"_legacy"]
        if as_dict:
            return data.to_dict()
        return data

    def get_dividends(self, proxy=None):
        if self._history is None:
            self.history(period="max", proxy=proxy)
        if self._history is not None and "Dividends" in self._history:
            dividends = self._history["Dividends"]
            return dividends[dividends != 0]
        return []

    def get_splits(self, proxy=None):
        if self._history is None:
            self.history(period="max", proxy=proxy)
        if self._history is not None and "Stock Splits" in self._history:
            splits = self._history["Stock Splits"]
            return splits[splits != 0]
        return []

    def get_actions(self, proxy=None):
        if self._history is None:
            self.history(period="max", proxy=proxy)
        if self._history is not None and "Dividends" in self._history and "Stock Splits" in self._history:
            actions = self._history[["Dividends", "Stock Splits"]]
            return actions[actions != 0].dropna(how='all').fillna(0)
        return []

    def get_shares(self, proxy=None, as_dict=False, *args, **kwargs):
        self._get_fundamentals(proxy=proxy)
        data = self._shares
        if as_dict:
            return data.to_dict()
        return data

    def get_isin(self, proxy=None):
        # *** experimental ***
        if self._isin is not None:
            return self._isin

        ticker = self.ticker.upper()

        if "-" in ticker or "^" in ticker:
            self._isin = '-'
            return self._isin

        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        q = ticker
        self.get_info(proxy=proxy)
        if "shortName" in self._info:
            q = self._info['shortName']

        url = 'https://markets.businessinsider.com/ajax/' \
              'SearchController_Suggest?max_results=25&query=%s' \
            % urlencode(q)
        session = self.session or _requests
        data = session.get(
            url=url,
            proxies=proxy,
            headers=utils.user_agent_headers
        ).text

        search_str = '"{}|'.format(ticker)
        if search_str not in data:
            if q.lower() in data.lower():
                search_str = '"|'
                if search_str not in data:
                    self._isin = '-'
                    return self._isin
            else:
                self._isin = '-'
                return self._isin

        self._isin = data.split(search_str)[1].split('"')[0].split('|')[0]
        return self._isin

    def get_news(self, proxy=None):
        if self._news:
            return self._news

        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        # Getting data from json
        url = "{}/v1/finance/search?q={}".format(self._base_url, self.ticker)
        session = self.session or _requests
        data = session.get(
            url=url,
            proxies=proxy,
            headers=utils.user_agent_headers
        )
        if "Will be right back" in data.text:
            raise RuntimeError("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\n"
                               "Our engineers are working quickly to resolve "
                               "the issue. Thank you for your patience.")
        data = data.json()

        # parse news
        self._news = data.get("news", [])
        return self._news

    def get_earnings_dates(self, proxy=None):
        if self._earnings_dates is not None:
            return self._earnings_dates

        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        page_size = 100  # YF caps at 100, don't go higher
        page_offset = 0
        dates = None
        while True:
            url = "{}/calendar/earnings?symbol={}&offset={}&size={}".format(
                _ROOT_URL_, self.ticker, page_offset, page_size)

            session = self.session or _requests
            data = session.get(
                url=url,
                proxies=proxy,
                headers=utils.user_agent_headers
            ).text

            if "Will be right back" in data:
                raise RuntimeError("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\n"
                                   "Our engineers are working quickly to resolve "
                                   "the issue. Thank you for your patience.")

            try:
                data = _pd.read_html(data)[0]
            except ValueError:
                if page_offset == 0:
                    # Should not fail on first page
                    if "Showing Earnings for:" in data:
                        # Actually YF was successful, problem is company doesn't have earnings history
                        dates = utils.empty_earnings_dates_df()
                break

            if dates is None:
                dates = data
            else:
                dates = _pd.concat([dates, data], axis=0)
            page_offset += page_size

        if dates is None:
            raise Exception("No data found, symbol may be delisted")
        dates = dates.reset_index(drop=True)

        # Drop redundant columns
        dates = dates.drop(["Symbol", "Company"], axis=1)

        # Convert types
        for cn in ["EPS Estimate", "Reported EPS", "Surprise(%)"]:
            dates.loc[dates[cn] == '-', cn] = "NaN"
            dates[cn] = dates[cn].astype(float)

        # Convert % to range 0->1:
        dates["Surprise(%)"] *= 0.01

        # Parse earnings date string
        cn = "Earnings Date"
        # - remove AM/PM and timezone from date string
        tzinfo = dates[cn].str.extract('([AP]M[a-zA-Z]*)$')
        dates[cn] = dates[cn].replace(' [AP]M[a-zA-Z]*$', '', regex=True)
        # - split AM/PM from timezone
        tzinfo = tzinfo[0].str.extract('([AP]M)([a-zA-Z]*)', expand=True)
        tzinfo.columns = ["AM/PM", "TZ"]
        # - combine and parse
        dates[cn] = dates[cn] + ' ' + tzinfo["AM/PM"]
        dates[cn] = _pd.to_datetime(dates[cn], format="%b %d, %Y, %I %p")
        # - instead of attempting decoding of ambiguous timezone abbreviation, just use 'info':
        dates[cn] = dates[cn].dt.tz_localize(
            tz=self.info["exchangeTimezoneName"])

        dates = dates.set_index("Earnings Date")

        self._earnings_dates = dates

        return dates

    def get_earnings_history(self, proxy=None):
        if self._earnings_history:
            return self._earnings_history

        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        url = "{}/calendar/earnings?symbol={}".format(_ROOT_URL_, self.ticker)
        session = self.session or _requests
        data = session.get(
            url=url,
            proxies=proxy,
            headers=utils.user_agent_headers
        ).text

        if "Will be right back" in data:
            raise RuntimeError("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\n"
                               "Our engineers are working quickly to resolve "
                               "the issue. Thank you for your patience.")

        try:
            # read_html returns a list of pandas Dataframes of all the tables in `data`
            data = _pd.read_html(data)[0]
            data.replace("-", _np.nan, inplace=True)

            data['EPS Estimate'] = _pd.to_numeric(data['EPS Estimate'])
            data['Reported EPS'] = _pd.to_numeric(data['Reported EPS'])
            self._earnings_history = data
        # if no tables are found a ValueError is thrown
        except ValueError:
            print("Could not find data for {}.".format(self.ticker))
            return
        return data

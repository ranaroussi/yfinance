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

from pytz import UnknownTimeZoneError

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
        self._financials = None
        self._balancesheet = None
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
                auto_adjust=True, back_adjust=False, repair=False, keepna=False,
                proxy=None, rounding=False, timeout=10, **kwargs):
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
            repair: bool
                Detect currency unit 100x mixups and attempt repair
                Default is False
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
                Default is 10 seconds.
            **kwargs: dict
                debug: bool
                    Optional. If passed as False, will suppress
                    error message printing to console.
                raise_errors: bool
                    Optional. If True, then raise errors as
                    exceptions instead of printing to console.
        """

        # Work with errors
        debug_mode = True
        if "debug" in kwargs and isinstance(kwargs["debug"], bool):
            debug_mode = kwargs["debug"]
        raise_errors = False
        if "raise_errors" in kwargs and isinstance(kwargs["raise_errors"], bool):
            raise_errors = kwargs["raise_errors"]

        if start or period is None or period.lower() == "max":
            # Check can get TZ. Fail => probably delisted
            tz = self._get_ticker_tz(debug_mode, proxy, timeout)
            if tz is None:
                # Every valid ticker has a timezone. Missing = problem
                err_msg = "No timezone found, symbol certainly delisted"
                shared._DFS[self.ticker] = utils.empty_df()
                shared._ERRORS[self.ticker] = err_msg
                if "many" not in kwargs and debug_mode:
                    if raise_errors:
                        raise Exception('%s: %s' % (self.ticker, err_msg))
                    else:
                        print('- %s: %s' % (self.ticker, err_msg))
                return utils.empty_df()

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

        err_msg = "No data found for this date range, symbol may be delisted"
        fail = False
        if data is None or not type(data) is dict:
            fail = True
        elif type(data) is dict and 'status_code' in data.keys():
            err_msg += "(Yahoo status_code = {})".format(data["status_code"])
            fail = True
        elif "chart" in data and data["chart"]["error"]:
            err_msg = data["chart"]["error"]["description"]
            fail = True
        elif not "chart" in data or data["chart"]["result"] is None or not data["chart"]["result"]:
            fail = True
        elif not period is None and not "timestamp" in data["chart"]["result"][0] and not period in data["chart"]["result"][0]["meta"]["validRanges"]:
            # User provided a bad period. The minimum should be '1d', but sometimes Yahoo accepts '1h'.
            err_msg = "Period '{}' is invalid, must be one of {}".format(period, data["chart"]["result"][0]["meta"]["validRanges"])
            fail = True
        if fail:
            shared._DFS[self.ticker] = utils.empty_df()
            shared._ERRORS[self.ticker] = err_msg
            if "many" not in kwargs and debug_mode:
                if raise_errors:
                    raise Exception('%s: %s' % (self.ticker, err_msg))
                else:
                    print('%s: %s' % (self.ticker, err_msg))
            return utils.empty_df()

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
                if raise_errors:
                    raise Exception('%s: %s' % (self.ticker, err_msg))
                else:
                    print('%s: %s' % (self.ticker, err_msg))
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

        tz_exchange = data["chart"]["result"][0]["meta"]["exchangeTimezoneName"]

        # Note: ordering is important. If you change order, run the tests!
        quotes = utils.fix_Yahoo_returning_live_separate(quotes, params["interval"], tz_exchange)
        quotes = utils.set_df_tz(quotes, params["interval"], tz_exchange)
        quotes = utils.fix_Yahoo_dst_issue(quotes, params["interval"])
        if repair:
            # Do this before auto/back adjust
            quotes = self._fix_unit_mixups(quotes, interval, tz_exchange)

        # Auto/back adjust
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
                if raise_errors:
                    raise Exception('%s: %s' % (self.ticker, err_msg))
                else:
                    print('%s: %s' % (self.ticker, err_msg))

        if rounding:
            quotes = _np.round(quotes, data[
                "chart"]["result"][0]["meta"]["priceHint"])
        quotes['Volume'] = quotes['Volume'].fillna(0).astype(_np.int64)

        # actions
        dividends, splits = utils.parse_actions(data["chart"]["result"][0])
        if start is not None:
            startDt = _pd.to_datetime(_datetime.datetime.utcfromtimestamp(start))
            if dividends is not None:
                dividends = dividends[dividends.index>=startDt]
            if splits is not None:
                splits = splits[splits.index>=startDt]
        if end is not None:
            endDt = _pd.to_datetime(_datetime.datetime.utcfromtimestamp(end))
            if dividends is not None:
                dividends = dividends[dividends.index<endDt]
            if splits is not None:
                splits = splits[splits.index<endDt]
        if splits is not None:
            splits = utils.set_df_tz(splits, interval, tz_exchange)
        if dividends is not None:
            dividends = utils.set_df_tz(dividends, interval, tz_exchange)

        # Combine
        df = quotes.sort_index()
        if dividends.shape[0] > 0:
            df = utils.safe_merge_dfs(df, dividends, interval)
        if "Dividends" in df.columns:
            df.loc[df["Dividends"].isna(),"Dividends"] = 0
        else:
            df["Dividends"] = 0.0
        if splits.shape[0] > 0:
            df = utils.safe_merge_dfs(df, splits, interval)
        if "Stock Splits" in df.columns:
            df.loc[df["Stock Splits"].isna(),"Stock Splits"] = 0
        else:
            df["Stock Splits"] = 0.0

        if params["interval"][-1] in ("m",'h'):
            df.index.name = "Datetime"
        else:
            df.index.name = "Date"

        # duplicates and missing rows cleanup
        df = df[~df.index.duplicated(keep='first')]
        self._history = df.copy()
        if not actions:
            df = df.drop(columns=["Dividends", "Stock Splits"])
        if not keepna:
            mask_nan_or_zero = (df.isna()|(df==0)).all(axis=1)
            df = df.drop(mask_nan_or_zero.index[mask_nan_or_zero])

        return df

    # ------------------------

    def _fix_unit_mixups(self, df, interval, tz_exchange):
        # Sometimes Yahoo returns few prices in cents/pence instead of $/£
        # I.e. 100x bigger
        # Easy to detect and fix, just look for outliers = ~100x local median

        if df.shape[0] == 0:
            return df
        if df.shape[0] == 1:
            # Need multiple rows to confidently identify outliers
            return df

        if df.index.tz is None:
            df.index = df.index.tz_localize(tz_exchange)
        else:
            df.index = df.index.tz_convert(tz_exchange)

        # Only import scipy if users actually want function. To avoid
        # adding it to dependencies.
        from scipy import ndimage as _ndimage

        data_cols = ["Open","High","Low","Close"]
        data_cols = [c for c in data_cols if c in df.columns]
        n = df.shape[0]
        median = _ndimage.median_filter(df[data_cols].values, size=(3,3), mode='mirror')

        if (median==0).any():
            raise Exception("median contains zeroes, why?")
        ratio = df[data_cols].values/median
        # ratio_rounded = (ratio/5).round()*5 # round ratio to nearest 5
        ratio_rounded = (ratio/10).round()*10 # round ratio to nearest 10
        f = (ratio_rounded)==100

        # Store each mixup:
        mixups = {}
        for j in range(len(data_cols)):
            fj = f[:,j]
            if fj.any():
                dc = data_cols[j]
                for i in _np.where(fj)[0]:
                    idx = df.index[i]
                    if idx not in mixups:
                        mixups[idx] = {"data":df.loc[idx,data_cols], "fields":set([dc])}
                    else:
                        mixups[idx]["fields"].add(dc)
        n_mixups = len(mixups)

        if len(mixups) > 0:
            # Problem with Yahoo's mixup is they calculate high & low after, so they can be corrupted.
            # If interval is weekly then can correct with daily. But if smaller intervals then 
            # restricted to recent times:
            # - daily = hourly restricted to last 730 days
            sub_interval = None
            td_range = None
            if interval == "1wk":
                # Correct by fetching week of daily data
                sub_interval = "1d"
                td_range = _datetime.timedelta(days=7)
            elif interval == "1d":
                # Correct by fetching day of hourly data
                sub_interval = "1h"
                td_range = _datetime.timedelta(days=1)
            else:
                print("WARNING: Have not implemented repair for '{}' interval. Contact developers".format(interval))
                return df

            # This first pass will correct all errors in Open/Close/Adj Close columns.
            # It will also *attempt* to correct Low/High columns, but only if can get price data.
            for idx in sorted(list(mixups.keys())):
                m = mixups[idx]
                # Although only some fields in row exhibit 100x error, normally the other fields are also corrupted, 
                # so need to recalculate all fields in row.

                if td_range is None:
                    raise Exception("was hoping this wouldn't happen")

                start = idx.date()
                if sub_interval=="1h" and (_datetime.date.today()-start) > _datetime.timedelta(days=729):
                    # Don't bother requesting more price data, Yahoo will reject
                    pass
                else:
                    if sub_interval=="1h":
                        df_fine = self.history(start=idx.date(), end=idx.date()+td_range, interval=sub_interval, auto_adjust=False)
                    else:
                        df_fine = self.history(start=idx.date()-td_range, end=idx.date()+td_range, interval=sub_interval, auto_adjust=False)

                    # First, check whether df_fine has different split-adjustment than df.
                    # If it is different, then adjust df_fine to match df
                    good_fields = list(set(data_cols)-m["fields"])
                    median = df.loc[idx,good_fields].median()
                    median_fine = _np.median(df_fine[good_fields].values)
                    ratio = round(median/median_fine, 1)
                    ratio_rcp = round(median_fine/median, 1)
                    if ratio==1 and ratio_rcp==1:
                        # Good!
                        pass
                    else:
                        if ratio>1:
                            # data has different split-adjustment than fine-grained data
                            # Adjust fine-grained to match
                            df_fine[data_cols] *= ratio
                        elif ratio_rcp>1:
                            # data has different split-adjustment than fine-grained data
                            # Adjust fine-grained to match
                            df_fine[data_cols] *= 1.0/ratio_rcp
                        median_fine = _np.median(df_fine[good_fields].values)
                        ratio = round(median/median_fine, 1)
                        ratio_rcp = round(median_fine/median, 1)

                    if sub_interval != "1h":
                        # dt_before_week = df_fine.index[df_fine.index.get_loc(idx)-1]
                        df_last_week = df_fine[df_fine.index<idx]
                        df_fine = df_fine[df_fine.index>=idx]

                    if "High" in m["fields"]:
                        df.loc[idx, "High"] = df_fine["High"].max()
                        m["fields"].remove("High")
                    if "Low" in m["fields"]:
                        df.loc[idx, "Low"] = df_fine["Low"].min()
                        m["fields"].remove("Low")
                    if "Open" in m["fields"]:
                        if sub_interval != "1h" and idx != df_fine.index[0]:
                            # Exchange closed Monday. In this case, Yahoo sets Open to last week close
                            df.loc[idx, "Open"] = df_last_week["Close"][-1]
                            df.loc[idx, "Low"] = min(df.loc[idx, "Open"], df.loc[idx, "Low"])
                        else:
                            df.loc[idx, "Open"] = df_fine["Open"].iloc[0]
                        m["fields"].remove("Open")
                    if "Close" in m["fields"]:
                        df.loc[idx, "Close"] = df_fine["Close"].iloc[-1]
                        m["fields"].remove("Close")
                        # Assume 'Adj Close' also corrupted, easier than detecting whether true
                        df.loc[idx, "Adj Close"] = df_fine["Adj Close"].iloc[-1]

                if len(m["fields"])==0:
                    del mixups[idx]

            # This second pass will *crudely* "fix" any remaining errors in High/Low
            # simply by ensuring they don't contradict e.g. Low = 100x High
            if len(mixups)>0:
                for idx in sorted(list(mixups.keys())):
                    m = mixups[idx]
                    row = df.loc[idx,["Open","Close"]]
                    if "High" in m["fields"]:
                        df.loc[idx,"High"] = row.max()
                        m["fields"].remove("High")
                    if "Low" in m["fields"]:
                        df.loc[idx,"Low"] = row.min()
                        m["fields"].remove("Low")

                    if len(m["fields"])==0:
                        del mixups[idx]

            n_fixed = n_mixups - len(mixups)
            print("{}: fixed {} currency unit mixups in {} price data".format(self.ticker, n_fixed, interval))
            if len(mixups)>0:
                print("    ... and failed to correct {}".format(len(mixups)))

        return df


    def _get_ticker_tz(self, debug_mode, proxy, timeout):
        if self._tz is not None:
            return self._tz
        cache = utils.get_tz_cache()
        tz = cache.lookup(self.ticker)

        if tz and not utils.is_valid_timezone(tz):
            # Clear from cache and force re-fetch
            cache.store(self.ticker, None)
            tz = None

        if tz is None:
            tz = self._fetch_ticker_tz(debug_mode, proxy, timeout)

            if utils.is_valid_timezone(tz):
                # info fetch is relatively slow so cache timezone
                cache.store(self.ticker, tz)
            else:
                tz = None

        self._tz = tz
        return tz

      
    def _fetch_ticker_tz(self, debug_mode, proxy, timeout):
        # Query Yahoo for basic price data just to get returned timezone

        params = {"range":"1d", "interval":"1d"}

        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        # Getting data from json
        url = "{}/v8/finance/chart/{}".format(self._base_url, self.ticker)

        session = self.session or _requests
        try:
            data = session.get(url=url, params=params, proxies=proxy, headers=utils.user_agent_headers, timeout=timeout)
            data = data.json()
        except Exception as e:
            if debug_mode:
                print("Failed to get ticker '{}' reason: {}".format(self.ticker, e))
            return None
        else:
            error = data.get('chart', {}).get('error', None)
            if error:
                # explicit error from yahoo API
                if debug_mode:
                    print("Got error from yahoo api for ticker {}, Error: {}".format(self.ticker, error))
            else:
                try:
                    return data["chart"]["result"][0]["meta"]["exchangeTimezoneName"]
                except Exception as err:
                    if debug_mode:
                        print("Could not get exchangeTimezoneName for ticker '{}' reason: {}".format(self.ticker, err))
                        print("Got response: ")
                        print("-------------")
                        print(" {}".format(data))
                        print("-------------")
        return None

    def _get_info(self, proxy=None):
        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        if (self._info is not None) or (self._sustainability is not None) or (self._recommendations):
            # No need to fetch
            return

        ticker_url = "{}/{}".format(self._scrape_url, self.ticker)

        # get info and sustainability
        data = utils.get_json(ticker_url, proxy, self.session)

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
            if not 'website' in self._info:
                self._info['logo_url'] = 'https://logo.clearbit.com/%s.com' % self._info['shortName'].split(' ')[0].split(',')[0]
            else:
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
        data = utils.get_json(ticker_url + '/financials', proxy, self.session)

        # generic patterns
        self._earnings = {"yearly": utils.empty_df(), "quarterly": utils.empty_df()}
        self._cashflow = {"yearly": utils.empty_df(), "quarterly": utils.empty_df()}
        self._balancesheet = {"yearly": utils.empty_df(), "quarterly": utils.empty_df()}
        self._financials = {"yearly": utils.empty_df(), "quarterly": utils.empty_df()}
        for key in (
            (self._cashflow, 'cashflowStatement', 'cashflowStatements'),
            (self._balancesheet, 'balanceSheet', 'balanceSheetStatements'),
            (self._financials, 'incomeStatement', 'incomeStatementHistory')
        ):
            item = key[1] + 'History'
            if isinstance(data.get(item), dict):
                try:
                    key[0]['yearly'] = cleanup(data[item][key[2]])
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

    def get_financials(self, proxy=None, as_dict=False, freq="yearly"):
        self._get_fundamentals(proxy=proxy)
        data = self._financials[freq]
        if as_dict:
            return data.to_dict()
        return data

    def get_balancesheet(self, proxy=None, as_dict=False, freq="yearly"):
        self._get_fundamentals(proxy=proxy)
        data = self._balancesheet[freq]
        if as_dict:
            return data.to_dict()
        return data

    def get_balance_sheet(self, proxy=None, as_dict=False, freq="yearly"):
        return self.get_balancesheet(proxy, as_dict, freq)

    def get_cashflow(self, proxy=None, as_dict=False, freq="yearly"):
        self._get_fundamentals(proxy=proxy)
        data = self._cashflow[freq]
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
            tz=self.get_info()["exchangeTimezoneName"])

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

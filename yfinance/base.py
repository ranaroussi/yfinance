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
from typing import Optional

import pandas as _pd
import numpy as _np
import pandas as pd

from .data import TickerData

from urllib.parse import quote as urlencode

from . import utils

from . import shared
from .scrapers.analysis import Analysis
from .scrapers.fundamentals import Fundamentals
from .scrapers.holders import Holders
from .scrapers.quote import Quote

_BASE_URL_ = 'https://query2.finance.yahoo.com'
_SCRAPE_URL_ = 'https://finance.yahoo.com/quote'
_ROOT_URL_ = 'https://finance.yahoo.com'


class TickerBase:
    def __init__(self, ticker, session=None):
        self.ticker = ticker.upper()
        self.session = session
        self._history = None
        self._base_url = _BASE_URL_
        self._scrape_url = _SCRAPE_URL_
        self._tz = None

        self._isin = None
        self._news = []
        self._shares = None

        self._earnings_dates = None
        self._earnings_history = None

        self._earnings = None
        self._financials = None

        # accept isin as ticker
        if utils.is_isin(self.ticker):
            self.ticker = utils.get_ticker_by_isin(self.ticker, None, session)

        self._data: TickerData = TickerData(self.ticker, session=session)

        self._analysis = Analysis(self._data)
        self._holders = Holders(self._data)
        self._quote = Quote(self._data)
        self._fundamentals = Fundamentals(self._data)

    def stats(self, proxy=None):
        ticker_url = "{}/{}".format(self._scrape_url, self.ticker)

        # get info and sustainability
        data = self._data.get_json_data_stores(proxy=proxy)["QuoteSummaryStore"]
        return data

    def history(self, period="1mo", interval="1d",
                start=None, end=None, prepost=False, actions=True,
                auto_adjust=True, back_adjust=False, repair=False, keepna=False,
                proxy=None, rounding=False, timeout=10,
                debug=True, raise_errors=False) -> pd.DataFrame:
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
            debug: bool
                If passed as False, will suppress
                error message printing to console.
            raise_errors: bool
                If True, then raise errors as
                exceptions instead of printing to console.
        """

        if start or period is None or period.lower() == "max":
            # Check can get TZ. Fail => probably delisted
            tz = self._get_ticker_tz(debug, proxy, timeout)
            if tz is None:
                # Every valid ticker has a timezone. Missing = problem
                err_msg = "No timezone found, symbol may be delisted"
                shared._DFS[self.ticker] = utils.empty_df()
                shared._ERRORS[self.ticker] = err_msg
                if debug:
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
                    _UNIX_TIMESTAMP_1900 = -2208994789
                    start = _UNIX_TIMESTAMP_1900
            else:
                start = utils._parse_user_dt(start, tz)
            params = {"period1": start, "period2": end}
        else:
            period = period.lower()
            params = {"range": period}

        params["interval"] = interval.lower()
        params["includePrePost"] = prepost

        # 1) fix weired bug with Yahoo! - returning 60m for 30m bars
        if params["interval"] == "30m":
            params["interval"] = "15m"

        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}

        #if the ticker is MUTUALFUND or ETF, then get capitalGains events
        data = self.get_info(proxy)
        if 'quoteType' in data and data['quoteType'] in ('MUTUALFUND', 'ETF'):
            params["events"] = "div,splits,capitalGains"
        else:
            params["events"] = "div,splits"

        # Getting data from json
        url = "{}/v8/finance/chart/{}".format(self._base_url, self.ticker)

        data = None

        try:
            data = self._data.get(
                url=url,
                params=params,
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
        elif type(data) is dict and 'status_code' in data:
            err_msg += "(Yahoo status_code = {})".format(data["status_code"])
            fail = True
        elif "chart" in data and data["chart"]["error"]:
            err_msg = data["chart"]["error"]["description"]
            fail = True
        elif "chart" not in data or data["chart"]["result"] is None or not data["chart"]["result"]:
            fail = True
        elif period is not None and "timestamp" not in data["chart"]["result"][0] and period not in \
                data["chart"]["result"][0]["meta"]["validRanges"]:
            # User provided a bad period. The minimum should be '1d', but sometimes Yahoo accepts '1h'.
            err_msg = "Period '{}' is invalid, must be one of {}".format(period, data["chart"]["result"][0]["meta"][
                "validRanges"])
            fail = True
        if fail:
            shared._DFS[self.ticker] = utils.empty_df()
            shared._ERRORS[self.ticker] = err_msg
            if debug:
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
                if quotes.index[quotes.shape[0] - 1] >= endDt:
                    quotes = quotes.iloc[0:quotes.shape[0] - 1]
        except Exception:
            shared._DFS[self.ticker] = utils.empty_df()
            shared._ERRORS[self.ticker] = err_msg
            if debug:
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
        quotes = utils.set_df_tz(quotes, params["interval"], tz_exchange)
        quotes = utils.fix_Yahoo_dst_issue(quotes, params["interval"])
        quotes = utils.fix_Yahoo_returning_live_separate(quotes, params["interval"], tz_exchange)
        if repair:
            # Do this before auto/back adjust
            quotes = self._fix_zero_prices(quotes, interval, tz_exchange)
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
            if debug:
                if raise_errors:
                    raise Exception('%s: %s' % (self.ticker, err_msg))
                else:
                    print('%s: %s' % (self.ticker, err_msg))

        if rounding:
            quotes = _np.round(quotes, data[
                "chart"]["result"][0]["meta"]["priceHint"])
        quotes['Volume'] = quotes['Volume'].fillna(0).astype(_np.int64)

        # actions
        dividends, splits, capital_gains = utils.parse_actions(data["chart"]["result"][0])
        if start is not None:
            # Note: use pandas Timestamp as datetime.utcfromtimestamp has bugs on windows
            # https://github.com/python/cpython/issues/81708
            startDt = _pd.Timestamp(start, unit='s')
            if dividends is not None:
                dividends = dividends[dividends.index>=startDt]
            if "capitalGains" in params["events"] and capital_gains is not None:
                capital_gains = capital_gains[capital_gains.index>=startDt]
            if splits is not None:
                splits = splits[splits.index >= startDt]
        if end is not None:
            endDt = _pd.Timestamp(end, unit='s')
            if dividends is not None:
                dividends = dividends[dividends.index<endDt]
            if "capitalGains" in params["events"] and capital_gains is not None:
                capital_gains = capital_gains[capital_gains.index<endDt]
            if splits is not None:
                splits = splits[splits.index < endDt]
        if splits is not None:
            splits = utils.set_df_tz(splits, interval, tz_exchange)
        if dividends is not None:
            dividends = utils.set_df_tz(dividends, interval, tz_exchange)
        if capital_gains is not None:
            capital_gains = utils.set_df_tz(capital_gains, interval, tz_exchange)

        # Prepare for combine
        intraday = params["interval"][-1] in ("m", 'h')
        if not intraday:
            # If localizing a midnight during DST transition hour when clocks roll back,
            # meaning clock hits midnight twice, then use the 2nd (ambiguous=True)
            quotes.index = _pd.to_datetime(quotes.index.date).tz_localize(tz_exchange, ambiguous=True)
            if dividends.shape[0] > 0:
                dividends.index = _pd.to_datetime(dividends.index.date).tz_localize(tz_exchange, ambiguous=True)
            if splits.shape[0] > 0:
                splits.index = _pd.to_datetime(splits.index.date).tz_localize(tz_exchange, ambiguous=True)

        # Combine
        df = quotes.sort_index()
        if dividends.shape[0] > 0:
            df = utils.safe_merge_dfs(df, dividends, interval)
        if "Dividends" in df.columns:
            df.loc[df["Dividends"].isna(), "Dividends"] = 0
        else:
            df["Dividends"] = 0.0
        if splits.shape[0] > 0:
            df = utils.safe_merge_dfs(df, splits, interval)
        if "Stock Splits" in df.columns:
            df.loc[df["Stock Splits"].isna(), "Stock Splits"] = 0
        else:
            df["Stock Splits"] = 0.0
        if "capitalGains" in params["events"]:
            if capital_gains.shape[0] > 0:
                df = utils.safe_merge_dfs(df, capital_gains, interval)
            if "Capital Gains" in df.columns:
                df.loc[df["Capital Gains"].isna(),"Capital Gains"] = 0
            else:
                df["Capital Gains"] = 0.0

        if intraday:
            df.index.name = "Datetime"
        else:
            df.index.name = "Date"

        # duplicates and missing rows cleanup
        df = df[~df.index.duplicated(keep='first')]
        self._history = df.copy()
        if not actions:
            df = df.drop(columns=["Dividends", "Stock Splits", "Capital Gains"], errors='ignore')
        if not keepna:
            mask_nan_or_zero = (df.isna() | (df == 0)).all(axis=1)
            df = df.drop(mask_nan_or_zero.index[mask_nan_or_zero])

        return df

    # ------------------------

    def _reconstruct_interval(self, df_row, interval, bad_fields):
        if isinstance(df_row, _pd.DataFrame) or not isinstance(df_row, _pd.Series):
            raise Exception("'df_row' must be a Pandas Series not", type(df_row))
        if not isinstance(bad_fields, (list, set, _np.ndarray)):
            raise Exception("'bad_fields' must be a list/set not", type(bad_fields))

        data_cols = [c for c in ["Open", "High", "Low", "Close", "Adj Close"] if c in df_row.index]

        # If interval is weekly then can construct with daily. But if smaller intervals then
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
            return df_row

        idx = df_row.name
        start = idx.date()
        if sub_interval == "1h" and (_datetime.date.today() - start) > _datetime.timedelta(days=729):
            # Don't bother requesting more price data, Yahoo will reject
            return None
        else:
            new_vals = {}

            if sub_interval == "1h":
                df_fine = self.history(start=start, end=start + td_range, interval=sub_interval, auto_adjust=False)
            else:
                df_fine = self.history(start=start - td_range, end=start + td_range, interval=sub_interval,
                                       auto_adjust=False)

            # First, check whether df_fine has different split-adjustment than df_row.
            # If it is different, then adjust df_fine to match df_row
            good_fields = list(set(data_cols) - set(bad_fields) - set("Adj Close"))
            if len(good_fields) == 0:
                raise Exception(
                    "No good fields, so cannot determine whether different split-adjustment. Contact developers")
            # median = df_row.loc[good_fields].median()
            # median_fine = _np.median(df_fine[good_fields].values)
            # ratio = median/median_fine
            # Better method to calculate split-adjustment:
            df_fine_from_idx = df_fine[df_fine.index >= idx]
            ratios = []
            for f in good_fields:
                if f == "Low":
                    ratios.append(df_row[f] / df_fine_from_idx[f].min())
                elif f == "High":
                    ratios.append(df_row[f] / df_fine_from_idx[f].max())
                elif f == "Open":
                    ratios.append(df_row[f] / df_fine_from_idx[f].iloc[0])
                elif f == "Close":
                    ratios.append(df_row[f] / df_fine_from_idx[f].iloc[-1])
            ratio = _np.mean(ratios)
            #
            ratio_rcp = round(1.0 / ratio, 1)
            ratio = round(ratio, 1)
            if ratio == 1 and ratio_rcp == 1:
                # Good!
                pass
            else:
                if ratio > 1:
                    # data has different split-adjustment than fine-grained data
                    # Adjust fine-grained to match
                    df_fine[data_cols] *= ratio
                elif ratio_rcp > 1:
                    # data has different split-adjustment than fine-grained data
                    # Adjust fine-grained to match
                    df_fine[data_cols] *= 1.0 / ratio_rcp

            if sub_interval != "1h":
                df_last_week = df_fine[df_fine.index < idx]
                df_fine = df_fine[df_fine.index >= idx]

            if "High" in bad_fields:
                new_vals["High"] = df_fine["High"].max()
            if "Low" in bad_fields:
                new_vals["Low"] = df_fine["Low"].min()
            if "Open" in bad_fields:
                if sub_interval != "1h" and idx != df_fine.index[0]:
                    # Exchange closed Monday. In this case, Yahoo sets Open to last week close
                    new_vals["Open"] = df_last_week["Close"][-1]
                    if "Low" in new_vals:
                        new_vals["Low"] = min(new_vals["Open"], new_vals["Low"])
                    elif new_vals["Open"] < df_row["Low"]:
                        new_vals["Low"] = new_vals["Open"]
                else:
                    new_vals["Open"] = df_fine["Open"].iloc[0]
            if "Close" in bad_fields:
                new_vals["Close"] = df_fine["Close"].iloc[-1]
                # Assume 'Adj Close' also corrupted, easier than detecting whether true
                new_vals["Adj Close"] = df_fine["Adj Close"].iloc[-1]

        return new_vals

    def _fix_unit_mixups(self, df, interval, tz_exchange):
        # Sometimes Yahoo returns few prices in cents/pence instead of $/£
        # I.e. 100x bigger
        # Easy to detect and fix, just look for outliers = ~100x local median

        if df.shape[0] == 0:
            return df
        if df.shape[0] == 1:
            # Need multiple rows to confidently identify outliers
            return df

        df2 = df.copy()

        if df.index.tz is None:
            df2.index = df2.index.tz_localize(tz_exchange)
        else:
            df2.index = df2.index.tz_convert(tz_exchange)

        # Only import scipy if users actually want function. To avoid
        # adding it to dependencies.
        from scipy import ndimage as _ndimage

        data_cols = ["High", "Open", "Low", "Close"]  # Order important, separate High from Low
        data_cols = [c for c in data_cols if c in df2.columns]
        median = _ndimage.median_filter(df2[data_cols].values, size=(3, 3), mode="wrap")

        if (median == 0).any():
            raise Exception("median contains zeroes, why?")
        ratio = df2[data_cols].values / median
        ratio_rounded = (ratio / 20).round() * 20  # round ratio to nearest 20
        f = ratio_rounded == 100

        # Store each mixup:
        mixups = {}
        for j in range(len(data_cols)):
            fj = f[:, j]
            if fj.any():
                dc = data_cols[j]
                for i in _np.where(fj)[0]:
                    idx = df2.index[i]
                    if idx not in mixups:
                        mixups[idx] = {"data": df2.loc[idx, data_cols], "fields": {dc}}
                    else:
                        mixups[idx]["fields"].add(dc)
        n_mixups = len(mixups)

        if len(mixups) > 0:
            # This first pass will correct all errors in Open/Close/AdjClose columns.
            # It will also attempt to correct Low/High columns, but only if can get price data.
            for idx in sorted(list(mixups.keys())):
                m = mixups[idx]
                new_values = self._reconstruct_interval(df2.loc[idx], interval, m["fields"])
                if not new_values is None:
                    for k in new_values:
                        df2.loc[idx, k] = new_values[k]
                    del mixups[idx]

            # This second pass will *crudely* "fix" any remaining errors in High/Low
            # simply by ensuring they don't contradict e.g. Low = 100x High
            if len(mixups) > 0:
                for idx in sorted(list(mixups.keys())):
                    m = mixups[idx]
                    row = df2.loc[idx, ["Open", "Close"]]
                    if "High" in m["fields"]:
                        df2.loc[idx, "High"] = row.max()
                        m["fields"].remove("High")
                    if "Low" in m["fields"]:
                        df2.loc[idx, "Low"] = row.min()
                        m["fields"].remove("Low")

                    if len(m["fields"]) == 0:
                        del mixups[idx]

            n_fixed = n_mixups - len(mixups)
            print("{}: fixed {} currency unit mixups in {} price data".format(self.ticker, n_fixed, interval))
            if len(mixups) > 0:
                print("    ... and failed to correct {}".format(len(mixups)))

        return df2

    def _fix_zero_prices(self, df, interval, tz_exchange):
        # Sometimes Yahoo returns prices=0 when obviously wrong e.g. Volume>0 and Close>0.
        # Easy to detect and fix

        if df.shape[0] == 0:
            return df
        if df.shape[0] == 1:
            # Need multiple rows to confidently identify outliers
            return df

        df2 = df.copy()

        if df2.index.tz is None:
            df2.index = df2.index.tz_localize(tz_exchange)
        else:
            df2.index = df2.index.tz_convert(tz_exchange)

        data_cols = ["Open", "High", "Low", "Close"]
        data_cols = [c for c in data_cols if c in df2.columns]
        f_zeroes = (df2[data_cols] == 0.0).values.any(axis=1)

        n_fixed = 0
        for i in _np.where(f_zeroes)[0]:
            idx = df2.index[i]
            df_row = df2.loc[idx]
            bad_fields = df2.columns[df_row.values == 0.0].values
            new_values = self._reconstruct_interval(df2.loc[idx], interval, bad_fields)
            if not new_values is None:
                for k in new_values:
                    df2.loc[idx, k] = new_values[k]
                n_fixed += 1

        if n_fixed > 0:
            print("{}: fixed {} price=0.0 errors in {} price data".format(self.ticker, n_fixed, interval))
        return df2

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

        params = {"range": "1d", "interval": "1d"}

        # Getting data from json
        url = "{}/v8/finance/chart/{}".format(self._base_url, self.ticker)

        try:
            data = self._data.get(url=url, params=params, proxy=proxy, timeout=timeout)
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

    def get_recommendations(self, proxy=None, as_dict=False):
        self._quote.proxy = proxy
        data = self._quote.recommendations
        if as_dict:
            return data.to_dict()
        return data

    def get_calendar(self, proxy=None, as_dict=False):
        self._quote.proxy = proxy
        data = self._quote.calendar
        if as_dict:
            return data.to_dict()
        return data

    def get_major_holders(self, proxy=None, as_dict=False):
        self._holders.proxy = proxy
        data = self._holders.major
        if as_dict:
            return data.to_dict()
        return data

    def get_institutional_holders(self, proxy=None, as_dict=False):
        self._holders.proxy = proxy
        data = self._holders.institutional
        if data is not None:
            if as_dict:
                return data.to_dict()
            return data

    def get_mutualfund_holders(self, proxy=None, as_dict=False):
        self._holders.proxy = proxy
        data = self._holders.mutualfund
        if data is not None:
            if as_dict:
                return data.to_dict()
            return data

    def get_info(self, proxy=None) -> dict:
        self._quote.proxy = proxy
        data = self._quote.info
        return data

    def get_sustainability(self, proxy=None, as_dict=False):
        self._quote.proxy = proxy
        data = self._quote.sustainability
        if as_dict:
            return data.to_dict()
        return data

    def get_recommendations_summary(self, proxy=None, as_dict=False):
        self._quote.proxy = proxy
        data = self._quote.recommendations
        if as_dict:
            return data.to_dict()
        return data

    def get_analyst_price_target(self, proxy=None, as_dict=False):
        self._analysis.proxy = proxy
        data = self._analysis.analyst_price_target
        if as_dict:
            return data.to_dict()
        return data

    def get_rev_forecast(self, proxy=None, as_dict=False):
        self._analysis.proxy = proxy
        data = self._analysis.rev_est
        if as_dict:
            return data.to_dict()
        return data

    def get_earnings_forecast(self, proxy=None, as_dict=False):
        self._analysis.proxy = proxy
        data = self._analysis.eps_est
        if as_dict:
            return data.to_dict()
        return data

    def get_trend_details(self, proxy=None, as_dict=False):
        self._analysis.proxy = proxy
        data = self._analysis.analyst_trend_details
        if as_dict:
            return data.to_dict()
        return data

    def get_earnings_trend(self, proxy=None, as_dict=False):
        self._analysis.proxy = proxy
        data = self._analysis.earnings_trend
        if as_dict:
            return data.to_dict()
        return data

    def get_earnings(self, proxy=None, as_dict=False, freq="yearly"):
        self._fundamentals.proxy = proxy
        data = self._fundamentals.earnings[freq]
        if as_dict:
            dict_data = data.to_dict()
            dict_data['financialCurrency'] = 'USD' if 'financialCurrency' not in self._earnings else self._earnings[
                'financialCurrency']
            return dict_data
        return data

    def get_income_stmt(self, proxy=None, as_dict=False, freq="yearly"):
        self._fundamentals.proxy = proxy
        data = self._fundamentals.financials.get_income(freq=freq, proxy=proxy)
        if as_dict:
            return data.to_dict()
        return data

    def get_balance_sheet(self, proxy=None, as_dict=False, freq="yearly"):
        self._fundamentals.proxy = proxy
        data = self._fundamentals.financials.get_balance_sheet(freq=freq, proxy=proxy)
        if as_dict:
            return data.to_dict()
        return data

    def get_cashflow(self, proxy=None, as_dict=False, freq="yearly"):
        self._fundamentals.proxy = proxy
        data = self._fundamentals.financials.get_cash_flow(freq=freq, proxy=proxy)
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

    def get_capital_gains(self, proxy=None):
        if self._history is None:
            self.history(period="max", proxy=proxy)
        if self._history is not None and "Capital Gains" in self._history:
            capital_gains = self._history["Capital Gains"]
            return capital_gains[capital_gains != 0]
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
            action_columns = ["Dividends", "Stock Splits"]
            if "Capital Gains" in self._history:
                action_columns.append("Capital Gains")
            actions = self._history[action_columns]
            return actions[actions != 0].dropna(how='all').fillna(0)
        return []

    def get_shares(self, proxy=None, as_dict=False):
        self._fundamentals.proxy = proxy
        data = self._fundamentals.shares
        if as_dict:
            return data.to_dict()
        return data

    def get_isin(self, proxy=None) -> Optional[str]:
        # *** experimental ***
        if self._isin is not None:
            return self._isin

        ticker = self.ticker.upper()

        if "-" in ticker or "^" in ticker:
            self._isin = '-'
            return self._isin

        q = ticker

        self._quote.proxy = proxy
        if self._quote.info is None:
            # Don't print error message cause self._quote.info will print one
            return None
        if "shortName" in self._quote.info:
            q = self._quote.info['shortName']

        url = 'https://markets.businessinsider.com/ajax/' \
              'SearchController_Suggest?max_results=25&query=%s' \
              % urlencode(q)
        data = self._data.get(url=url, proxy=proxy).text

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

        # Getting data from json
        url = "{}/v1/finance/search?q={}".format(self._base_url, self.ticker)
        data = self._data.get(url=url, proxy=proxy)
        if "Will be right back" in data.text:
            raise RuntimeError("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\n"
                               "Our engineers are working quickly to resolve "
                               "the issue. Thank you for your patience.")
        data = data.json()

        # parse news
        self._news = data.get("news", [])
        return self._news

    def get_earnings_dates(self, proxy=None) -> Optional[pd.DataFrame]:
        if self._earnings_dates is not None:
            return self._earnings_dates

        page_size = 100  # YF caps at 100, don't go higher
        page_offset = 0
        dates = None
        while True:
            url = "{}/calendar/earnings?symbol={}&offset={}&size={}".format(
                _ROOT_URL_, self.ticker, page_offset, page_size)

            data = self._data.get(url=url, proxy=proxy).text

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

        if dates is None or dates.shape[0] == 0:
            err_msg = "No earnings dates found, symbol may be delisted"
            print('- %s: %s' % (self.ticker, err_msg))
            return None
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
        self._quote.proxy = proxy
        dates[cn] = dates[cn].dt.tz_localize(
            tz=self._quote.info["exchangeTimezoneName"])

        dates = dates.set_index("Earnings Date")

        self._earnings_dates = dates

        return dates

    def get_earnings_history(self, proxy=None) -> Optional[pd.DataFrame]:
        if self._earnings_history is not None:
            return self._earnings_history

        url = "{}/calendar/earnings?symbol={}".format(_ROOT_URL_, self.ticker)
        data = self._data.get(url=url, proxy=proxy).text

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
            print("Could not find earnings history data for {}.".format(self.ticker))
            return
        return data

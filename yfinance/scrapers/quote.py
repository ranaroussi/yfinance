import datetime
import json

import pandas as pd

from yfinance import utils
from yfinance.data import TickerData


class Quote:

    def __init__(self, data: TickerData, proxy=None):
        self._data = data
        self.proxy = proxy

        self._info = None
        self._sustainability = None
        self._recommendations = None
        self._calendar = None

        self._already_scraped = False

    @property
    def info(self) -> dict:
        if self._info is None:
            self._scrape(self.proxy)

        return self._info

    @property
    def sustainability(self) -> pd.DataFrame:
        if self._sustainability is None:
            self._scrape(self.proxy)
        return self._sustainability

    @property
    def recommendations(self) -> pd.DataFrame:
        if self._recommendations is None:
            self._scrape(self.proxy)
        return self._recommendations

    @property
    def calendar(self) -> pd.DataFrame:
        if self._calendar is None:
            self._scrape(self.proxy)
        return self._calendar

    def _scrape(self, proxy):
        if self._already_scraped:
            return
        self._already_scraped = True

        # get info and sustainability
        json_data = self._data.get_json_data_stores(proxy=proxy)
        try:
            quote_summary_store = json_data['QuoteSummaryStore']
        except KeyError:
            err_msg = "No summary info found, symbol may be delisted"
            print('- %s: %s' % (self._data.ticker, err_msg))
            return None

        # sustainability
        d = {}
        try:
            if isinstance(quote_summary_store.get('esgScores'), dict):
                for item in quote_summary_store['esgScores']:
                    if not isinstance(quote_summary_store['esgScores'][item], (dict, list)):
                        d[item] = quote_summary_store['esgScores'][item]

                s = pd.DataFrame(index=[0], data=d)[-1:].T
                s.columns = ['Value']
                s.index.name = '%.f-%.f' % (
                    s[s.index == 'ratingYear']['Value'].values[0],
                    s[s.index == 'ratingMonth']['Value'].values[0])

                self._sustainability = s[~s.index.isin(
                    ['maxAge', 'ratingYear', 'ratingMonth'])]
        except Exception:
            pass

        self._info = {}
        try:
            items = ['summaryProfile', 'financialData', 'quoteType',
                     'defaultKeyStatistics', 'assetProfile', 'summaryDetail']
            for item in items:
                if isinstance(quote_summary_store.get(item), dict):
                    self._info.update(quote_summary_store[item])
        except Exception:
            pass

        # For ETFs, provide this valuable data: the top holdings of the ETF
        try:
            if 'topHoldings' in quote_summary_store:
                self._info.update(quote_summary_store['topHoldings'])
        except Exception:
            pass

        try:
            if not isinstance(quote_summary_store.get('summaryDetail'), dict):
                # For some reason summaryDetail did not give any results. The price dict
                # usually has most of the same info
                self._info.update(quote_summary_store.get('price', {}))
        except Exception:
            pass

        try:
            # self._info['regularMarketPrice'] = self._info['regularMarketOpen']
            self._info['regularMarketPrice'] = quote_summary_store.get('price', {}).get(
                'regularMarketPrice', self._info.get('regularMarketOpen', None))
        except Exception:
            pass

        try:
            self._info['preMarketPrice'] = quote_summary_store.get('price', {}).get(
                'preMarketPrice', self._info.get('preMarketPrice', None))
        except Exception:
            pass

        self._info['logo_url'] = ""
        try:
            if not 'website' in self._info:
                self._info['logo_url'] = 'https://logo.clearbit.com/%s.com' % \
                                         self._info['shortName'].split(' ')[0].split(',')[0]
            else:
                domain = self._info['website'].split(
                    '://')[1].split('/')[0].replace('www.', '')
                self._info['logo_url'] = 'https://logo.clearbit.com/%s' % domain
        except Exception:
            pass

        # events
        try:
            cal = pd.DataFrame(quote_summary_store['calendarEvents']['earnings'])
            cal['earningsDate'] = pd.to_datetime(
                cal['earningsDate'], unit='s')
            self._calendar = cal.T
            self._calendar.index = utils.camel2title(self._calendar.index)
            self._calendar.columns = ['Value']
        except Exception as e:
            pass

        # analyst recommendations
        try:
            rec = pd.DataFrame(
                quote_summary_store['upgradeDowngradeHistory']['history'])
            rec['earningsDate'] = pd.to_datetime(
                rec['epochGradeDate'], unit='s')
            rec.set_index('earningsDate', inplace=True)
            rec.index.name = 'Date'
            rec.columns = utils.camel2title(rec.columns)
            self._recommendations = rec[[
                'Firm', 'To Grade', 'From Grade', 'Action']].sort_index()
        except Exception:
            pass

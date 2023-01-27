import datetime
import json

import pandas as pd

from yfinance import utils
from yfinance.data import TickerData


info_retired_keys_price = {"currentPrice", "dayHigh", "dayLow", "open", "previousClose", "volume"}
info_retired_keys_price.update({"regularMarket"+s for s in ["DayHigh", "DayLow", "Open", "PreviousClose", "Price", "Volume"]})
info_retired_keys_price.update({"fiftyTwoWeekLow", "fiftyTwoWeekHigh", "fiftyTwoWeekChange", "fiftyDayAverage", "twoHundredDayAverage"})
info_retired_keys_price.update({"averageDailyVolume10Day", "averageVolume10days", "averageVolume"})
info_retired_keys_exchange = {"currency", "exchange", "exchangeTimezoneName", "exchangeTimezoneShortName"}
info_retired_keys_marketCap = {"marketCap"}
info_retired_keys_symbol = {"symbol"}
info_retired_keys = info_retired_keys_price | info_retired_keys_exchange | info_retired_keys_marketCap | info_retired_keys_symbol


PRUNE_INFO = True
# PRUNE_INFO = False


from collections.abc import MutableMapping
class InfoDictWrapper(MutableMapping):
    """ Simple wrapper around info dict, intercepting 'gets' to 
    print how-to-migrate messages for specific keys. Requires
    override dict API"""

    def __init__(self, info):
        self.info = info

    def keys(self):
        return self.info.keys()

    def __str__(self):
        return self.info.__str__()

    def __repr__(self):
        return self.info.__repr__()

    def __contains__(self, k):
        return k in self.info.keys()

    def __getitem__(self, k):
        if k in info_retired_keys_price:
            print(f"Price data removed from info (key='{k}'). Use Ticker.fast_info or history() instead")
            return None
        elif k in info_retired_keys_exchange:
            print(f"Exchange data removed from info (key='{k}'). Use Ticker.fast_info or Ticker.get_history_metadata() instead")
            return None
        elif k in info_retired_keys_marketCap:
            print(f"Market cap removed from info (key='{k}'). Use Ticker.fast_info instead")
            return None
        elif k in info_retired_keys_symbol:
            print(f"Symbol removed from info (key='{k}'). You know this already")
            return None
        return self.info[self._keytransform(k)]

    def __setitem__(self, k, value):
        self.info[self._keytransform(k)] = value

    def __delitem__(self, k):
        del self.info[self._keytransform(k)]

    def __iter__(self):
        return iter(self.info)
    
    def __len__(self):
        return len(self.info)

    def _keytransform(self, k):
        return k



class Quote:

    def __init__(self, data: TickerData, proxy=None):
        self._data = data
        self.proxy = proxy

        self._info = None
        self._retired_info = None
        self._sustainability = None
        self._recommendations = None
        self._calendar = None

        self._already_scraped = False
        self._already_scraped_complementary = False

    @property
    def info(self) -> dict:
        if self._info is None:
            self._scrape(self.proxy)
            self._scrape_complementary(self.proxy)

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

        # Delete redundant info[] keys, because values can be accessed faster
        # elsewhere - e.g. price keys. Hope is reduces Yahoo spam effect.
        # But record the dropped keys, because in rare cases they are needed.
        self._retired_info = {}
        for k in info_retired_keys:
            if k in self._info:
                self._retired_info[k] = self._info[k]
                if PRUNE_INFO:
                    del self._info[k]
        if PRUNE_INFO:
            # InfoDictWrapper will explain how to access above data elsewhere
            self._info = InfoDictWrapper(self._info)

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

    def _scrape_complementary(self, proxy):
        if self._already_scraped_complementary:
            return
        self._already_scraped_complementary = True

        self._scrape(proxy)
        if self._info is None:
            return

        # Complementary key-statistics. For now just want 'trailing PEG ratio'
        keys = {"trailingPegRatio"}
        if keys:
            # Simplified the original scrape code for key-statistics. Very expensive for fetching
            # just one value, best if scraping most/all:
            #
            # p = _re.compile(r'root\.App\.main = (.*);')
            # url = 'https://finance.yahoo.com/quote/{}/key-statistics?p={}'.format(self._ticker.ticker, self._ticker.ticker)
            # try:
            #     r = session.get(url, headers=utils.user_agent_headers)
            #     data = _json.loads(p.findall(r.text)[0])
            #     key_stats = data['context']['dispatcher']['stores']['QuoteTimeSeriesStore']["timeSeries"]
            #     for k in keys:
            #         if k not in key_stats or len(key_stats[k])==0:
            #             # Yahoo website prints N/A, indicates Yahoo lacks necessary data to calculate
            #             v = None
            #         else:
            #             # Select most recent (last) raw value in list:
            #             v = key_stats[k][-1]["reportedValue"]["raw"]
            #         self._info[k] = v
            # except Exception:
            #     raise
            #     pass
            #
            # For just one/few variable is faster to query directly:
            url = "https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{}?symbol={}".format(
                self._data.ticker, self._data.ticker)
            for k in keys:
                url += "&type=" + k
            # Request 6 months of data
            start = pd.Timestamp.utcnow().floor("D") - datetime.timedelta(days=365 // 2)
            start = int(start.timestamp())
            end = pd.Timestamp.utcnow().ceil("D")
            end = int(end.timestamp())
            url += f"&period1={start}&period2={end}"

            json_str = self._data.cache_get(url=url, proxy=proxy).text
            json_data = json.loads(json_str)
            key_stats = json_data["timeseries"]["result"][0]
            if k not in key_stats:
                # Yahoo website prints N/A, indicates Yahoo lacks necessary data to calculate
                v = None
            else:
                # Select most recent (last) raw value in list:
                v = key_stats[k][-1]["reportedValue"]["raw"]
            self._info[k] = v

import datetime
import functools
from functools import lru_cache

import pandas as pd
import requests as requests
import re

from frozendict import frozendict

try:
    import ujson as json
except ImportError:
    import json as json

cache_maxsize = 64


def lru_cache_freezeargs(func):
    """
    Decorator transforms mutable dictionary arguments into immutable
    Needed so lru_cache can cache method calls what has dict arguments.
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        args = tuple([frozendict(arg) if isinstance(arg, dict) else arg for arg in args])
        kwargs = {k: frozendict(v) if isinstance(v, dict) else v for k, v in kwargs.items()}
        return func(*args, **kwargs)

    # copy over the lru_cache extra methods to this wrapper to be able to access them
    # after this decorator has been applied
    wrapped.cache_info = func.cache_info
    wrapped.cache_clear = func.cache_clear
    return wrapped


class TickerData:
    """
    Have one place to retrieve data from Yahoo API in order to ease caching and speed up operations
    """
    user_agent_headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    def __init__(self, ticker: str, session=None):
        self._ticker = ticker
        self._session = session or requests

    @lru_cache_freezeargs
    @lru_cache(maxsize=cache_maxsize)
    def get(self, url, user_agent_headers=None, params=None, proxy=None, timeout=30):
        proxy = self._get_proxy(proxy)
        response = self._session.get(
            url=url,
            params=params,
            proxies=proxy,
            timeout=timeout,
            headers=user_agent_headers or self.user_agent_headers)
        return response

    def _get_proxy(self, proxy):
        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, dict) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}
        return proxy

    @lru_cache_freezeargs
    @lru_cache(maxsize=cache_maxsize)
    def get_json_data_stores(self, url, proxy=None):
        '''
        get_json_data_stores returns a python dictionary of the data stores in yahoo finance web page.
        '''
        html = self.get(url=url, proxy=proxy).text

        json_str = html.split('root.App.main =')[1].split(
            '(this)')[0].split(';\n}')[0].strip()
        data = json.loads(json_str)['context']['dispatcher']['stores']

        # return data
        new_data = json.dumps(data).replace('{}', 'null')
        new_data = re.sub(
            r'{[\'|\"]raw[\'|\"]:(.*?),(.*?)}', r'\1', new_data)

        return json.loads(new_data)

    # Note cant use lru_cache as financials_data is a nested dict (freezeargs only handle flat dicts)
    def get_financials_time_series(self, timescale, financials_data, proxy=None):

        acceptable_timestamps = ["annual", "quarterly"]
        if timescale not in acceptable_timestamps:
            raise Exception("timescale '{}' must be one of: {}".format(timescale, acceptable_timestamps))

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

        keys = _finditem1("key", financials_data['FinancialTemplateStore'])

        # Step 2: construct url:
        ts_url_base = "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{0}?symbol={0}".format(
            self._ticker)
        if len(keys) == 0:
            raise Exception("Fetching keys failed")
        url = ts_url_base + "&type=" + ",".join([timescale + k for k in keys])
        # Yahoo returns maximum 4 years or 5 quarters, regardless of start_dt:
        start_dt = datetime.datetime(2016, 12, 31)
        end = (datetime.datetime.now() + datetime.timedelta(days=366))
        url += "&period1={}&period2={}".format(int(start_dt.timestamp()), int(end.timestamp()))

        # Step 3: fetch and reshape data
        json_str = self.get(url=url, proxy=proxy).text
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

        return df

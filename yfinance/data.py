import functools
from functools import lru_cache
from urllib.parse import quote_plus

import requests as requests
from frozendict import frozendict

from . import utils

cache_maxsize = 64


def lru_cache_freezeargs(func):
    """
    Decorator transforms mutable dictionary and list arguments into immutable types
    Needed so lru_cache can cache method calls what has dict or list arguments.
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        args = tuple([frozendict(arg) if isinstance(arg, dict) else arg for arg in args])
        kwargs = {k: frozendict(v) if isinstance(v, dict) else v for k, v in kwargs.items()}
        args = tuple([tuple(arg) if isinstance(arg, list) else arg for arg in args])
        kwargs = {k: tuple(v) if isinstance(v, list) else v for k, v in kwargs.items()}
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
    max_retries = 10

    def __init__(self, ticker: str, session=None):
        self.ticker = ticker
        self._session = session or requests

    def get(self, url, user_agent_headers=None, params=None, proxy=None,
            timeout=30):
        proxy = self._get_proxy(proxy)
        yahoo_cookie = self._get_yahoo_cookie()

        cookies = None

        if yahoo_cookie is not None and params is not None:
            crumb = self._get_yahoo_crumb(cookie=yahoo_cookie)
            params["crumb"] = quote_plus(crumb)
            cookies = {yahoo_cookie.name: yahoo_cookie.value}

        response = self._session.get(
            url=url,
            params=params,
            proxies=proxy,
            timeout=timeout,
            cookies=cookies,
            allow_redirects=True,
            headers=user_agent_headers or self.user_agent_headers)
        return response

    @lru_cache_freezeargs
    @lru_cache(maxsize=cache_maxsize)
    def cache_get(self, url, user_agent_headers=None, params=None, proxy=None, timeout=30):
        return self.get(url, user_agent_headers, params, proxy, timeout)

    def _get_proxy(self, proxy):
        # setup proxy in requests format
        if proxy is not None:
            if isinstance(proxy, (dict, frozendict)) and "https" in proxy:
                proxy = proxy["https"]
            proxy = {"https": proxy}
        return proxy

    def get_raw_json(self, url, user_agent_headers=None, params=None, proxy=None, timeout=30):
        retry = 0
        response = None

        while response is None or (retry < self.max_retries and 400 <= response.status_code):
            response = self.get(url, user_agent_headers=user_agent_headers, params=params,
                                proxy=proxy, timeout=timeout)
            retry += 1

        if 400 <= response.status_code:
            utils.warn_for_status(response)
            return None

        return response.json()

    def _get_yahoo_cookie(self):
        cookie = None

        headers = self.user_agent_headers
        response = requests.get("https://fc.yahoo.com",
                                headers=headers,
                                allow_redirects=True)

        if not response.cookies:
            raise Exception("Failed to obtain Yahoo auth cookie.")

        cookie = list(response.cookies)[0]

        return cookie

    def _get_yahoo_crumb(self, cookie, timeout=30):
        crumb = None

        crumb_response = requests.get(
            "https://query1.finance.yahoo.com/v1/test/getcrumb",
            headers=self.user_agent_headers,
            cookies={cookie.name: cookie.value},
            allow_redirects=True,
            timeout=timeout
        )
        crumb = crumb_response.text

        if crumb is None:
            raise Exception("Failed to retrieve Yahoo crumb.")

        return crumb

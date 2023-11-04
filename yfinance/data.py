import functools
from functools import lru_cache

import logging

import requests as requests
from bs4 import BeautifulSoup
import re
import random
import time

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

    Credit for code for cookie & crumb goes to StackOverflow:
    https://stackoverflow.com/questions/76065035/yahoo-finance-v7-api-now-requiring-cookies-python
    """
    user_agent_headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    def __init__(self, ticker: str, session=None):
        self.ticker = ticker
        self._session = session or requests

        try:
            self._session.cache
        except AttributeError:
            # Not caching
            self._session_is_caching = False
        else:
            # Is caching
            self._session_is_caching = True
        if self._session_is_caching and utils.cookie is None:
            print("!! WARNING: cookie & crumb does not work well with requests_cache. Am using requests_cache.cache_disabled() to ensure cookie & crumb are fresh, but that isn't thread-safe.")

    @utils.log_indent_decorator
    def _get_cookie(self, proxy=None, timeout=30):
        if utils.reuse_cookie and utils.cookie is not None:
            utils.get_yf_logger().debug('reusing cookie')
            return utils.cookie

        # To avoid infinite recursion, do NOT use self.get()

        s = self._get_crumb_session()
        response = s.get(
            url='https://fc.yahoo.com',
            headers=self.user_agent_headers,
            proxies=proxy,
            timeout=timeout)

        if not response.cookies:
            raise Exception("Failed to obtain Yahoo auth cookie.")

        utils.cookie = list(response.cookies)[0]
        utils.get_yf_logger().debug(f"cookie = '{utils.cookie}'")
        return utils.cookie

    @utils.log_indent_decorator
    def _get_crumb_basic(self):#, proxy=None, timeout=30):
        if utils.reuse_crumb and utils.crumb is not None:
            utils.get_yf_logger().debug('reusing crumb')
            return utils.crumb

        s = self._get_crumb_session()

        cookie = self._get_cookie()
        crumb_response = s.get(
                    url="https://query1.finance.yahoo.com/v1/test/getcrumb",
                    headers=self.user_agent_headers,
                    cookies={cookie.name: cookie.value},
            proxies=proxy,
            timeout=timeout)
        utils.crumb = crumb_response.text
        if utils.crumb is None or '<html>' in utils.crumb:
            raise Exception("Failed to fetch crumb")

        utils.get_yf_logger().debug(f"crumb = '{utils.crumb}'")
        return utils.crumb

    @utils.log_indent_decorator
    def _get_crumb_botunit_old(self):#, proxy=None, timeout=30):
        # Credit goes to @bot-unit #1729

        if utils.reuse_crumb and utils.crumb is not None:
            utils.get_yf_logger().debug('reusing crumb')
            return utils.crumb

        # Cookie?
        if (not utils.reuse_cookie) or (utils.cookie is None):
            if self._session_is_caching:
                with self._session.cache_disabled():
                    response = self._session.get('https://guce.yahoo.com/consent', headers=self.user_agent_headers)
            else:
                response = self._session.get('https://guce.yahoo.com/consent', headers=self.user_agent_headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            csrfTokenInput = soup.find('input', attrs={'name': 'csrfToken'})
            csrfToken = csrfTokenInput['value']
            utils.get_yf_logger().debug(f"csrfToken='{csrfToken}'")
            sessionIdInput = soup.find('input', attrs={'name': 'sessionId'})
            sessionId = sessionIdInput['value']
            utils.get_yf_logger().debug(f"sessionId='{sessionId}'")

            originalDoneUrl = 'https://finance.yahoo.com/'
            namespace = 'yahoo'
            data = {
                'agree': ['agree', 'agree'],
                'consentUUID': 'default',
                'sessionId': sessionId,
                'csrfToken': csrfToken,
                'originalDoneUrl': originalDoneUrl,
                'namespace': namespace,
            }
            if self._session_is_caching:
                with self._session.cache_disabled():
                    self._session.post(f'https://consent.yahoo.com/v2/collectConsent?sessionId={sessionId}', data=data, headers=self.user_agent_headers)
                    self._session.get(f'https://guce.yahoo.com/copyConsent?sessionId={sessionId}', headers=self.user_agent_headers)
            else:
                self._session.post(f'https://consent.yahoo.com/v2/collectConsent?sessionId={sessionId}', data=data, headers=self.user_agent_headers)
                self._session.get(f'https://guce.yahoo.com/copyConsent?sessionId={sessionId}', headers=self.user_agent_headers)
            utils.cookie = True
        else:
            utils.get_yf_logger().debug('reusing cookie')

        if self._session_is_caching:
            with self._session.cache_disabled():
                r = self._session.get('https://query2.finance.yahoo.com/v1/test/getcrumb', headers=self.user_agent_headers)
        else:
            r = self._session.get('https://query2.finance.yahoo.com/v1/test/getcrumb', headers=self.user_agent_headers)
        utils.crumb = r.text

        if utils.crumb is None or '<html>' in utils.crumb:
            raise Exception("Failed to fetch crumb")

        utils.get_yf_logger().debug(f"crumb = '{utils.crumb}'")
        return utils.crumb

    @utils.log_indent_decorator
    def _get_cookie_botunit(self):
        if utils.reuse_cookie and utils.cookie is not None:
            utils.get_yf_logger().debug('reusing cookie')
            return

        s = self._get_crumb_session()
        response = s.get('https://guce.yahoo.com/consent', headers=self.user_agent_headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        csrfTokenInput = soup.find('input', attrs={'name': 'csrfToken'})
        csrfToken = csrfTokenInput['value']
        utils.get_yf_logger().debug(f'csrfToken = {csrfToken}')
        sessionIdInput = soup.find('input', attrs={'name': 'sessionId'})
        sessionId = sessionIdInput['value']
        utils.get_yf_logger().debug(f"sessionId='{sessionId}")

        originalDoneUrl = 'https://finance.yahoo.com/'
        namespace = 'yahoo'
        data = {
            'agree': ['agree', 'agree'],
            'consentUUID': 'default',
            'sessionId': sessionId,
            'csrfToken': csrfToken,
            'originalDoneUrl': originalDoneUrl,
            'namespace': namespace,
        }
        s.post(f'https://consent.yahoo.com/v2/collectConsent?sessionId={sessionId}', data=data, headers=self.user_agent_headers)
        s.get(f'https://guce.yahoo.com/copyConsent?sessionId={sessionId}', headers=self.user_agent_headers)
        utils.cookie = True

    @utils.log_indent_decorator
    def _get_crumb_botunit(self, try_count=0):
        if utils.reuse_cookie and utils.cookie is None:
            utils.get_yf_logger().debug('reusing cookie')
            self._get_cookie_botunit()

        s = self._get_crumb_session()
        crumb = s.get('https://query2.finance.yahoo.com/v1/test/getcrumb', headers=self.user_agent_headers).text
        if crumb == '':
            if try_count:
                raise ValueError("_get_crumb_botunit() keeps failing to fetch crumb")
            utils.cookies = False
            self._get_crumb_botunit(try_count+1)

        utils.get_yf_logger().debug(f'crumb = {crumb}')
        utils.crumb = crumb
        utils.crumb_timestamp = time.time()
        return utils.crumb

    @utils.log_indent_decorator
    def _get_crumb(self, proxy=None, timeout=30):
        try:
            return self._get_crumb_botunit_old()
            # return self._get_crumb_botunit()#proxy, timeout)
        except Exception as e:
            if "csrfToken" in str(e):
                # _get_crumb_botunit() fails in USA, but not sure _get_crumb_basic() fixes fetch
                return self._get_crumb_basic()#proxy, timeout)
            else:
                raise

    @utils.log_indent_decorator
    def get(self, url, user_agent_headers=None, params=None, proxy=None, timeout=30):
        utils.get_yf_logger().debug(f'get(): {url}')
        proxy = self._get_proxy(proxy)

        # Add cookie & crumb
        # if cookies is None:
        #     cookie = self._get_cookie()
        #     cookies = {cookie.name: cookie.value}
        # Update: don't need cookie
        if params is None:
            params = {}
        if 'crumb' not in params:
            # Be careful which URLs get a crumb, because crumb breaks some fetches
            if 'finance/quoteSummary' in url:
                params['crumb'] = self._get_crumb()

        response = self._session.get(
            url=url,
            params=params,
            proxies=proxy,
            timeout=timeout,
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
        utils.get_yf_logger().debug(f'get_raw_json(): {url}')
        response = self.get(url, user_agent_headers=user_agent_headers, params=params, proxy=proxy, timeout=timeout)
        response.raise_for_status()
        return response.json()

import functools
from functools import lru_cache

import logging

import requests as requests
from bs4 import BeautifulSoup
import re
import random
import time
import pickle
import os

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
            utils.print_once("!! WARNING: cookie & crumb does not work well with requests_cache. Am using requests_cache.cache_disabled() to ensure cookie & crumb are fresh, but that isn't thread-safe.")

    @utils.log_indent_decorator
    def _save_session_cookies(self):
        fn = os.path.join(utils._DBManager.get_location(), 'session-cookies.pkl')
        try:
            with open(fn, 'wb') as file:
                pickle.dump(self._session.cookies, file)
            return True
        except:
            return False
    @utils.log_indent_decorator
    def _load_session_cookies(self):
        fn = os.path.join(utils._DBManager.get_location(), 'session-cookies.pkl')
        if os.path.exists(fn):
            try:
                with open(fn, 'rb') as file:
                    cookies = pickle.load(file)
                    self._session.cookies.update(cookies)
                    return True
            except:
                return False
        return False

    def _save_cookie_basic(self, cookie):
        fn = os.path.join(utils._DBManager.get_location(), 'cookie.pkl')
        try:
            with open(fn, 'wb') as file:
                pickle.dump(cookie, file)
            return True
        except:
            return False
    def _load_cookie_basic(self):
        fn = os.path.join(utils._DBManager.get_location(), 'cookie.pkl')
        if os.path.exists(fn):
            try:
                with open(fn, 'rb') as file:
                    cookie = pickle.load(file)
                if cookie == '':
                    cookie = None
                return cookie
            except:
                return None
        return None

    @utils.log_indent_decorator
    def _get_cookie_basic(self, proxy=None, timeout=30):
        if utils.reuse_cookie and utils.cookie is not None:
            utils.get_yf_logger().debug('reusing cookie')
            return utils.cookie

        utils.cookie = self._load_cookie_basic()
        if utils.cookie is not None:
            return utils.cookie

        # To avoid infinite recursion, do NOT use self.get()
        # - 'allow_redirects' copied from @psychoz971 solution - does it help USA?
        response = self._session.get(
            url='https://fc.yahoo.com',
            headers=self.user_agent_headers,
            proxies=proxy,
            timeout=timeout,
            allow_redirects=True)

        if not response.cookies:
            return None
        utils.cookie = list(response.cookies)[0]
        if utils.cookie == '':
            return None
        utils.get_yf_logger().debug(f"cookie = '{utils.cookie}'")
        return utils.cookie

    @utils.log_indent_decorator
    def _get_crumb_basic(self, proxy=None, timeout=30):
        if utils.reuse_crumb and utils.crumb is not None:
            utils.get_yf_logger().debug('reusing crumb')
            return utils.crumb

        cookie = self._get_cookie_basic()
        if cookie is None:
            return None

        # - 'allow_redirects' copied from @psychoz971 solution - does it help USA?
        if self._session_is_caching:
            with self._session.cache_disabled():
                crumb_response = self._session.get(
                    url="https://query1.finance.yahoo.com/v1/test/getcrumb",
                    headers=self.user_agent_headers,
                    cookies={cookie.name: cookie.value},
                    proxies=proxy,
                    timeout=timeout,
                    allow_redirects=True)
        else:
            crumb_response = self._session.get(
                url="https://query1.finance.yahoo.com/v1/test/getcrumb",
                headers=self.user_agent_headers,
                cookies={cookie.name: cookie.value},
                proxies=proxy,
                timeout=timeout,
                allow_redirects=True)
        utils.crumb = crumb_response.text
        if utils.crumb is None or '<html>' in utils.crumb:
            return None

        utils.get_yf_logger().debug(f"crumb = '{utils.crumb}'")
        return utils.crumb

    @utils.log_indent_decorator
    def _get_cookie_botunit(self, proxy, timeout):
        if utils.reuse_cookie and utils.cookie is not None:
            utils.get_yf_logger().debug('reusing cookie')
            return True

        elif self._load_session_cookies():
            utils.get_yf_logger().debug('reusing persistent cookie')
            utils.cookie = True
            return True

        if self._session_is_caching:
            with self._session.cache_disabled():
                response = self._session.get(
                                    url='https://guce.yahoo.com/consent', 
                                    headers=self.user_agent_headers,
                                    proxies=proxy,
                                    timeout=timeout)
        else:
            response = self._session.get(
                                url='https://guce.yahoo.com/consent', 
                                headers=self.user_agent_headers,
                                proxies=proxy,
                                timeout=timeout)

        soup = BeautifulSoup(response.content, 'html.parser')
        csrfTokenInput = soup.find('input', attrs={'name': 'csrfToken'})
        if csrfTokenInput is None:
            return False
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
        if self._session_is_caching:
            with self._session.cache_disabled():
                self._session.post(
                            url=f'https://consent.yahoo.com/v2/collectConsent?sessionId={sessionId}',
                            data=data,
                            headers=self.user_agent_headers,
                            proxies=proxy,
                            timeout=timeout)
                self._session.get(
                            url=f'https://guce.yahoo.com/copyConsent?sessionId={sessionId}', 
                            data=data,
                            headers=self.user_agent_headers,
                            proxies=proxy,
                            timeout=timeout)
        else:
            self._session.post(
                        url=f'https://consent.yahoo.com/v2/collectConsent?sessionId={sessionId}',
                        data=data,
                        headers=self.user_agent_headers,
                        proxies=proxy,
                        timeout=timeout)
            self._session.get(
                        url=f'https://guce.yahoo.com/copyConsent?sessionId={sessionId}',
                        headers=self.user_agent_headers,
                        proxies=proxy,
                        timeout=timeout)
        utils.cookie = True
        return True

    @utils.log_indent_decorator
    def _get_crumb_botunit(self, proxy=None, timeout=30):
        # Credit goes to @bot-unit #1729

        if utils.reuse_crumb and utils.crumb is not None:
            utils.get_yf_logger().debug('reusing crumb')
            return utils.crumb

        if not self._get_cookie_botunit(proxy, timeout):
            # This cookie stored in session
            return None

        if self._session_is_caching:
            with self._session.cache_disabled():
                r = self._session.get(
                            url='https://query2.finance.yahoo.com/v1/test/getcrumb', 
                            headers=self.user_agent_headers,
                            proxies=proxy,
                            timeout=timeout)
        else:
            r = self._session.get(
                        url='https://query2.finance.yahoo.com/v1/test/getcrumb', 
                        headers=self.user_agent_headers,
                        proxies=proxy,
                        timeout=timeout)
        utils.crumb = r.text

        if utils.crumb is None or '<html>' in utils.crumb or utils.crumb == '':
            return None

        utils.get_yf_logger().debug(f"crumb = '{utils.crumb}'")
        return utils.crumb

    @utils.log_indent_decorator
    def _get_cookie_and_crumb(self, proxy=None, timeout=30):
        cookie, crumb = None, None

        crumb = self._get_crumb_botunit()
        if crumb is not None:
            self._save_session_cookies()
        else:
            # Fallback strategy
            cookie = self._get_cookie_basic(proxy, timeout)
            crumb = self._get_crumb_basic(proxy, timeout)
            self._save_cookie_basic(cookie)

        return cookie, crumb

    @utils.log_indent_decorator
    def get(self, url, user_agent_headers=None, params=None, proxy=None, timeout=30):
        if len(url) > 200:
            utils.get_yf_logger().debug(f'get(): {url[:200]}...')
        else:
            utils.get_yf_logger().debug(f'get(): {url}')
        proxy = self._get_proxy(proxy)

        if params is None:
            params = {}
        if 'crumb' in params:
            raise Exception("Don't manually add 'crumb' to params dict, let data.py handle it")

        # Be careful which URLs get a crumb, because crumb breaks some fetches
        cookie, crumb = None, None
        if 'finance/quoteSummary' in url:
            cookie, crumb = self._get_cookie_and_crumb()
        if crumb is not None:
            params['crumb'] = crumb
        if cookie is not None:
            # USA fallback adds cookie to GET parameters
            cookies = {cookie.name: cookie.value}
        else:
            cookies = None

        response = self._session.get(
            url=url,
            params=params,
            cookies=cookies,
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

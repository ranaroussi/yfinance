"""
Tests for YfData sessions
"""

from tests.context import yfinance as yf
from yfinance.exceptions import YFDataException

import curl_cffi
import requests
from curl_adapter import CurlCffiAdapter

import unittest

class TestData(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        # yf.enable_debug_mode()
        # cls.session = session_gbl
        cls.ticker = 'AMD'

    # @classmethod
    # def tearDownClass(cls):
    #     if cls.session is not None:
    #         cls.session.close()

    def test_curlCffi(self):
        session = curl_cffi.requests.Session(impersonate="chrome")

        dat = yf.Ticker(self.ticker, session=session)

        df = dat.history(period='1mo')
        self.assertIsNotNone(df)
        self.assertGreater(len(df), 1)

        df = dat.history(period='1mo')
        self.assertIsNotNone(df)
        self.assertGreater(len(df), 1)

    def test_requestsWithCurl(self):
        session = requests.Session()
        session.mount("http://", CurlCffiAdapter())
        session.mount("https://", CurlCffiAdapter())

        dat = yf.Ticker(self.ticker, session=session)

        df = dat.history(period='1mo')
        self.assertIsNotNone(df)
        self.assertGreater(len(df), 1)

        df = dat.history(period='1mo')
        self.assertIsNotNone(df)
        self.assertGreater(len(df), 1)

    def test_cookie_strat_switch(self):
        session = curl_cffi.requests.Session(impersonate="chrome")
        dat = yf.Ticker(self.ticker, session=session)
        dat._data._set_cookie_strategy('csrf')
        dat._data._set_cookie_strategy('basic')
        dat._data._set_cookie_strategy('csrf')
        dat._data._set_cookie_strategy('basic')
        
        session = requests.Session()
        session.mount("http://", CurlCffiAdapter())
        session.mount("https://", CurlCffiAdapter())
        dat = yf.Ticker(self.ticker, session=session)
        dat._data._n_strategy_flips = 0
        dat._data._set_cookie_strategy('csrf')
        dat._data._set_cookie_strategy('basic')
        dat._data._set_cookie_strategy('csrf')
        dat._data._set_cookie_strategy('basic')

    def test_cookie_csrf_strategy(self):
        session = curl_cffi.requests.Session(impersonate="chrome")
        dat = yf.Ticker(self.ticker, session=session)
        dat._data._set_cookie_strategy('csrf')
        df = dat.history(period='1mo')
        
        session = requests.Session()
        session.mount("http://", CurlCffiAdapter())
        session.mount("https://", CurlCffiAdapter())
        dat = yf.Ticker(self.ticker, session=session)
        dat._data._n_strategy_flips = 0
        dat._data._set_cookie_strategy('csrf')
        df = dat.history(period='1mo')

    def test_requestsWithoutCurlRaise(self):
        session = requests.Session()

        # One of these functions below should raise this exception:
        with self.assertRaises(YFDataException) as context:
            dat = yf.Ticker(self.ticker, session=session)
            df = dat.history(period='1mo')

        self.assertIn("curl", str(context.exception).lower())  # Optional: check message content

    def test_requestsWithCurlAndRateLimiter(self):
        ReqSession = requests.Session
        from requests_ratelimiter import LimiterMixin
        from pyrate_limiter import Duration, RequestRate, Limiter
        class LimiterSession(LimiterMixin, ReqSession):
            """Session class with cURL adapter and rate-limiting."""
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.mount("http://", CurlCffiAdapter())
                self.mount("https://", CurlCffiAdapter())
        limiter = Limiter(RequestRate(1, 5 * Duration.SECOND))
        session = LimiterSession(limiter=limiter)

        dat = yf.Ticker(self.ticker, session=session)

        df = dat.history(period='1mo')
        self.assertIsNotNone(df)
        self.assertGreater(len(df), 1)

        df = dat.history(period='1mo')
        self.assertIsNotNone(df)
        self.assertGreater(len(df), 1)

from unittest.mock import patch
from requests.exceptions import HTTPError
class TestDataWithBlock(unittest.TestCase):
    def setUp(self):
        self.blocked_url = "https://fc.yahoo.com"

    def send_with_block_check(self, original_send, request, **kwargs):
        if self.blocked_url_fragment in request.url:
            raise HTTPError(f"Blocked URL: {request.url}")
        return original_send(request, **kwargs)

    def test_requestsWithCurl_blocked_url(self):
        # Create real session with adapters
        session = requests.Session()
        session.mount("http://", CurlCffiAdapter())
        session.mount("https://", CurlCffiAdapter())

        # Save unpatched version of send
        real_send = requests.sessions.Session.send

        def send_with_block_check(self_obj, request, **kwargs):
            if self.blocked_url_fragment in request.url:
                raise HTTPError(f"Blocked URL: {request.url}")
            return real_send(self_obj, request, **kwargs)

        yf.enable_debug_mode()
        with patch('requests.sessions.Session.send', new=send_with_block_check):
            dat = yf.Ticker('AAPL', session=session)
            df = dat.history(period='1mo')
            print(df.shape)

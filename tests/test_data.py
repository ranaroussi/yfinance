"""Tests for yfinance.data internal helpers."""
import unittest
from functools import lru_cache

from yfinance._http import new_session
from yfinance.data import SingletonMeta, YfData, _normalize_proxy, lru_cache_freezeargs
from yfinance.exceptions import YFDataException
from yfinance.utils import frozendict


class TestProxyConfig(unittest.TestCase):
    def test_proxy_string_applies_to_http_and_https(self):
        proxy = "http://user:pass@example.com:8080"

        self.assertEqual(
            _normalize_proxy(proxy),
            {"http": proxy, "https": proxy},
        )

    def test_proxy_mapping_is_preserved(self):
        proxy = {"http": "http://proxy:8080", "https": "https://proxy:8443"}

        self.assertIs(_normalize_proxy(proxy), proxy)

    def test_proxy_none_is_preserved(self):
        self.assertIsNone(_normalize_proxy(None))


class TestFrozenDict(unittest.TestCase):
    def test_is_dict_subclass(self):
        d = frozendict({"a": 1, "b": 2})
        self.assertEqual(d["a"], 1)
        self.assertEqual(d.get("b"), 2)
        self.assertEqual(set(d.keys()), {"a", "b"})
        self.assertIsInstance(d, dict)

    def test_is_hashable(self):
        d1 = frozendict({"a": 1, "b": 2})
        d2 = frozendict({"b": 2, "a": 1})
        self.assertEqual(hash(d1), hash(d2))
        self.assertEqual(len({d1, d2}), 1)

    def test_mutation_raises(self):
        d = frozendict({"a": 1})
        with self.assertRaises(TypeError):
            d["b"] = 2
        with self.assertRaises(TypeError):
            del d["a"]
        with self.assertRaises(AttributeError):
            d.pop("a")
        with self.assertRaises(AttributeError):
            d.clear()
        with self.assertRaises(AttributeError):
            d.update({"x": 9})

    def test_lru_cache_integration(self):
        call_count = {"n": 0}

        @lru_cache_freezeargs
        @lru_cache(maxsize=8)
        def fn(params):
            call_count["n"] += 1
            return sum(params.values())

        self.assertEqual(fn({"a": 1, "b": 2}), 3)
        self.assertEqual(fn({"a": 1, "b": 2}), 3)
        self.assertEqual(call_count["n"], 1)
        self.assertEqual(fn({"a": 1, "b": 3}), 4)
        self.assertEqual(call_count["n"], 2)


class TestYfDataLoginCookies(unittest.TestCase):
    def test_set_login_cookies_invalidates_crumb(self):
        # Logging in (or switching accounts) must drop a crumb minted under a
        # different login state so the next request re-mints a matched crumb.
        # Use a throwaway instance (YfData is a singleton) to avoid leaking the
        # test cookies into the shared session other tests rely on.
        SingletonMeta._instances.pop(YfData, None)
        try:
            data = YfData()
            data._crumb = "stale-crumb-sentinel"
            data.set_login_cookies("T-test-value", "Y-test-value")
            self.assertIsNone(data._crumb)
        finally:
            SingletonMeta._instances.pop(YfData, None)


class TestCookieStrategyLoginPreservation(unittest.TestCase):
    """The csrf->basic strategy toggle must not wipe user-set login cookies.

    Uses a throwaway YfData (the class is a SingletonMeta singleton) so the test
    cookies / login flag don't leak into the shared session other tests use.
    """

    def setUp(self):
        SingletonMeta._instances.pop(YfData, None)
        self.data = YfData()
        self.data._cookie_strategy = 'csrf'
        self.data._cookie = None
        self.data._crumb = None
        self.data._logged_in = False
        self.data._session.cookies.clear()
        self.data._session.cookies.update({"T": "tval", "Y": "yval", "A3": "a3val"})

    def tearDown(self):
        SingletonMeta._instances.pop(YfData, None)

    def test_logged_in_switch_keeps_login_cookies(self):
        self.data._logged_in = True
        self.data._set_cookie_strategy('basic')  # csrf->basic == the clearing branch
        self.assertEqual(self.data._session.cookies.get("T"), "tval")
        self.assertEqual(self.data._session.cookies.get("Y"), "yval")

    def test_logged_out_switch_still_clears_jar(self):
        # Not logged in: the toggle clears as before, so a bad/anonymous session
        # can recover a fresh anonymous cookie.
        self.data._logged_in = False
        self.data._set_cookie_strategy('basic')
        self.assertIsNone(self.data._session.cookies.get("T"))
        self.assertIsNone(self.data._session.cookies.get("Y"))

    def test_switch_resets_crumb_even_when_logged_in(self):
        # The anonymous cookie/crumb refresh path must stay intact.
        self.data._logged_in = True
        self.data._crumb = "stale-crumb"
        self.data._set_cookie_strategy('basic')
        self.assertIsNone(self.data._crumb)

    def test_set_login_cookies_marks_logged_in(self):
        self.data._logged_in = False
        self.data.set_login_cookies("T-v", "Y-v")
        self.assertTrue(self.data._logged_in)

    def test_optimistic_login_survives_strategy_toggle(self):
        # set_login_cookies marks _logged_in True optimistically; a strategy
        # toggle that fires before check_login confirms (e.g. the subscriptions
        # request itself 4xx-ing) must still keep the freshly-set T/Y.
        self.data._session.cookies.clear()
        self.data.set_login_cookies("T-opt", "Y-opt")  # -> _logged_in = True
        self.data._cookie_strategy = 'csrf'
        self.data._set_cookie_strategy('basic')
        self.assertEqual(self.data._session.cookies.get("T"), "T-opt")
        self.assertEqual(self.data._session.cookies.get("Y"), "Y-opt")


class TestSetSessionCacheDetection(unittest.TestCase):
    """A session is caching only when a cache is active, not when `.cache` merely exists.

    curl_cffi >= 0.16 Sessions always expose `.cache` (None when disabled).
    """

    def setUp(self):
        SingletonMeta._instances.pop(YfData, None)
        self.data = YfData()

    def tearDown(self):
        SingletonMeta._instances.pop(YfData, None)

    def test_session_with_disabled_cache_is_accepted(self):
        session = new_session()
        session.cache = None
        self.data._set_session(session)
        self.assertIs(self.data._session, session)

    def test_session_with_active_cache_is_rejected(self):
        session = new_session()
        session.cache = object()
        with self.assertRaises(YFDataException):
            self.data._set_session(session)


if __name__ == "__main__":
    unittest.main()

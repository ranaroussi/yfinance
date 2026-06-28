"""Tests for the HTTP backend abstraction.

Verifies the fallback to plain ``requests`` when ``curl_cffi`` is
unavailable, and the ``YF_DISABLE_CURL_CFFI`` env-var opt-out.
"""
import importlib
import os
import sys
import unittest


def _reload_http(curl_cffi_available: bool, disable_env: bool):
    """Reload ``yfinance._http`` simulating curl_cffi presence / opt-out."""
    saved_modules = {k: sys.modules[k] for k in list(sys.modules) if k == "yfinance._http"}
    saved_env = os.environ.get("YF_DISABLE_CURL_CFFI")
    if disable_env:
        os.environ["YF_DISABLE_CURL_CFFI"] = "1"
    elif "YF_DISABLE_CURL_CFFI" in os.environ:
        del os.environ["YF_DISABLE_CURL_CFFI"]

    saved_curl = sys.modules.get("curl_cffi")
    if not curl_cffi_available:
        sys.modules["curl_cffi"] = None  # type: ignore[assignment]

    try:
        sys.modules.pop("yfinance._http", None)
        return importlib.import_module("yfinance._http")
    finally:
        # Restore originals so other tests aren't affected.
        if saved_curl is not None:
            sys.modules["curl_cffi"] = saved_curl
        elif not curl_cffi_available:
            sys.modules.pop("curl_cffi", None)
        if saved_env is None:
            os.environ.pop("YF_DISABLE_CURL_CFFI", None)
        else:
            os.environ["YF_DISABLE_CURL_CFFI"] = saved_env
        sys.modules.pop("yfinance._http", None)
        for k, v in saved_modules.items():
            sys.modules[k] = v


class TestHttpBackend(unittest.TestCase):

    def test_fallback_when_curl_cffi_missing(self):
        import requests as _stdlib_requests
        mod = _reload_http(curl_cffi_available=False, disable_env=False)
        self.assertFalse(mod.HAS_CURL_CFFI)
        session = mod.new_session()
        self.assertIsInstance(session, _stdlib_requests.Session)
        self.assertIn("Chrome", session.headers["User-Agent"])

    def test_disable_env_var_forces_fallback(self):
        import requests as _stdlib_requests
        mod = _reload_http(curl_cffi_available=True, disable_env=True)
        self.assertFalse(mod.HAS_CURL_CFFI)
        self.assertIsInstance(mod.new_session(), _stdlib_requests.Session)

    def test_cookie_jar_works_for_requests_session(self):
        mod = _reload_http(curl_cffi_available=False, disable_env=False)
        session = mod.new_session()
        jar = mod.cookie_jar(session)
        # requests.Session.cookies subclasses CookieJar directly.
        from http.cookiejar import CookieJar
        self.assertIsInstance(jar, CookieJar)

    def test_is_supported_session_accepts_requests_session(self):
        import requests as _stdlib_requests
        mod = _reload_http(curl_cffi_available=False, disable_env=False)
        self.assertTrue(mod.is_supported_session(_stdlib_requests.Session()))
        self.assertFalse(mod.is_supported_session(object()))

    def test_yfdata_rejects_caching_sessions(self):
        from yfinance.data import YfData
        from yfinance.exceptions import YFDataException

        class CachingSession:
            cache = object()

        with self.assertRaisesRegex(YFDataException, "requests_cache.CachedSession"):
            YfData(session=CachingSession())


if __name__ == "__main__":
    unittest.main()

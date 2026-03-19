"""
Tests for cache

To run all tests in suite from commandline:
   python -m unittest tests.cache

Specific test class:
   python -m unittest tests.cache.TestCache

"""
import os
import unittest

from tests.context import yfinance as yf


class TestCacheNoPermission(unittest.TestCase):
    """Verify cache fallback behavior for unwritable locations."""

    @classmethod
    def setUpClass(cls):
        """Point the cache at an unwritable location."""
        if os.name == "nt":  # Windows
            cls.cache_path = "C:\\Windows\\System32\\yf-cache"
        else:  # Unix/Linux/MacOS
            # Use a writable directory
            cls.cache_path = "/yf-cache"
        yf.set_tz_cache_location(cls.cache_path)

    def test_tz_cache_root_store(self):
        """Failed writes should replace the cache with a dummy cache."""
        # Test that if cache path in read-only filesystem, no exception.
        ticker_symbol = 'AMZN'
        timezone_name = "America/New_York"

        # During attempt to store, will discover cannot write
        cache = yf.cache.get_tz_cache()
        assert cache is not None
        cache.store("AMZN", timezone_name)

        # Handling the store failure replaces cache with a dummy
        cache = yf.cache.get_tz_cache()
        assert cache is not None
        self.assertTrue(cache.dummy)
        cache.store(ticker_symbol, timezone_name)

    def test_tz_cache_root_lookup(self):
        """Failed reads should replace the cache with a dummy cache."""
        # Test that if cache path in read-only filesystem, no exception.
        tkr = 'AMZN'
        # During attempt to lookup, will discover cannot write
        cache = yf.cache.get_tz_cache()
        assert cache is not None
        cache.lookup(tkr)

        # Handling the lookup failure replaces cache with a dummy
        cache = yf.cache.get_tz_cache()
        assert cache is not None
        self.assertTrue(cache.dummy)
        cache.lookup(tkr)

if __name__ == '__main__':
    unittest.main()

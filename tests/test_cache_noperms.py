"""
Tests for cache

To run all tests in suite from commandline:
   python -m unittest tests.cache

Specific test class:
   python -m unittest tests.cache.TestCache

"""
from tests.context import yfinance as yf

import unittest
import os


class TestCacheNoPermission(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.name == "nt":  # Windows
            cls.cache_path = "C:\\Windows\\System32\\yf-cache"
        else:  # Unix/Linux/MacOS
            # Use a writable directory
            cls.cache_path = "/yf-cache"
        yf.set_tz_cache_location(cls.cache_path)

    def test_tzCacheRootStore(self):
        # Test that if cache path in read-only filesystem, no exception.
        tkr = 'AMZN'
        tz1 = "America/New_York"

        # During attempt to store, will discover cannot write
        yf.cache.get_tz_cache().store(tkr, tz1)

        # Handling the store failure replaces cache with a dummy
        cache = yf.cache.get_tz_cache()
        self.assertTrue(cache.dummy)
        cache.store(tkr, tz1)

    def test_tzCacheRootLookup(self):
        # Test that if cache path in read-only filesystem, no exception.
        tkr = 'AMZN'
        # During attempt to lookup, will discover cannot write
        yf.cache.get_tz_cache().lookup(tkr)

        # Handling the lookup failure replaces cache with a dummy
        cache = yf.cache.get_tz_cache()
        self.assertTrue(cache.dummy)
        cache.lookup(tkr)

if __name__ == '__main__':
    unittest.main()

"""
Tests for utils

To run all tests in suite from commandline:
   python -m unittest tests.utils

Specific test class:
   python -m unittest tests.utils.TestTicker

"""
# import pandas as pd
# import numpy as np

from .context import yfinance as yf
from .context import session_gbl

import unittest
# import requests_cache
import tempfile
import os


class TestCache(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempCacheDir = tempfile.TemporaryDirectory()
        yf.set_tz_cache_location(cls.tempCacheDir.name)

    @classmethod
    def tearDownClass(cls):
        cls.tempCacheDir.cleanup()

    def test_storeTzNoRaise(self):
        # storing TZ to cache should never raise exception
        tkr = 'AMZN'
        tz1 = "America/New_York"
        tz2 = "London/Europe"
        cache = yf.utils.get_tz_cache()
        cache.store(tkr, tz1)
        cache.store(tkr, tz2)

    def test_setTzCacheLocation(self):
        self.assertEqual(yf.utils._DBManager.get_location(), self.tempCacheDir.name)

        tkr = 'AMZN'
        tz1 = "America/New_York"
        cache = yf.utils.get_tz_cache()
        cache.store(tkr, tz1)

        self.assertTrue(os.path.exists(os.path.join(self.tempCacheDir.name, "tkr-tz.db")))


class TestCacheNoPermission(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        yf.set_tz_cache_location("/root/yf-cache")

    def test_tzCacheRootStore(self):
        # Test that if cache path in read-only filesystem, no exception.
        tkr = 'AMZN'
        tz1 = "America/New_York"

        # During attempt to store, will discover cannot write
        yf.utils.get_tz_cache().store(tkr, tz1)

        # Handling the store failure replaces cache with a dummy
        cache = yf.utils.get_tz_cache()
        self.assertTrue(cache.dummy)
        cache.store(tkr, tz1)

    def test_tzCacheRootLookup(self):
        # Test that if cache path in read-only filesystem, no exception.
        tkr = 'AMZN'
        # During attempt to lookup, will discover cannot write
        yf.utils.get_tz_cache().lookup(tkr)

        # Handling the lookup failure replaces cache with a dummy
        cache = yf.utils.get_tz_cache()
        self.assertTrue(cache.dummy)
        cache.lookup(tkr)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestCache('Test cache'))
    suite.addTest(TestCacheNoPermission('Test cache no permission'))
    return suite


if __name__ == '__main__':
    unittest.main()

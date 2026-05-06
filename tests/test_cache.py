from tests.context import yfinance as yf

import unittest
import tempfile
import os


class TestCache(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original_cache_dir = yf.cache._TzDBManager.get_location()
        cls.tempCacheDir = tempfile.TemporaryDirectory()
        yf.set_tz_cache_location(cls.tempCacheDir.name)

    @classmethod
    def tearDownClass(cls):
        yf.cache._TzDBManager.close_db()
        yf.cache._TzCacheManager._tz_cache = None
        yf.cache._CookieCacheManager._Cookie_cache = None
        yf.cache._ISINCacheManager._isin_cache = None
        cls.tempCacheDir.cleanup()
        yf.set_tz_cache_location(cls.original_cache_dir)

    def test_storeTzNoRaise(self):
        # storing TZ to cache should never raise exception
        tkr = 'AMZN'
        tz1 = "America/New_York"
        tz2 = "London/Europe"
        cache = yf.cache.get_tz_cache()
        cache.store(tkr, tz1)
        cache.store(tkr, tz2)

    def test_setTzCacheLocation(self):
        self.assertEqual(yf.cache._TzDBManager.get_location(), self.tempCacheDir.name)

        tkr = 'AMZN'
        tz1 = "America/New_York"
        cache = yf.cache.get_tz_cache()
        cache.store(tkr, tz1)

        self.assertTrue(os.path.exists(os.path.join(self.tempCacheDir.name, "tkr-tz.db")))


if __name__ == '__main__':
    unittest.main()

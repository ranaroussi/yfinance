"""
Tests for cache

To run all tests in suite from commandline:
   python -m unittest tests.cache

Specific test class:
   python -m unittest tests.cache.TestCache

"""
import os
import shutil
import tempfile
import unittest

from tests.context import yfinance as yf


class TestCache(unittest.TestCase):
    """Verify timezone cache behavior."""

    @classmethod
    def setUpClass(cls):
        """Use a temporary cache directory for tests."""
        manager = getattr(yf.cache, "_TzDBManager")
        cls._original_cache_dir = manager.get_location()
        cls.temp_cache_dir = tempfile.mkdtemp()
        yf.set_tz_cache_location(cls.temp_cache_dir)

    @classmethod
    def tearDownClass(cls):
        """Restore the original cache location and clean up."""
        # Restore original cache location before deleting the temp dir so that
        # subsequent tests don't inherit a stale (closed/deleted) DB connection.
        yf.set_tz_cache_location(cls._original_cache_dir)
        shutil.rmtree(cls.temp_cache_dir, ignore_errors=True)

    def test_store_tz_no_raise(self):
        """Storing timezones should not raise."""
        # storing TZ to cache should never raise exception
        symbol = 'AMZN'
        first_timezone = "America/New_York"
        second_timezone = "London/Europe"
        cache = yf.cache.get_tz_cache()
        assert cache is not None
        cache.store(symbol, first_timezone)
        cache.store(symbol, second_timezone)

    def test_set_tz_cache_location(self):
        """Changing the cache directory should update the backing store."""
        manager = getattr(yf.cache, "_TzDBManager")
        self.assertEqual(manager.get_location(), self.temp_cache_dir)

        ticker_symbol = 'AMZN'
        timezone_name = "America/New_York"
        cache = yf.cache.get_tz_cache()
        assert cache is not None
        cache.store(ticker_symbol, timezone_name)

        self.assertTrue(os.path.exists(os.path.join(self.temp_cache_dir, "tkr-tz.db")))


if __name__ == '__main__':
    unittest.main()

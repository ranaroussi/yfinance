"""
Tests for utils

To run all tests in suite from commandline:
   python -m unittest tests.utils

Specific test class:
   python -m unittest tests.utils.TestTicker

"""
from datetime import datetime
from unittest import TestSuite

import pandas as pd
# import numpy as np

from tests.context import yfinance as yf

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


class TestCacheNoPermission(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        yf.set_tz_cache_location("/root/yf-cache")

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


class TestPandas(unittest.TestCase):
    date_strings = ["2024-08-07 09:05:00+02:00", "2024-08-07 09:05:00-04:00"]

    @unittest.expectedFailure
    def test_mixed_timezones_to_datetime_fails(self):
        series = pd.Series(self.date_strings)
        series = series.map(pd.Timestamp)
        converted = pd.to_datetime(series)
        self.assertIsNotNone(converted[0].tz)

    def test_mixed_timezones_to_datetime(self):
        series = pd.Series(self.date_strings)
        series = series.map(pd.Timestamp)
        converted = pd.to_datetime(series, utc=True)
        self.assertIsNotNone(converted[0].tz)
        i = 0
        for dt in converted:
            dt: datetime
            ts: pd.Timestamp = series[i]
            self.assertEqual(dt.isoformat(), ts.tz_convert(tz="UTC").isoformat())
            i += 1


def suite():
    ts: TestSuite = unittest.TestSuite()
    ts.addTest(TestCache('Test cache'))
    ts.addTest(TestCacheNoPermission('Test cache no permission'))
    ts.addTest(TestPandas("Test pandas"))
    return ts


if __name__ == '__main__':
    unittest.main()

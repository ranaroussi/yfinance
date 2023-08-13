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


class TestUtils(unittest.TestCase):
    session = None

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


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestUtils('Test utils'))
    return suite


if __name__ == '__main__':
    unittest.main()

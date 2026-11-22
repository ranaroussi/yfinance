"""
Tests that deprecation warnings point at the caller's code.

Python's default warning filter only shows a DeprecationWarning when it is
attributed to __main__. Without stacklevel=2 the warning is attributed to
yfinance's own module, so users running a script never see it.

To run from commandline:
   python -m unittest tests.test_deprecation_warnings
"""
import unittest
import warnings

import yfinance as yf
from yfinance import utils


class TestDeprecationWarningStacklevel(unittest.TestCase):
    def _assert_points_here(self, caught):
        deprecations = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertTrue(deprecations, "expected a DeprecationWarning")
        for w in deprecations:
            # Warning must be attributed to the calling file (this test), not yfinance internals
            self.assertEqual(w.filename, __file__)

    def test_set_config_proxy(self):
        old = yf.config.network.proxy
        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                yf.set_config(proxy=old)
            self._assert_points_here(caught)
        finally:
            yf.config.network.proxy = old

    def test_set_config_retries(self):
        old = yf.config.network.retries
        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                yf.set_config(retries=old)
            self._assert_points_here(caught)
        finally:
            yf.config.network.retries = old

    def test_enable_debug_mode(self):
        try:
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                yf.enable_debug_mode()
            self._assert_points_here(caught)
        finally:
            utils._disable_debug_mode()


if __name__ == '__main__':
    unittest.main()

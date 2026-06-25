"""Tests for yfinance.scrapers.funds error handling."""
import unittest
from unittest.mock import MagicMock, patch

from yfinance.config import YfConfig
from yfinance.scrapers.funds import FundsData


class TestFundsDataErrorHandling(unittest.TestCase):
    def setUp(self):
        self._orig_hide = YfConfig.debug.hide_exceptions
        YfConfig.debug.hide_exceptions = True

    def tearDown(self):
        YfConfig.debug.hide_exceptions = self._orig_hide

    def test_no_fund_data_does_not_raise_unbound_local(self):
        # Yahoo's quoteSummary endpoint returns {"quoteSummary": {"result": None}}
        # for symbols that carry no fund data (e.g. a plain equity ticker).
        # Indexing result["quoteSummary"]["result"][0] then raises TypeError,
        # not KeyError, so it lands in the broad `except Exception` handler.
        # That handler logged `data`, a name that is never bound when the
        # indexing itself fails, so it raised UnboundLocalError and masked the
        # original error instead of logging the response.
        fd = FundsData(MagicMock(), "AAPL")
        no_fund_response = {"quoteSummary": {"result": None}}

        with patch.object(fd, "_fetch", return_value=no_fund_response):
            # Should swallow-and-log the parse failure, never leak
            # UnboundLocalError out of the error handler.
            fd._fetch_and_parse()

        self.assertIsNone(fd._quote_type)


if __name__ == "__main__":
    unittest.main()

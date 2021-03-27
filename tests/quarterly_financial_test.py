import data.valid
import unittest
from unittest.mock import Mock
from unittest.mock import patch
import sys
sys.path.append("..")
from yfinance import ticker

class TickerMock(ticker.Ticker):
    def _get_fundamentals(self, proxy=None):
        self._financials['quarterly'] = data.valid.msft_quarterly

class QuarterlyPositiveTestCse(unittest.TestCase) :
    def setUp(self):
        self.testTicker = TickerMock("MSFT")
        self.utils_mock = patch('yfinance.base.utils.get_json', autospec=True).start()
        self.addCleanup(patch.stopall)

    def tearDown(self):
        pass

    def test_quarterly_financials_is_in_correct_format(self):
        info = self.testTicker.quarterly_financials
        self.assertTrue(info is data.valid.msft_quarterly)

if __name__ == "__main__":
    unittest.main()
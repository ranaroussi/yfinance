import unittest
from unittest.mock import Mock
from unittest.mock import patch
import yfinance as yf
import doctest

class FinancialsTestCase(unittest.TestCase):
    def setUp(self):
        self.msft = yf.Ticker("MSFT")
        self.utils_mock = patch('yfinance.base.utils.get_json', autospec=True).start()
        self.addCleanup(patch.stopall)

    def tearDown(self):
        pass

    def test_get_financials_invalid_data_returns_empty(self):
        try:
            financials = self.msft.financials 
            self.assertTrue(financials.empty)
        except:
            # Ensure financials call does not throw exception, graceful behavior only
            self.assertTrue(False)

def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite("yfinance.ticker"))
    return tests

if __name__ == "__main__":
    unittest.main()
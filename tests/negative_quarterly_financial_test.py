import unittest
from unittest.mock import Mock
from unittest.mock import patch
import yfinance as yf


class FinancialsTestCase(unittest.TestCase):
    def setUp(self):
        self.msft = yf.Ticker("MSFT")
        self.utils_mock = patch('yfinance.base.utils.get_json', autospec=True).start()
        self.addCleanup(patch.stopall)
      
    def test_get_financials_quarterly_invalid_data_returns_empty_dataframe(self):
        try:
            financials = self.msft.quarterly_financials
            self.assertTrue(financials.empty)
        except:
            self.assertTrue(False)
        
if __name__ == "__main__":
    unittest.main()

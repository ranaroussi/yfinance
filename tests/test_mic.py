import unittest

from yfinance.mic import market_suffix, yahoo_ticker


class TestMarketSuffix(unittest.TestCase):
    def test_valid_mic(self):
        """MIC code exists → returns correct suffix"""
        self.assertEqual(market_suffix("XPAR"), ".PA")
        self.assertEqual(market_suffix("xpar"), ".PA")  # lowercase should work
        self.assertEqual(market_suffix("XNYS"), "")     # NYSE has no suffix

    def test_invalid_mic(self):
        """MIC code does not exist → raises ValueError"""
        with self.assertRaises(ValueError) as cm:
            market_suffix("XXXX")
        self.assertIn("Unknown MIC code: XXXX", str(cm.exception))

    def test_case_insensitive(self):
        """MIC code lookup should be case-insensitive"""
        self.assertEqual(market_suffix("xnym"), ".NYM")
        self.assertEqual(market_suffix("XNYM"), ".NYM")


class TestYahooTicker(unittest.TestCase):
    def test_valid_tuple(self):
        """Valid MIC → ticker includes correct suffix"""
        self.assertEqual(yahoo_ticker("OR", "XPAR"), "OR.PA")
        self.assertEqual(yahoo_ticker("AAPL", "XNYS"), "AAPL")  # NYSE has no suffix
        self.assertEqual(yahoo_ticker("PETR4", "BVMF"), "PETR4.SA")

    def test_invalid_mic(self):
        """Invalid MIC → raises ValueError"""
        with self.assertRaises(ValueError) as cm:
            yahoo_ticker("ABC", "XXXX")
        self.assertIn("Unknown MIC code: XXXX", str(cm.exception))

    def test_lowercase_mic(self):
        """MIC code can be lowercase"""
        self.assertEqual(yahoo_ticker("OR", "xpar"), "OR.PA")

if __name__ == "__main__":
    unittest.main()

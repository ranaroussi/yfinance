import unittest

import yfinance as yf


class TestAustralianMicTicker(unittest.TestCase):
    def test_australian_market_identifier_codes(self):
        cases = (
            ("BHP", "XASX", "BHP.AX"),
            ("IBTC", "CHIA", "IBTC.XA"),
            ("IBTC", ".chia", "IBTC.XA"),
            ("IBTC", "XAUS", "IBTC.XA"),
        )
        for symbol, mic, expected in cases:
            with self.subTest(symbol=symbol, mic=mic):
                self.assertEqual(yf.Ticker((symbol, mic)).ticker, expected)

from .context import yfinance as yf

import unittest

class TestTicker(unittest.TestCase):
	def setUp(self):
		pass

	def tearDown(self):
		pass


	def test_getTz(self):
		tkrs = []
		tkrs.append("IMP.JO")
		tkrs.append("BHG.JO")
		tkrs.append("SSW.JO")
		tkrs.append("BP.L")
		tkrs.append("INTC")
		test_run = False
		for tkr in tkrs:
			# First step: remove ticker from tz-cache
			yf.utils.get_tz_cache().store(tkr, None)

			# Test:
			dat = yf.Ticker(tkr)
			tz = dat._get_ticker_tz(debug_mode=False, proxy=None, timeout=None)

			self.assertIsNotNone(tz)


if __name__ == '__main__':
	unittest.main()

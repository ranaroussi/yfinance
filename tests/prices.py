from .context import yfinance as yf

import unittest

class TestPriceHistory(unittest.TestCase):
	def setUp(self):
		pass

	def tearDown(self):
		pass

	def test_weeklyWithEvents(self):
		# Reproduce issue #521
		tkr1 = "QQQ"
		tkr2 = "GDX"
		start_d = "2014-12-29"
		end_d = "2020-11-29"
		df1 = yf.Ticker(tkr1).history(start=start_d, end=end_d, interval="1wk", actions=True)
		df2 = yf.Ticker(tkr2).history(start=start_d, end=end_d, interval="1wk", actions=True)
		try:
			self.assertTrue(df1.index.equals(df2.index))
		except:
			missing_from_df1 = df2.index.difference(df1.index)
			missing_from_df2 = df1.index.difference(df2.index)
			print("{} missing these dates: {}".format(tkr1, missing_from_df1))
			print("{} missing these dates: {}".format(tkr2, missing_from_df2))
			raise

		# Test that index same with and without events:
		tkrs = [tkr1, tkr2]
		for tkr in tkrs:
			df1 = yf.Ticker(tkr).history(start=start_d, end=end_d, interval="1wk", actions=True)
			df2 = yf.Ticker(tkr).history(start=start_d, end=end_d, interval="1wk", actions=False)
			try:
				self.assertTrue(df1.index.equals(df2.index))
			except:
				missing_from_df1 = df2.index.difference(df1.index)
				missing_from_df2 = df1.index.difference(df2.index)
				print("{}-with-events missing these dates: {}".format(tkr, missing_from_df1))
				print("{}-without-events missing these dates: {}".format(tkr, missing_from_df2))
				raise

if __name__ == '__main__':
	unittest.main()

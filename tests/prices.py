from .context import yfinance as yf

import unittest

import datetime as _dt
import pytz as _tz

class TestPriceHistory(unittest.TestCase):
	def setUp(self):
		pass

	def tearDown(self):
		pass


	def test_duplicatingDaily(self):
		tkrs = []
		tkrs.append("IMP.JO")
		tkrs.append("BHG.JO")
		tkrs.append("SSW.JO")
		tkrs.append("BP.L")
		tkrs.append("INTC")
		test_run = False
		for tkr in tkrs:
			dat = yf.Ticker(tkr)
			tz = dat._get_ticker_tz()

			dt_utc = _tz.timezone("UTC").localize(_dt.datetime.utcnow())
			dt = dt_utc.astimezone(_tz.timezone(tz))
			if dt.time() < _dt.time(17,0):
				continue
			test_run = True

			df = dat.history(start=dt.date()-_dt.timedelta(days=7), interval="1d")

			dt0 = df.index[-2]
			dt1 = df.index[-1]
			try:
				self.assertNotEqual(dt0, dt1)
			except:
				print("Ticker = ", tkr)
				raise

		if not test_run:
			self.skipTest("Skipping test_duplicatingDaily() because only expected to fail just after market close")


	def test_duplicatingWeekly(self):
		tkrs = ['MSFT', 'IWO', 'VFINX', '^GSPC', 'BTC-USD']
		test_run = False
		for tkr in tkrs:
			dat = yf.Ticker(tkr)
			tz = dat._get_ticker_tz()

			dt = _tz.timezone(tz).localize(_dt.datetime.now())
			if dt.date().weekday() not in [1,2,3,4]:
				continue
			test_run = True

			df = dat.history(start=dt.date()-_dt.timedelta(days=7), interval="1wk")
			dt0 = df.index[-2]
			dt1 = df.index[-1]
			try:
				self.assertNotEqual(dt0.week, dt1.week)
			except:
				print("Ticker={}: Last two rows within same week:".format(tkr))
				print(df.iloc[df.shape[0]-2:])
				raise

		if not test_run:
			self.skipTest("Skipping test_duplicatingWeekly() because not possible to fail Monday/weekend")


	def test_intraDayWithEvents(self):
		# TASE dividend release pre-market, doesn't merge nicely with intra-day data so check still present

		tkr = "ICL.TA"
		start_d = _dt.date.today() - _dt.timedelta(days=365)
		end_d = None
		df_daily = yf.Ticker(tkr).history(start=start_d, end=end_d, interval="1d", actions=True)
		df_daily_divs = df_daily["Dividends"][df_daily["Dividends"]!=0]
		if df_daily_divs.shape[0]==0:
			self.skipTest("Skipping test_intraDayWithEvents() because 'ICL.TA' has no dividend in last 12 months")
		
		last_div_date = df_daily_divs.index[-1]
		start_d = last_div_date.date()
		end_d = last_div_date.date() + _dt.timedelta(days=1)
		df = yf.Ticker(tkr).history(start=start_d, end=end_d, interval="15m", actions=True)
		self.assertTrue((df["Dividends"]!=0.0).any())


	def test_dailyWithEvents(self):
		# Reproduce issue #521
		tkr1 = "QQQ"
		tkr2 = "GDX"
		start_d = "2014-12-29"
		end_d = "2020-11-29"
		df1 = yf.Ticker(tkr1).history(start=start_d, end=end_d, interval="1d", actions=True)
		df2 = yf.Ticker(tkr2).history(start=start_d, end=end_d, interval="1d", actions=True)
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
			df1 = yf.Ticker(tkr).history(start=start_d, end=end_d, interval="1d", actions=True)
			df2 = yf.Ticker(tkr).history(start=start_d, end=end_d, interval="1d", actions=False)
			try:
				self.assertTrue(df1.index.equals(df2.index))
			except:
				missing_from_df1 = df2.index.difference(df1.index)
				missing_from_df2 = df1.index.difference(df2.index)
				print("{}-with-events missing these dates: {}".format(tkr, missing_from_df1))
				print("{}-without-events missing these dates: {}".format(tkr, missing_from_df2))
				raise


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


	def test_monthlyWithEvents(self):
		tkr1 = "QQQ"
		tkr2 = "GDX"
		start_d = "2014-12-29"
		end_d = "2020-11-29"
		df1 = yf.Ticker(tkr1).history(start=start_d, end=end_d, interval="1mo", actions=True)
		df2 = yf.Ticker(tkr2).history(start=start_d, end=end_d, interval="1mo", actions=True)
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
			df1 = yf.Ticker(tkr).history(start=start_d, end=end_d, interval="1mo", actions=True)
			df2 = yf.Ticker(tkr).history(start=start_d, end=end_d, interval="1mo", actions=False)
			try:
				self.assertTrue(df1.index.equals(df2.index))
			except:
				missing_from_df1 = df2.index.difference(df1.index)
				missing_from_df2 = df1.index.difference(df2.index)
				print("{}-with-events missing these dates: {}".format(tkr, missing_from_df1))
				print("{}-without-events missing these dates: {}".format(tkr, missing_from_df2))
				raise


	def test_tz_dst_ambiguous(self):
		# Reproduce issue #1100

		try:
			yf.Ticker("ESLT.TA").history(start="2002-10-06", end="2002-10-09", interval="1d")
		except _tz.exceptions.AmbiguousTimeError:
			raise Exception("Ambiguous DST issue not resolved")


if __name__ == '__main__':
	unittest.main()

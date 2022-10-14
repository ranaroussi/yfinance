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


if __name__ == '__main__':
	unittest.main()

from .context import yfinance as yf

import unittest

import datetime as _dt
import pytz as _tz
import numpy as _np
import pandas as _pd

# Create temp session
import requests_cache, tempfile
td = tempfile.TemporaryDirectory()

class TestPriceHistory(unittest.TestCase):
	def setUp(self):
		global td ; self.td = td
		self.session = requests_cache.CachedSession(self.td.name+'/'+"yfinance.cache")

	def tearDown(self):
		self.session.close()


	def test_daily_index(self):
		tkrs = []
		tkrs.append("BHP.AX")
		tkrs.append("IMP.JO")
		tkrs.append("BP.L")
		tkrs.append("PNL.L")
		tkrs.append("INTC")

		intervals=["1d","1wk","1mo"]

		for tkr in tkrs:
			dat = yf.Ticker(tkr, session=self.session)

			for interval in intervals:
				df = dat.history(period="5y", interval=interval)

				f = df.index.time==_dt.time(0)
				self.assertTrue(f.all())


	def test_duplicatingDaily(self):
		tkrs = []
		tkrs.append("IMP.JO")
		tkrs.append("BHG.JO")
		tkrs.append("SSW.JO")
		tkrs.append("BP.L")
		tkrs.append("INTC")
		test_run = False
		for tkr in tkrs:
			dat = yf.Ticker(tkr, session=self.session)
			tz = dat._get_ticker_tz(debug_mode=False, proxy=None, timeout=None)

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
			dat = yf.Ticker(tkr, session=self.session)
			tz = dat._get_ticker_tz(debug_mode=False, proxy=None, timeout=None)

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
		# tkr = "ESLT.TA"
		# tkr = "ONE.TA"
		# tkr = "MGDL.TA"
		start_d = _dt.date.today() - _dt.timedelta(days=60)
		end_d = None
		df_daily = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="1d", actions=True)
		df_daily_divs = df_daily["Dividends"][df_daily["Dividends"]!=0]
		if df_daily_divs.shape[0]==0:
			self.skipTest("Skipping test_intraDayWithEvents() because 'ICL.TA' has no dividend in last 60 days")
		
		last_div_date = df_daily_divs.index[-1]
		start_d = last_div_date.date()
		end_d = last_div_date.date() + _dt.timedelta(days=1)
		df = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="15m", actions=True)
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
			df1 = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="1d", actions=True)
			df2 = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="1d", actions=False)
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
			df1 = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="1wk", actions=True)
			df2 = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="1wk", actions=False)
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
			df1 = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="1mo", actions=True)
			df2 = yf.Ticker(tkr, session=self.session).history(start=start_d, end=end_d, interval="1mo", actions=False)
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
			yf.Ticker("ESLT.TA", session=self.session).history(start="2002-10-06", end="2002-10-09", interval="1d")
		except _tz.exceptions.AmbiguousTimeError:
			raise Exception("Ambiguous DST issue not resolved")


	def test_repair_weekly(self):
		# Sometimes, Yahoo returns prices 100x the correct value. 
		# Suspect mixup between £/pence or $/cents etc.
		# E.g. ticker PNL.L

		# Setup:
		tkr = "PNL.L"
		error_threshold = 1000.0
		start = "2020-01-06"
		end = min(_dt.date.today(), _dt.date(2023,1,1))

		# Run test

		dat = yf.Ticker(tkr, session=self.session)
		df_bad = dat.history(start=start, end=end, interval="1wk", auto_adjust=False, repair=False)

		# Record the errors that will be repaired
		data_cols = ["Low","High","Open","Close","Adj Close"]
		f_outlier = _np.where(df_bad[data_cols]>error_threshold)
		indices = None
		if len(f_outlier[0])==0:
			self.skipTest("Skipping test_repair_weekly() because no price 100x errors to repair")
		indices = []
		for i in range(len(f_outlier[0])):
			indices.append((f_outlier[0][i], f_outlier[1][i]))

		df = dat.history(start=start, end=end, interval="1wk", auto_adjust=False, repair=True)

		# First test - no errors left after repair
		df_data = df[data_cols].values
		for i,j in indices:
			try:
				self.assertTrue(df_data[i,j] < error_threshold)
			except:
				print("Detected uncorrected error: idx={}, {}={}".format(df.index[i], data_cols[j], df_data[i,j]))
				raise

		# Second test - all differences between pre- and post-repair should be ~100x
		ratio = (df_bad[data_cols].values/df[data_cols].values).round(2)
		# - round near-100 ratios to 100:
		f_near_100 = (ratio>90)&(ratio<110)
		ratio[f_near_100] = (ratio[f_near_100]/10).round().astype(int)*10 # round ratio to nearest 10
		# - now test
		f_100 = ratio==100
		f_1 = ratio==1
		self.assertTrue((f_100|f_1).all())

		# Third test: compare directly against daily data, unadjusted
		df = dat.history(start=start, end=end, interval="1wk", auto_adjust=False, repair=True)
		for i in indices:
			dt = df.index[i[0]]

			df_daily = dat.history(start=dt, end=dt+_dt.timedelta(days=7), interval="1d", auto_adjust=False, repair=True)

			# Manually construct weekly price data from daily
			df_yf_weekly = df_daily.copy()
			df_yf_weekly["_weekStart"] = _pd.to_datetime(df_yf_weekly.index.tz_localize(None).to_period('W-SUN').start_time).tz_localize(df.index.tz)
			df_yf_weekly.loc[df_yf_weekly["Stock Splits"]==0,"Stock Splits"]=1
			df_yf_weekly = df_yf_weekly.groupby("_weekStart").agg(
				Open=("Open", "first"),
				Close=("Close", "last"),
				AdjClose=("Adj Close", "last"),
				Low=("Low", "min"),
				High=("High", "max"),
				Volume=("Volume", "sum"),
				Dividends=("Dividends", "sum"),
				StockSplits=("Stock Splits", "prod")).rename(columns={"StockSplits":"Stock Splits","AdjClose":"Adj Close"})
			df_yf_weekly.loc[df_yf_weekly["Stock Splits"]==1,"Stock Splits"]=0
			if df_yf_weekly.index[0] not in df_daily.index:
				# Exchange closed Monday. In this case, Yahoo sets Open to last week close
				df_daily_last_week = dat.history(start=dt-_dt.timedelta(days=7), end=dt, interval="1d", auto_adjust=False, repair=True)
				df_yf_weekly["Open"] = df_daily_last_week["Close"][-1]
				df_yf_weekly["Low"] = _np.minimum(df_yf_weekly["Low"], df_yf_weekly["Open"])

			# Compare fetched-weekly vs constructed-weekly:
			df_yf_weekly = df_yf_weekly[df.columns]
			try:
				# Note: Adj Close has tiny variance depending on date range requested
				data_cols = ["Open","Close","Low","High"]
				self.assertTrue(_np.equal(df.loc[dt,data_cols].values, df_yf_weekly[data_cols].iloc[0].values).all())
				self.assertLess(abs(df.loc[dt,"Adj Close"]/df_yf_weekly["Adj Close"].iloc[0] -1.0), 0.000001)
			except:
				for c in df.columns:
					if c=="Adj Close":
						fail = abs(df.loc[dt,c]/df_yf_weekly[c].iloc[0] -1.0) < 0.000001
					else:
						fail = df.loc[dt,c] != df_yf_weekly[c].iloc[0]
					if fail:
						print("dt = ",dt)
						print("df.loc[dt]:", type(df.loc[dt]))
						print(df.loc[dt].to_dict())
						print("df_yf_weekly.iloc[0]:", type(df_yf_weekly.iloc[0]))
						print(df_yf_weekly.iloc[0].to_dict())
						print("Result:", df.loc[dt,c])
						print("Answer:", df_yf_weekly[c].iloc[0])
						raise Exception("Mismatch in column '{}'".format(c))


	def test_repair_weekly2_preSplit(self):
		# Sometimes, Yahoo returns prices 100x the correct value. 
		# Suspect mixup between £/pence or $/cents etc.
		# E.g. ticker PNL.L

		# PNL.L has a stock-split in 2022. Sometimes requesting data before 2022 is not split-adjusted.

		# Setup:
		tkr = "PNL.L"
		error_threshold = 1000.0
		start = "2020-01-06"
		end = "2021-06-01"

		# Run test

		dat = yf.Ticker(tkr, session=self.session)
		df_bad = dat.history(start=start, end=end, interval="1wk", auto_adjust=False, repair=False)

		# Record the errors that will be repaired
		data_cols = ["Low","High","Open","Close","Adj Close"]
		f_outlier = _np.where(df_bad[data_cols]>error_threshold)
		indices = None
		if len(f_outlier[0])==0:
			self.skipTest("Skipping test_repair_weekly() because no price 100x errors to repair")
		indices = []
		for i in range(len(f_outlier[0])):
			indices.append((f_outlier[0][i], f_outlier[1][i]))

		df = dat.history(start=start, end=end, interval="1wk", auto_adjust=False, repair=True)

		# First test - no errors left after repair
		df_data = df[data_cols].values
		for i,j in indices:
			try:
				self.assertTrue(df_data[i,j] < error_threshold)
			except:
				print("Detected uncorrected error: idx={}, {}={}".format(df.index[i], data_cols[j], df_data[i,j]))
				raise

		# Second test - all differences between pre- and post-repair should be ~100x
		ratio = (df_bad[data_cols].values/df[data_cols].values).round(2)
		# - round near-100 ratios to 100:
		f_near_100 = (ratio>90)&(ratio<110)
		ratio[f_near_100] = (ratio[f_near_100]/10).round().astype(int)*10 # round ratio to nearest 10
		# - now test
		f_100 = ratio==100
		f_1 = ratio==1
		self.assertTrue((f_100|f_1).all())

		# Third test: compare directly against daily data, unadjusted
		df = dat.history(start=start, end=end, interval="1wk", auto_adjust=False, repair=True)
		for i in indices:
			dt = df.index[i[0]]

			df_daily = dat.history(start=dt, end=dt+_dt.timedelta(days=7), interval="1d", auto_adjust=False, repair=True)

			# Manually construct weekly price data from daily
			df_yf_weekly = df_daily.copy()
			df_yf_weekly["_weekStart"] = _pd.to_datetime(df_yf_weekly.index.tz_localize(None).to_period('W-SUN').start_time).tz_localize(df.index.tz)
			df_yf_weekly.loc[df_yf_weekly["Stock Splits"]==0,"Stock Splits"]=1
			df_yf_weekly = df_yf_weekly.groupby("_weekStart").agg(
				Open=("Open", "first"),
				Close=("Close", "last"),
				AdjClose=("Adj Close", "last"),
				Low=("Low", "min"),
				High=("High", "max"),
				Volume=("Volume", "sum"),
				Dividends=("Dividends", "sum"),
				StockSplits=("Stock Splits", "prod")).rename(columns={"StockSplits":"Stock Splits","AdjClose":"Adj Close"})
			df_yf_weekly.loc[df_yf_weekly["Stock Splits"]==1,"Stock Splits"]=0
			if df_yf_weekly.index[0] not in df_daily.index:
				# Exchange closed Monday. In this case, Yahoo sets Open to last week close
				df_daily_last_week = dat.history(start=dt-_dt.timedelta(days=7), end=dt, interval="1d", auto_adjust=False, repair=True)
				df_yf_weekly["Open"] = df_daily_last_week["Close"][-1]
				df_yf_weekly["Low"] = _np.minimum(df_yf_weekly["Low"], df_yf_weekly["Open"])

			# Compare fetched-weekly vs constructed-weekly:
			df_yf_weekly = df_yf_weekly[df.columns]
			try:
				# Note: Adj Close has tiny variance depending on date range requested
				data_cols = ["Open","Close","Low","High"]
				self.assertTrue(_np.equal(df.loc[dt,data_cols].values, df_yf_weekly[data_cols].iloc[0].values).all())
				self.assertLess(abs(df.loc[dt,"Adj Close"]/df_yf_weekly["Adj Close"].iloc[0] -1.0), 0.000001)
			except:
				for c in df.columns:
					if c=="Adj Close":
						fail = abs(df.loc[dt,c]/df_yf_weekly[c].iloc[0] -1.0) < 0.000001
					else:
						fail = df.loc[dt,c] != df_yf_weekly[c].iloc[0]
					if fail:
						print("dt = ",dt)
						print("df.loc[dt]:", type(df.loc[dt]))
						print(df.loc[dt].to_dict())
						print("df_yf_weekly.iloc[0]:", type(df_yf_weekly.iloc[0]))
						print(df_yf_weekly.iloc[0].to_dict())
						print("Result:", df.loc[dt,c])
						print("Answer:", df_yf_weekly[c].iloc[0])
						raise Exception("Mismatch in column '{}'".format(c))


	def test_repair_daily(self):
		# Sometimes, Yahoo returns prices 100x the correct value. 
		# Suspect mixup between £/pence or $/cents etc.
		# E.g. ticker PNL.L

		tkr = "PNL.L"
		start = "2020-01-01"
		end = min(_dt.date.today(), _dt.date(2023,1,1))
		dat = yf.Ticker(tkr, session=self.session)

		data_cols = ["Low","High","Open","Close","Adj Close"]
		df_bad = dat.history(start=start, end=end, interval="1d", auto_adjust=False, repair=False)
		f_outlier = _np.where(df_bad[data_cols]>1000.0)
		indices = None
		if len(f_outlier[0])==0:
			self.skipTest("Skipping test_repair_daily() because no price 100x errors to repair")

		# Outliers detected
		indices = []
		for i in range(len(f_outlier[0])):
			indices.append((f_outlier[0][i], f_outlier[1][i]))

		df = dat.history(start=start, end=end, interval="1d", auto_adjust=False, repair=True)

		# First test - no errors left
		df_data = df[data_cols].values
		for i,j in indices:
			try:
				self.assertTrue(df_data[i,j] < 1000.0)
			except:
				print("Detected uncorrected error: idx={}, {}={}".format(df.index[i], data_cols[j], df_data[i,j]))
				# print(df.iloc[i-1:i+2])
				raise

		# Second test - all differences should be either ~1x or ~100x
		ratio = df_bad[data_cols].values/df[data_cols].values
		ratio = ratio.round(2)
		# - round near-100 ratio to 100:
		f = ratio>90
		ratio[f] = (ratio[f]/10).round().astype(int)*10 # round ratio to nearest 10
		# - now test
		f_100 = ratio==100
		f_1 = ratio==1
		self.assertTrue((f_100|f_1).all())


if __name__ == '__main__':
	unittest.main()

    # # Run tests sequentially:
    # import inspect
    # test_src = inspect.getsource(TestPriceHistory)
    # unittest.TestLoader.sortTestMethodsUsing = lambda _, x, y: (
    #     test_src.index(f"def {x}") - test_src.index(f"def {y}")
    # )
    # unittest.main(verbosity=2)

td.cleanup()


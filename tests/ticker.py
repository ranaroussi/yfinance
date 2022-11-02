from .context import yfinance as yf

import unittest

# Create temp session
import requests_cache, tempfile
td = tempfile.TemporaryDirectory()

class TestTicker(unittest.TestCase):
    def setUp(self):
        global td
        self.td = td
        self.session = requests_cache.CachedSession(self.td.name + '/' + "yfinance.cache")

    def tearDown(self):
        self.session.close()

    def test_getTz(self):
        tkrs = ["IMP.JO", "BHG.JO", "SSW.JO", "BP.L", "INTC"]
        for tkr in tkrs:
            # First step: remove ticker from tz-cache
            yf.utils.get_tz_cache().store(tkr, None)

            # Test:
            dat = yf.Ticker(tkr, session=self.session)
            tz = dat._get_ticker_tz(debug_mode=False, proxy=None, timeout=None)

            self.assertIsNotNone(tz)

    def test_badTicker(self):
        # Check yfinance doesn't die when ticker delisted

        tkr = "AM2Z.TA"
        dat = yf.Ticker(tkr, session=self.session)
        dat.history(period="1wk")
        dat.history(start="2022-01-01")
        dat.history(start="2022-01-01", end="2022-03-01")
        yf.download([tkr], period="1wk")
        dat.isin
        dat.major_holders
        dat.institutional_holders
        dat.mutualfund_holders
        dat.dividends
        dat.splits
        dat.actions
        dat.shares
        dat.info
        dat.calendar
        dat.recommendations
        dat.earnings
        dat.quarterly_earnings
        dat.income_stmt
        dat.quarterly_income_stmt
        dat.balance_sheet
        dat.quarterly_balance_sheet
        dat.cashflow
        dat.quarterly_cashflow
        dat.recommendations_summary
        dat.analyst_price_target
        dat.revenue_forecasts
        dat.sustainability
        dat.options
        dat.news
        dat.earnings_trend
        dat.earnings_dates
        dat.earnings_forecasts

if __name__ == '__main__':
    unittest.main()

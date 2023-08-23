import pandas as pd

from yfinance import utils
from yfinance.data import TickerData


class Analysis:
    _SCRAPE_URL_ = 'https://finance.yahoo.com/quote'

    def __init__(self, data: TickerData, proxy=None):
        self._data = data
        self.proxy = proxy

        self._earnings_trend = None
        self._analyst_trend_details = None
        self._analyst_growth_estimates = None
        self._rev_est = None
        self._eps_est = None
        self._already_scraped = False

    @property
    def earnings_trend(self) -> pd.DataFrame:
        if self._earnings_trend is None and not self._already_scraped:
            self._scrape(self.proxy)
        return self._earnings_trend

    @property
    def analyst_trend_details(self) -> pd.DataFrame:
        if self._analyst_trend_details is None and not self._already_scraped:
            self._scrape(self.proxy)
        return self._analyst_trend_details

    @property
    def analyst_growth_estimates(self) -> pd.DataFrame:
        if self._analyst_growth_estimates is None and not self._already_scraped:
            self._scrape(self.proxy)
        return self._analyst_growth_estimates

    @property
    def rev_est(self) -> pd.DataFrame:
        if self._rev_est is None and not self._already_scraped:
            self._scrape(self.proxy)
        return self._rev_est

    @property
    def eps_est(self) -> pd.DataFrame:
        if self._eps_est is None and not self._already_scraped:
            self._scrape(self.proxy)
        return self._eps_est

    def _scrape(self, proxy):
        ticker_url = f"{self._SCRAPE_URL_}/{self._data.ticker}"
        try:
            resp = self._data.cache_get(ticker_url + '/analysis', proxy=proxy)
            analysis = pd.read_html(resp.text)
        except Exception:
            analysis = []

        analysis_dict = {df.columns[0]: df for df in analysis}

        for key, item in analysis_dict.items():
            if key == 'Earnings History':
                self._earnings_trend = item
            elif key == 'EPS Trend':
                self._analyst_trend_details = item
            elif key == 'Growth Estimates':
                self._analyst_growth_estimates = item
            elif key == 'Revenue Estimate':
                self._rev_est = item
            elif key == 'Earnings Estimate':
                self._eps_est = item

        self._already_scraped = True

from io import StringIO

import pandas as pd

from yfinance.data import YfData
from yfinance.exceptions import YFNotImplementedError


class Analysis:
    analysis_url = 'https://finance.yahoo.com/quote'

    def __init__(self, data: YfData, symbol: str, proxy=None):
        self._data = data
        self._symbol = symbol
        self.proxy = proxy

        self._earnings_estimate = None
        self._revenue_estimate = None
        self._earnings_history = None
        self._eps_trend = None
        self._eps_revisions = None
        self._growth_estimates = None

        self._already_scraped = False

    @property
    def earnings_estimate(self) -> pd.DataFrame:
        if self._earnings_estimate is None:
            self._scrape_analysis(self.proxy)
        return self._earnings_estimate

    @property
    def revenue_estimate(self) -> pd.DataFrame:
        if self._revenue_estimate is None:
            self._scrape_analysis(self.proxy)
        return self._revenue_estimate

    @property
    def earnings_history(self) -> pd.DataFrame:
        if self._earnings_history is None:
            self._scrape_analysis(self.proxy)
        return self._earnings_history

    @property
    def eps_trend(self) -> pd.DataFrame:
        if self._eps_trend is None:
            self._scrape_analysis(self.proxy)
        return self._eps_trend

    @property
    def eps_revisions(self) -> pd.DataFrame:
        if self._eps_revisions is None:
            self._scrape_analysis(self.proxy)
        return self._eps_revisions

    @property
    def growth_estimates(self) -> pd.DataFrame:
        if self._growth_estimates is None:
            self._scrape_analysis(self.proxy)
        return self._growth_estimates

    def _scrape_analysis(self, proxy):
        ticker_url = f"{self.analysis_url}/{self._symbol}"
        resp = self._data.cache_get(ticker_url + '/analysis', proxy=proxy)
        if "Will be right back" in resp.text:
            raise RuntimeError("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\n"
                               "Our engineers are working quickly to resolve "
                               "the issue. Thank you for your patience.")
        try:
            analysis = pd.read_html(StringIO(resp.text))
            self._earnings_estimate = analysis[0]
            self._revenue_estimate = analysis[1]
            self._earnings_history = analysis[2]
            self._eps_trend = analysis[3]
            self._eps_revisions = analysis[4]
            self._growth_estimates = analysis[5]
        except ValueError:
            return

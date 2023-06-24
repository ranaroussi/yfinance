import pandas as pd

from yfinance.data import TickerData

class Holdings:
    _SCRAPE_URL_ = 'https://finance.yahoo.com/quote'

    def __init__(self, data: TickerData, proxy=None):
        self._data = data
        self.proxy = proxy
        self._major = None

    @property
    def major(self) -> pd.DataFrame:
        if self._major is None:
            self._scrape(self.proxy)
        return self._major

    def _scrape(self, proxy):
        ticker_url = "{}/{}".format(self._SCRAPE_URL_, self._data.ticker)
        try:
            resp = self._data.cache_get(ticker_url + '/holdings', proxy)
            holdings = pd.read_html(resp.text)
        except Exception:
            holdings = []

        if len(holdings) >= 1:
            self._major = {i:d for i,d in enumerate(holdings)}

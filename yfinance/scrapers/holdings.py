from io import StringIO
import pandas as pd

from yfinance.data import YfData

class Holdings:
    _SCRAPE_URL_ = 'https://finance.yahoo.com/quote'

    def __init__(self, data: YfData, symbol: str, proxy=None):
        self._data = data
        self._symbol = symbol
        self.proxy = proxy
        self._major = None

    @property
    def major(self) -> pd.DataFrame:
        if self._major is None:
            self._scrape(self.proxy)
        return self._major

    def _scrape(self, proxy):
        ticker_url = "{}/{}".format(self._SCRAPE_URL_, self._symbol)
        try:
            resp = self._data.cache_get(ticker_url + '/holdings', proxy)
            if "/holdings" not in resp.url:
                raise Exception(f'{self.ticker}: does not have holdings')
            holdings = pd.read_html(StringIO(resp.text))
        except Exception:
            holdings = []

        if len(holdings) >= 1:
            self._major = {i:d for i,d in enumerate(holdings)}

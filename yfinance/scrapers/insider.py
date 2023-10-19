import pandas as pd

from yfinance.data import TickerData


class Insider:
    _SCRAPE_URL_ = 'https://finance.yahoo.com/quote'

    def __init__(self, data: TickerData, proxy=None):
        self._data = data
        self.proxy = proxy

        self._insider_roster = None

    @property
    def roster(self) -> pd.DataFrame:
        if self._insider_roster is None:
            self._scrape(self.proxy)
        return self._insider_roster

    def _scrape(self, proxy):
        ticker_url = f"{self._SCRAPE_URL_}/{self._data.ticker}"
        try:
            resp = self._data.cache_get(ticker_url + '/insider-roster', proxy=proxy)
            insider_roster = pd.read_html(resp.text)
        except Exception:
            insider_roster = []

        if len(insider_roster) >= 1:
            self._insider_roster = insider_roster[0]

        if self._insider_roster is not None:
            if 'Date' in self._insider_roster:
                self._insider_roster['Date'] = pd.to_datetime(
                    self._insider_roster['Date'])

import pandas as pd

from yfinance.data import TickerData

class Holders:
    _SCRAPE_URL_ = 'https://finance.yahoo.com/quote'

    def __init__(self, data: TickerData, proxy=None):
        self._data = data
        self.proxy = proxy

        self._major = None
        self._institutional = None
        self._mutualfund = None

    @property
    def major(self) -> pd.DataFrame:
        if self._major is None:
            self._scrape(self.proxy)
        return self._major

    @property
    def institutional(self) -> pd.DataFrame:
        if self._institutional is None:
            self._scrape(self.proxy)
        return self._institutional

    @property
    def mutualfund(self) -> pd.DataFrame:
        if self._mutualfund is None:
            self._scrape(self.proxy)
        return self._mutualfund

    def _scrape(self, proxy):
        ticker_url = "{}/{}".format(self._SCRAPE_URL_, self._data.ticker)
        try:
            resp = self._data.cache_get(ticker_url + '/holders', proxy)
            holders = pd.read_html(resp.text)
        except Exception:
            holders = []

        if len(holders) >= 3:
            self._major = holders[0]
            self._institutional = holders[1]
            self._mutualfund = holders[2]
        elif len(holders) >= 2:
            self._major = holders[0]
            self._institutional = holders[1]
        elif len(holders) >= 1:
            self._major = holders[0]

        if self._institutional is not None:
            if 'Date Reported' in self._institutional:
                self._institutional['Date Reported'] = pd.to_datetime(
                    self._institutional['Date Reported'])
            if '% Out' in self._institutional:
                self._institutional['% Out'] = self._institutional[
                                                   '% Out'].str.replace('%', '').astype(float) / 100

        if self._mutualfund is not None:
            if 'Date Reported' in self._mutualfund:
                self._mutualfund['Date Reported'] = pd.to_datetime(
                    self._mutualfund['Date Reported'])
            if '% Out' in self._mutualfund:
                self._mutualfund['% Out'] = self._mutualfund[
                                                '% Out'].str.replace('%', '').astype(float) / 100

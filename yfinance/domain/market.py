from ..data import YfData
from ..data import utils
from ..const import _QUERY1_URL_
# from .domain import Domain

class Market():
    def __init__(self, market:'str', session=None, proxy=None, timeout=30, raise_errors=True):
        self.market = market
        self.session = session
        self.proxy = proxy
        self.timeout = timeout
        self.raise_errors = raise_errors

        self._data = YfData(session=self.session)
        self._logger = utils.get_yf_logger()
        
        self._status = None
        self._summary = None

    def _fetch_json(self, url, params):
        data = self._data.cache_get(url=url, params=params, proxy=self.proxy, timeout=self.timeout)
        if data is None or "Will be right back" in data.text:
            raise RuntimeError("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\n"
                               "Our engineers are working quickly to resolve "
                               "the issue. Thank you for your patience.")
        try:
            return data.json()
        except _json.JSONDecodeError:
            self._logger.error(f"{self.market}: Failed to retrieve market data and recieved faulty data.")
            return {}

    @property
    def status(self):
        if self._status is not None:
            return self._status

        url = f"{_QUERY1_URL_}/v6/finance/markettime"
        params = {
            "formatted": True,
            "key": "finance",
            "lang": "en-GB",
            "market": self.market
        }
        self._status = self._fetch_json(url, params)
        try:
            self._status = self._status['finance']['marketTimes'][0]['marketTime'][0]
        except:
            pass
        return self._status

    @property
    def summary(self):
        if self._summary is not None:
            return self._summary

        url = f"{_QUERY1_URL_}/v6/finance/quote/marketSummary"
        fields = ["shortName", "regularMarketPrice", "regularMarketChange", "regularMarketChangePercent"]
        params = {
            "fields": ",".join(fields),
            "formatted": False,
            "lang": "en-US",
            "region": self.market
        }
        self._summary = self._fetch_json(url, params)
        try:
            self._summary = self._summary['marketSummaryResponse']['result']
            self._summary = {x['exchange']:x for x in self._summary}
        except:
            pass
        return self._summary


import json as _json
import time

from . import utils
from .const import _QUERY1_URL_
from .data import YfData

class Summary:
    def __init__(self, market, region="US", session=None, proxy=None, timeout=30, raise_errors=True, refresh=3600):
        """
        market: The market area you want to get the summary for.
        region: Used for yahoo logging purposes.
        refresh: The number of seconds to wait before refreshing the data.
        """
        self.market = market
        self.region = region

        self.session = session
        self.proxy = proxy
        self.timeout = timeout
        self.raise_errors = raise_errors

        self.refresh = refresh

        self._data = YfData(session=session)
        self._logger = utils.get_yf_logger()
        self._last_refresh = 0

    def _request(self, formatted=False, fields=["shortName", "regularMarketPrice", "regularMarketChange", "regularMarketChangePercent"]):
        url = f"{_QUERY1_URL_}/v6/finance/quote/marketSummary"
        params = {
            "fields": ",".join(fields),
            "formatted": formatted,
            "lang": "en-US",
            "market": self.market,
            "region": self.region
        }

        self._logger.debug(f'{self.market}: Yahoo GET parameters: {str(dict(params))}')

        data = self._data.cache_get(url=url, params=params, proxy=self.proxy, timeout=self.timeout)
        if data is None or "Will be right back" in data.text:
            raise RuntimeError("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\n"
                               "Our engineers are working quickly to resolve "
                               "the issue. Thank you for your patience.")
        try:
            data = data.json()
        except _json.JSONDecodeError:
            self._logger.error(f"{self.market}: Failed to retrieve the market summary and received faulty response instead.")
            data = {}

        return data
    
    def refresh_data(self, formatted=False, fields=["shortName", "regularMarketPrice", "regularMarketChange", "regularMarketChangePercent"], force=False):
        if force or time.time() - self._last_refresh > self.refresh:
            # As the data is very unlikely to change, we can cache it for a long time.
            self._last_refresh = time.time()
            self._last_request = self._request(formatted=formatted, fields=fields)["marketSummaryResponse"]
        
        return self._last_request

    def __getitem__(self, name: str) -> 'dict':
        self.refresh_data()
        for data in self.data:
            if data["symbol"] == name:
                return data
        raise AttributeError(f"{name} is not a symbol in {self.market}. Valid symbols are: {", ".join([data["symbol"] for data in self.data])}")
    
    @property
    def data(self) -> 'list[dict]':
        self.refresh_data()
        return self._last_request["result"]
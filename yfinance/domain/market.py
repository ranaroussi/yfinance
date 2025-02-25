import datetime as dt

from ..data import YfData
from ..data import utils
from ..const import _QUERY1_URL_
import json as _json

class Market():
    def __init__(self, market:'str',  session=None, proxy=None, timeout=30):
        self.market = market
        self.session = session
        self.proxy = proxy
        self.timeout = timeout

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
        
    def _parse_data(self):
        # Fetch both to ensure they are at the same time
        if (self._status is not None) and (self._summary is not None):
            return
        
        self._logger.debug(f"{self.market}: Parsing market data")

        # Summary

        summary_url = f"{_QUERY1_URL_}/v6/finance/quote/marketSummary"
        summary_fields = ["shortName", "regularMarketPrice", "regularMarketChange", "regularMarketChangePercent"]
        summary_params = {
            "fields": ",".join(summary_fields),
            "formatted": False,
            "lang": "en-US",
            "market": self.market
        }

        status_url = f"{_QUERY1_URL_}/v6/finance/markettime"
        status_params = {
            "formatted": True,
            "key": "finance",
            "lang": "en-GB",
            "market": self.market
        }

        self._summary = self._fetch_json(summary_url, summary_params)
        self._status = self._fetch_json(status_url, status_params)

        try:
            self._summary = self._summary['marketSummaryResponse']['result']
            self._summary = {x['exchange']:x for x in self._summary}
        except Exception as e:
            self._logger.error(f"{self.market}: Failed to parse market summary")
            self._logger.debug(f"{type(e)}: {e}")


        try:
            # Unpack
            self._status = self._status['finance']['marketTimes'][0]['marketTime'][0]
            self._status['timezone'] = self._status['timezone'][0]
            del self._status['time']  # redundant
            try:
                self._status.update(
                    open  = dt.datetime.fromisoformat(self._status["open"]),
                    close = dt.datetime.fromisoformat(self._status["close"]),
                    tz = dt.timezone(self._status["timezone"]["gmtoffset"], self._status["timezone"]["short"])
                )
            except Exception as e:
                self._logger.error(f"{self.market}: Failed to update market status")
                self._logger.debug(f"{type(e)}: {e}")
        except Exception as e:
            self._logger.error(f"{self.market}: Failed to parse market status")
            self._logger.debug(f"{type(e)}: {e}")




    @property
    def status(self):
        self._parse_data()
        return self._status


    @property
    def summary(self):
        self._parse_data()
        return self._summary

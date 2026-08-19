import datetime as dt
import json as _json
from enum import Enum

from ..config import YfConfig
from ..const import _QUERY1_URL_
from ..data import utils, YfData
from ..exceptions import YFDataException


class MarketRegion(str, Enum):
    """Market regions accepted by Yahoo's ``quote/marketSummary`` endpoint.

    Members are plain strings, so ``MarketRegion.EUROPE == "EUROPE"`` and
    ``Market("EUROPE")`` continues to work. Pass an enum member for IDE
    autocomplete and static checking: ``Market(MarketRegion.EUROPE)``.
    """
    US = "US"
    GB = "GB"
    ASIA = "ASIA"
    EUROPE = "EUROPE"
    RATES = "RATES"
    COMMODITIES = "COMMODITIES"
    CURRENCIES = "CURRENCIES"
    CRYPTOCURRENCIES = "CRYPTOCURRENCIES"


class Market:
    def __init__(self, market, session=None, timeout=30):
        try:
            self.market = MarketRegion(market).value
        except ValueError:
            valid = [m.value for m in MarketRegion]
            raise ValueError(
                f"Unknown market {market!r}. Valid markets: {valid}"
            ) from None
        self.session = session
        self.timeout = timeout

        self._data = YfData(session=self.session)

        self._logger = utils.get_yf_logger()
        
        self._status = None
        self._summary = None

    def _fetch_json(self, url, params):
        data = self._data.cache_get(url=url, params=params, timeout=self.timeout)
        if data is None or "Will be right back" in data.text:
            raise YFDataException("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***")
        try:
            return data.json()
        except _json.JSONDecodeError:
            if not YfConfig.debug.hide_exceptions:
                raise
            self._logger.error(f"{self.market}: Failed to retrieve market data and received faulty data.")
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
            "lang": "en-US",
            "market": self.market
        }

        self._summary = self._fetch_json(summary_url, summary_params)
        self._status = self._fetch_json(status_url, status_params)

        try:
            self._summary = self._summary['marketSummaryResponse']['result']
            self._summary = {x['exchange']:x for x in self._summary}
        except Exception as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            self._logger.error(f"{self.market}: Failed to parse market summary")
            self._logger.debug(f"{type(e)}: {e}")


        try:
            # Unpack
            self._status = self._status['finance']['marketTimes'][0]['marketTime'][0]
            self._status['timezone'] = self._status['timezone'][0]
            del self._status['time']  # redundant
            # Yahoo's markettime endpoint silently ignores the `market` param
            # and always returns U.S. data. Detect the mismatch so callers
            # aren't misled into believing they got regional status data.
            if self.market != "US" and self._status.get("id") == "us":
                self._logger.warning(
                    f"{self.market}: Yahoo markettime endpoint does not support "
                    f"market={self.market!r}; status data unavailable."
                )
                self._status = None
        except Exception as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            self._logger.error(f"{self.market}: Failed to parse market status")
            self._logger.debug(f"{type(e)}: {e}")
        if self._status is None:
            return
        try:
            self._status.update({
                "open": dt.datetime.fromisoformat(self._status["open"]),
                "close": dt.datetime.fromisoformat(self._status["close"]),
                "tz": dt.timezone(dt.timedelta(hours=int(self._status["timezone"]["gmtoffset"]))/1000, self._status["timezone"]["short"])
            })
        except Exception as e:
            if not YfConfig.debug.hide_exceptions:
                raise
            self._logger.error(f"{self.market}: Failed to update market status")
            self._logger.debug(f"{type(e)}: {e}")




    @property
    def status(self):
        self._parse_data()
        return self._status


    @property
    def summary(self):
        self._parse_data()
        return self._summary

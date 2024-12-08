import json as _json
import time

from . import utils
from .const import _QUERY1_URL_
from .data import YfData

class MarketSummary:
    def __init__(self, data) -> None:
        # Basic information
        self.name: 'str' = data.get("shortName")
        self.region: 'str' = data.get("region")
        self.market: 'str' = data.get("market")
        self.quote_type: 'str' = data.get("quoteType")
        self.type_display: 'str' = data.get("typeDisp")
        self.symbol: 'str' = data.get("symbol")
        
        # Market data
        self.price: 'float' = data.get("regularMarketPrice")
        self.change: 'float' = data.get("regularMarketChange")
        self.change_percent: 'float' = data.get("regularMarketChangePercent")
        self.previous_close: 'float' = data.get("regularMarketPreviousClose")
        self.market_time: 'int' = data.get("regularMarketTime")
        self.market_state: 'str' = data.get("marketState")
        self.price_hint: 'int' = data.get("priceHint")
        
        # Exchange information
        self.exchange: 'str' = data.get("exchange")
        self.full_exchange_name: 'str' = data.get("fullExchangeName")
        self.timezone: 'str' = data.get("exchangeTimezoneName")
        self.timezone_short: 'str' = data.get("exchangeTimezoneShortName")
        self.gmt_offset: 'int' = data.get("gmtOffSetMilliseconds")
        self.exchange_delay: 'int' = data.get("exchangeDataDelayedBy")
        
        # Quote information
        self.quote_source: 'str' = data.get("quoteSourceName")
        self.source_interval: 'int' = data.get("sourceInterval")
        
        # Trading properties
        self.triggerable: 'bool' = data.get("triggerable")
        self.tradeable: 'bool' = data.get("tradeable")
        self.crypto_tradeable: 'bool' = data.get("cryptoTradeable")
        self.has_pre_post_market: 'bool' = data.get("hasPrePostMarketData")
        self.first_trade_date: 'int' = data.get("firstTradeDateMilliseconds")
        
        # Additional properties
        self.esg_populated: 'bool' = data.get("esgPopulated")
        self.price_alert_confidence: 'str' = data.get("customPriceAlertConfidence")



class Summary:
    def __init__(self, market, region="US", session=None, proxy=None, timeout=30, raise_errors=True, refresh=3600):
        """
        market: The market area you want to get the summary for.
        region: Can only be US
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
        self._last_data = []

    def _request(self, formatted=False, fields=["shortName", "regularMarketPrice", "regularMarketChange", "regularMarketChangePercent"]):
        url = f"{_QUERY1_URL_}/v6/finance/quote/marketSummary"
        params = {
            "fields": fields,
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
            self._convert_data()
        
        return self._last_request
    
    def _convert_data(self):
        self._last_data = [MarketSummary(data) for data in self._last_request["results"]]

    def __getitem__(self, name: str) -> 'MarketSummary':
        self.refresh_data()
        for data in self._last_data:
            if data.name == name:
                return data
        raise AttributeError(f"{name} not found")

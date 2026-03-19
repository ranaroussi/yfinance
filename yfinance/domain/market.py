"""Market status and summary accessors."""

import datetime as dt
from typing import Any, Dict, Optional

from frozendict import frozendict

from ..http import parse_json_response
from ..config import YF_CONFIG as YfConfig
from ..const import _QUERY1_URL_
from ..data import YfData, utils

_PARSE_ERROR_TYPES = (IndexError, KeyError, TypeError, ValueError)


class Market:
    """Fetch and expose Yahoo Finance market status and summary data."""

    def __init__(self, market: str, session=None, timeout: int = 30):
        """Initialize a market helper for a given Yahoo market code."""
        self.market = market
        self.session = session
        self.timeout = timeout

        self._data = YfData(session=self.session)
        self._logger = utils.get_yf_logger()

        self._status: Optional[Dict[str, Any]] = None
        self._summary: Optional[Dict[str, Any]] = None

    def _fetch_json(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Fetch JSON for a market endpoint and handle Yahoo downtime pages."""
        data = self._data.cache_get(url=url, params=frozendict(params), timeout=self.timeout)
        return parse_json_response(
            data,
            self._logger,
            "%s: Failed to retrieve market data and received faulty data.",
            self.market,
        )

    def _parse_data(self) -> None:
        """Fetch and parse market summary and market-time status payloads."""
        if self._status is not None and self._summary is not None:
            return

        self._logger.debug("%s: Parsing market data", self.market)

        summary_url = f"{_QUERY1_URL_}/v6/finance/quote/marketSummary"
        summary_fields = [
            "shortName",
            "regularMarketPrice",
            "regularMarketChange",
            "regularMarketChangePercent",
        ]
        summary_params = {
            "fields": ",".join(summary_fields),
            "formatted": False,
            "lang": "en-US",
            "market": self.market,
        }

        status_url = f"{_QUERY1_URL_}/v6/finance/markettime"
        status_params = {
            "formatted": True,
            "key": "finance",
            "lang": "en-US",
            "market": self.market,
        }

        summary_payload = self._fetch_json(summary_url, summary_params)
        status_payload = self._fetch_json(status_url, status_params)
        self._summary = summary_payload
        self._status = status_payload

        try:
            summary_result = summary_payload["marketSummaryResponse"]["result"]
            self._summary = {item["exchange"]: item for item in summary_result}
        except _PARSE_ERROR_TYPES as err:
            if not YfConfig.debug.hide_exceptions:
                raise
            self._logger.error("%s: Failed to parse market summary", self.market)
            self._logger.debug("%s: %s", type(err).__name__, err)

        try:
            status = status_payload["finance"]["marketTimes"][0]["marketTime"][0]
            status["timezone"] = status["timezone"][0]
            del status["time"]  # Redundant with open and close fields.
            self._status = status
        except _PARSE_ERROR_TYPES as err:
            if not YfConfig.debug.hide_exceptions:
                raise
            self._logger.error("%s: Failed to parse market status", self.market)
            self._logger.debug("%s: %s", type(err).__name__, err)
            return

        try:
            gmtoffset = int(status["timezone"]["gmtoffset"]) / 1000
            timezone_name = status["timezone"]["short"]
            status.update(
                {
                    "open": dt.datetime.fromisoformat(status["open"]),
                    "close": dt.datetime.fromisoformat(status["close"]),
                    "tz": dt.timezone(
                        dt.timedelta(hours=gmtoffset),
                        timezone_name,
                    ),
                }
            )
            self._status = status
        except _PARSE_ERROR_TYPES as err:
            if not YfConfig.debug.hide_exceptions:
                raise
            self._logger.error("%s: Failed to update market status", self.market)
            self._logger.debug("%s: %s", type(err).__name__, err)

    @property
    def status(self) -> Optional[Dict[str, Any]]:
        """Return parsed market status details."""
        self._parse_data()
        return self._status

    @property
    def summary(self) -> Optional[Dict[str, Any]]:
        """Return parsed market summary keyed by exchange."""
        self._parse_data()
        return self._summary

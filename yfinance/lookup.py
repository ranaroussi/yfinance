"""Yahoo Finance lookup endpoint wrapper."""

import time

import pandas as pd

from . import utils
from .http import parse_json_response
from .const import _QUERY1_URL_
from .data import YfData
from .exceptions import YFDataException

LOOKUP_TYPES = [
    "all",
    "equity",
    "mutualfund",
    "etf",
    "index",
    "future",
    "currency",
    "cryptocurrency",
]

_LOOKUP_RETRYABLE_ERROR_CODES = {"Internal Server Error"}
_LOOKUP_MAX_ERROR_RETRIES = 2
_LOOKUP_RETRY_DELAY_SECONDS = 0.5


class Lookup:
    """
    Fetches quote (ticker) lookups from Yahoo Finance.

    :param query: The search query for financial data lookup.
    :type query: str
    :param session: Custom HTTP session for requests (default None).
    :param timeout: Request timeout in seconds (default 30).
    :param raise_errors: Raise exceptions on error (default True).
    """

    def __init__(self, query: str, session=None, timeout=30, raise_errors=True):
        self.session = session
        self._data = YfData(session=self.session)

        self.query = query

        self.timeout = timeout
        self.raise_errors = raise_errors

        self._logger = utils.get_yf_logger()

        self._cache = {}

    def _request_lookup(self, params: dict) -> dict:
        url = f"{_QUERY1_URL_}/v1/finance/lookup"
        data = self._data.get(url=url, params=params, timeout=self.timeout)
        return parse_json_response(
            data,
            self._logger,
            "%s: 'lookup' fetch received faulty data",
            self.query,
        )

    @staticmethod
    def _extract_error(data: dict) -> dict:
        return data.get("finance", {}).get("error", {})

    def _fetch_lookup(self, lookup_type="all", count=25) -> dict:
        cache_key = (lookup_type, count)
        if cache_key in self._cache:
            return self._cache[cache_key]

        params = {
            "query": self.query,
            "type": lookup_type,
            "start": 0,
            "count": count,
            "formatted": False,
            "fetchPricingData": True,
            "lang": "en-US",
            "region": "US"
        }

        self._logger.debug(
            "GET Lookup for ticker (%s) with parameters: %s",
            self.query,
            dict(params),
        )

        data = {}
        error = {}
        for attempt in range(_LOOKUP_MAX_ERROR_RETRIES + 1):
            data = self._request_lookup(params)
            error = self._extract_error(data)
            if not error:
                break

            if (
                error.get("code") not in _LOOKUP_RETRYABLE_ERROR_CODES
                or attempt == _LOOKUP_MAX_ERROR_RETRIES
            ):
                raise YFDataException(f"{self.query}: 'lookup' fetch returned error: {error}")

            self._logger.debug(
                "Retrying Lookup for ticker (%s) after transient error: %s (attempt %d/%d)",
                self.query,
                error,
                attempt + 1,
                _LOOKUP_MAX_ERROR_RETRIES,
            )
            time.sleep(_LOOKUP_RETRY_DELAY_SECONDS * (attempt + 1))

        self._cache[cache_key] = data
        return data

    @staticmethod
    def _parse_response(response: dict) -> pd.DataFrame:
        finance = response.get("finance", {})
        result = finance.get("result", [])
        result = result[0] if len(result) > 0 else {}
        documents = result.get("documents", [])
        df = pd.DataFrame(documents)
        if "symbol" not in df.columns:
            return pd.DataFrame()
        return df.set_index("symbol")

    def _get_data(self, lookup_type: str, count: int = 25) -> pd.DataFrame:
        return self._parse_response(self._fetch_lookup(lookup_type, count))

    def get_all(self, count=25) -> pd.DataFrame:
        """
        Returns all available financial instruments.

        :param count: The number of results to retrieve.
        :type count: int
        """
        return self._get_data("all", count)

    def get_stock(self, count=25) -> pd.DataFrame:
        """
        Returns stock related financial instruments.

        :param count: The number of results to retrieve.
        :type count: int
        """
        return self._get_data("equity", count)

    def get_mutualfund(self, count=25) -> pd.DataFrame:
        """
        Returns mutual funds related financial instruments.

        :param count: The number of results to retrieve.
        :type count: int
        """
        return self._get_data("mutualfund", count)

    def get_etf(self, count=25) -> pd.DataFrame:
        """
        Returns ETFs related financial instruments.

        :param count: The number of results to retrieve.
        :type count: int
        """
        return self._get_data("etf", count)

    def get_index(self, count=25) -> pd.DataFrame:
        """
        Returns Indices related financial instruments.

        :param count: The number of results to retrieve.
        :type count: int
        """
        return self._get_data("index", count)

    def get_future(self, count=25) -> pd.DataFrame:
        """
        Returns Futures related financial instruments.

        :param count: The number of results to retrieve.
        :type count: int
        """
        return self._get_data("future", count)

    def get_currency(self, count=25) -> pd.DataFrame:
        """
        Returns Currencies related financial instruments.

        :param count: The number of results to retrieve.
        :type count: int
        """
        return self._get_data("currency", count)

    def get_cryptocurrency(self, count=25) -> pd.DataFrame:
        """
        Returns Cryptocurrencies related financial instruments.

        :param count: The number of results to retrieve.
        :type count: int
        """
        return self._get_data("cryptocurrency", count)

    @property
    def all(self) -> pd.DataFrame:
        """Returns all available financial instruments."""
        return self._get_data("all")

    @property
    def stock(self) -> pd.DataFrame:
        """Returns stock related financial instruments."""
        return self._get_data("equity")

    @property
    def mutualfund(self) -> pd.DataFrame:
        """Returns mutual funds related financial instruments."""
        return self._get_data("mutualfund")

    @property
    def etf(self) -> pd.DataFrame:
        """Returns ETFs related financial instruments."""
        return self._get_data("etf")

    @property
    def index(self) -> pd.DataFrame:
        """Returns Indices related financial instruments."""
        return self._get_data("index")

    @property
    def future(self) -> pd.DataFrame:
        """Returns Futures related financial instruments."""
        return self._get_data("future")

    @property
    def currency(self) -> pd.DataFrame:
        """Returns Currencies related financial instruments."""
        return self._get_data("currency")

    @property
    def cryptocurrency(self) -> pd.DataFrame:
        """Returns Cryptocurrencies related financial instruments."""
        return self._get_data("cryptocurrency")

"""Primary yfinance client API and compatibility helpers."""

import warnings

from . import cache, utils
from .version import VERSION
from .search import Search
from .lookup import Lookup
from .ticker import Ticker
from .calendars import Calendars
from .tickers import Tickers
from .multi import download
from .live import WebSocket, AsyncWebSocket
from .utils import enable_debug_mode
from .cache import set_tz_cache_location
from .domain.sector import Sector
from .domain.industry import Industry
from .domain.market import Market
from .config import YF_CONFIG as config
from .screener import client as screener_client
from .screener import query as screener_query

__version__ = VERSION
__author__ = "Ran Aroussi"

EquityQuery = screener_query.EquityQuery
FundQuery = screener_query.FundQuery
screen = screener_client.screen
PREDEFINED_SCREENER_QUERIES = screener_client.PREDEFINED_SCREENER_QUERIES

warnings.filterwarnings(
    "default",
    category=DeprecationWarning,
    module="^yfinance",
)

__all__ = [
    "download",
    "Market",
    "Search",
    "Lookup",
    "Ticker",
    "Tickers",
    "enable_debug_mode",
    "cache",
    "set_tz_cache_location",
    "Sector",
    "Industry",
    "WebSocket",
    "AsyncWebSocket",
    "Calendars",
    "utils",
]

__all__ += ["EquityQuery", "FundQuery", "screen", "PREDEFINED_SCREENER_QUERIES"]

_NOTSET = object()


def set_config(proxy=_NOTSET, retries=_NOTSET, verify=_NOTSET):
    """Set deprecated config values while mapping to the new config object."""
    if proxy is not _NOTSET:
        warnings.warn(
            "Set proxy via new config control: yf.config.network.proxy = proxy",
            DeprecationWarning,
        )
        config.network.proxy = proxy
    if retries is not _NOTSET:
        warnings.warn(
            "Set retries via new config control: yf.config.network.retries = retries",
            DeprecationWarning,
        )
        config.network.retries = retries
    if verify is not _NOTSET:
        warnings.warn(
            "Set verify via new config control: yf.config.network.verify = verify",
            DeprecationWarning,
        )
        config.network.verify = verify


__all__ += ["config", "set_config"]

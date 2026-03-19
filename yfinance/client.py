#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# yfinance - market data downloader
# https://github.com/ranaroussi/yfinance
#
# Copyright 2017-2019 Ran Aroussi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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


def set_config(proxy=_NOTSET, retries=_NOTSET):
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


__all__ += ["config", "set_config"]

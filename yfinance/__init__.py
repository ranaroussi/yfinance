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

from typing import TYPE_CHECKING, TypedDict, Optional
from . import version
from .search import Search
from .ticker import Ticker
from .tickers import Tickers
from .multi import download
from .utils import enable_debug_mode
from .cache import set_tz_cache_location
from .domain.sector import Sector
from .domain.industry import Industry
from .domain.market import Market

from .screener.query import EquityQuery, FundQuery
from .screener.screener import screen, PREDEFINED_SCREENER_QUERIES

__version__ = version.version
__author__ = "Ran Aroussi"

import warnings
warnings.filterwarnings("default", category=DeprecationWarning, module="^yfinance")

if TYPE_CHECKING:
    import requests
    CONFIG = TypedDict("CONFIG", {"proxy": Optional[str], "timeout": int, "lang": str, "region": str, "session": requests.Session, "url": str})

__all__ = ['download', 'Market', 'Search', 'Ticker', 'Tickers', 'enable_debug_mode', 'set_tz_cache_location', 'Sector', 'Industry']
# screener stuff:
__all__ += ['EquityQuery', 'FundQuery', 'screen', 'PREDEFINED_SCREENER_QUERIES']

def set_config(proxy=None, timeout=30, lang="en-US", region="US", session=None, url="finance.yahoo.com") -> 'CONFIG':
    from .data import YfData
    from .utils import deprecated
    YfData.set_config(proxy, timeout, lang, region, session, url)
    return {"proxy": proxy, "timeout": timeout, "lang": lang, "region": region, "session": session, "url": url}

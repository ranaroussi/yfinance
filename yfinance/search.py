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

"""Yahoo Finance search endpoint wrapper."""

import json as _json

from frozendict import frozendict

from . import utils
from .config import YF_CONFIG as YfConfig
from .const import _BASE_URL_
from .data import YfData
from .exceptions import YFDataException


class Search:
    """Fetch and expose structured Yahoo Finance search results."""

    _POSITIONAL_OPTION_NAMES = (
        "max_results",
        "news_count",
        "lists_count",
        "include_cb",
        "include_nav_links",
        "include_research",
        "include_cultural_assets",
        "enable_fuzzy_query",
        "recommended",
        "session",
        "timeout",
        "raise_errors",
    )

    _DEFAULT_OPTIONS = {
        "max_results": 8,
        "news_count": 8,
        "lists_count": 8,
        "include_cb": True,
        "include_nav_links": False,
        "include_research": False,
        "include_cultural_assets": False,
        "enable_fuzzy_query": False,
        "recommended": 8,
        "session": None,
        "timeout": 30,
        "raise_errors": True,
    }

    def __init__(self, query, *args, **kwargs):
        """
        Fetch and organize Yahoo Finance search results.

        Args:
            query: Search query (ticker symbol or company name).
            *args: Legacy positional options in historical constructor order.
            **kwargs: Named options matching the legacy constructor fields.
        """
        self._config = self._build_config(query, args, kwargs)
        self._data = YfData(session=self._config["session"])
        self._logger = utils.get_yf_logger()
        self._results = {
            "response": {},
            "all": {},
            "quotes": [],
            "news": [],
            "lists": [],
            "research": [],
            "nav": [],
        }

        self.search()

    @classmethod
    def _build_config(cls, query, args, kwargs):
        """Build constructor options while preserving legacy call styles."""
        config = dict(cls._DEFAULT_OPTIONS)
        config["query"] = query

        if len(args) > len(cls._POSITIONAL_OPTION_NAMES):
            max_positional = len(cls._POSITIONAL_OPTION_NAMES) + 1
            passed_positional = len(args) + 1
            raise TypeError(
                f"Search() takes at most {max_positional} positional arguments "
                f"but {passed_positional} were given"
            )

        for option_name, option_value in zip(cls._POSITIONAL_OPTION_NAMES, args):
            config[option_name] = option_value

        for option_name in cls._POSITIONAL_OPTION_NAMES:
            if option_name in kwargs:
                config[option_name] = kwargs.pop(option_name)

        if kwargs:
            unknown = ", ".join(sorted(kwargs))
            raise TypeError(f"Search() got unexpected keyword arguments: {unknown}")

        return config

    def __getattr__(self, item):
        """Preserve read access to historical configuration attributes."""
        if item in self._config:
            return self._config[item]
        raise AttributeError(f"{type(self).__name__!s} object has no attribute {item!r}")

    def search(self) -> 'Search':
        """Search using the query parameters defined in the constructor."""
        url = f"{_BASE_URL_}/v1/finance/search"
        raw_params = {
            "q": self._config["query"],
            "quotesCount": self._config["max_results"],
            "enableFuzzyQuery": self._config["enable_fuzzy_query"],
            "newsCount": self._config["news_count"],
            "quotesQueryId": "tss_match_phrase_query",
            "newsQueryId": "news_cie_vespa",
            "listsCount": self._config["lists_count"],
            "enableCb": self._config["include_cb"],
            "enableNavLinks": self._config["include_nav_links"],
            "enableResearchReports": self._config["include_research"],
            "enableCulturalAssets": self._config["include_cultural_assets"],
            "recommendedCount": self._config["recommended"],
        }
        params = frozendict(raw_params)

        self._logger.debug(
            "%s: Yahoo GET parameters: %s",
            self._config["query"],
            dict(params),
        )

        response = self._data.cache_get(url=url, params=params, timeout=self._config["timeout"])
        if response is None:
            raise YFDataException("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***")
        if "Will be right back" in response.text:
            raise YFDataException("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***")
        try:
            data = response.json()
        except _json.JSONDecodeError:
            if not YfConfig.debug.hide_exceptions:
                raise
            self._logger.error("%s: 'search' fetch received faulty data", self._config["query"])
            data = {}

        quotes = [quote for quote in data.get("quotes", []) if "symbol" in quote]
        self._results["response"] = data
        self._results["quotes"] = quotes
        self._results["news"] = data.get("news", [])
        self._results["lists"] = data.get("lists", [])
        self._results["research"] = data.get("researchReports", [])
        self._results["nav"] = data.get("nav", [])
        self._results["all"] = {
            "quotes": self._results["quotes"],
            "news": self._results["news"],
            "lists": self._results["lists"],
            "research": self._results["research"],
            "nav": self._results["nav"],
        }

        return self

    @property
    def quotes(self) -> 'list':
        """Get the quotes from the search results."""
        return self._results["quotes"]

    @property
    def news(self) -> 'list':
        """Get the news from the search results."""
        return self._results["news"]

    @property
    def lists(self) -> 'list':
        """Get the lists from the search results."""
        return self._results["lists"]

    @property
    def research(self) -> 'list':
        """Get the research reports from the search results."""
        return self._results["research"]

    @property
    def nav(self) -> 'list':
        """Get the navigation links from the search results."""
        return self._results["nav"]

    @property
    def all(self) -> 'dict[str,list]':
        """Get all the results from the search results: filtered down version of response."""
        return self._results["all"]

    @property
    def response(self) -> 'dict':
        """Get the raw response from the search results."""
        return self._results["response"]

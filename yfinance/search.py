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

import json as _json

from . import utils
from .const import _BASE_URL_
from .data import YfData

class Search:
    def __init__(self, query, max_results=8, news_count=8, lists_count=8, include_cb=True, include_nav_links=False,
                 include_research=False, include_cultural_assets=False, enable_fuzzy_query=False, recommended=8,
                 session=None, proxy=None, timeout=30, raise_errors=True):
        """
        Fetches and organizes search results from Yahoo Finance, including stock quotes and news articles.

        Args:
            query: The search query (ticker symbol or company name).
            max_results: Maximum number of stock quotes to return (default 8).
            news_count: Number of news articles to include (default 8).
            lists_count: Number of lists to include (default 8).
            include_cb: Include the company breakdown (default True).
            include_nav_links: Include the navigation links (default False).
            include_research: Include the research reports (default False).
            include_cultural_assets: Include the cultural assets (default False).
            enable_fuzzy_query: Enable fuzzy search for typos (default False).
            recommended: Recommended number of results to return (default 8).
            session: Custom HTTP session for requests (default None).
            proxy: Proxy settings for requests (default None).
            timeout: Request timeout in seconds (default 30).
            raise_errors: Raise exceptions on error (default True).
        """
        self.fields = ["quotes" if max_results else "", "news" if news_count else "", "lists" if lists_count else "", "cb" if include_cb else "", "nav_links" if include_nav_links else "", "research" if include_research else "", "cultural_assets" if include_cultural_assets else ""]
        self.query = query
        self.max_results = max_results
        self.enable_fuzzy_query = enable_fuzzy_query
        self.news_count = news_count
        self.session = session
        self.proxy = proxy
        self.timeout = timeout
        self.raise_errors = raise_errors

        self.lists_count = lists_count
        self.include_cb = include_cb
        self.nav_links = include_nav_links
        self.enable_research = include_research
        self.enable_cultural_assets = include_cultural_assets
        self.recommended = recommended

        self._response = {}
        self._all = {}
        self._quotes = []
        self._news = []
        self._lists = []
        self._research = []
        self._nav = []

        self._data = YfData()
        self._logger = utils.get_yf_logger()


        self.search()

    def fetch(self, query:'str', fields:'list[str]', session=None, proxy=None, timeout=30, **kwargs:'dict') -> 'dict':        
        params = {
            "q": query,
            "enableFuzzyQuery": "enable_fuzzy_query" in fields,
            "quotesQueryId": "tss_match_phrase_query",
            "newsQueryId": "news_cie_vespa",
            "enableCb": "cb" in fields,
            "enableNavLinks": "nav_links" in fields,
            "enableResearchReports": "research" in fields,
            "enableCulturalAssets": "cultural_assets" in fields,
        }

        
        if "quotes" in fields:
            params["quotesCount"] = kwargs.get("max_results", 8)
            params["recommendedCount"] = kwargs.get("recommended", 8)
        if "news" in fields:
            params["newsCount"] = kwargs.get("news_count", 8)
        if "lists" in fields:
            params["listsCount"] = kwargs.get("lists_count", 8)
        
        url = f"{_BASE_URL_}/v1/finance/search"
        data = self._data.cache_get(url=url, params=params, proxy=proxy, timeout=timeout, session=session)
        if data is None or "Will be right back" in data.text:
            raise RuntimeError("*** YAHOO! FINANCE IS CURRENTLY DOWN! ***\n"
                               "Our engineers are working quickly to resolve "
                               "the issue. Thank you for your patience.")
        try:
            data = data.json()
        except _json.JSONDecodeError:
            self._logger.error(f"{query}: Failed to retrieve search results and received faulty response instead.")
            data = {}
        return data


    def search(self) -> 'Search':
        """Search using the query parameters defined in the constructor."""
        self._logger.debug(f'{self.query}: Fields: {self.fields}')

        data = self.fetch(self.query, self.fields, session=self.session, proxy=self.proxy, timeout=self.timeout)

        self._response = data
        # Filter quotes to only include symbols
        self._quotes = [quote for quote in data.get("quotes", []) if "symbol" in quote]
        self._news = data.get("news", [])
        self._lists = data.get("lists", [])
        self._research = data.get("researchReports", [])
        self._nav = data.get("nav", [])

        self._all = {
            "quotes": self._quotes,
            "news": self._news,
            "lists": self._lists,
            "research": self._research,
            "nav": self._nav
        }

        return self



    def search_quotes(self) -> 'list':
        """Search using the query parameters defined in the constructor, but only return the quotes."""
        self._logger.debug(f'{self.query}: Fields: [quotes]')
        data = self.fetch(self.query, ["quotes"], session=self.session, proxy=self.proxy, timeout=self.timeout)

        self._quotes = data.get("quotes", [])
        self._response = data
        self._all["quotes"] = self._quotes
        return self.quotes

    def search_news(self) -> 'list':
        """Search using the query parameters defined in the constructor, but only return the news."""
        self._logger.debug(f'{self.query}: Fields: [news]')
        data = self.fetch(self.query, ["news"], session=self.session, proxy=self.proxy, timeout=self.timeout)

        self._news = data.get("news", [])
        self._response = data
        self._all["news"] = self._news
        return self.news
    
    def search_lists(self) -> 'list':
        """Search using the query parameters defined in the constructor, but only return the lists."""
        self._logger.debug(f'{self.query}: Fields: [lists]')
        data = self.fetch(self.query, ["lists"], session=self.session, proxy=self.proxy, timeout=self.timeout)

        self._lists = data.get("lists", [])
        self._response = data
        self._all["lists"] = self._lists
        return self.lists
    
    def search_research(self) -> 'list':
        """Search using the query parameters defined in the constructor, but only return the research reports."""
        self._logger.debug(f'{self.query}: Fields: [research]')
        data = self.fetch(self.query, ["research"], session=self.session, proxy=self.proxy, timeout=self.timeout)

        self._research = data.get("researchReports", [])
        self._response = data
        self._all["research"] = self._research
        return self.research

    def search_nav(self) -> 'list':
        """Search using the query parameters defined in the constructor, but only return the navigation links."""
        self._logger.debug(f'{self.query}: Fields: [nav]')
        data = self.fetch(self.query, ["nav"], session=self.session, proxy=self.proxy, timeout=self.timeout)

        self._nav = data.get("nav", [])
        self._response = data
        self._all["nav"] = self._nav
        return self.nav

    @property
    def quotes(self) -> 'list':
        """Get the quotes from the search results."""
        return self._quotes

    @property
    def news(self) -> 'list':
        """Get the news from the search results."""
        return self._news

    @property
    def lists(self) -> 'list':
        """Get the lists from the search results."""
        return self._lists

    @property
    def research(self) -> 'list':
        """Get the research reports from the search results."""
        return self._research

    @property
    def nav(self) -> 'list':
        """Get the navigation links from the search results."""
        return self._nav

    @property
    def all(self) -> 'dict[str,list]':
        """Get all the results from the search results: filtered down version of response."""
        return self._all

    @property
    def response(self) -> 'dict':
        """Get the raw response from the search results."""
        return self._response

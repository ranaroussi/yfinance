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

"""Container helpers for downloading data for multiple tickers."""

from __future__ import print_function

from typing import Dict

import pandas as _pd

from .options import TICKERS_DOWNLOAD_ARG_NAMES, TICKERS_DOWNLOAD_DEFAULTS, bind_options
from . import multi
from .ticker import Ticker
from .live import WebSocket
from .data import YfData


class Tickers:
    """Wrapper around multiple :class:`Ticker` objects."""

    _DOWNLOAD_ARG_NAMES = TICKERS_DOWNLOAD_ARG_NAMES
    _DOWNLOAD_DEFAULTS = TICKERS_DOWNLOAD_DEFAULTS

    def __repr__(self):
        return f"yfinance.Tickers object <{','.join(self.symbols)}>"

    def __init__(self, tickers, session=None):
        tickers = tickers if isinstance(
            tickers, list) else tickers.replace(',', ' ').split()
        self.symbols = [ticker.upper() for ticker in tickers]
        self.tickers: Dict[str, Ticker] = {
            ticker: Ticker(ticker, session=session) for ticker in self.symbols
        }

        self._data = YfData(session=session)

        self._message_handler = None
        self.ws = None

        # self.tickers = _namedtuple(
        #     "Tickers", ticker_objects.keys(), rename=True
        # )(*ticker_objects.values())

    def _parse_download_options(self, args, kwargs):
        options, passthrough = bind_options(
            "download",
            self._DOWNLOAD_ARG_NAMES,
            self._DOWNLOAD_DEFAULTS,
            args,
            kwargs,
        )
        return options, passthrough

    def history(self, *args, **kwargs):
        """Alias for :meth:`download` with the same arguments."""

        return self.download(*args, **kwargs)

    def download(self, *args, **kwargs):
        """Download price history for all symbols in this container."""

        options, passthrough = self._parse_download_options(args, kwargs)

        data = multi.download(
            self.symbols,
            start=options["start"],
            end=options["end"],
            actions=options["actions"],
            auto_adjust=options["auto_adjust"],
            repair=options["repair"],
            period=options["period"],
            interval=options["interval"],
            prepost=options["prepost"],
            group_by="ticker",
            threads=options["threads"],
            progress=options["progress"],
            timeout=options["timeout"],
            **passthrough,
        )
        if data is None:
            data = _pd.DataFrame()

        for symbol in self.symbols:
            ticker_obj = self.tickers.get(symbol)
            if ticker_obj is not None and symbol in data:
                setattr(ticker_obj, "_history", data[symbol])

        if options["group_by"] == "column" and isinstance(data.columns, _pd.MultiIndex):
            data.columns = data.columns.swaplevel(0, 1)
            data.sort_index(level=0, axis=1, inplace=True)

        return data

    def news(self):
        """Get the latest news entries for each symbol."""

        return {ticker: list(Ticker(ticker).news) for ticker in self.symbols}

    def live(self, message_handler=None, verbose=True):
        """Start a websocket stream subscribed to all symbols."""

        self._message_handler = message_handler

        self.ws = WebSocket(verbose=verbose)
        self.ws.subscribe(self.symbols)
        self.ws.listen(self._message_handler)

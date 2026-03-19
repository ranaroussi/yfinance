=============
API Reference
=============

Overview
--------

The `yfinance` package provides easy access to Yahoo! Finance's API to retrieve market data. It includes classes and functions for downloading historical market data, accessing ticker information, managing cache, and more.


Public API
==========

The following are the publicly available classes, and functions exposed by the `yfinance.client` module:

- :attr:`Ticker <yfinance.client.Ticker>`: Class for accessing single ticker data.
- :attr:`Tickers <yfinance.client.Tickers>`: Class for handling multiple tickers.
- :attr:`Market <yfinance.client.Market>`: Class for accessing market summary.
- :attr:`Calendars <yfinance.client.Calendars>`: Class for accessing calendar events data.
- :attr:`download <yfinance.client.download>`: Function to download market data for multiple tickers.
- :attr:`Search <yfinance.client.Search>`: Class for accessing search results.
- :attr:`Lookup <yfinance.client.Lookup>`: Class for looking up tickers.
- :class:`WebSocket <yfinance.client.WebSocket>`: Class for synchronously streaming live market data.
- :class:`AsyncWebSocket <yfinance.client.AsyncWebSocket>`: Class for asynchronously streaming live market data.
- :attr:`Sector <yfinance.client.Sector>`: Domain class for accessing sector information.
- :attr:`Industry <yfinance.client.Industry>`: Domain class for accessing industry information.
- :attr:`EquityQuery <yfinance.client.EquityQuery>`: Class to build equity query filters.
- :attr:`FundQuery <yfinance.client.FundQuery>`: Class to build fund query filters.
- :attr:`screen <yfinance.client.screen>`: Run equity/fund queries.
- :attr:`config.debug.logging <yfinance.client.config>`: Enable verbose debug logging (``yf.config.debug.logging = True``).
- :attr:`set_tz_cache_location <yfinance.client.set_tz_cache_location>`: Function to set the timezone cache location.

.. toctree::
   :maxdepth: 1
   :hidden:


   yfinance.ticker_tickers
   yfinance.stock
   yfinance.market
   yfinance.calendars
   yfinance.financials
   yfinance.analysis
   yfinance.search
   yfinance.lookup
   yfinance.websocket
   yfinance.sector_industry
   yfinance.screener
   yfinance.functions

   yfinance.funds_data
   yfinance.price_history

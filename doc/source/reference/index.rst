=============
API Reference
=============

Overview
--------

The `yfinance` package provides easy access to Yahoo! Finance's API to retrieve market data. It includes classes and functions for downloading historical market data, accessing ticker information, managing cache, and more.


Public API
==========

The following are the publicly available classes, and functions exposed by the `yfinance` package:

- :attr:`Ticker <yfinance.Ticker>`: Class for accessing single ticker data.
- :attr:`Tickers <yfinance.Tickers>`: Class for handling multiple tickers.
- :attr:`download <yfinance.download>`: Function to download market data for multiple tickers.
- :attr:`Search <yfinance.Search>`: Class for accessing search results.
- :attr:`Sector <yfinance.Sector>`: Domain class for accessing sector information.
- :attr:`Industry <yfinance.Industry>`: Domain class for accessing industry information.
- :attr:`Market <yfinance.Market>`: Class for accessing market status & summary.
- :attr:`EquityQuery <yfinance.EquityQuery>`: Class to build equity query filters.
- :attr:`FundQuery <yfinance.FundQuery>`: Class to build fund query filters.
- :attr:`screen <yfinance.screen>`: Run equity/fund queries.
- :attr:`enable_debug_mode <yfinance.enable_debug_mode>`: Function to enable debug mode for logging.
- :attr:`set_tz_cache_location <yfinance.set_tz_cache_location>`: Function to set the timezone cache location.

.. toctree::
   :maxdepth: 1
   :hidden:


   yfinance.ticker_tickers
   yfinance.stock
   yfinance.financials
   yfinance.analysis
   yfinance.market
   yfinance.search
   yfinance.sector_industry
   yfinance.screener
   yfinance.functions

   yfinance.funds_data
   yfinance.price_history

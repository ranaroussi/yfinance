=====================
Market
=====================

.. currentmodule:: yfinance


Class
------------
The `Market` class, allows you to access market data in a Pythonic way.

.. autosummary::
   :toctree: api/

   Market

Market Sample Code
------------------

.. literalinclude:: examples/market.py
   :language: python


Markets
------------
There are 8 different markets available in Yahoo Finance.

* US
* GB

\ 

* ASIA
* EUROPE

\ 

* RATES
* COMMODITIES
* CURRENCIES
* CRYPTOCURRENCIES

.. note::
   Only `Market.summary` returns regional data for all of the values above.
   `Market.status` is backed by Yahoo's `markettime` endpoint, which currently
   ignores the `market` parameter and only returns U.S. data; for any non-`US`
   market, `status` will therefore be `None` and a warning is logged.
=====================
Ticker and Tickers
=====================

.. currentmodule:: yfinance


Class
------------
The `Ticker` module, allows you to access ticker data in a Pythonic way.

.. autosummary::
   :toctree: api/

   Ticker
   Tickers


Ticker Sample Code
------------------
The `Ticker` module, allows you to access ticker data in a Pythonic way.

.. literalinclude:: examples/ticker.py
   :language: python

To initialize multiple `Ticker` objects, use

.. literalinclude:: examples/tickers.py
   :language: python

For tickers that are ETFs/Mutual Funds, `Ticker.funds_data` provides access to fund related data. 

Funds' Top Holdings and other data with category average is returned as `pd.DataFrame`.

.. literalinclude:: examples/funds_data.py
   :language: python

If you want to use a proxy server for downloading data, use:

.. literalinclude:: examples/proxy.py
   :language: python

To initialize multiple `Ticker` objects, use `Tickers` module

.. literalinclude:: examples/tickers.py
   :language: python

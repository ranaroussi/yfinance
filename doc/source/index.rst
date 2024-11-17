yfinance documentation
======================

Download Market Data from Yahoo! Finance's API
----------------------------------------------

.. admonition:: IMPORTANT LEGAL DISCLAIMER

   **Yahoo!, Y!Finance, and Yahoo! finance are registered trademarks of Yahoo, Inc.**

   yfinance is **not** affiliated, endorsed, or vetted by Yahoo, Inc. It's
   an open-source tool that uses Yahoo's publicly available APIs, and is
   intended for research and educational purposes.

   **You should refer to Yahoo!'s terms of use**
   (`here <https://policies.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.htm>`__),
   (`here <https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html>`__),
   and (`here <https://policies.yahoo.com/us/en/yahoo/terms/index.htm>`__)
   for details on your rights to use the actual data downloaded.
   Remember - the Yahoo! finance API is intended for personal use only.

Install
-------

.. code-block:: bash

    $ pip install yfinance

Quick start
-----------

Showing a small sample of yfinance API, the full API is much bigger and covered in :doc:`reference/index`.

.. code-block:: python

   import yfinance as yf
   dat = yf.Ticker("MSFT")


One ticker symbol

.. code-block:: python

   dat = yf.Ticker("MSFT")
   dat.info
   dat.calendar
   dat.analyst_price_targets
   dat.quarterly_income_stmt
   dat.history(period='1mo')
   dat.option_chain(dat.options[0]).calls

Multiple ticker symbols

.. code-block:: python

   tickers = yf.Tickers('MSFT AAPL GOOG')
   tickers.tickers['MSFT'].info
   yf.download(['MSFT', 'AAPL', 'GOOG'], period='1mo')

Funds

.. code-block:: python

   spy = yf.Ticker('SPY').funds_data
   spy.description
   spy.top_holdings

.. toctree::
   :maxdepth: 1
   :titlesonly:

   advanced/index
   reference/index
   development/index

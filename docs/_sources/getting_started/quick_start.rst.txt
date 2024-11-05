********************
Quick Start
********************

The Ticker module allows you to access ticker data in a more Pythonic way:

.. code-block:: python

   import yfinance as yf

   msft = yf.Ticker("MSFT")

   # get all stock info
   msft.info

   # get historical market data
   hist = msft.history(period="1mo")

   # show actions (dividends, splits, capital gains)
   msft.actions
   msft.dividends
   msft.splits

To work with multiple tickers, use:

.. code-block:: python

   tickers = yf.Tickers('msft aapl goog')
   tickers.tickers['MSFT'].info
   tickers.tickers['AAPL'].history(period="1mo")

Yahoo! Finance Fix for Pandas Datareader
========================================

.. image:: https://img.shields.io/badge/python-2.7,%203.4+-blue.svg?style=flat
    :target: https://pypi.python.org/pypi/fix-yahoo-finance
    :alt: Python version

.. image:: https://img.shields.io/pypi/v/fix-yahoo-finance.svg?maxAge=60
    :target: https://pypi.python.org/pypi/fix-yahoo-finance
    :alt: PyPi version

.. image:: https://img.shields.io/pypi/status/fix-yahoo-finance.svg?maxAge=60
    :target: https://pypi.python.org/pypi/fix-yahoo-finance
    :alt: PyPi status

.. image:: https://img.shields.io/travis/ranaroussi/fix-yahoo-finance/master.svg?maxAge=1
    :target: https://travis-ci.org/ranaroussi/fix-yahoo-finance
    :alt: Travis-CI build status

.. image:: https://img.shields.io/github/stars/ranaroussi/fix-yahoo-finance.svg?style=social&label=Star&maxAge=60
    :target: https://github.com/ranaroussi/fix-yahoo-finance
    :alt: Star this repo

.. image:: https://img.shields.io/twitter/follow/aroussi.svg?style=social&label=Follow&maxAge=60
    :target: https://twitter.com/aroussi
    :alt: Follow me on twitter

\

`Yahoo! finance <https://ichart.finance.yahoo.com>`_ has decommissioned
their historical data API, causing many programs that relied on it to stop working.

**fix-yahoo-finance** fixes the problem by scraping the data from Yahoo! finance
and returning a Pandas DataFrame in the same format as **pandas_datareader**'s
``get_data_yahoo()``.

By basically "hijacking" ``pandas_datareader.data.get_data_yahoo()`` method,
**fix-yahoo-finance**'s implantation is easy and only requires to import
``fix_yahoo_finance`` into your code.

`Changelog » <./CHANGELOG.rst>`__

-----

==> Check out this `Blog post <https://aroussi.com/#post/python-yahoo-finance>`_ for a detailed tutorial with code examples.

-----

Quick Start
===========

The Ticker module
~~~~~~~~~~~~~~~~~

The ``Ticker`` module, which allows you to access
ticker data in amore Pythonic way:

.. code:: python

    import fix_yahoo_finance as yf

    msft = yf.Ticker("MSFT")

    # get stock info
    msft.info

    # get historical market data
    hist = msft.history(period="max")

    # show actions (dividends, splits)
    msft.actions

    # show dividends
    msft.dividends

    # show splits
    msft.splits


Fetching data for multiple tickers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    import fix_yahoo_finance as yf
    data = yf.download("SPY AAPL", start="2017-01-01", end="2017-04-30")


I've also added some options to make life easier :)

.. code:: python

    data = yf.download(  # or pdr.get_data_yahoo(...
            # tickers list or string as well
            tickers = "SPY IWM TLT",

            # use "period" instead of start/end
            # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
            # (optional, default is '1mo')
            period = "mtd",

            # fetch data by interval (including intraday if period < 60 days)
            # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
            # (optional, default is '1d')
            interval = "1m",

            # group by ticker (to access via data['SPY'])
            # (optional, default is 'column')
            group_by = 'ticker',

            # adjust all OHLC automatically
            # (optional, default is False)
            auto_adjust = True,

            # download pre/post regular market hours data
            # (optional, default is False)
            prepost = True
        )


``pandas_datareader`` override
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    from pandas_datareader import data as pdr

    import fix_yahoo_finance as yf
    yf.pdr_override() # <== that's all it takes :-)

    # download dataframe
    data = pdr.get_data_yahoo("SPY", start="2017-01-01", end="2017-04-30")


Installation
------------

Install ``fix_yahoo_finance`` using ``pip``:

.. code:: bash

    $ pip install fix_yahoo_finance --upgrade --no-cache-dir


Requirements
------------

* `Python <https://www.python.org>`_ >= 2.7, 3.4+
* `Pandas <https://github.com/pydata/pandas>`_ (tested to work with >=0.23.1)
* `Numpy <http://www.numpy.org>`_ >= 1.11.1
* `requests <http://docs.python-requests.org/en/master/>`_ >= 2.14.2


Optional (if you want to use ``pandas_datareader``)
---------------------------------------------------

* `pandas_datareader <https://github.com/pydata/pandas-datareader>`_ >= 0.4.0

Legal Stuff
------------

**fix-yahoo-finance** is distributed under the **Apache Software License**. See the `LICENSE.txt <./LICENSE.txt>`_ file in the release for details.


P.S.
------------

Please drop me an note with any feedback you have.

**Ran Aroussi**

Yahoo! Finance Fix for Pandas Datareader
========================================

.. image:: https://img.shields.io/pypi/pyversions/fix-yahoo-finance.svg?maxAge=60
    :target: https://pypi.python.org/pypi/fix-yahoo-finance
    :alt: Python version

.. image:: https://img.shields.io/travis/ranaroussi/fix-yahoo-finance/master.svg?
    :target: https://travis-ci.org/ranaroussi/fix-yahoo-finance
    :alt: Travis-CI build status

.. image:: https://img.shields.io/pypi/v/fix-yahoo-finance.svg?maxAge=60
    :target: https://pypi.python.org/pypi/fix-yahoo-finance
    :alt: PyPi version

.. image:: https://img.shields.io/pypi/status/fix-yahoo-finance.svg?maxAge=60
    :target: https://pypi.python.org/pypi/fix-yahoo-finance
    :alt: PyPi status

.. image:: https://img.shields.io/github/stars/ranaroussi/fix-yahoo-finance.svg?style=social&label=Star&maxAge=60
    :target: https://github.com/ranaroussi/fix-yahoo-finance
    :alt: Star this repo

.. image:: https://img.shields.io/twitter/follow/aroussi.svg?style=social&label=Follow%20Me&maxAge=60
    :target: https://twitter.com/aroussi
    :alt: Follow me on twitter

\

`Yahoo! finance <https://ichart.finance.yahoo.com>`_ has decommissioned
their historical data API, causing many programs that relied on it to stop working.

**fix-yahoo-finance** offers a **temporary fix** to the problem
by scraping the data from Yahoo! finance using and return a Pandas
DataFrame/Panel in the same format as **pandas_datareader**'s ``get_data_yahoo()``.

By basically "hijacking" ``pandas_datareader.data.get_data_yahoo()`` method,
**fix-yahoo-finance**'s implantation is easy and only requires to import
``fix_yahoo_finance`` into your code.


`Changelog » <./CHANGELOG.rst>`__

-----

Quick Start
===========

.. code:: python

    from pandas_datareader import data as pdr
    import fix_yahoo_finance  # <== that's all it takes :-)

    # download dataframe
    data = pdr.get_data_yahoo("SPY", start="2017-01-01", end="2017-04-30")

    # download Panel
    data = pdr.get_data_yahoo(["SPY", "IWM"], start="2017-01-01", end="2017-04-30")


I've also added some options to make life easier :)

Below is the full list of acceptable parameters:

.. code:: python

    data = pdr.get_data_yahoo(
                # tickers list (single tickers accepts a string as well)
                tickers = ["SPY", "IWM", "..."],

                # start date (YYYY-MM-DD / datetime.datetime object)
                # (optional, defaults is 1950-01-01)
                start = "2017-01-01",

                # end date (YYYY-MM-DD / datetime.datetime object)
                # (optional, defaults is Today)
                end = "2017-04-30",

                # return a multi-index dataframe
                # (optional, default is Panel, which is deprecated)
                as_panel = False,

                # group by ticker (to access via data['SPY'])
                # (optional, default is 'column')
                group_by = 'ticker',

                # adjust all OHLC automatically
                # (optional, default is False)
                auto_adjust = True,

                # download dividend + stock splits data
                # (optional, default is None)
                # options are:
                #   - True (returns history + actions)
                #   - 'only' (actions only)
                actions = True,

                # How may threads to use?
                threads = 10
            )


It can also be used as a stand-alone library (without ``pandas_datareader``) if you want:

.. code:: python

    import fix_yahoo_finance as yf
    data = yf.download("SPY", start="2017-01-01", end="2017-04-30")


Installation
------------

Install ``fix_yahoo_finance`` using ``pip``:

.. code:: bash

    $ pip install fix_yahoo_finance --upgrade --no-cache-dir


Requirements
------------

* `Python <https://www.python.org>`_ >=3.4
* `Pandas <https://github.com/pydata/pandas>`_ (tested to work with >=0.18.1)
* `Numpy <http://www.numpy.org>`_ >= 1.11.1
* `requests <http://docs.python-requests.org/en/master/>`_ >= 2.14.2
* `multitasking <https://github.com/ranaroussi/multitasking>`_ >= 0.0.3


Optional (if you want to use ``pandas_datareader``)
---------------------------------------------------

* `pandas_datareader <https://github.com/pydata/pandas-datareader>`_ >= 0.4.0

Legal Stuff
------------

**fix-yahoo-finance** is distributed under the **GNU Lesser General Public License v3.0**. See the `LICENSE.txt <./LICENSE.txt>`_ file in the release for details.


P.S.
------------

Please drop me an note with any feedback you have.

**Ran Aroussi**

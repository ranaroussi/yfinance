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

**fix-yahoo-finance** offers a **temporary fix** to the problem
by using `Selenium <http://www.seleniumhq.org>`_ to scrape the data
from Yahoo! finance and return a Pandas DataFrame/Panel in the same
format as the original ``get_data_yahoo()`` method.

`Yahoo! finance <https://ichart.finance.yahoo.com>`_ has decommissioned their historical data API,
causing many programs that relied on the ``get_data_yahoo()``
to stop working.

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
                auto_adjust = True
            )


Installation
------------

Install ``fix_yahoo_finance`` using ``pip``:

.. code:: bash

    $ pip install fix_yahoo_finance --upgrade --no-cache-dir


Requirements
------------

* `Python <https://www.python.org>`_ >=3.4
* `Pandas <https://github.com/pydata/pandas>`_ (tested to work with >=0.18.1)
* `pandas_datareader <https://github.com/pydata/pandas-datareader>`_ >= 0.4.0
* `Numpy <http://www.numpy.org>`_ >= 1.11.1
* `Selenium <http://www.seleniumhq.org>`_ >= 3.0.2
* `PyVirtualDisplay <https://github.com/ponty/pyvirtualdisplay>`_ >= 0.2.1 (optional, to)

Legal Stuff
------------

**fix-yahoo-finance** is distributed under the **GNU Lesser General Public License v3.0**. See the `LICENSE.txt <./LICENSE.txt>`_ file in the release for details.


P.S.
------------

Please drop me an note with any feedback you have.

**Ran Aroussi**

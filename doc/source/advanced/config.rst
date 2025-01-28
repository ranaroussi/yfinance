======
Config
======

`yfinance` allows you to set configurations for requests.

You can set:
* proxy
* timeout
* lang
* region
* session
* url

.. code-block:: python

    yf.set_config(proxy=None, timeout=30, lang="en-US", region="US", session=None, url="finance.yahoo.com")

.. important::
    DO NOT ADD `https://` to start of url. This will cause an error.
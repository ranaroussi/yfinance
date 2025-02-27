======
Config
======

.. currentmodule:: yfinance

`yfinance` allows you to set configurations for requests.

You can set:
* proxy
* timeout
* lang
* region
* session
* url

.. important::
    DO NOT ADD `https://` to start of url. This will cause an error.

There are two ways to set config:

1. Using the `set_config` function

.. code-block:: python
    :linenos:

    import yfinance as yf
    yf.set_config(proxy="http://127.0.0.1:1080")
    yf.set_config(timeout=30)
    yf.set_config(lang="en")
    yf.set_config(region="US")
    yf.set_config(session=False)
    yf.set_config(url="https://finance.yahoo.com")

or you can do them all at once:

.. code-block:: python
    :linenos:

    yf.set_config(
        proxy="http://127.0.0.1:1080",
        timeout=30,
        lang="en",
        region="US",
        session=False,
        url="https://finance.yahoo.com"
    )

2. Using the `reset_config` function

.. code-block:: python
    :linenos:

    yf.reset_config(proxy=True, timeout=True, lang=True, region=True, session=True, url=True) # default

.. note::
    This reverts all settings to `YfData.DEFAULT_{SETTING}`

****
URLS
****

The `url` parameter is used to set the base url for the Yahoo API. This is useful if you are using a different API

You can also set the url for each endpoint individually

.. autosummary::
   :toctree: api/

   YfData

.. code-block:: python
    :linenos:

    class URLS(yf.URLS):
        @property
        def {ENDPOINT}_URL(self):
            return custom_endpoint

    yf.YfData.URL = URLS()
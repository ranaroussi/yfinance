******
Config
******

`yfinance` has a new global config for sharing common values:

.. code-block:: python

  >>> import yfinance as yf
  >>> yf.config
  {
    "network": {
      "proxy": null,
      "retries": 0
    },
    "debug": {
      "hide_exceptions": true,
      "logging": false
    }
  }
  >>> yf.config.network
  {
    "proxy": null,
    "retries": 0
  }


Network
-------

* **proxy** - Set proxy for all yfinance data fetches.

  .. code-block:: python

     yf.config.network.proxy = "PROXY_SERVER"

* **retries** - Configure automatic retry for transient network errors. The retry mechanism uses exponential backoff (1s, 2s, 4s...).

  .. code-block:: python

     yf.config.network.retries = 2

Debug
-----

* **hide_exceptions** - Set to `False` to stop yfinance hiding exceptions.

  .. code-block:: python

     yf.config.debug.hide_exceptions = False

* **logging** - Set to `True` to enable verbose debug logging.

  .. code-block:: python

     yf.config.debug.logging = True

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
      "retries": 0,
      "cache_ttl": 60
    },
    "debug": {
      "hide_exceptions": true,
      "logging": false
    }
  }
  >>> yf.config.network
  {
    "proxy": null,
    "retries": 0,
    "cache_ttl": 60
  }


Network
-------

* **proxy** - Set proxy for all yfinance data fetches.

  .. code-block:: python

     yf.config.network.proxy = "PROXY_SERVER"

* **retries** - Configure automatic retry for transient network errors. The retry mechanism uses exponential backoff (1s, 2s, 4s...).

  .. code-block:: python

     yf.config.network.retries = 2

* **cache_ttl** - Seconds that repeated identical requests reuse a cached
  response (default 60). ``0`` disables the cache, ``None`` never expires
  entries. Error responses are never cached.

  .. code-block:: python

     yf.config.network.cache_ttl = 10    # live service: keep data fresh
     yf.config.network.cache_ttl = None  # backtest: fetch once per process

Debug
-----

* **hide_exceptions** - Set to `False` to stop yfinance hiding exceptions.

  .. code-block:: python

     yf.config.debug.hide_exceptions = False

* **logging** - Set to `True` to enable verbose debug logging.

  .. code-block:: python

     yf.config.debug.logging = True

Locale
------

Localized fields (``longName``, ``shortName``, ...) follow
``yf.config.locale``. The default is ``en-US`` / ``US``. Switch the Yahoo
locale once at session start and every subsequent v7 / v10 endpoint call
(``Ticker.info``, ``Ticker.fast_info``, ``Ticker.calendar``,
``earnings_dates``, ...) inherits it:

.. code-block:: python

   import yfinance as yf
   yf.config.locale.lang = "zh-Hant-HK"
   yf.config.locale.region = "HK"
   yf.Ticker("1810.HK").info["longName"]   # → '小米集團－Ｗ'

   yf.config.locale.lang = "ja-JP"
   yf.config.locale.region = "JP"
   yf.Ticker("7203.T").info["longName"]    # → 'トヨタ自動車'

Yahoo only returns translated values for tickers natively listed in that
locale; ``Ticker("AAPL")`` keeps returning ``"Apple Inc."`` under
``ja-JP``.

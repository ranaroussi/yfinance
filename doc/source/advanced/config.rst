******
Config
******

`yfinance` has a new global config for sharing common values.

Proxy
-----

Set proxy once in config, affects all yfinance data fetches.

.. code-block:: python

   import yfinance as yf
   yf.set_config(proxy="PROXY_SERVER")

Retries
-------

Configure automatic retry for transient network errors. The retry mechanism uses exponential backoff (1s, 2s, 4s...).

.. code-block:: python

   import yfinance as yf
   yf.set_config(retries=2)

Set to 0 to disable retries (default behavior).

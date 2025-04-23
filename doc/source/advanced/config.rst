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

************
Proxy Server
************

You can download data via a proxy:

.. code-block:: python

   msft = yf.Ticker("MSFT")
   msft.history(..., proxy="PROXY_SERVER")


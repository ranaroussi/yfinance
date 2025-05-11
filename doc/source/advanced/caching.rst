Caching
=======

Persistent Cache
----------------

To reduce Yahoo, yfinance store some data locally: timezones to localize dates, and cookie. Cache location is:

- Windows = C:/Users/\<USER\>/AppData/Local/py-yfinance
- Linux = /home/\<USER\>/.cache/py-yfinance
- MacOS = /Users/\<USER\>/Library/Caches/py-yfinance

You can direct cache to use a different location with :attr:`set_tz_cache_location <yfinance.set_tz_cache_location>`:

.. code-block:: python

    import yfinance as yf
    yf.set_tz_cache_location("custom/cache/location")
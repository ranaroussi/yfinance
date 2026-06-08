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

Custom Session Caching
----------------------

Do not pass a caching session, such as ``requests_cache.CachedSession``, to
yfinance. Those sessions wrap ``requests.Session`` and are not compatible with
yfinance's ``curl_cffi`` session handling. They can also miss when Yahoo rotates
cookies or crumbs.

For yfinance's supported persistent cache, leave ``session`` unset and configure
the cache location with :attr:`set_tz_cache_location <yfinance.set_tz_cache_location>`.

Caching
=======

Smarter Scraping
----------------

Install the `nospam` package to cache API calls and reduce spam to Yahoo:

.. code-block:: bash

   pip install yfinance[nospam]

To use a custom `requests` session, pass a `session=` argument to
the Ticker constructor. This allows for caching calls to the API as well as a custom way to modify requests via  the `User-agent` header.

.. code-block:: python

   import requests_cache
   session = requests_cache.CachedSession('yfinance.cache')
   session.headers['User-agent'] = 'my-program/1.0'
   ticker = yf.Ticker('MSFT', session=session)
   
   # The scraped response will be stored in the cache
   ticker.actions


Combine `requests_cache` with rate-limiting to avoid triggering Yahoo's rate-limiter/blocker that can corrupt data.

.. code-block:: python

   from requests import Session
   from requests_cache import CacheMixin, SQLiteCache
   from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
   from pyrate_limiter import Duration, RequestRate, Limiter
   class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
      pass

   session = CachedLimiterSession(
      limiter=Limiter(RequestRate(2, Duration.SECOND*5)),  # max 2 requests per 5 seconds
      bucket_class=MemoryQueueBucket,
      backend=SQLiteCache("yfinance.cache"),
   )


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
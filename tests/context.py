# -*- coding: utf-8 -*-

import appdirs as _ad
import datetime as _dt
import sys
import os
_parent_dp = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
_src_dp = _parent_dp
sys.path.insert(0, _src_dp)

import yfinance


# Optional: see the exact requests that are made during tests:
# import logging
# logging.basicConfig(level=logging.DEBUG)


# Use adjacent cache folder for testing, delete if already exists and older than today
testing_cache_dirpath = os.path.join(_ad.user_cache_dir(), "py-yfinance-testing")
yfinance.set_tz_cache_location(testing_cache_dirpath)
if os.path.isdir(testing_cache_dirpath):
    mtime = _dt.datetime.fromtimestamp(os.path.getmtime(testing_cache_dirpath))
    if mtime.date() < _dt.date.today():
        import shutil
        shutil.rmtree(testing_cache_dirpath)


# Setup a session to rate-limit and cache persistently:
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    pass
from pyrate_limiter import Duration, RequestRate, Limiter
history_rate = RequestRate(1, Duration.SECOND*2)
limiter = Limiter(history_rate)
cache_fp = os.path.join(testing_cache_dirpath, "unittests-cache")
session_gbl = CachedLimiterSession(
    limiter=limiter,
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache(cache_fp, expire_after=_dt.timedelta(hours=1)),
)
# Use this instead if only want rate-limiting:
# from requests_ratelimiter import LimiterSession
# session_gbl = LimiterSession(limiter=limiter)


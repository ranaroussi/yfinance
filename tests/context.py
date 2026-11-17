# -*- coding: utf-8 -*-

import platformdirs as _ad
import datetime as _dt
import sys
import os
import yfinance

_parent_dp = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
_src_dp = _parent_dp
sys.path.insert(0, _src_dp)

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

# Since switching to curl_cffi, requests_cache/requests_ratelimiter no longer work; use no session.
session_gbl = None

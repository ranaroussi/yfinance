---
name: Change Log
description: bugs found and fixed in March 2026
type: project
---

Main repo is found at: https://github.com/ranaroussi/yfinance
This fork is found at: https://github.com/ryroeu/yfinance

**Why:** Needed to understand project state before contributing. Established clean baseline.
**How to apply:** When working on new features, run `python -m pytest tests/ --ignore=tests/test_price_repair.py --ignore=tests/test_live.py --ignore=tests/test_cache_noperms.py` as the baseline. Expect 100 passing, No rate-limit flakes.

**Changes:**
1. `cache.py:set_cache_location()` — didn't reset `_TzCacheManager._tz_cache`, `_CookieCacheManager._Cookie_cache`, `_ISINCacheManager._isin_cache` singletons. After `test_cache.py` ran, all subsequent tests got stale closed-DB connections → `peewee.OperationalError`. Fixed by resetting singletons in `set_cache_location()`.
2. `tests/test_cache.py:tearDownClass` — called `close_db()` + deleted tempdir but didn't restore cache location. Fixed by calling `set_tz_cache_location(original_dir)` before cleanup.
3. `scrapers/history/client.py` — `quotes.index[0].floor('D')` raises `ValueError` for DST-ambiguous timestamps (e.g. Israeli stocks on ESLT.TA). Fixed with try/except, fallback to `pd.Timestamp(date).tz_localize(tz, ambiguous=True, nonexistent='shift_forward')`.
4. `scrapers/funds.py:190` — `data["topHoldings"]` and `data["fundProfile"]` raised `KeyError` when Yahoo API omits those keys for some funds. Fixed by using `.get()` with empty dict fallback. Also added explicit `YFDataException` for non-ETF/MUTUALFUND tickers based on `_quote_type` check.
5. `setup.py` — `lxml` was missing from `install_requires`. `pd.read_html()` requires it. Added `lxml>=4.9.1`.
6. `utils.py:636` — `pd.Timedelta('1d')` causes pandas deprecation warning (`'d'` deprecated in favor of `'D'`). Fixed by normalising the interval string.
7. `tests/test_search.py:test_fuzzy_query` — checked `search.quotes[0]['symbol'] == 'AAPL'` but Yahoo ranking changed. Fixed to check AAPL appears anywhere in results.
8. `tests/test_prices.py:test_prune_post_intraday_asx` — used hardcoded 2024 date range which is now outside Yahoo's 730-day 1h data window. Fixed to use dynamic rolling 180-day window.
9. `tests/test_ticker.py:assert_attribute_type` — `isinstance(Union[A,B], _GenericAlias)` returns False in Python 3.14. Fixed to use `getattr(expected_type, '__origin__', None) is Union`.
10. `new files created to address too many lines and simplify future code changes` utils_doc, utils_financial, utils_price, fundamentals_keys, price_repair_assumptions_cases, price_repair_cases, price_repair_support, ticker_core_cases, ticker_financial_cases, ticker_info_cases, ticker_support
11. `extensive syntax remediation using pyright and pylint`
12. `new files created to address too many lines and simplify future code changes` price_repair_assumptions_cases, price_repair_cases, price_repair_support, ticker_core_cases, ticker_financial_cases, ticker_info_cases, ticker_support, history/helpers, history/price_repair, history/flow, history/dividend_repair, history/capital_gains, history/reconstruct, history/repair_workflows, history/split_repair
13. `extensive syntax remediation using pyright and pylint`
14. `created new files to address duplication` utils, http, options
15. `fixed broken code in test scripts`:
Fix 1 — utils.py:safe_merge_dfs: Removed _interval_to_timedelta_strict wrapper that incorrectly rejected relativedelta for day/week/month intervals (1d, 1wk, 1mo, 3mo). Now uses _interval_to_timedelta directly.

Fix 2 — fetch.py:_slice_actions_to_window: Empty capital_gains DataFrame was replaced with a tz-naive index AFTER set_df_tz, then sliced with a tz-aware start_d. Fixed by passing tz=state.tz_exchange to the empty DatetimeIndex.

Fix 3 — utils_price.py:safe_merge_dfs_impl: When all events were out-of-range, _handle_out_of_range_events set df_main["Dividends"] = 0.0 and returned empty df_sub. The subsequent df_main.join(df_sub) then failed with a column overlap error. Fixed by returning early when df_sub is empty.

Fix 4 — Test issues (3 fixes):
-test_calendars.py: Asserted event time ≥ current time (fails for morning events run in the afternoon) → compare .date() instead
-test_intraday_with_events / test_intraday_with_events_tase: Compared tz-aware daily index to tz-naive intraday index → normalize daily dates to tz-naive before comparing
-test_prune_post_intraday_us: Hardcoded Thanksgiving 2024 date, whose full-year 1h data exceeds Yahoo's 730-day retention → updated to Thanksgiving 2025
16. `fixed broken code that surfaced during testing`:
-tests/test_ticker.py — Rewrote load_tests (unittest-only protocol) as direct imports, so pytest now discovers 25 ticker tests it was silently skipping.
-tests/ticker_financial_cases.py — Added supports_trailing=False for get_balance_sheet since trailing frequency is correctly rejected for point-in-time statements.
-yfinance/multi.py — Fixed a thread-safety race condition: _download_one was mutating the global YF_CONFIG.debug.hide_exceptions = False and restoring it in finally, but the restore could happen after the main thread moved on (polling only on dfs being populated). Replaced with raise_errors=True passed directly to history(), eliminating the global mutation entirely.
17. `fixed broken code that surfaced during testing`: 99 passed. The 8 sub-failures were the same root cause as before: hide_exceptions = False left leaked. The tearDown in TestTickerCore now resets it after every test in that class, so TestTickerInfoCases (which runs later in the same file) always sees hide_exceptions = True and HTTP errors are suppressed rather than raised.
18. `fixed pyright flagging the TestSuite`: 
The root cause: pytest scanned the module's namespace, found TestSuite (imported directly), and tried to collect it as a test class. Removing the explicit import and using unittest.TestSuite as both the type annotation and constructor call resolves it.
19. `fixed deprecated code`: The fix replaces raise_errors=True (deprecated) with temporarily setting YfConfig.debug.hide_exceptions = False before the call and restoring it in a finally block — which is exactly what the deprecation warning was telling callers to do.
20. `fix for race condition in threaded download path`: Removed deprecated raise_errors parameter entirely from _HistoryRequest, _FetchState, options.py, and fetch.py. Renamed hide_exceptions → raise_on_error (inverted polarity: False = log, True = raise) across all source and test files. Fixed the thread-safety race condition by moving the single save/restore of raise_on_error up to _download_all_tickers — the one dispatcher that owns both the threaded and synchronous paths — so worker threads only ever read the global, never write it. Previously each thread saved and restored independently, causing concurrent threads to corrupt each other's saved value.
21. `fix for outdated pandas behavior`: The test was holding onto outdated pandas behavior — older pandas silently dropped timezone info, newer pandas (correctly) raises ValueError. Replaced @expectedFailure with assertRaises(ValueError), turning it from a documentation stub into a real passing test.
22. `added export code to init files`
23. `updated requirements.txt`
24. `updated version to 2.0.0`
25. `confirmed that these known issues are fixed`: 2688, 1924, 1801, 1951, 930
26. `confirmed that these known issues are fixed`: 1804 and 1765
27. `confirmed that these known issues are fixed`: 1811 and 2146
28. `confirmed that these known issues are fixed`: 2500
29. `confirmed that these known issues are fixed`: 1855
30. `confirmed that these known issues are fixed`: 1957
31. `confirmed that these known issues are fixed`: 2348
32. `confirmed that these known issues are fixed`: 1518
33. `fixed unavailable-quote fast_info metadata handling`: missing `currency`-style metadata keys now return `None` instead of raising `KeyError` (`#1951`)
34. `confirmed that these known issues are fixed`: 1820
35. `confirmed that these known issues are fixed`: 2044
36. `confirmed that these known issues are fixed`: 1115
37. `confirmed that these known issues are fixed`: 521
38. `confirmed that these known issues are fixed`: 1718
39. `confirmed that these known issues are fixed`: 1813
40. `confirmed that these known issues are fixed`: 1895
41. `confirmed that these known issues are fixed`: 860
42. `confirmed that these known issues are fixed`: 1272
43. `confirmed that these known issues are fixed`: 1382
44. `confirmed that these known issues are fixed`: 610
45. `confirmed that these known issues are fixed`: 515
46. `confirmed that these known issues are fixed`: 469
47. `confirmed that these known issues are fixed`: 445
48. `confirmed that these known issues are fixed`: 2670, 2333, 2350, 2360

### python -m pytest tests/ --ignore=tests/test_price_repair.py --ignore=tests/test_live.py --ignore=tests/test_cache_noperms.py
* 100 passed in 86.40s (0:01:26)

### python -m pytest tests/ -v 
* 116 passed, 71 subtests passed in 132.79s (0:02:12)

### python -m pytest tests/issues/test.py tests/issues/test_history.py tests/issues/test_fast_info.py -q
* 42 passed, 90 subtests passed in 16.81s

### pylint and pyright
all python code passes pylint 10/10 and 0 pyright errors

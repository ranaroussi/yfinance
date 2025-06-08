Change Log
===========

0.2.61
------
Fix ALL type hints in websocket #2493

0.2.60
------
Fix cookie reuse, and handle DNS blocking fc.yahoo.com #2483
Fixes for websocket:
- relax protobuf version #2485
- increase websockets version #2485
- fix type hints #2488
Fix predefined screen offset #2440

0.2.59
------
Fix the fix for rate-limit #2452
Feature: live price data websocket #2201

0.2.58
------
Fix false rate-limit problem #2430
Fix predefined screen size/count #2425

0.2.57
------
Fix proxy msg & pass-thru #2418

0.2.56
------
Features:
- Ticker lookups #2364
- Config #2391
Fixes:
- converting end epoch to localized dt #2378
- info IndexError #2382
- AttributeError: module 'requests.cookies' has no attribute 'update' #2388
- fix_Yahoo_returning_live_separate() #2389

0.2.55
------
Features
- TTM financials #2321
Fixes
- info IndexError #2354
- earnings dates TZ #2366
- price repair tweaks & fixes #2368
- history caching #2345
- backup fetch TZ from info #2369
Maintenance
- log user agent #2326

0.2.54
------
Hotfix user-agent #2277

0.2.53
------
Fixes:
- Fix: Failed to parse holders JSON data  #2234
- Fix: Bad data in Holders #2244
- Stop CSRF-cookie-fetch fail killing yfinance #2249
- Fix Market Docs #2250
- Fix: Broken "See also" links in documentation #2253
- Fix: Interval check and error message formatting in multi.py #2256
Improve:
- Add pre- / post-stock prices (and other useful information) #2212
- Warn user when use download() without specifying auto_adjust #2230
- Refactor: Earnings Dates – Switch to API Fetching #2247
- Improve prices div repair #2260
Maintenance:
- Add GitHub Actions workflow and fix failing tests #2233

0.2.52
------
Features:
- Improve Screener & docs #2207
- Add Market summary & status #2175
- Support custom period in Ticker.history() #2192
- raise YfRateLimitError if rate limited #2108
- add more options to Search #2191
Fixes:
- remove hardcoded keys in Analysis #2194
- handle Yahoo changed Search response #2202
Maintenance:
- add optional dependencies to requirements.txt #2199

0.2.51
------
Features:
- Screener tweaks #2168
- Search #2160
- get_news() expose count #2173
Fixes:
- earnings_dates #2169

0.2.50
------
Fixes:
- price repair #2111 #2139
- download() appearance 2109
- isin() error #2099
- growth_estimates #2127
Also new docs #2132

0.2.49
------
Fix prices-clean rarely discarding good data #2122

0.2.47 and 0.2.48
-----------------
Add yf.download(multi_level_index)

0.2.46
------
Fix regression in 0.2.45 #2094

0.2.45
------
Features:
- Screener #2066 @ericpien
Fixes
- Tickers keyerror #2068 @antoniouaa
- IndexError in some history() debug messages #2087
- improve dividend repair #2090
Maintenance
- fix unit tests contextual imports #2067
- fix typos #2072 @algonell
- add Pyright type checking #2059 @marco-carvalho

0.2.44
------
Features:
- fetch funds #2041
- fetch sector & industry #2058
Fixes:
- improve dividend repair #2062

0.2.43
------
Fix price-repair bug introduced in 0.2.42 #2036

0.2.42
------
Features:
- fetch SEC filings #2009
- fetch analysis #2023 @Fidasek009
- price repair extended to dividends & adjust #2031
Fixes:
- fix error on empty options chain #1995 @stevenbischoff
- use dict.get() to safely access key in Holders #2013 @ericpien
- fix datetime conversion with mixed timezones when ignore_tz is False #2016 @mreiche
- handle faulty response object when getting news. #2021 @ericpien
Maintenance:
- prices: improve exceptions and logging #2000

0.2.41
------
Improvements:
- add keys to financials #1965 #1985
- fetch Sustainability #1959
- improve price-repair-zeroes #1990
Fixes (prices):
- fetching when period='max' #1967
- metadata: Fix '1wk is invalid' & repeated calls #1970
- Pandas warnings #1955 #1981
- price repair syntax errors #1989
Maintenance:
- deprecate Ticker.earnings #1977

0.2.40
------
Fix typo in 0.2.39 c7af213

0.2.39
------
Fixes:
- Fix switching session from/to requests_cache #1930
Price repair:
- Fix potential for price repair to discard price=0 rows #1874
- Don't price-repair FX volume=0, is normal #1920
- Improve 'sudden change' repair for splits & currency  #1931
Information:
- Fix help(yf.download) not showing the information about the function #1913 @vittoboa
- Add more specific error throwing based on PR 1918 #1928 @elibroftw @marcofognog
Maintenance:
- Replace dead 'appdirs' package with 'platformdirs' #1896
- Deprecate 'pandas_datareader', remove a deprecated argument #1897
- Fix: datetime.datetime.utcnow() is deprecated ... #1922

0.2.38
------
Fix holders & insiders #1908

0.2.37
------
Small fixes:
- Fix Pandas warnings #1838 #1844
- Fix price repair bug, typos, refactor #1866 #1865 #1849
- Stop disabling logging #1841

0.2.36
------
Small fixes:
- Update README.md for better copy-ability  #1823 
- Name download() column levels  #1795 
- Fix history(keepna=False) when repair=True  #1824 
- Replace empty list with empty pd.Series  #1724 
- Handle peewee with old sqlite  #1827 
- Fix JSON error handling  #1830 #1833

0.2.35
------
Internal fixes for 0.2.34

0.2.34
------
Features:
- Add Recommendations Trend Summary #1754
- Add Recommendation upgrades & downgrades #1773
- Add Insider Roster & Transactions #1772
- Moved download() progress bar to STDERR #1776
- PIP optional dependencies #1771
- Set sensible min versions for optional 'nospam' reqs #1807
Fixes
- Fix download() DatetimeIndex on invalid symbols #1779
- Fix invalid date entering cache DB #1796
- Fix Ticker.calendar fetch #1790
- Fixed adding complementary to info #1774
- Ticker.earnings_dates: fix warning "Value 'NaN' has dtype incompatible with float64" #1810
- Minor fixes for price repair and related tests #1768
- Fix price repair div adjust #1798
- Fix 'raise_errors' argument ignored in Ticker.history() #1806
Maintenance
- Fix regression: _get_ticker_tz() args were being swapped. Improve its unit test #1793
- Refactor Ticker proxy #1711
- Add Ruff linter checks #1756
- Resolve Pandas FutureWarnings #1766

0.2.33
------
Cookie fixes:
- fix backup strategy #1759
- fix Ticker(ISIN) #1760

0.2.32
------
Add cookie & crumb to requests #1657

0.2.31
------
- Fix TZ cache exception blocking import #1705 #1709
- Fix merging pre-market events with intraday prices #1703

0.2.30
------
- Fix OperationalError #1698

0.2.29
------
- Fix pandas warning when retrieving quotes. #1672
- Replace sqlite3 with peewee for 100% thread-safety #1675
- Fix merging events with intraday prices #1684
- Fix error when calling enable_debug_mode twice #1687
- Price repair fixes #1688

0.2.28
------
- Fix TypeError: 'FastInfo' object is not callable #1636
- Improve & fix price repair #1633 #1660
- option_chain() also return underlying data #1606

0.2.27
------
Bug fixes:
- fix merging 1d-prices with out-of-range divs/splits #1635
- fix multithread error 'tz already in cache' #1648

0.2.26
------
Proxy improvements
- bug fixes #1371
- security fix #1625

0.2.25
------
Fix single ISIN as ticker #1611
Fix 'Only 100 years allowed' error #1576

0.2.24
------
Fix info[] missing values #1603

0.2.23
------
Fix 'Unauthorized' error #1595

0.2.22
------
Fix unhandled 'sqlite3.DatabaseError' #1574

0.2.21
------
Fix financials tables #1568
Price repair update: fix Yahoo messing up dividend and split adjustments #1543
Fix logging behaviour #1562
Fix merge future div/split into prices #1567

0.2.20
------
Switch to `logging` module #1493 #1522 #1541
Price history:
- optimise #1514
- fixes #1523
- fix TZ-cache corruption #1528

0.2.18
------
Fix 'fast_info' error '_np not found' #1496
Fix bug in timezone cache #1498

0.2.17
------
Fix prices error with Pandas 2.0 #1488

0.2.16
------
Fix 'fast_info deprecated' msg appearing at Ticker() init

0.2.15
------
Restore missing Ticker.info keys #1480

0.2.14
------
Fix Ticker.info dict by fetching from API #1461

0.2.13
------
Price bug fixes:
- fetch big-interval with Capital Gains #1455
- merging dividends & splits with prices #1452

0.2.12
------
Disable annoying 'backup decrypt' msg

0.2.11
------
Fix history_metadata accesses for unusual symbols #1411

0.2.10
------
General
- allow using sqlite3 < 3.8.2 #1380
- add another backup decrypt option #1379
Prices
- restore original download() timezone handling #1385
- fix & improve price repair #1289 2a2928b 86d6acc
- drop intraday intervals if in post-market but prepost=False #1311
Info
- fast_info improvements:
  - add camelCase keys, add dict functions values() & items() #1368
  - fix fast_info["previousClose"] #1383
- catch TypeError Exception #1397

0.2.9
-----
- Fix fast_info bugs #1362

0.2.7
-----
- Fix Yahoo decryption, smarter this time #1353
- Rename basic_info -> fast_info #1354

0.2.6
-----
- Fix Ticker.basic_info lazy-loading #1342

0.2.5
-----
- Fix Yahoo data decryption again #1336
- New: Ticker.basic_info - faster Ticker.info #1317

0.2.4
-----
- Fix Yahoo data decryption #1297
- New feature: 'Ticker.get_shares_full()' #1301
- Improve caching of financials data #1284
- Restore download() original alignment behaviour #1283
- Fix the database lock error in multithread download #1276

0.2.3
-----
- Make financials API '_' use consistent

0.2.2
-----
- Restore 'financials' attribute (map to 'income_stmt')

0.2.1
-----
Release!

0.2.0rc5
--------
- Improve financials error handling #1243
- Fix '100x price' repair #1244

0.2.0rc4
--------
- Access to old financials tables via `get_income_stmt(legacy=True)`
- Optimise scraping financials & fundamentals, 2x faster
- Add 'capital gains' alongside dividends & splits for ETFs, and metadata available via `history_metadata`, plus a bunch of price fixes
For full list of changes see #1238

0.2.0rc2
--------
Financials
- fix financials tables to match website  #1128 #1157
- lru_cache to optimise web requests  #1147
Prices
- improve price repair  #1148
- fix merging dividends/splits with day/week/monthly prices  #1161
- fix the Yahoo DST fixes  #1143
- improve bad/delisted ticker handling  #1140
Misc
- fix 'trailingPegRatio'  #1138
- improve error handling  #1118

0.2.0rc1
--------
Jumping to 0.2 for this big update. 0.1.* will continue to receive bug-fixes
- timezone cache performance massively improved. Thanks @fredrik-corneliusson #1113 #1112 #1109 #1105 #1099
- price repair feature #1110
- fix merging of dividends/splits with prices #1069 #1086 #1102
- fix Yahoo returning latest price interval across 2 rows #1070
- optional: raise errors as exceptions: raise_errors=True #1104
- add proper unit tests #1069

0.1.81
------
- Fix unhandled tz-cache exception #1107

0.1.80
------
- Fix `download(ignore_tz=True)` for single ticker #1097
- Fix rare case of error "Cannot infer DST time" #1100

0.1.79
------
- Fix when Yahoo returns price=NaNs on dividend day

0.1.78
------
- Fix download() when different timezones #1085

0.1.77
------
- Fix user experience bug #1078

0.1.75
------
- Fixed datetime-related issues: #1048
- Add 'keepna' argument #1032
- Speedup Ticker() creation #1042
- Improve a bugfix #1033

0.1.74
------
- Fixed bug introduced in 0.1.73 (sorry :/)

0.1.73
------
- Merged several PR that fixed misc issues

0.1.72
------
- Misc bugfixs

0.1.71
------
- Added Tickers(…).news()
- Return empty DF if YF missing earnings dates
- Fix EPS % to 0->1
- Fix timezone handling
- Fix handling of missing data
- Clean&format earnings_dates table
- Add ``.get_earnings_dates()`` to retrieve earnings calendar
- Added ``.get_earnings_history()`` to fetch earnings data

0.1.70
------
- Bug fixed - Closes #937

0.1.69
------
- Bug fixed - #920

0.1.68
------
- Upgraded requests dependency
- Removed Python 3.5 support

0.1.67
------
- Added legal disclaimers to make sure people are aware that this library is not affiliated, endorsed, or vetted by Yahoo, Inc.

0.1.66
------
- Merged PR to allow yfinance to be pickled

0.1.65
------
- Merged PRs to fix some bugs
- Added lookup by ISIN ``utils.get_all_by_isin(...)``, ``utils.get_ticker_by_isin(...)``, ``utils.get_info_by_isin(...)``, ``utils.get_news_by_isin(...)``
- ``yf.Ticker``, ``yf.Tickers``, and ``yf.download`` will auto-detect ISINs and convert them to tickers
- Propagating timeout parameter through code, setting request.get(timeout)
- Adds ``Ticker.analysis`` and ``Ticker.get_analysis(...)``

0.1.64
------
- Merged PRs to fix some bugs
- Added ``Ticker.stats()`` method
- Added ``Ticker.news`` property
- Providing topHoldings for ETFs
- Replaceed drop duplicate prices with indexes
- Added pre-market price to ``Ticker.info``


0.1.63
------
- Duplicates and missing rows cleanup

0.1.62
------
- Added UserAgent to all requests (via ```utils.user_agent_headers```)

0.1.61
------
- Switched to using ```query2.finance.yahoo.com```, which used HTTP/1.1

0.1.60
------
- Gracefully fail on misc operations (options, auto/back adjustments, etc)
- Added financial data to ```info()```
- Using session headers
- Get price even if open price not available
- Argument added for silencing error printing
- Merged PRs to fix some bugs

0.1.59
------
- Added custom requests session instance support in holders

0.1.58
------
- Allow specifying a custom requests session instance

0.1.57
------
- Added Conversion rate hint using 'financialCurrency' property in earnings
- Add important try+catch statements
- Fixed issue with 1 hour interval
- Merged PRs to fix some bugs
- Fixed issue with special characters in tickers

0.1.56
------
- Updated numpy version
- Merged PRs to fix some bugs

0.1.55
------
- Fixed institutional investors and mutual fund holders issue (#459)
- Fix for UTC timestamps in options chains (#429)

0.1.54
------
- ISIN lookup working with intl. tickers

0.1.53
------
- Added ``Ticker.isin`` + ``Ticker.get_isin(...)``. This is still experimental. Do not rely on it for production.
- Bug fixed: holders were always returning results for MSFT

0.1.52
------
- Improved JSON regex parsing

0.1.51
------
- Added holdings data (``Ticker.major_holders`` and ``Ticker.institutional_holders``)
- Added logo url to ``Ticker.info``
- Handling different date formats in fundamentals
- Faster JSON parsing using regex
- Trying to re-download JSON twice before giving up
- Using ujson instead of json if installed
- Fixed (more) ``ticker.info`` issues
- Misc bugfixes

0.1.50
------
- Fixed ``ticker.info`` issues
- Handle sustainability index error
- Added test script based on @GregoryMorse's pull request

0.1.49
------
- Fixed ``elementwise comparison`` warning

0.1.48
------
- Fixed issues related to non-publicly traded tickers (crypto, currency, etc)

0.1.47
------
- Fixed options-related bug that was caused by code refactoring

0.1.46
------
- Rerwote all fundamental-related methods, which now support quarterly financials, cashflow, balance sheets, and earnings, analysts recommendations, and earnings calendar data
- Code refactoring

0.1.45
------
- Added sustainability data/error handling for ETF/MF (by GregoryMorse)
- Avoid rounding the values retrieved from Yahoo by default (by aglebov)
- Added 'rename=True' for the namedtuple (raffieeey)

0.1.44
------
- Improved ``Tickers`` module (see https://github.com/ranaroussi/yfinance/issues/86)
- Misc bugfixes

0.1.43
------
- Bugfixes

0.1.42
------
- Fix data realignment when Yahoo returns with missing/malform data

0.1.41
------
- Added methods for downloading option chain

0.1.40
------
- Fixed issue related to threads when downloading many symbols
- Fix issue relared to missing data

0.1.39
------
- Added ``Ticker('XXX').financials``, ``Ticker('XXX').balance_sheet``, and ``Ticker('XXX').cashflow``
- Proxy can be used when downloading actions

0.1.38
------
- Making sure tickers are always uppercase
- Added Tickers to ``__all__``
- Updated readme to reflect current library structure

0.1.37
------
- Overriding old ``pandas_datareader.data.DataReader`` when calling ``pdr_override()``
- ``Tickers()`` returns a named tuple of ``Ticker()`` objects

0.1.36
------
- Package renamed to ``yfinance``
- Added option to specify proxy server

0.1.35
------
- Updated requirements

0.1.34
------
- Intercept yahoo "site down" message
- Better period handling
- Threading is True by default

0.1.33
------
- Better error handling

0.1.32
------
- Better error handling
- Updated min. versions for requirements

0.1.31
------
- Include ticker in error message if error is raised

0.1.30
------
- Fixed Yahoo!'s 30m bars being returned as 60m/15m

0.1.29
------
- Fixed issue with Pandas "DataFrame constructor not properly called!"
- If ``threads`` is set to True, it will default to number of tickers (max = @ of CPU cores)

0.1.28
------
- Threading defaults to ``False``

0.1.27
------
- Threading is back :)

0.1.26
------
- Fixed weird bug with Yahoo!, which is returning 60m interval when requesting for 30m interval, by requesting 15m interval and resampling the returned data
- ``Ticker.history()`` auto-adjusts data by default

0.1.21 - 0.1.25
------
- Bugfixs

0.1.2
------
- Round prices based on metadata decimals

0.1.1
------
- Setting Volume colume as np.int64 dtype to avoid integer overflow on Windows

0.1.0
-------
- Works with v8 API
- Introduced Ticker module
- Complete re-write of the entire code
- Skipped a bunch of version :)

0.0.22
-------
- Deprecated Panel support

0.0.21
-------
- Code cleanup

0.0.20
-------
- Fixed issue with progress bar (issue #42)

0.0.19
-------
- Misc bugfixes

0.0.18
-------
- Minor Bugfixes
- Added deprecation warning for future versions regarding auto-overriding pandas_datareader

0.0.17
-------
- Handles duplicate index

0.0.16
-------
- Progress bar bugfix

0.0.15
-------
- Bugfix (closing issue #11)

0.0.14
-------
- Added support for Python 2.7
- Confirming valid data returned before adding it to ``_DFS_``

0.0.13
-------
- Removed debugging code

0.0.12
-------
- Minor bug fix (closing #6)

0.0.11
-------
- Downloads ONLY dividend and stock splits data using ``actions='only'``)

0.0.10
-------
- Downloads dividend and stock splits data (use ``actions=True``)

0.0.9
-------
- Add ``threads`` parameter to ``download()`` (# of threads to use)

0.0.8
-------
- Removed 5 second wait for every failed fetch
- Reduced TTL for Yahoo!'s cookie
- Keeps track of failed downloads and tries to re-download all failed downloads one more time before giving up
- Added progress bar (can be turned off using ``progress=False``)

0.0.7
-------
- ``pandas_datareader`` is optional (can be called via ``download()`` or via ``pdr.get_data_yahoo()``)
- Tries to re-fetch Yahoo cookie in case of timeout/error

0.0.6
-------
- Forcing index to be of datetime type

0.0.5
-------
- Works using ``requests`` = no need for Selenium, PyVirtualDisplay, or Chrome Driver

0.0.4
-------
- Removed ALL debugging code :)

0.0.3
-------
- Removed debugging code

0.0.2
-------
- Option to explicitly specify the location of the Chrome driver

0.0.1
-------
- Initial release (alpha)

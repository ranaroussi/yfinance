Change Log
===========

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
- Added progress bar (can be turned off useing ``progress=False``)

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

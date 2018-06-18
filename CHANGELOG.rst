Change Log
===========

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

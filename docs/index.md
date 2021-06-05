Yahoo! Finance market data downloader
=====================================

Ever since [Yahoo! finance](https://finance.yahoo.com) decommissioned
their historical data API, many programs that relied on it to stop
working.

**yfinance** aims to solve this problem by offering a reliable,
threaded, and Pythonic way to download historical market data from
Yahoo! finance.

NOTE
----

The library was originally named `fix-yahoo-finance`, but I've since
renamed it to `yfinance` as I no longer consider it a mere "fix". For
reasons of backward-compatibility, `fix-yahoo-finance` now import and
uses `yfinance`, but you should install and use `yfinance` directly.
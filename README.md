# Download market data from Yahoo! Finance's API

<table border=1 cellpadding=10><tr><td>

#### \*\*\* IMPORTANT LEGAL DISCLAIMER \*\*\*

---

**Yahoo!, Y!Finance, and Yahoo! finance are registered trademarks of
Yahoo, Inc.**

yfinance is **not** affiliated, endorsed, or vetted by Yahoo, Inc. It's
an open-source tool that uses Yahoo's publicly available APIs, and is
intended for research and educational purposes.

**You should refer to Yahoo!'s terms of use**
([here](https://policies.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.htm),
[here](https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html), and
[here](https://policies.yahoo.com/us/en/yahoo/terms/index.htm)) **for
details on your rights to use the actual data downloaded. Remember - the
Yahoo! finance API is intended for personal use only.**

</td></tr></table>

---

<a target="new" href="https://pypi.python.org/pypi/yfinance"><img border=0 src="https://img.shields.io/badge/python-2.7,%203.6+-blue.svg?style=flat" alt="Python version"></a>
<a target="new" href="https://pypi.python.org/pypi/yfinance"><img border=0 src="https://img.shields.io/pypi/v/yfinance.svg?maxAge=60%" alt="PyPi version"></a>
<a target="new" href="https://pypi.python.org/pypi/yfinance"><img border=0 src="https://img.shields.io/pypi/status/yfinance.svg?maxAge=60" alt="PyPi status"></a>
<a target="new" href="https://pypi.python.org/pypi/yfinance"><img border=0 src="https://img.shields.io/pypi/dm/yfinance.svg?maxAge=2592000&label=installs&color=%2327B1FF" alt="PyPi downloads"></a>
<a target="new" href="https://travis-ci.com/github/ranaroussi/yfinance"><img border=0 src="https://img.shields.io/travis/ranaroussi/yfinance/main.svg?maxAge=1" alt="Travis-CI build status"></a>
<a target="new" href="https://www.codefactor.io/repository/github/ranaroussi/yfinance"><img border=0 src="https://www.codefactor.io/repository/github/ranaroussi/yfinance/badge" alt="CodeFactor"></a>
<a target="new" href="https://github.com/ranaroussi/yfinance"><img border=0 src="https://img.shields.io/github/stars/ranaroussi/yfinance.svg?style=social&label=Star&maxAge=60" alt="Star this repo"></a>
<a target="new" href="https://twitter.com/aroussi"><img border=0 src="https://img.shields.io/twitter/follow/aroussi.svg?style=social&label=Follow&maxAge=60" alt="Follow me on twitter"></a>


**yfinance** offers a threaded and Pythonic way to download market data from [Yahoo!Ⓡ finance](https://finance.yahoo.com).

→ Check out this [Blog post](https://aroussi.com/#post/python-yahoo-finance) for a detailed tutorial with code examples.

[Changelog »](https://github.com/ranaroussi/yfinance/blob/main/CHANGELOG.rst)

---

## News [2023-01-27]
Since December 2022 Yahoo has been encrypting the web data that `yfinance` scrapes for non-market data. Fortunately the decryption keys are available, although Yahoo moved/changed them several times hence `yfinance` breaking several times. `yfinance` is now better prepared for any future changes by Yahoo.

Why is Yahoo doing this? We don't know. Is it to stop scrapers? Maybe, so we've implemented changes to reduce load on Yahoo. In December we rolled out version 0.2 with optimised scraping. Then in 0.2.6 introduced `Ticker.fast_info`, providing much faster access to some `info` elements wherever possible e.g. price stats and forcing users to switch (sorry but we think necessary). `info` will continue to exist for as long as there are elements without a fast alternative.

## Quick Start

### The Ticker module

The `Ticker` module, which allows you to access ticker data in a more Pythonic way:

```python
import yfinance as yf

msft = yf.Ticker("MSFT")

# get all stock info (slow)
msft.info
# fast access to subset of stock info (opportunistic)
msft.fast_info

# get historical market data
hist = msft.history(period="1mo")

# show meta information about the history (requires history() to be called first)
msft.history_metadata

# show actions (dividends, splits, capital gains)
msft.actions
msft.dividends
msft.splits
msft.capital_gains  # only for mutual funds & etfs

# show share count
# - yearly summary:
msft.shares
# - accurate time-series count:
msft.get_shares_full(start="2022-01-01", end=None)

# show financials:
# - income statement
msft.income_stmt
msft.quarterly_income_stmt
# - balance sheet
msft.balance_sheet
msft.quarterly_balance_sheet
# - cash flow statement
msft.cashflow
msft.quarterly_cashflow
# see `Ticker.get_income_stmt()` for more options

# show holders
msft.major_holders
msft.institutional_holders
msft.mutualfund_holders

# show earnings
msft.earnings
msft.quarterly_earnings

# show sustainability
msft.sustainability

# show analysts recommendations
msft.recommendations
msft.recommendations_summary
# show analysts other work
msft.analyst_price_target
msft.revenue_forecasts
msft.earnings_forecasts
msft.earnings_trend

# show next event (earnings, etc)
msft.calendar

# Show future and historic earnings dates, returns at most next 4 quarters and last 8 quarters by default. 
# Note: If more are needed use msft.get_earnings_dates(limit=XX) with increased limit argument.
msft.earnings_dates

# show ISIN code - *experimental*
# ISIN = International Securities Identification Number
msft.isin

# show options expirations
msft.options

# show news
msft.news

# get option chain for specific expiration
opt = msft.option_chain('YYYY-MM-DD')
# data available via: opt.calls, opt.puts
```

If you want to use a proxy server for downloading data, use:

```python
import yfinance as yf

msft = yf.Ticker("MSFT")

msft.history(..., proxy="PROXY_SERVER")
msft.get_actions(proxy="PROXY_SERVER")
msft.get_dividends(proxy="PROXY_SERVER")
msft.get_splits(proxy="PROXY_SERVER")
msft.get_capital_gains(proxy="PROXY_SERVER")
msft.get_balance_sheet(proxy="PROXY_SERVER")
msft.get_cashflow(proxy="PROXY_SERVER")
msft.option_chain(..., proxy="PROXY_SERVER")
...
```

To initialize multiple `Ticker` objects, use

```python
import yfinance as yf

tickers = yf.Tickers('msft aapl goog')

# access each ticker using (example)
tickers.tickers['MSFT'].info
tickers.tickers['AAPL'].history(period="1mo")
tickers.tickers['GOOG'].actions
```

### Fetching data for multiple tickers

```python
import yfinance as yf
data = yf.download("SPY AAPL", start="2017-01-01", end="2017-04-30")
```

`yf.download()` and `Ticker.history()` have many options for configuring fetching and processing, e.g.:

```python
yf.download(tickers = "SPY AAPL",  # list of tickers
            period = "1y",         # time period
            interval = "1d",       # trading interval
            ignore_tz = True,      # ignore timezone when aligning data from different exchanges?
            prepost = False)       # download pre/post market hours data?
```

Review the [Wiki](https://github.com/ranaroussi/yfinance/wiki) for more options and detail.

### Smarter scraping

To use a custom `requests` session (for example to cache calls to the
API or customize the `User-agent` header), pass a `session=` argument to
the Ticker constructor.

```python
import requests_cache
session = requests_cache.CachedSession('yfinance.cache')
session.headers['User-agent'] = 'my-program/1.0'
ticker = yf.Ticker('msft', session=session)
# The scraped response will be stored in the cache
ticker.actions
```

Combine a `requests_cache` with rate-limiting to avoid triggering Yahoo's rate-limiter/blocker that can corrupt data.
```python
from requests import Session
from requests_cache import CacheMixin, SQLiteCache
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
    """ """

session = CachedLimiterSession(
    per_second=0.9,
    bucket_class=MemoryQueueBucket,
    backend=SQLiteCache("yfinance.cache"),
)
```

### Managing Multi-Level Columns

The following answer on Stack Overflow is for [How to deal with
multi-level column names downloaded with
yfinance?](https://stackoverflow.com/questions/63107801)

-   `yfinance` returns a `pandas.DataFrame` with multi-level column
    names, with a level for the ticker and a level for the stock price
    data
    -   The answer discusses:
        -   How to correctly read the the multi-level columns after
            saving the dataframe to a csv with `pandas.DataFrame.to_csv`
        -   How to download single or multiple tickers into a single
            dataframe with single level column names and a ticker column

### Timezone cache store

When fetching price data, all dates are localized to stock exchange timezone. 
But timezone retrieval is relatively slow, so yfinance attemps to cache them 
in your users cache folder. 
You can direct cache to use a different location with `set_tz_cache_location()`:
```python
import yfinance as yf
yf.set_tz_cache_location("custom/cache/location")
...
```

---

## `pandas_datareader` override

If your code uses `pandas_datareader` and you want to download data
faster, you can "hijack" `pandas_datareader.data.get_data_yahoo()`
method to use **yfinance** while making sure the returned data is in the
same format as **pandas\_datareader**'s `get_data_yahoo()`.

```python
from pandas_datareader import data as pdr

import yfinance as yf
yf.pdr_override() # <== that's all it takes :-)

# download dataframe
data = pdr.get_data_yahoo("SPY", start="2017-01-01", end="2017-04-30")
```

---

## Installation

Install `yfinance` using `pip`:

``` {.sourceCode .bash}
$ pip install yfinance --upgrade --no-cache-dir
```

To install `yfinance` using `conda`, see
[this](https://anaconda.org/ranaroussi/yfinance).

### Requirements

-   [Python](https://www.python.org) \>= 2.7, 3.4+
-   [Pandas](https://github.com/pydata/pandas) \>= 1.3.0
-   [Numpy](http://www.numpy.org) \>= 1.16.5
-   [requests](http://docs.python-requests.org/en/master) \>= 2.26
-   [lxml](https://pypi.org/project/lxml) \>= 4.9.1
-   [appdirs](https://pypi.org/project/appdirs) \>= 1.4.4
-   [pytz](https://pypi.org/project/pytz) \>=2022.5
-   [frozendict](https://pypi.org/project/frozendict) \>= 2.3.4
-   [beautifulsoup4](https://pypi.org/project/beautifulsoup4) \>= 4.11.1
-   [html5lib](https://pypi.org/project/html5lib) \>= 1.1
-   [cryptography](https://pypi.org/project/cryptography) \>= 3.3.2

### Optional (if you want to use `pandas_datareader`)

-   [pandas\_datareader](https://github.com/pydata/pandas-datareader)
    \>= 0.4.0

---

### Legal Stuff

**yfinance** is distributed under the **Apache Software License**. See
the [LICENSE.txt](./LICENSE.txt) file in the release for details.


AGAIN - yfinance is **not** affiliated, endorsed, or vetted by Yahoo, Inc. It's
an open-source tool that uses Yahoo's publicly available APIs, and is
intended for research and educational purposes. You should refer to Yahoo!'s terms of use
([here](https://policies.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.htm),
[here](https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html), and
[here](https://policies.yahoo.com/us/en/yahoo/terms/index.htm)) for
detailes on your rights to use the actual data downloaded.

---

### P.S.

Please drop me an note with any feedback you have.

**Ran Aroussi**

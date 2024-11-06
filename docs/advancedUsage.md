Advanced Usage
==============

Using Proxies
-------------

If you want to use a proxy server for downloading data, use:

``` python
import yfinance as yf

msft = yf.Ticker("MSFT")

msft.history(..., proxy="PROXY_SERVER")
msft.get_actions(proxy="PROXY_SERVER")
msft.get_dividends(proxy="PROXY_SERVER")
msft.get_splits(proxy="PROXY_SERVER")
msft.get_balance_sheet(proxy="PROXY_SERVER")
msft.get_cashflow(proxy="PROXY_SERVER")
msft.option_chain(..., proxy="PROXY_SERVER")
...
```

To use a custom `requests` session (for example to cache calls to the
API or customize the `User-agent` header), pass a `session=` argument to
the Ticker constructor.

``` python
import requests_cache
import yfinance as yf
session = requests_cache.CachedSession('yfinance.cache')
session.headers['User-agent'] = 'my-program/1.0'
ticker = yf.Ticker('msft aapl goog', session=session)
# The scraped response will be stored in the cache
ticker.actions
```

To initialize multiple `Ticker` objects, use

``` python
import yfinance as yf

tickers = yf.Tickers('msft aapl goog')
# ^ returns a named tuple of Ticker objects

# access each ticker using (example)
tickers.tickers.MSFT.info
tickers.tickers.AAPL.history(period="1mo")
tickers.tickers.GOOG.actions
```

Fetching data for multiple tickers
----------------------------------

``` python
import yfinance as yf
data = yf.download("SPY AAPL", start="2017-01-01", end="2017-04-30")
```

I've also added some options to make life easier :)

``` python
import yfinance as yf
data = yf.download(  # or pdr.get_data_yahoo(...
        # tickers list or string as well
        tickers="SPY AAPL MSFT",

        # use "period" instead of start/end
        # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        # (optional, default is '1mo')
        period="ytd",

        # fetch data by interval (including intraday if period < 60 days)
        # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        # (optional, default is '1d')
        interval="1m",

        # group by ticker (to access via data['SPY'])
        # (optional, default is 'column')
        group_by="ticker",

        # adjust all OHLC automatically
        # (optional, default is False)
        auto_adjust=True,

        # download pre/post regular market hours data
        # (optional, default is False)
        prepost=True,

        # use threads for mass downloading? (True/False/Integer)
        # (optional, default is True)
        threads=True,

        # proxy URL scheme use use when downloading?
        # (optional, default is None)
        proxy=None
    )
```

Managing Multi-Level Columns
----------------------------

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

`pandas_datareader` override
----------------------------

If your code uses `pandas_datareader` and you want to download data
faster, you can "hijack" `pandas_datareader.data.get_data_yahoo()`
method to use **yfinance** while making sure the returned data is in the
same format as **pandas\_datareader**'s `get_data_yahoo()`.

``` python
from pandas_datareader import data as pdr
import yfinance as yf

yf.pdr_override()  # <== that's all it takes :-)

# download dataframe
data = pdr.get_data_yahoo("SPY", start="2017-01-01", end="2017-04-30")
```
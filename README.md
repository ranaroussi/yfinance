<img src="./doc/yfinance-gh-logo-dark.webp#gh-dark-mode-only" height="100">
<img src="./doc/yfinance-gh-logo-light.webp#gh-light-mode-only" height="100">

# Download market data from Yahoo! Finance's API

<a target="new" href="https://pypi.python.org/pypi/yfinance"><img border=0 src="https://img.shields.io/badge/python-2.7,%203.6+-blue.svg?style=flat" alt="Python version"></a>
<a target="new" href="https://pypi.python.org/pypi/yfinance"><img border=0 src="https://img.shields.io/pypi/v/yfinance.svg?maxAge=60%" alt="PyPi version"></a>
<a target="new" href="https://pypi.python.org/pypi/yfinance"><img border=0 src="https://img.shields.io/pypi/status/yfinance.svg?maxAge=60" alt="PyPi status"></a>
<a target="new" href="https://pypi.python.org/pypi/yfinance"><img border=0 src="https://img.shields.io/pypi/dm/yfinance.svg?maxAge=86400&label=installs&color=%2327B1FF" alt="PyPi downloads"></a>
<a target="new" href="https://github.com/ranaroussi/yfinance"><img border=0 src="https://img.shields.io/github/stars/ranaroussi/yfinance.svg?style=social&label=Star&maxAge=60" alt="Star this repo"></a>
<a target="new" href="https://x.com/intent/follow?screen_name=aroussi"><img border=0 src="https://img.shields.io/twitter/follow/aroussi.svg?style=social&label=Follow&maxAge=60" alt="Follow me on twitter"></a>

<a href="https://trendshift.io/repositories/4578" target="_blank"><img src="https://trendshift.io/api/badge/repositories/4578" alt="ranaroussi%2Fyfinance | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>

**yfinance** offers a Pythonic way to fetch financial & market data from [Yahoo!Ⓡ finance](https://finance.yahoo.com).

> **v2.0.0 — Polars-native:** yfinance now returns [Polars](https://pola.rs) DataFrames instead of pandas.
> Install the optional `[pandas]` extra and use `as_pandas=True` on `history()` for backward compatibility.

---

> [!IMPORTANT]  
> **Yahoo!, Y!Finance, and Yahoo! finance are registered trademarks of Yahoo, Inc.**
>
> yfinance is **not** affiliated, endorsed, or vetted by Yahoo, Inc. It's an open-source tool that uses Yahoo's publicly available APIs, and is intended for research and educational purposes.
> 
> **You should refer to Yahoo!'s terms of use** ([here](https://policies.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.htm), [here](https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html), and [here](https://policies.yahoo.com/us/en/yahoo/terms/index.htm)) **for details on your rights to use the actual data downloaded.
>
> Remember - the Yahoo! finance API is intended for personal use only.**

---

> [!TIP]
> THE NEW DOCUMENTATION WEBSITE IS NOW LIVE! 🤘
> 
> Visit [**ranaroussi.github.io/yfinance**](https://ranaroussi.github.io/yfinance)

---

## Main components

- `Ticker`: single ticker data
- `Tickers`: multiple tickers' data
- `download`: download market data for multiple tickers
- `Market`: get information about a market
- `WebSocket` and `AsyncWebSocket`: live streaming data
- `Search`: quotes and news from search
- `Sector` and `Industry`: sector and industry information
- `EquityQuery` and `Screener`: build query to screen market

## Installation

Using uv (recommended):

```sh
uv add yfinance
```

Using pip:

```sh
pip install yfinance
```

For pandas compatibility (optional):

```sh
uv add 'yfinance[pandas]'
# or
pip install 'yfinance[pandas]'
```

## Quick Start

```python
import yfinance as yf
import polars as pl

msft = yf.Ticker("MSFT")

# Returns a polars DataFrame with a "Date" or "Datetime" column
hist = msft.history(period="1mo")
print(hist.head())

# Filter by date range
from datetime import date
recent = hist.filter(pl.col("Date") >= date(2024, 1, 1))

# Download multiple tickers — returns long-form DataFrame with "Ticker" column
data = yf.download(["AAPL", "MSFT"], start="2024-01-01")
print(data.filter(pl.col("Ticker") == "AAPL").head())

# Convert to per-ticker dict
by_ticker = yf.download_to_dict(data)
aapl = by_ticker["AAPL"]

# Optional: convert to pandas for compatibility
hist_pd = msft.history(period="1mo", as_pandas=True)
```

---

### [yfinance relies on the community to investigate bugs and contribute code. Here's how you can help.](https://github.com/ranaroussi/yfinance/blob/main/CONTRIBUTING.md)

---

![Star History Chart](https://api.star-history.com/svg?repos=ranaroussi/yfinance)

---

### Legal Stuff

**yfinance** is distributed under the **Apache Software License**. See
the [LICENSE.txt](https://github.com/ranaroussi/yfinance/blob/main/LICENSE.txt) file in the release for details.

AGAIN - yfinance is **not** affiliated, endorsed, or vetted by Yahoo, Inc. It's
an open-source tool that uses Yahoo's publicly available APIs, and is
intended for research and educational purposes. You should refer to Yahoo!'s terms of use
([here](https://policies.yahoo.com/us/en/yahoo/terms/product-atos/apiforydn/index.htm),
[here](https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html), and
[here](https://policies.yahoo.com/us/en/yahoo/terms/index.htm)) for
details on your rights to use the actual data downloaded.

---

### P.S.

Please drop me a note with any feedback you have.

**Ran Aroussi**


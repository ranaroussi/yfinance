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
<a target="new" href="https://www.codefactor.io/repository/github/ranaroussi/yfinance"><img border=0 src="https://www.codefactor.io/repository/github/ranaroussi/yfinance/badge" alt="CodeFactor"></a>
<a target="new" href="https://github.com/ranaroussi/yfinance"><img border=0 src="https://img.shields.io/github/stars/ranaroussi/yfinance.svg?style=social&label=Star&maxAge=60" alt="Star this repo"></a>
<a target="new" href="https://twitter.com/aroussi"><img border=0 src="https://img.shields.io/twitter/follow/aroussi.svg?style=social&label=Follow&maxAge=60" alt="Follow me on twitter"></a>


**yfinance** offers a threaded and Pythonic way to download market data from [Yahoo!Ⓡ finance](https://finance.yahoo.com).

## Main Features
- `Ticker` module: Class for accessing single ticker data.
- `Tickers` module: Class for handling multiple tickers.
- `download` Efficiently download market data for multiple tickers.
- `Sector` and `Industry` modules : Classes for accessing sector and industry information.
- Market Screening: `EquityQuery` and `Screener` to build query and screen the market.
- Caching and Smart Scraping

## Documentation
The official documentation is available on [ranaroussi.github.io/yfinance](https://ranaroussi.github.io/yfinance/index.html)

## Installation

Install `yfinance` using `pip`:

``` {.sourceCode .bash}
$ pip install yfinance --upgrade --no-cache-dir
```

[With Conda](https://anaconda.org/ranaroussi/yfinance).

To install with optional dependencies, replace `optional` with: `nospam` for [caching-requests](#smarter-scraping), `repair` for [price repair](https://github.com/ranaroussi/yfinance/wiki/Price-repair), or `nospam,repair` for both:

``` {.sourceCode .bash}
$ pip install "yfinance[optional]"
```

[Required dependencies](./requirements.txt) , [all dependencies](./setup.py#L62).

The list of changes can be found in the [changelog](https://github.com/ranaroussi/yfinance/blob/main/CHANGELOG.rst)


## Developers: want to contribute?
`yfinance` relies on community to investigate bugs and contribute code. Developer guide: https://github.com/ranaroussi/yfinance/discussions/1084

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
details on your rights to use the actual data downloaded.

---

### P.S.

Please drop me a note with any feedback you have.

**Ran Aroussi**

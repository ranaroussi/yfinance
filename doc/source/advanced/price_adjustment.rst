****************
Price adjustment
****************

By default, ``Ticker.history()`` and ``yf.download()`` return prices **adjusted for splits and dividends** (``auto_adjust=True``). This means the ``Close`` column is already the *adjusted* close — there is no separate ``Adj Close`` column in this mode.

Pass ``auto_adjust=False`` to get Yahoo's split-adjusted OHLC together with an ``Adj Close`` column that also includes dividends.

In other words:

- ``auto_adjust=True`` (default): ``Open``, ``High``, ``Low`` and ``Close`` are all adjusted for splits *and* dividends — the prices you want for total-return-consistent calculations such as backtesting.
- ``auto_adjust=False``: Yahoo's OHLC as returned (already split-adjusted) plus an ``Adj Close`` column that additionally accounts for dividends.

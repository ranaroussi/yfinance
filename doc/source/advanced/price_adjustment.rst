****************
Price adjustment
****************

By default, ``Ticker.history()`` and ``yf.download()`` return prices **adjusted for splits and dividends** (``auto_adjust=True``). This means the ``Close`` column is already the *adjusted* close — there is no separate ``Adj Close`` column in this mode.

Pass ``auto_adjust=False`` to get Yahoo's split-adjusted OHLC together with an ``Adj Close`` column that also includes dividends.

In other words:

- ``auto_adjust=True`` (default): ``Open``, ``High``, ``Low`` and ``Close`` are all adjusted for splits *and* dividends — the prices you want for total-return-consistent calculations such as backtesting.
- ``auto_adjust=False``: Yahoo's OHLC as returned (already split-adjusted) plus an ``Adj Close`` column that additionally accounts for dividends.

How the dividend adjustment is calculated
-----------------------------------------

On each ex-dividend date, every price before that date is multiplied by::

    1 - dividend / close on the trading day before the ex-date

The factors from all later dividends multiply together. So the oldest prices are scaled down the most, and prices after the latest dividend are not changed at all. ``auto_adjust=True`` applies the same scaling to ``Open``, ``High`` and ``Low``.

You can rebuild ``Adj Close`` from the unadjusted data:

.. code-block:: python

    import yfinance as yf

    h = yf.Ticker("KO").history(period="max", auto_adjust=False, actions=True)
    close, div = h["Close"], h["Dividends"]
    factor = (1 - div / close.shift(1)).where(div > 0, 1.0)
    # each row is scaled by the factors of every later ex-date
    mult = factor[::-1].cumprod()[::-1].shift(-1, fill_value=1.0)
    adj = close * mult

``adj`` matches ``h["Adj Close"]`` to within about 0.01%. The small remainder comes from rounding in the dividend amounts.

Adjusted prices change over time
--------------------------------

Every new dividend rescales all the prices before it. So the adjusted history is rewritten each time a stock goes ex-dividend, and adjusted prices you saved last month will not match a download made today, even for the same dates.

Returns between any two dates are not affected, because the same factor applies to both prices and cancels out. Problems start when old and new downloads are mixed. If you append newly downloaded adjusted rows to adjusted rows stored earlier, you get a false jump where they join. Either download the full history again, or store the unadjusted ``Close`` and ``Dividends`` and adjust them yourself.

Adjusted close and total return
-------------------------------

Returns from adjusted prices are total returns, but with a specific assumption about reinvestment. The calculation above is the same as reinvesting each dividend at the previous close minus the dividend, roughly where the stock is expected to open on the ex-date. Published fund total returns usually reinvest at the ex-date close or on the payment date instead, so they will differ slightly.

The difference is small. From January 2023 to September 2026, the gap between the return from ``Adj Close`` and the return with dividends reinvested at the ex-date close was 1.6 basis points for SPY, 1.9 for KO and 0.3 for AGG, over the whole period. If your figure is much further from a published one than that, check the dividend data and the reinvestment date first.

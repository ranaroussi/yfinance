************
Price Repair
************

The new argument ``repair=True`` in ``history()`` and ``download()`` will attempt to fix a variety of price errors caused by Yahoo. Only US market data appears perfect, I guess Yahoo doesn't care much about rest of world?

The returned table will have a new column ``Repaired?`` that specifies if row was repaired.

Price repair
============

Missing dividend adjustment
---------------------------

If dividend in data but preceding ``Adj Close`` = ``Close``, then manually apply dividend-adjustment to ``Adj Close``.
Note: ``Repaired?`` is NOT set to ``True`` because fix only changes ``Adj Close``

.. figure:: /_static/images/repair-prices-missing-div-adjust.png
   :alt: 8TRA.DE: repair missing dividend adjustment
   :width: 80%
   :align: left

   8TRA.DE

.. container:: clearer

   ..

Missing split adjustment
------------------------

If stock split in data but preceding price data is not adjusted, then manually apply stock split.
Requires date range include 1 day after stock split for calibration - sometimes Yahoo fails to adjust prices on stock split day.

.. figure:: /_static/images/repair-prices-missing-split-adjust.png
   :alt: MOB.ST: repair missing split adjustment
   :width: 80%
   :align: left

   MOB.ST

.. container:: clearer

   ..

Missing data
------------

If price data is clearly missing or corrupt, then reconstructed using smaller interval e.g. ``1h`` to fix ``1d`` data.

.. figure:: /_static/images/repair-prices-missing-row.png
   :alt: 1COV.DE: repair missing row
   :width: 80%
   :align: left

   1COV.DE missing row

.. container:: clearer

   ..

.. figure:: /_static/images/repair-prices-missing-volume-intraday.png
   :alt: 1COV.DE: repair missing Volume, but intraday price changed
   :width: 80%
   :align: left

   1COV.DE missing Volume, but intraday price changed

.. container:: clearer

   ..

.. figure:: /_static/images/repair-prices-missing-volume-daily.png
   :alt: 0316.HK: repair missing Volume, but daily price changed
   :width: 80%
   :align: left

   0316.HK missing Volume, but daily price changed

.. container:: clearer

   ..

100x errors
-----------

Sometimes Yahoo mixes up currencies e.g. $/cents or £/pence. So some prices are 100x wrong.
Sometimes they are spread randomly through data - these detected with ``scipy`` module.
Other times they are in a block, because Yahoo decided one day to permanently switch currency.

.. figure:: /_static/images/repair-prices-100x.png
   :alt: AET.L: repair 100x
   :width: 80%
   :align: left

   AET.L

Price reconstruction - algorithm notes
--------------------------------------

Spam minimised by grouping fetches. Tries to be aware of data limits e.g. ``1h`` cannot be fetched beyond 2 years.

If Yahoo eventually does fix the bad data that required reconstruction, you will see it's slightly different to reconstructed prices and volume often significantly different. Best I can do, and beats missing data.

Dividend repair (new)
=====================

Fix errors in dividends:

1. adjustment missing or 100x too small/big for the dividend
2. duplicate dividend (within 7 days)
3. dividend 100x too big/small for the ex-dividend price drop
4. ex-div date wrong (price drop is few days/weeks after)

Most errors I've seen are on London stock exchange (£/pence mixup), but no exchange is safe.

IMPORTANT - false positives
---------------------------

Because fixing (3) relies on price action, there is a chance of a "false positive" (FP) - thinking an error exists when data is good.
FP rate increases with longer intervals, so only 1d intervals are repaired. If you request repair on multiday intervals (weekly etc), then: 1d is fetched from Yahoo, repaired, then resampled - **this has nice side-effect of solving Yahoo's flawed way of div-adjusting multiday intervals.**

FP rate on 1d is tiny. They tend to happen with tiny dividends e.g. 0.5%, mistaking normal price volatility for an ex-div drop 100x bigger than the dividend, causing repair of the "too small" dividend (repair logic already tries to account for normal volatility by subtracting median). Either accept the risk, or fetch 6-12 months of prices with at least 2 dividends - then can analyse the dividends together to identify false positives.

Adjustment missing
------------------

1398.HK

.. code-block:: text

   # ORIGINAL:
                              Close  Adj Close  Dividends
   2024-07-08 00:00:00+08:00   4.33       4.33   0.335715
   2024-07-04 00:00:00+08:00   4.83       4.83   0.000000

.. code-block:: text

   # REPAIRED:
                              Close  Adj Close  Dividends
   2024-07-08 00:00:00+08:00   4.33   4.330000   0.335715
   2024-07-04 00:00:00+08:00   4.83   4.494285   0.000000

Adjustment too small
--------------------

3IN.L

.. code-block:: text

   # ORIGINAL:
                              Close  Adj Close  Dividends
   2024-06-13 00:00:00+01:00  3.185   3.185000    0.05950
   2024-06-12 00:00:00+01:00  3.270   3.269405    0.00000

.. code-block:: text

   # REPAIRED:
                              Close  Adj Close  Dividends
   2024-06-13 00:00:00+01:00  3.185   3.185000    0.05950
   2024-06-12 00:00:00+01:00  3.270   3.210500    0.00000

Duplicate (within 7 days)
-------------------------

ALC.SW

.. code-block:: text

   # ORIGINAL:
                                  Close  Adj Close  Dividends
   2023-05-10 00:00:00+02:00  70.580002  70.352142       0.21
   2023-05-09 00:00:00+02:00  65.739998  65.318443       0.21
   2023-05-08 00:00:00+02:00  66.379997  65.745682       0.00

.. code-block:: text

   # REPAIRED:
                                  Close  Adj Close  Dividends
   2023-05-10 00:00:00+02:00  70.580002  70.352142       0.00
   2023-05-09 00:00:00+02:00  65.739998  65.527764       0.21
   2023-05-08 00:00:00+02:00  66.379997  65.956371       0.00

Dividend too big
----------------

HLCL.L

.. code-block:: text

   # ORIGINAL:
                              Close  Adj Close  Dividends
   2024-06-27 00:00:00+01:00  2.360     2.3600       1.78
   2024-06-26 00:00:00+01:00  2.375     2.3572       0.00

   # REPAIRED:
                              Close  Adj Close  Dividends
   2024-06-27 00:00:00+01:00  2.360     2.3600     0.0178
   2024-06-26 00:00:00+01:00  2.375     2.3572     0.0000

Dividend & adjust too big
-------------------------

LTI.L

.. code-block:: text

   # ORIGINAL:
                              Close  Adj Close     Adj  Dividends
   2024-08-08 00:00:00+01:00  768.0      768.0  1.0000     5150.0
   2024-08-07 00:00:00+01:00  819.0    -4331.0 -5.2882        0.0
                              Close  Adj Close     Adj  Dividends
   2024-08-08 00:00:00+01:00  768.0      768.0  1.0000       51.5
   2024-08-07 00:00:00+01:00  819.0      767.5  0.9371        0.0

Dividend too small
------------------

BVT.L

.. code-block:: text

   # ORIGINAL:
                               Close  Adj Close     Adj  Dividends
   2022-02-03 00:00:00+00:00  0.7534   0.675197  0.8962    0.00001
   2022-02-01 00:00:00+00:00  0.7844   0.702970  0.8962    0.00000

.. code-block:: text

   # REPAIRED:
                               Close  Adj Close     Adj  Dividends
   2022-02-03 00:00:00+00:00  0.7534   0.675197  0.8962      0.001
   2022-02-01 00:00:00+00:00  0.7844   0.702075  0.8950      0.000

Adjusted 2x on day before
-------------------------

clue: Close < Low

2020.OL

.. code-block:: text

   # ORIGINAL:
                                     Low       Close   Adj Close  Dividends
   2023-12-21 00:00:00+01:00  120.199997  121.099998  118.868782       0.18
   2023-12-20 00:00:00+01:00  122.000000  121.900002  119.477371       0.00

.. code-block:: text

   # REPAIRED:
                                     Low       Close   Adj Close  Dividends
   2023-12-21 00:00:00+01:00  120.199997  121.099998  118.868782       0.18
   2023-12-20 00:00:00+01:00  122.000000  122.080002  119.654045       0.00

ex-div date wrong
-----------------

TETY.ST

.. code-block:: text

   # ORIGINAL:
                                  Close  Adj Close  Dividends
   2022-06-22 00:00:00+02:00  66.699997  60.085415        0.0
   2022-06-21 00:00:00+02:00  71.599998  64.499489        0.0
   2022-06-20 00:00:00+02:00  71.800003  64.679657        5.0
   2022-06-17 00:00:00+02:00  71.000000  59.454838        0.0

.. code-block:: text

   # REPAIRED:
                                  Close  Adj Close  Dividends
   2022-06-22 00:00:00+02:00  66.699997  60.085415        5.0
   2022-06-21 00:00:00+02:00  71.599998  60.007881        0.0
   2022-06-20 00:00:00+02:00  71.800003  60.175503        0.0
   2022-06-17 00:00:00+02:00  71.000000  59.505021        0.0

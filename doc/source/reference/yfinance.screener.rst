=========================
Screener & Query
=========================

.. currentmodule:: yfinance

Query Market Data
~~~~~~~~~~~~~~~~~~~~~
The `Screener` module allow you to access the sector and industry information.


.. autosummary:: 
   :toctree: api/

   screener.EquityQuery
   screener.FundQuery
   screener.screen
   screener.industry
   const.SECTOR_INDUSTY_MAPPING
   screener.PREDEFINED_SCREENER_QUERIES



Screener Sample Code
------------------

All the screener objects are available in the `yfinance.screener` module.

.. literalinclude:: examples/screener.py
   :language: python
   :lines: 1-6

You can craft a query using the `EquityQuery` and `FundQuery` objects.

.. literalinclude:: examples/screener.py
   :language: python
   :lines: 10-12

The `industry` function allows you to run a screener query for industry.

.. literalinclude:: examples/screener.py
   :language: python
   :lines: 16-17

The `screen` function allows you to run a screener query.

.. literalinclude:: examples/screener.py
   :language: python
   :lines: 21-25

.. important::
   Valid operators are:

   * AND
   * OR
   * EQ
   * BTWN
   * GT
   * LT
   * GTE
   * LTE
   * IS-IN

.. note::
   The `IS-IN` operator is a custom operator and is not part of the Yahoo API.

.. seealso::
   :attr:`EquityQuery.valid_fields <yfinance.screener.EquityQuery.valid_fields>`
      supported operand values for query
   :attr:`EquityQuery.valid_values <yfinance.screener.EquityQuery.valid_values>`
      supported `EQ query operand parameters`
   :attr:`FundQuery.valid_fields <yfinance.screener.FundQuery.valid_fields>`
      supported operand values for query
   :attr:`FundQuery.valid_values <yfinance.screener.FundQuery.valid_values>`
      supported `EQ query operand parameters`
   :attr:`SECTOR_INDUSTY_MAPPING <yfinance.const.SECTOR_INDUSTY_MAPPING>`
      mapping of SECTORS to INDUSTRIES
   :attr:`PREDEFINED_SCREENER_QUERIES <yfinance.screener.PREDEFINED_SCREENER_QUERIES>`
      predefined screener queries
   
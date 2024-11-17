=======================
Sector and Industry
=======================

.. currentmodule:: yfinance

Sector class
--------------
The `Sector` and `Industry` modules provide access to the Sector and Industry information.

.. autosummary::
   :toctree: api/
   :recursive:

   Sector
   Industry

.. seealso::
   :attr:`Sector.industries <yfinance.Sector.industries>`
      Map of sector and industry

Sample Code
---------------------
To initialize, use the relevant sector or industry key as below.

.. literalinclude:: examples/sector_industry.py
   :language: python

The modules can be chained with Ticker as below.

.. literalinclude:: examples/sector_industry_ticker.py
   :language: python

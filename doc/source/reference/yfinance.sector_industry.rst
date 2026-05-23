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

Region scoping
--------------
By default ``Sector`` and ``Industry`` return U.S. data. Pass a Yahoo region
(ISO 3166-1 alpha-2 country code, case-insensitive) to scope ``top_companies``,
``top_etfs``, ``top_mutual_funds`` and the industry top performing/growth lists::

   yf.Sector("technology", region="GB").top_companies     # UK
   yf.Sector("technology", region="DE").top_companies     # Germany
   yf.Industry("software-infrastructure", region="JP")    # Japan

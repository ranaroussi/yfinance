=========================
Functions and Utilities
=========================

.. currentmodule:: yfinance
   
Download Market Data
~~~~~~~~~~~~~~~~~~~~~
The `download` function allows you to retrieve market data for multiple tickers at once.

.. autosummary:: 
   :toctree: api/

   download

Query Market Data
~~~~~~~~~~~~~~~~~~~~~
The `Sector` and `Industry` modules allow you to access the sector and industry information.

.. autosummary:: 
   :toctree: api/

   EquityQuery
   Screener

.. seealso::
   :attr:`EquityQuery.valid_operand_fields <yfinance.EquityQuery.valid_operand_fields>`
      supported operand values for query
   :attr:`EquityQuery.valid_eq_operand_map <yfinance.EquityQuery.valid_eq_operand_map>`
      supported `EQ query operand parameters`
   :attr:`Screener.predefined_bodies <yfinance.Screener.predefined_bodies>`
      supported predefined screens
   

Enable Debug Mode
~~~~~~~~~~~~~~~~~
Enables logging of debug information for the `yfinance` package.

.. autosummary:: 
   :toctree: api/

   enable_debug_mode

Set Timezone Cache Location
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Sets the cache location for timezone data.

.. autosummary:: 
   :toctree: api/

   set_tz_cache_location

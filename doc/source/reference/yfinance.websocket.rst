=====================
WebSocket
=====================

.. currentmodule:: yfinance

The `WebSocket` module allows you to stream live price data from Yahoo Finance using both synchronous and asynchronous clients.

Classes
------------

.. autosummary::
   :toctree: api/

   WebSocket
   AsyncWebSocket

Synchronous WebSocket
----------------------

The `WebSocket` class provides a synchronous interface for subscribing to price updates.

Sample Code:

.. literalinclude:: examples/live_sync.py
   :language: python

Asynchronous WebSocket
-----------------------

The `AsyncWebSocket` class provides an asynchronous interface for subscribing to price updates.

Sample Code:

.. literalinclude:: examples/live_async.py
   :language: python

.. note::
    If you're running asynchronous code in a Jupyter notebook, you may encounter issues with event loops. To resolve this, you need to import and apply `nest_asyncio` to allow nested event loops.

    Add the following code before running asynchronous operations:

    .. code-block:: python

        import nest_asyncio
        nest_asyncio.apply()

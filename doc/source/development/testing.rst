Unit Tests
----------

Tests are written using `pytest`. Here are some ways to run tests:

- **Run all tests**:

  .. code-block:: bash

     pytest

- **Run all ticker tests**:

  .. code-block:: bash

     pytest tests/test_ticker.py

- **Run a subset of ticker tests**:

  .. code-block:: bash

     pytest tests/test_ticker.py::TestTicker

- **Run a specific test**:

  .. code-block:: bash

     pytest tests/test_ticker.py::TestTicker::test_ticker_missing

- **General command**:

  .. code-block:: bash

     pytest tests/{file}.py::{class}::{method}

.. seealso::

    See the ``pytest`` documentation <https://docs.pytest.org/en/stable/contents.html>`_ for more information.


Writing Tests
-------------

All tests run offline using mock data. All ``YfData.get`` and ``YfData.post`` calls
are patched to route through ``tests/mocks/router.py`` instead of hitting Yahoo Finance,
which returns synthetic data from the appropriate fixture factory in ``tests/mocks/fixtures/``.

**Structure**::

    tests/
    ├── conftest.py              # autouse fixture that activates the mock
    └── mocks/
        ├── router.py            # maps URLs to fixture factories
        └── fixtures/
            ├── chart.py         # /v8/finance/chart
            ├── quote_summary.py # /v10/finance/quoteSummary
            ├── search.py        # /v1/finance/search
            ├── options.py       # /v7/finance/options
            └── ...              # one module per Yahoo Finance endpoint group

Each fixture module exposes a ``make_response(ticker, params)`` function that
returns a ``MockResponse``.

**Adding a new test**

Just write a normal ``pytest`` function - no extra setup required. The mock
activates automatically:

.. code-block:: python

    def test_my_new_feature():
        ticker = yf.Ticker("AAPL")
        hist = ticker.history(period="1mo")
        assert not hist.empty

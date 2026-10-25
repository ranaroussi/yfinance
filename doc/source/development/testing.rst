Unit Tests
----------

Tests are written using `pytest`. Install dev dependencies first if you haven't already:

.. code-block:: bash

   pip install -e ".[dev]"

Here are some ways to run tests:

- **Run all tests**:

  .. code-block:: bash

     pytest

- **Run all tests in a file**:

  .. code-block:: bash

     pytest tests/test_prices.py

- **Run a specific test**:

  .. code-block:: bash

     pytest tests/test_prices_repair.py::TestPriceRepair::test_ticker_missing

- **General command**:

  .. code-block:: bash

     pytest tests/{file}.py::{class}::{method}

Offline tests (record and replay)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Most tests fetch live data from Yahoo, which can rate-limit or block, e.g. on
GitHub runners. Environment variable ``YF_TEST_MODE`` lets tests run from stored
responses in ``tests/data/responses/`` instead:

.. code-block:: bash

   # Fetch from Yahoo and save every response
   YF_TEST_MODE=record pytest tests/test_ticker.py::TestTickerHistory

   # Serve saved responses, never touch network
   YF_TEST_MODE=replay pytest tests/test_ticker.py::TestTickerHistory

Default is ``live``, so nothing changes unless the variable is set.
In replay mode a request without a recording fails the test, and the message
gives the exact command to record it. Commit the new JSON files with the test.
Details in ``tests/replay.py``.

.. note::

    The tests are currently failing already

    Standard result:

    **Failures:** 11

    **Errors:** 93

    **Skipped:** 1

.. seealso::

    See the `pytest documentation <https://docs.pytest.org/>`_ for more information.

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

- **Run a specific test class**:

  .. code-block:: bash

     pytest tests/test_prices.py::TestPriceRepair

- **Run a specific test**:

  .. code-block:: bash

     pytest tests/test_prices_repair.py::TestPriceRepair::test_ticker_missing

- **General command**:

  .. code-block:: bash

     pytest tests/{file}.py::{class}::{method}

.. note::

    The tests are currently failing already

    Standard result:

    **Failures:** 11

    **Errors:** 93

    **Skipped:** 1

.. seealso::

    See the `pytest documentation <https://docs.pytest.org/>`_ for more information.

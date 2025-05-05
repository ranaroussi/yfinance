Unit Tests
----------

Tests are written using Python&apos;s `unittest` module. Here are some ways to run tests:

- **Run all price tests**:

  .. code-block:: bash

     python -m unittest tests.test_prices

- **Run a subset of price tests**:

  .. code-block:: bash

     python -m unittest tests.test_prices.TestPriceRepair

- **Run a specific test**:

  .. code-block:: bash

     python -m unittest tests.test_prices_repair.TestPriceRepair.test_ticker_missing

- **General command**:

  ..code-block:: bash

     python -m unittest tests.{file}.{class}.{method}

- **Run all tests**:

  .. code-block:: bash

     python -m unittest discover -s tests

.. note::

    The tests are currently failing already

    Standard result:

    **Failures:** 11

    **Errors:** 93

    **Skipped:** 1

.. seealso::

    See the ` ``unittest`` module <https://docs.python.org/3/library/unittest.html>`_ for more information.
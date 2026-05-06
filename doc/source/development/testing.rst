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
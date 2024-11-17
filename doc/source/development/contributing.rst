********************************
Contributiong to yfinance
********************************

`yfinance` relies on the community to investigate bugs and contribute code. Here’s how you can help:

Contributing
------------

1. Fork the repository on GitHub.
2. Clone your forked repository:

   .. code-block:: bash

      git clone https://github.com/your-username/yfinance.git

3. Create a new branch for your feature or bug fix:

   .. code-block:: bash

      git checkout -b feature-branch-name

4. Make your changes, commit them, and push your branch to GitHub. To keep the commit history and `network graph <https://github.com/ranaroussi/yfinance/network>`_ compact:

   Use short summaries for commits

   .. code-block:: shell

      git commit -m "short summary" -m "full commit message"

   **Squash** tiny or negligible commits with meaningful ones.

   .. code-block:: shell

      git rebase -i HEAD~2
      git push --force-with-lease origin <branch-name>

5. Open a pull request on the `yfinance` GitHub page.

For more information, see the `Developer Guide <https://github.com/ranaroussi/yfinance/discussions/1084>`_.

Branches
---------

To support rapid development without breaking stable versions, this project uses a two-layer branch model:

.. image:: assets/branches.png
   :alt: Branching Model

`Inspiration <https://miro.medium.com/max/700/1*2YagIpX6LuauC3ASpwHekg.png>`_

- **dev**: New features and some bug fixes are merged here. This branch allows collective testing, conflict resolution, and further stabilization before merging into the stable branch.
- **main**: Stable branch where PIP releases are created.

By default, branches target **main**, but most contributions should target **dev**. 

**Exceptions**:
Direct merges to **main** are allowed if:

- `yfinance` is massively broken
- Part of `yfinance` is broken, and the fix is simple and isolated

Unit Tests
----------

Tests are written using Python’s `unittest` module. Here are some ways to run tests:

- **Run all price tests**:

  .. code-block:: shell

     python -m unittest tests.test_prices

- **Run a subset of price tests**:

  .. code-block:: shell

     python -m unittest tests.test_prices.TestPriceRepair

- **Run a specific test**:

  .. code-block:: shell

     python -m unittest tests.test_prices.TestPriceRepair.test_ticker_missing

- **Run all tests**:

  .. code-block:: shell

     python -m unittest discover -s tests

Rebasing
--------------

If asked to move your branch from **main** to **dev**:

1. Ensure all relevant branches are pulled.
2. Run:

   .. code-block:: shell

      git checkout <your-branch>
      git rebase --onto dev main <branch-name>
      git push --force-with-lease origin <branch-name>

Running the GitHub Version of yfinance
--------------------------------------

To download and run a GitHub version of `yfinance`, refer to `GitHub discussion <https://github.com/ranaroussi/yfinance/discussions/1080>`_
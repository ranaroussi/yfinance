Running a branch
================

With PIP
--------

.. code-block:: bash

   pip install git+https://github.com/{user}/{repo}.git@{branch}

E.g.:

.. code-block:: bash

   pip install git+https://github.com/ranaroussi/yfinance.git@feature/name

With Git
--------

1: Download from GitHub:

.. code-block:: bash
    git clone https://github.com/{user}/{repo}.git
    pip install -r ./yfinance/requirements.txt

Or if a specific branch:

.. code-block:: bash
    git clone -b {branch} https://github.com/{user}/{repo}.git
    pip install -r ./yfinance/requirements.txt

.. NOTE::
    Only do the next part if you are installing globally

    If you are installing for 1 specific project, then you can skip this step
    and just `git clone` in the project directory

2. Add download location to Python search path

Two different ways, choose one:

1) Add path to ``PYTHONPATH`` environment variable

2) Add to top of Python file: 
.. code-block:: python
    import sys
    sys.path.insert(0, "path/to/downloaded/yfinance")


3: Verify

.. code-block:: python
    import yfinance
    print(yfinance)

Output should be:

`<module 'yfinance' from 'path/to/downloaded/yfinance/yfinance/__init__.py'>`

If output looks like this then you did step 2 wrong

`<module 'yfinance' from '.../lib/python3.10/site-packages/yfinance/__init__.py'>`

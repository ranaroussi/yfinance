Installation
============

To install without ``curl_cffi`` for requests fallback:

.. code-block:: bash

    curl -fsSL https://raw.githubusercontent.com/ranaroussi/yfinance/main/requirements.txt | grep -vi '^curl_cffi' | pip install -r /dev/stdin
    pip install --no-deps yfinance

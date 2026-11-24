Installation
============

Price repair (``repair=True``) needs ``scipy`` and ``scikit-learn``. They are not installed by default, so install the ``repair`` extra:

.. code-block:: bash

    pip install "yfinance[repair]"

To install without ``curl_cffi`` for requests fallback:

.. code-block:: bash

    curl -fsSL https://raw.githubusercontent.com/ranaroussi/yfinance/main/requirements.txt | grep -vi '^curl_cffi' | pip install -r /dev/stdin
    pip install --no-deps yfinance

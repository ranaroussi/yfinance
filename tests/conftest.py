"""
Central pytest configuration.

The `mock_yf_http` fixture intercepts all Yahoo Finance HTTP calls for every
test by patching YfData.get and YfData.post with URL router.
"""

import pytest
from unittest.mock import patch

from yfinance.data import YfData
from tests.mocks import router


@pytest.fixture(autouse=True)
def mock_yf_http():
    YfData.cache_get.cache_clear()

    with patch.object(YfData, "get",  side_effect=router.route_get), \
         patch.object(YfData, "post", side_effect=router.route_post):
        yield

    YfData.cache_get.cache_clear()

import pytest

# Live tests import tests.context; the rest are offline unit tests. Auto-mark
# the live ones so PR CI can run `-m "not network"`.
_OFFLINE_MODULES = {
    "test_auth", "test_data", "test_http",
    "test_live", "test_screener", "test_utils",
}


def pytest_collection_modifyitems(config, items):
    for item in items:
        module = item.module.__name__.rsplit(".", 1)[-1]
        if module not in _OFFLINE_MODULES:
            item.add_marker(pytest.mark.network)

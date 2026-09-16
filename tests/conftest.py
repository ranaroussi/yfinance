import pytest

# Live tests import tests.context; the rest are offline unit tests. Auto-mark
# the live ones so PR CI can run `-m "not network"`. New modules default to
# offline, so they gate PRs instead of silently landing in the weekly job.
_NETWORK_MODULES = {
    "test_cache", "test_cache_noperms", "test_calendars",
    "test_download_concurrency", "test_lookup", "test_market",
    "test_multi", "test_price_repair", "test_prices", "test_search",
    "test_sector_region", "test_ticker", "test_ticker_locale",
}


def pytest_collection_modifyitems(items):
    for item in items:
        module = getattr(item, "module", None)
        if module is not None and module.__name__.rsplit(".", 1)[-1] in _NETWORK_MODULES:
            item.add_marker(pytest.mark.network)

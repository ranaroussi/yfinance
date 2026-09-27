import pytest

from tests import replay

replay.install()


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    replay.misses.clear()


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    # yfinance may swallow MissingRecordingError, so report the missing
    # recording instead of whatever failed after it.
    report = yield
    if replay.misses:
        report.outcome = "failed"
        report.longrepr = "\n".join(dict.fromkeys(replay.misses))
        replay.misses.clear()
    return report

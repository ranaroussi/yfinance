"""Concurrent ``yf.download()`` calls must not share scratch state.

These tests assert that two concurrent ``download()`` calls each receive a
DataFrame containing exactly the tickers they requested — no contamination,
no exceptions — and that ``download()`` no longer relies on module-level
dicts in ``yfinance.shared`` as scratch space.
"""
import json
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from tests.context import yfinance as yf
from yfinance.config import YfConfig
from yfinance.data import YfData


class TestDownloadGlobalConfig(unittest.TestCase):
    """``download()`` must leave the global config exactly as it found it.

    Runs offline: the fetch is stubbed out so every ticker fails, which is
    the path that used to override the exception-hiding setting.
    """

    @staticmethod
    def _boom(*args, **kwargs):
        raise RuntimeError("simulated network failure")

    def _download_offline(self):
        with patch.object(YfData, "get", self._boom), \
                patch.object(YfData, "cache_get", self._boom):
            yf.download(["ZZZFAKE"], period="5d", threads=False, progress=False)

    def test_download_leaves_network_config_untouched(self):
        before = json.loads(repr(YfConfig.network))
        self._download_offline()
        self.assertEqual(json.loads(repr(YfConfig.network)), before)

    def test_download_restores_hide_exceptions(self):
        # Whatever the user set must survive a download, including the
        # non-default value.
        backup = YfConfig.debug.hide_exceptions
        try:
            for value in (True, False):
                YfConfig.debug.hide_exceptions = value
                self._download_offline()
                self.assertEqual(YfConfig.debug.hide_exceptions, value)
        finally:
            YfConfig.debug.hide_exceptions = backup


class TestDownloadConcurrency(unittest.TestCase):

    def _fetch(self, tickers):
        return yf.download(
            tickers, period="5d", interval="1d",
            threads=False, progress=False, auto_adjust=False,
        )

    def test_concurrent_downloads_keep_results_separate(self):
        chunk_a = ["AAPL", "MSFT", "GOOG"]
        chunk_b = ["NVDA", "META", "AMZN"]

        with ThreadPoolExecutor(max_workers=2) as ex:
            futures = [ex.submit(self._fetch, chunk_a), ex.submit(self._fetch, chunk_b)]
            results = [f.result() for f in futures]

        # Each result must contain exactly its requested tickers — neither
        # missing any nor leaking the other call's tickers.
        got_a = set(results[0].columns.get_level_values("Ticker"))
        got_b = set(results[1].columns.get_level_values("Ticker"))
        self.assertEqual(got_a, set(chunk_a))
        self.assertEqual(got_b, set(chunk_b))

    def test_concurrent_downloads_do_not_raise(self):
        # Repeat several times to make races more likely to surface.
        chunks = [["AAPL", "MSFT"], ["NVDA", "META"], ["GOOG", "AMZN"], ["TSLA", "JPM"]]
        with ThreadPoolExecutor(max_workers=4) as ex:
            list(ex.map(self._fetch, chunks))

    def test_download_does_not_use_module_globals(self):
        # Pre-populate shared._DFS with a sentinel; if download() still
        # depended on it as scratch space, the sentinel would either be
        # wiped or leak into the result.
        from yfinance import shared
        sentinel = object()
        shared._DFS = {"__SENTINEL__": sentinel}

        df = self._fetch(["AAPL"])
        self.assertEqual(set(df.columns.get_level_values("Ticker")), {"AAPL"})
        self.assertIs(shared._DFS.get("__SENTINEL__"), sentinel)


if __name__ == "__main__":
    unittest.main()

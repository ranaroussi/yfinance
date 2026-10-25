import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pandas as pd

from tests.context import yfinance as yf


class TestDownloadThreadSafety(unittest.TestCase):

    def test_concurrent_downloads_return_only_own_tickers(self):
        """Concurrent download() calls must not mix results via shared state."""
        idx = pd.DatetimeIndex(['2024-01-02', '2024-01-03'], tz='America/New_York')
        aapl_df = pd.DataFrame(
            {'Open': [185.0, 186.0], 'Close': [185.5, 186.5]},
            index=idx,
        )
        msft_df = pd.DataFrame(
            {'Open': [375.0, 376.0], 'Close': [375.5, 376.5]},
            index=idx,
        )

        def mock_download_one(ctx, ticker, *args, **kwargs):
            time.sleep(0.05)
            sym = ticker.upper()
            df = aapl_df if sym == 'AAPL' else msft_df
            with ctx.lock:
                ctx.dfs[sym] = df
            return df

        results = {}
        errors = {}

        def do_download(tickers, key):
            try:
                results[key] = yf.download(
                    tickers, threads=False, progress=False,
                )
            except Exception as e:
                errors[key] = e

        with patch('yfinance.multi._download_one', side_effect=mock_download_one), \
             patch('yfinance.multi.YfData'):
            t1 = threading.Thread(target=do_download, args=(['AAPL'], 'aapl'))
            t2 = threading.Thread(target=do_download, args=(['MSFT'], 'msft'))
            t1.start()
            t2.start()
            t1.join(timeout=30)
            t2.join(timeout=30)

        self.assertFalse(errors, f"Download raised: {errors}")

        aapl_tickers = results['aapl'].columns.get_level_values('Ticker').unique().tolist()
        msft_tickers = results['msft'].columns.get_level_values('Ticker').unique().tolist()

        self.assertEqual(aapl_tickers, ['AAPL'])
        self.assertEqual(msft_tickers, ['MSFT'])


class TestInfoThreadSafety(unittest.TestCase):

    def test_multi_info_best_effort_partial_failure(self):
        class FakeTicker:
            def __init__(self, symbol, session=None):
                self.symbol = symbol.upper()

            @property
            def info(self):
                if self.symbol == "BAD":
                    raise RuntimeError("ticker failed")
                return {"symbol": self.symbol}

        with patch('yfinance.multi.Ticker', new=FakeTicker), \
             patch('yfinance.multi.YfData'):
            results = yf.multi.info(["AAPL", "BAD", "MSFT"], threads=False, progress=False)

        self.assertEqual(set(results.keys()), {"AAPL", "BAD", "MSFT"})
        self.assertEqual(results["AAPL"]["symbol"], "AAPL")
        self.assertEqual(results["MSFT"]["symbol"], "MSFT")
        self.assertEqual(results["BAD"], {})

    def test_concurrent_multi_info_calls_keep_results_separate(self):
        class FakeTicker:
            def __init__(self, symbol, session=None):
                self.symbol = symbol.upper()

            @property
            def info(self):
                time.sleep(0.02)
                return {"symbol": self.symbol}

        def fetch(tickers):
            return yf.multi.info(tickers, threads=False, progress=False)

        with patch('yfinance.multi.Ticker', new=FakeTicker), \
             patch('yfinance.multi.YfData'):
            with ThreadPoolExecutor(max_workers=2) as ex:
                f_a = ex.submit(fetch, ["AAPL", "MSFT"])
                f_b = ex.submit(fetch, ["NVDA", "META"])
                res_a = f_a.result()
                res_b = f_b.result()

        self.assertEqual(set(res_a.keys()), {"AAPL", "MSFT"})
        self.assertEqual(set(res_b.keys()), {"NVDA", "META"})

    def test_multi_info_threads_do_not_serialize_fetches(self):
        class SlowTicker:
            def __init__(self, symbol, session=None):
                self.symbol = symbol.upper()

            @property
            def info(self):
                time.sleep(0.15)
                return {"symbol": self.symbol}

        symbols = ["AAPL", "MSFT", "NVDA", "META"]

        with patch('yfinance.multi.Ticker', new=SlowTicker), \
             patch('yfinance.multi.YfData'):
            t0 = time.perf_counter()
            results = yf.multi.info(symbols, threads=True, progress=False)
            dt = time.perf_counter() - t0

        self.assertEqual(set(results.keys()), set(symbols))
        # Serial execution would be ~0.60s; allow generous margin for CI jitter.
        self.assertLess(dt, 0.50, f"Expected threaded info fetches, took {dt:.3f}s")


if __name__ == '__main__':
    unittest.main()

import threading
import time
import unittest
from unittest.mock import patch

import pandas as pd

from tests.context import yfinance as yf
from yfinance import shared


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

        def mock_download_one(ticker, *args, **kwargs):
            time.sleep(0.05)
            df = aapl_df if ticker.upper() == 'AAPL' else msft_df
            shared._DFS[ticker.upper()] = df
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


if __name__ == '__main__':
    unittest.main()

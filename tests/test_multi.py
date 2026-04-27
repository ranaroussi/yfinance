import threading
import time
import unittest
from unittest.mock import patch

import polars as pl

from tests.context import yfinance as yf
from yfinance import shared


class TestDownloadThreadSafety(unittest.TestCase):
    def test_concurrent_downloads_return_only_own_tickers(self):
        """Concurrent download() calls must not mix results via shared state."""
        # Build minimal polars DataFrames that _download_one would return
        import datetime as _dt

        dates = [_dt.date(2024, 1, 2), _dt.date(2024, 1, 3)]
        aapl_df = pl.DataFrame(
            {
                "Date": dates,
                "Open": [185.0, 186.0],
                "Close": [185.5, 186.5],
                "Ticker": ["AAPL", "AAPL"],
            }
        )
        msft_df = pl.DataFrame(
            {
                "Date": dates,
                "Open": [375.0, 376.0],
                "Close": [375.5, 376.5],
                "Ticker": ["MSFT", "MSFT"],
            }
        )

        def mock_download_one(ticker, *args, **kwargs):
            time.sleep(0.05)
            df = aapl_df if ticker.upper() == "AAPL" else msft_df
            shared._DFS[ticker.upper()] = df
            return df

        results = {}
        errors = {}

        def do_download(tickers, key):
            try:
                results[key] = yf.download(
                    tickers,
                    threads=False,
                    progress=False,
                )
            except Exception as e:
                errors[key] = e

        with (
            patch("yfinance.multi._download_one", side_effect=mock_download_one),
            patch("yfinance.multi.YfData"),
        ):
            t1 = threading.Thread(target=do_download, args=(["AAPL"], "aapl"))
            t2 = threading.Thread(target=do_download, args=(["MSFT"], "msft"))
            t1.start()
            t2.start()
            t1.join(timeout=30)
            t2.join(timeout=30)

        self.assertFalse(errors, f"Download raised: {errors}")

        aapl_result = results["aapl"]
        msft_result = results["msft"]

        self.assertIsInstance(aapl_result, pl.DataFrame)
        self.assertIsInstance(msft_result, pl.DataFrame)

        # In the polars long-form output, there is a "Ticker" column
        if "Ticker" in aapl_result.columns:
            aapl_tickers = aapl_result["Ticker"].unique().to_list()
            msft_tickers = msft_result["Ticker"].unique().to_list()
            self.assertEqual(sorted(aapl_tickers), ["AAPL"])
            self.assertEqual(sorted(msft_tickers), ["MSFT"])
        else:
            # Single-ticker download may not have a Ticker column — just check non-empty
            self.assertGreater(aapl_result.height, 0)
            self.assertGreater(msft_result.height, 0)


if __name__ == "__main__":
    unittest.main()

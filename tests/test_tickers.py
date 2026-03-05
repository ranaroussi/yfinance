import unittest
from unittest.mock import patch

from tests.context import yfinance as yf


class TestTickers(unittest.TestCase):
    def test_download_rejects_period_start_end_together(self):
        tickers = yf.Tickers("AAPL MSFT")

        with self.assertRaises(ValueError) as exc:
            tickers.download(period="1mo", start="2025-01-01", end="2025-02-01", progress=False)

        self.assertIn("at most 2", str(exc.exception))

    @patch("yfinance.tickers.multi.download")
    def test_history_forwards_when_date_args_valid(self, mock_download):
        mock_download.return_value = {"AAPL": None}
        tickers = yf.Tickers("AAPL")

        tickers.history(period=None, start="2025-01-01", end="2025-02-01", progress=False, group_by="ticker")

        self.assertTrue(mock_download.called)

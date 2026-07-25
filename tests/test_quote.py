import datetime
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from yfinance.scrapers.quote import FastInfo


class TestFastInfo(unittest.TestCase):
    def test_shares_does_not_use_pandas_timedelta(self):
        ticker = MagicMock()
        ticker.get_shares_full.return_value = pd.Series([100])

        with patch(
            "yfinance.scrapers.quote.pd.Timedelta",
            side_effect=AssertionError(
                "FastInfo.shares must not use pandas Timedelta with a Python date"
            ),
        ):
            shares = FastInfo(ticker).shares

        self.assertEqual(shares, 100)
        ticker.get_shares_full.assert_called_once()

        start = ticker.get_shares_full.call_args.kwargs["start"]
        self.assertIsInstance(start, datetime.date)


if __name__ == "__main__":
    unittest.main()

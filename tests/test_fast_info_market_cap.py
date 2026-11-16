"""fast_info market cap is in the main currency unit, like info["marketCap"]."""

import unittest

from yfinance.scrapers.quote import FastInfo


def _fast_info(currency, last_price):
    fi = FastInfo(None)
    fi._shares = 1000
    fi._last_price = last_price
    fi._currency = currency
    return fi


class TestFastInfoMarketCap(unittest.TestCase):
    def test_price_in_pence(self):
        self.assertEqual(_fast_info("GBp", 250.0).market_cap, 2500.0)

    def test_price_in_main_unit(self):
        self.assertEqual(_fast_info("USD", 250.0).market_cap, 250000.0)


if __name__ == "__main__":
    unittest.main()

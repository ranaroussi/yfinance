import unittest

from tests.context import yfinance as yf


class TestSectorRegion(unittest.TestCase):
    def test_default_region_is_us(self):
        s = yf.Sector("technology")
        self.assertEqual(s._region, "US")

    def test_region_is_normalized(self):
        for raw, expected in [("us", "US"), (" GB ", "GB"), ("Fr", "FR")]:
            s = yf.Sector("technology", region=raw)
            self.assertEqual(s._region, expected)

    def test_us_and_gb_top_companies_differ(self):
        us = yf.Sector("technology").top_companies
        gb = yf.Sector("technology", region="GB").top_companies
        self.assertIsNotNone(us)
        self.assertIsNotNone(gb)
        # UK-listed symbols carry the .L suffix, U.S. symbols do not.
        self.assertTrue(any(sym.endswith(".L") for sym in gb.index))
        self.assertFalse(any(sym.endswith(".L") for sym in us.index))

    def test_industry_region_propagates(self):
        ind = yf.Industry("software-infrastructure", region="DE")
        self.assertEqual(ind._region, "DE")


if __name__ == "__main__":
    unittest.main()

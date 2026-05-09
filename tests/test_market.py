import unittest

from tests.context import yfinance as yf


class TestMarketValidation(unittest.TestCase):
    def test_string_input(self):
        m = yf.Market("EUROPE")
        self.assertEqual(m.market, "EUROPE")

    def test_enum_input(self):
        m = yf.Market(yf.MarketRegion.EUROPE)
        self.assertEqual(m.market, "EUROPE")
        self.assertIsInstance(m.market, str)

    def test_invalid_market_raises(self):
        with self.assertRaises(ValueError) as ctx:
            yf.Market("FR")
        self.assertIn("FR", str(ctx.exception))
        self.assertIn("EUROPE", str(ctx.exception))

    def test_market_region_members(self):
        expected = {
            "US", "GB", "ASIA", "EUROPE",
            "RATES", "COMMODITIES", "CURRENCIES", "CRYPTOCURRENCIES",
        }
        self.assertEqual({m.value for m in yf.MarketRegion}, expected)

    def test_market_region_str_equality(self):
        self.assertEqual(yf.MarketRegion.EUROPE, "EUROPE")


class TestMarketFetch(unittest.TestCase):
    def test_us_summary_and_status(self):
        m = yf.Market("US")
        self.assertIsInstance(m.summary, dict)
        self.assertGreater(len(m.summary), 0)
        self.assertIsNotNone(m.status)
        self.assertEqual(m.status.get("id"), "us")

    def test_europe_summary_returns_regional_exchanges(self):
        m = yf.Market("EUROPE")
        self.assertIsInstance(m.summary, dict)
        # EUROPE summary should not be dominated by U.S. exchanges
        self.assertGreater(len(m.summary), 0)
        self.assertNotIn("SNP", m.summary)

    def test_non_us_status_is_none(self):
        # Yahoo's markettime endpoint silently ignores `market` and returns
        # U.S. data; Market should surface this as None rather than mislead.
        m = yf.Market("EUROPE")
        self.assertIsNone(m.status)


if __name__ == "__main__":
    unittest.main()

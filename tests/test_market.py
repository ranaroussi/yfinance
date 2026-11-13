import datetime as dt
import unittest
from unittest.mock import patch

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


class TestMarketStatusParsing(unittest.TestCase):
    """Offline parsing of Yahoo's markettime payload (no network)."""

    _STATUS_PAYLOAD = {
        "finance": {
            "marketTimes": [{
                "marketTime": [{
                    "id": "us",
                    "short": "US",
                    "time": ["2026-11-09T15:00:00Z", "regular"],
                    "open": "2026-11-09T14:30:00Z",
                    "close": "2026-11-09T21:00:00Z",
                    "timezone": [{
                        "short": "EST",
                        "long": "Eastern Standard Time",
                        "gmtoffset": "-18000",
                        "dst": "false",
                    }],
                }]
            }]
        }
    }

    _SUMMARY_PAYLOAD = {"marketSummaryResponse": {"result": [], "error": None}}

    def _market(self):
        m = yf.Market("US")
        with patch.object(m, "_fetch_json", side_effect=[self._SUMMARY_PAYLOAD, self._STATUS_PAYLOAD]):
            status = m.status
        return status

    def test_open_close_parsed_as_datetimes_on_all_pythons(self):
        # 'Z'-suffixed ISO strings only parse with datetime.fromisoformat on
        # Python 3.11+; yfinance supports older versions, so these must be
        # real datetimes everywhere.
        status = self._market()
        self.assertIsInstance(status["open"], dt.datetime)
        self.assertIsInstance(status["close"], dt.datetime)
        self.assertEqual(status["open"], dt.datetime(2026, 11, 9, 14, 30, tzinfo=dt.timezone.utc))
        self.assertEqual(status["close"], dt.datetime(2026, 11, 9, 21, 0, tzinfo=dt.timezone.utc))

    def test_tz_offset_uses_gmtoffset_seconds(self):
        # gmtoffset is seconds: "-18000" means UTC-5 (US Eastern), not UTC-18.
        status = self._market()
        self.assertEqual(status["tz"].utcoffset(None), dt.timedelta(hours=-5))
        self.assertEqual(str(status["tz"]), "EST")


if __name__ == "__main__":
    unittest.main()

"""
Regression tests for issue #2745:
get_history_metadata() raised YFPricesMissingError for valid tickers (e.g. ROG.SW)
when the intraday enrichment request returned no data, even though history() had
already populated _history_metadata with currency, exchangeName, timezone, etc.

Two bugs fixed:
  1. history() always overwrote _history_metadata with {} on a failed fetch,
     destroying previously cached good metadata.
  2. get_history_metadata() let the optional intraday fetch propagate
     YFPricesMissingError instead of returning the metadata already cached.
"""
import unittest
from unittest.mock import MagicMock

from yfinance.scrapers.history import PriceHistory
from yfinance.exceptions import YFPricesMissingError


def _make_ph(json_payload):
    """Return a PriceHistory whose HTTP layer returns json_payload."""
    mock_resp = MagicMock()
    mock_resp.text = ""
    mock_resp.json.return_value = json_payload

    mock_data = MagicMock()
    mock_data.get.return_value = mock_resp
    mock_data.cache_get.return_value = mock_resp

    return PriceHistory(data=mock_data, ticker="FAKE", tz="America/New_York")


def _good_chart(meta: dict) -> dict:
    """Minimal Yahoo chart payload with valid meta and one price row."""
    return {
        "chart": {
            "result": [{
                "meta": meta,
                "timestamp": [1700000000],
                "indicators": {
                    "quote": [{
                        "open": [100.0],
                        "high": [101.0],
                        "low": [99.0],
                        "close": [100.5],
                        "volume": [1000000],
                    }],
                    "adjclose": [{"adjclose": [100.5]}],
                },
                "events": {},
            }],
            "error": None,
        }
    }


_GOOD_META = {
    "currency": "CHF",
    "exchangeName": "EBS",
    "exchangeTimezoneName": "Europe/Zurich",
    "instrumentType": "EQUITY",
    "validRanges": ["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"],
    "regularMarketTime": 1700000000,
    "regularMarketPrice": 250.0,
    "priceHint": 2,
}

_EMPTY_CHART = {"chart": {"result": None, "error": None}}


class TestMetadataNotOverwrittenOnFailure(unittest.TestCase):
    """Fix 1: failed intraday fetch must not clobber previously cached metadata."""

    def test_good_metadata_preserved_after_empty_intraday(self):
        ph = _make_ph(_good_chart(_GOOD_META))
        # First call: daily history succeeds and caches metadata
        ph.history(period="1mo", raise_errors=False)
        cached = ph._history_metadata.copy()
        self.assertEqual(cached.get("currency"), "CHF")

        # Second call: intraday returns empty chart
        ph._data.get.return_value.json.return_value = _EMPTY_CHART
        ph.history(period="5d", interval="1h", raise_errors=False)

        # Metadata must not have been wiped out
        self.assertEqual(ph._history_metadata.get("currency"), "CHF",
                         "Good metadata was overwritten by an empty intraday response")

    def test_metadata_set_to_empty_when_never_populated(self):
        ph = _make_ph(_EMPTY_CHART)
        ph.history(period="1mo", raise_errors=False)
        # No prior metadata: it's fine to end up with {}
        self.assertEqual(ph._history_metadata, {})


class TestGetHistoryMetadataDoesNotRaise(unittest.TestCase):
    """Fix 2: get_history_metadata() must not raise when intraday enrichment fails."""

    def test_returns_cached_metadata_when_intraday_fails(self):
        ph = _make_ph(_good_chart(_GOOD_META))
        # Populate metadata via a daily history call
        ph.history(period="1mo", raise_errors=False)
        self.assertIsNotNone(ph._history_metadata)

        # Now make the intraday enrichment call fail
        ph._data.get.return_value.json.return_value = _EMPTY_CHART

        # Should NOT raise — should return whatever metadata we have
        try:
            md = ph.get_history_metadata()
        except YFPricesMissingError:
            self.fail("get_history_metadata() raised YFPricesMissingError "
                      "even though metadata was already cached")

        self.assertIn("currency", md)
        self.assertEqual(md["currency"], "CHF")

    def test_metadata_none_when_history_never_called(self):
        """Before any history() call, _history_metadata is None."""
        ph = _make_ph(_EMPTY_CHART)
        self.assertIsNone(ph._history_metadata)


if __name__ == "__main__":
    unittest.main()

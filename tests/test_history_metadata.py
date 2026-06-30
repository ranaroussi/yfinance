"""
Regression tests for PriceHistory.get_history_metadata().

These do not hit the network: the secondary intraday request that
get_history_metadata() makes to populate 'tradingPeriods' is mocked.
"""
import unittest
from unittest.mock import patch

from yfinance.scrapers.history import PriceHistory
from yfinance.exceptions import YFPricesMissingError


def _make_price_history():
    # PriceHistory only needs (data, ticker, tz) for these code paths;
    # _data is never touched because _get_history_cache is mocked.
    return PriceHistory(data=None, ticker="TEST", tz="UTC")


class TestGetHistoryMetadata(unittest.TestCase):
    def test_metadata_preserved_when_tradingperiods_fetch_fails(self):
        """A failing 'tradingPeriods' enrichment fetch must not discard or
        raise over metadata already populated by a prior history() call."""
        ph = _make_price_history()
        good = {"currency": "CHF", "exchangeName": "EBS", "YF repair?": False}
        ph._history_metadata = dict(good)
        ph._history_metadata_formatted = True  # skip network-free formatting step

        def _raise(*args, **kwargs):
            raise YFPricesMissingError("TEST", "")

        with patch.object(PriceHistory, "_get_history_cache", side_effect=_raise):
            md = ph.get_history_metadata()

        self.assertEqual(md.get("currency"), "CHF")
        self.assertEqual(md.get("exchangeName"), "EBS")

    def test_raises_when_no_prior_metadata(self):
        """If there is no prior metadata at all, the error must still surface."""
        ph = _make_price_history()
        ph._history_metadata = None

        def _raise(*args, **kwargs):
            raise YFPricesMissingError("TEST", "")

        with patch.object(PriceHistory, "_get_history_cache", side_effect=_raise):
            with self.assertRaises(YFPricesMissingError):
                ph.get_history_metadata()


if __name__ == "__main__":
    unittest.main()

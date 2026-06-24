"""
Unit tests for yfinance.scrapers.quote.Quote.

These are offline tests (the network fetches are mocked), covering the
no-data / partial-data paths of ``_fetch_info`` — see issue #2865.
"""

import unittest
from unittest.mock import MagicMock, patch

from yfinance.scrapers.quote import Quote


class TestQuoteFetchInfo(unittest.TestCase):
    def _make_quote(self):
        # _data is only used by the (mocked) fetch helpers, so a stub is fine.
        return Quote(MagicMock(), "TEST")

    def test_no_crash_when_both_fetches_return_none(self):
        """Both Yahoo endpoints empty must not raise TypeError (#2865)."""
        q = self._make_quote()
        with patch.object(q, "_fetch", return_value=None), \
             patch.object(q, "_fetch_additional_info", return_value=None):
            q._fetch_info()  # previously: "argument of type 'NoneType' is not ..."
        self.assertIsNone(q._info)

    def test_valid_result_kept_when_additional_info_is_none(self):
        """A valid primary result must not be discarded when there's no
        additional info (the old ``else`` branch nulled it)."""
        q = self._make_quote()
        valid = {"quoteSummary": {"result": [{"symbol": "TEST", "shortName": "Test Co"}]}}
        with patch.object(q, "_fetch", return_value=valid), \
             patch.object(q, "_fetch_additional_info", return_value=None):
            q._fetch_info()
        self.assertIsNotNone(q._info)
        self.assertEqual(q._info.get("shortName"), "Test Co")

    def test_additional_info_used_when_primary_is_none(self):
        """When only the additional-info endpoint responds, it is used."""
        q = self._make_quote()
        extra = {"quoteResponse": {"result": [{"symbol": "TEST", "longName": "Test Co"}]}}
        with patch.object(q, "_fetch", return_value=None), \
             patch.object(q, "_fetch_additional_info", return_value=extra):
            q._fetch_info()
        self.assertIsNotNone(q._info)
        self.assertEqual(q._info.get("longName"), "Test Co")


if __name__ == "__main__":
    unittest.main()

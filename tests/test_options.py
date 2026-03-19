"""Unit tests for shared option binding helpers."""

import re
import unittest

from yfinance.options import (
    HISTORY_REQUEST_ARG_NAMES,
    HISTORY_REQUEST_DEFAULTS,
    TICKERS_DOWNLOAD_ARG_NAMES,
    TICKERS_DOWNLOAD_DEFAULTS,
    bind_options,
)
from yfinance.const import holders_quote_summary_modules, quote_summary_valid_modules
from yfinance.scrapers.history.helpers import _parse_history_request


class TestBindOptions(unittest.TestCase):
    """Validate shared option binding logic."""

    def test_bind_options_returns_passthrough_kwargs(self):
        options, passthrough = bind_options(
            "download",
            TICKERS_DOWNLOAD_ARG_NAMES,
            TICKERS_DOWNLOAD_DEFAULTS,
            ("1mo",),
            {"timeout": 20, "session": object()},
        )

        self.assertEqual(options["period"], "1mo")
        self.assertEqual(options["timeout"], 20)
        self.assertIn("session", passthrough)

    def test_bind_options_rejects_duplicate_argument(self):
        with self.assertRaisesRegex(
            TypeError,
            re.escape("download() got multiple values for argument 'period'"),
        ):
            bind_options(
                "download",
                TICKERS_DOWNLOAD_ARG_NAMES,
                TICKERS_DOWNLOAD_DEFAULTS,
                ("1mo",),
                {"period": "5d"},
            )

    def test_parse_history_request_rejects_unexpected_keyword(self):
        with self.assertRaisesRegex(
            TypeError,
            re.escape("history() got an unexpected keyword argument 'proxy'"),
        ):
            _parse_history_request("history", (), {"proxy": "http://localhost"})

    def test_history_request_defaults_are_complete(self):
        request = _parse_history_request("history", (), {})

        self.assertEqual(request.interval, HISTORY_REQUEST_DEFAULTS["interval"])
        self.assertEqual(request.timeout, HISTORY_REQUEST_DEFAULTS["timeout"])
        self.assertEqual(len(HISTORY_REQUEST_ARG_NAMES), len(HISTORY_REQUEST_DEFAULTS))


class TestQuoteSummaryModuleConstants(unittest.TestCase):
    """Validate canonical quote-summary module subsets."""

    def test_holders_modules_are_valid_quote_summary_modules(self):
        self.assertTrue(holders_quote_summary_modules)
        self.assertTrue(set(holders_quote_summary_modules).issubset(quote_summary_valid_modules))


if __name__ == "__main__":
    unittest.main()
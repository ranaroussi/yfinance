"""Unit tests for shared Yahoo response helpers."""

import unittest
from unittest.mock import Mock

from yfinance.http import parse_json_response
from yfinance.exceptions import YFDataException


class TestParseJsonResponse(unittest.TestCase):
    """Validate shared Yahoo response parsing behavior."""

    def test_parse_json_response_returns_payload(self):
        """Return parsed JSON for a valid response payload."""
        logger = Mock()
        response = Mock(text="")
        response.json.return_value = {"quotes": []}

        parsed = parse_json_response(response, logger, "failure")

        self.assertEqual(parsed, {"quotes": []})
        logger.error.assert_not_called()

    def test_parse_json_response_rejects_downtime_page(self):
        """Raise the Yahoo downtime exception for maintenance pages."""
        logger = Mock()
        response = Mock(text="Will be right back")

        with self.assertRaises(YFDataException):
            parse_json_response(response, logger, "failure")


if __name__ == "__main__":
    unittest.main()

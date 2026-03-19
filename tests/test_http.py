"""Unit tests for shared Yahoo response helpers."""

import unittest

from yfinance.http import parse_json_response
from yfinance.exceptions import YFDataException


class _LoggerStub:
    def __init__(self):
        self.calls = []

    def error(self, message, *args):
        self.calls.append((message, args))


class _ResponseStub:
    def __init__(self, *, text="", payload=None, error=None):
        self.text = text
        self._payload = payload
        self._error = error

    def json(self):
        if self._error is not None:
            raise self._error
        return self._payload


class TestParseJsonResponse(unittest.TestCase):
    """Validate shared Yahoo response parsing behavior."""

    def test_parse_json_response_returns_payload(self):
        logger = _LoggerStub()
        response = _ResponseStub(payload={"quotes": []})

        parsed = parse_json_response(response, logger, "failure")

        self.assertEqual(parsed, {"quotes": []})
        self.assertEqual(logger.calls, [])

    def test_parse_json_response_rejects_downtime_page(self):
        logger = _LoggerStub()
        response = _ResponseStub(text="Will be right back")

        with self.assertRaises(YFDataException):
            parse_json_response(response, logger, "failure")


if __name__ == "__main__":
    unittest.main()
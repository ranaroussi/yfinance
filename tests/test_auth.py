"""Unit tests for yfinance.data.Auth login detection.

These are pure unit tests: the Yahoo homepage response is mocked, so they run
offline and require no Yahoo Finance login cookies. They lock in the fix for
``check_login()`` (which previously looked for the client-side-only
``<script id="nimbus-benji-config">`` element and therefore always returned
``False``) by parsing the server-rendered ``window.YAHOO.context`` object.
"""
import unittest
from unittest import mock

from yfinance.data import Auth, _extract_window_yahoo_context


# --- Synthetic fixtures (no real personal data) -----------------------------
# Mimic the real page: window.YAHOO.context is assigned inside a <script>.
_LOGGED_IN_HTML = """<!doctype html><html><head><script>
(function(){if(!window.YAHOO){window.YAHOO={}};window.YAHOO.context = {"authed":"1","user":{"age":"","alias":"unit-test@example.com","crumb":"abc123","firstName":"Unit","gender":"","guid":"ABCDEFGHIJKLMNOPQRSTUV1234","login":"unit-test@example.com","year":""},"feature":{}};})();
</script></head><body></body></html>"""

_LOGGED_OUT_HTML = """<!doctype html><html><head><script>
(function(){if(!window.YAHOO){window.YAHOO={}};window.YAHOO.context = {"authed":"0","user":{"age":"","crumb":"abc123","firstName":"","gender":"","year":""},"feature":{}};})();
</script></head><body></body></html>"""

_NO_CONTEXT_HTML = "<!doctype html><html><body><p>nothing here</p></body></html>"

# A logged-in context whose string values contain braces, to exercise the
# string-aware brace matching in _extract_window_yahoo_context.
_BRACES_IN_STRING_HTML = (
    """<script>window.YAHOO.context = """
    """{"authed":"1","user":{"guid":"GUID0000000000000000000000","""
    """"firstName":"a}b{c","alias":"x@y.com"}};</script>"""
)

# A decoy assignment inside a JS string literal appears BEFORE the real one;
# the real assignment must still be found.
_FALSE_MATCH_FIRST_HTML = (
    """<script>var s = "window.YAHOO.context = {fake}"; """
    """window.YAHOO.context = {"authed":"1","user":{"guid":"REALGUID000000000000000AB"}};"""
    """</script>"""
)

# A context assignment whose body is not valid JSON.
_INVALID_JSON_HTML = """<script>window.YAHOO.context = {not valid json,,};</script>"""

# Edge cases for the check_login decision logic.
_USER_NOT_DICT_HTML = """<script>window.YAHOO.context = {"authed":"1","user":"oops"};</script>"""
_AUTHED_MISSING_HTML = """<script>window.YAHOO.context = {"user":{"guid":"G000000000000000000000000"}};</script>"""
_EMPTY_GUID_HTML = """<script>window.YAHOO.context = {"authed":"1","user":{"guid":""}};</script>"""


def _auth_returning(html):
    """Build an Auth whose underlying data layer returns the given HTML."""
    auth = Auth()
    auth._user = None
    auth._data = mock.MagicMock()
    auth._data.get.return_value = mock.MagicMock(text=html)
    return auth


class TestExtractWindowYahooContext(unittest.TestCase):
    def test_parses_logged_in_context(self):
        ctx = _extract_window_yahoo_context(_LOGGED_IN_HTML)
        self.assertIsNotNone(ctx)
        self.assertEqual(ctx["authed"], "1")
        self.assertEqual(ctx["user"]["guid"], "ABCDEFGHIJKLMNOPQRSTUV1234")

    def test_handles_braces_inside_strings(self):
        ctx = _extract_window_yahoo_context(_BRACES_IN_STRING_HTML)
        self.assertIsNotNone(ctx)
        # The '}' and '{' inside the string value must not terminate the object.
        self.assertEqual(ctx["user"]["firstName"], "a}b{c")
        self.assertEqual(ctx["authed"], "1")

    def test_skips_false_match_in_string_literal(self):
        # The decoy "window.YAHOO.context = {fake}" must be skipped in favour of
        # the real, parseable assignment that follows it.
        ctx = _extract_window_yahoo_context(_FALSE_MATCH_FIRST_HTML)
        self.assertIsNotNone(ctx)
        self.assertEqual(ctx["user"]["guid"], "REALGUID000000000000000AB")

    def test_returns_none_on_invalid_json(self):
        self.assertIsNone(_extract_window_yahoo_context(_INVALID_JSON_HTML))

    def test_returns_none_when_absent(self):
        self.assertIsNone(_extract_window_yahoo_context(_NO_CONTEXT_HTML))


class TestAuthCheckLogin(unittest.TestCase):
    def test_true_when_logged_in(self):
        auth = _auth_returning(_LOGGED_IN_HTML)
        self.assertTrue(auth.check_login())

    def test_false_when_logged_out(self):
        auth = _auth_returning(_LOGGED_OUT_HTML)
        self.assertFalse(auth.check_login())

    def test_false_when_context_absent(self):
        auth = _auth_returning(_NO_CONTEXT_HTML)
        self.assertFalse(auth.check_login())

    def test_false_when_user_not_dict(self):
        # A truthy non-dict 'user' must not crash and must be treated as not logged in.
        auth = _auth_returning(_USER_NOT_DICT_HTML)
        self.assertFalse(auth.check_login())

    def test_false_when_authed_missing(self):
        # guid present but authed flag absent -> not logged in.
        auth = _auth_returning(_AUTHED_MISSING_HTML)
        self.assertFalse(auth.check_login())

    def test_false_when_guid_empty(self):
        # authed '1' but empty guid (a half-rendered/logged-out shape) -> False.
        auth = _auth_returning(_EMPTY_GUID_HTML)
        self.assertFalse(auth.check_login())

    def test_user_property_populated_when_logged_in(self):
        auth = _auth_returning(_LOGGED_IN_HTML)
        user = auth.user
        self.assertIsInstance(user, dict)
        self.assertEqual(user["guid"], "ABCDEFGHIJKLMNOPQRSTUV1234")
        self.assertEqual(user["alias"], "unit-test@example.com")

    def test_user_property_none_when_logged_out(self):
        auth = _auth_returning(_LOGGED_OUT_HTML)
        self.assertIsNone(auth.user)

    def test_check_login_caches_and_short_circuits(self):
        auth = _auth_returning(_LOGGED_IN_HTML)
        self.assertTrue(auth.check_login())
        self.assertTrue(auth.check_login())
        # Second call must use the cached self._user, not refetch.
        self.assertEqual(auth._data.get.call_count, 1)


if __name__ == "__main__":
    unittest.main()

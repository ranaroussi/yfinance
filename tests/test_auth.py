"""Unit tests for yfinance.data.Auth (login state + subscription tier).

Pure unit tests: the OBI subscriptions endpoint response is mocked, so they run
offline with no Yahoo login cookies. Auth derives both login state and the
subscription tier from a single lightweight JSON call (no web-page scraping).
The call is made live each time (not cached), so the answer never goes stale.
"""
import unittest
from unittest import mock

from yfinance.data import Auth, SingletonMeta, YfData, _SUBSCRIPTIONS_URL

_GUID = "ABCDEFGHIJKLMNOPQRSTUV1234"


def _result(guid=_GUID, tier=None, action="ACTIVE"):
    """Build a subscriptions ``result`` with an optional subscription tier id."""
    view = [{"action": action, "tier": tier}] if tier is not None else []
    return {"guid": guid, "subscriptionView": view}


def _auth(status=200, result=None):
    """Build an Auth whose subscriptions call returns the given status/result."""
    resp = mock.MagicMock()
    resp.status_code = status
    resp.json.return_value = {"result": result} if result is not None else {}
    auth = Auth()
    auth._data = mock.MagicMock()
    auth._data.get.return_value = resp
    return auth


def tearDownModule():
    # _auth() builds a real Auth() (which registers a YfData singleton) before
    # swapping in a mock, so drop the singleton afterwards to avoid leaking a
    # stale instance into other test modules.
    SingletonMeta._instances.pop(YfData, None)


class TestAuthLogin(unittest.TestCase):
    def test_logged_in_when_200_with_guid(self):
        auth = _auth(200, _result())
        self.assertTrue(auth.check_login())
        self.assertEqual(auth.user, {"guid": _GUID})

    def test_not_logged_in_when_401(self):
        auth = _auth(401)
        self.assertFalse(auth.check_login())
        self.assertIsNone(auth.user)

    def test_not_logged_in_when_403(self):
        auth = _auth(403)
        self.assertFalse(auth.check_login())
        self.assertIsNone(auth.user)

    def test_not_logged_in_when_200_without_guid(self):
        auth = _auth(200, {"subscriptionView": []})  # 200 but no guid -> not logged in
        self.assertFalse(auth.check_login())
        self.assertIsNone(auth.user)

    def test_not_logged_in_on_transient_error(self):
        # A 429/5xx can't confirm login; report not-logged-in for that call.
        self.assertFalse(_auth(429).check_login())
        self.assertFalse(_auth(500).check_login())

    def test_probe_is_a_single_subscriptions_get(self):
        # Login state comes from one lightweight GET to the subscriptions
        # endpoint; a 401/403 is the "not logged in" answer.
        auth = _auth(401)
        auth.check_login()
        auth._data.get.assert_called_once_with(_SUBSCRIPTIONS_URL)

    def test_check_login_is_live_not_cached(self):
        # Each call re-queries: a session that goes valid/invalid between calls
        # is reflected immediately rather than served from a stale cache.
        auth = _auth(401)
        self.assertFalse(auth.check_login())
        self.assertFalse(auth.check_login())
        self.assertEqual(auth._data.get.call_count, 2)


class TestAuthSubscriptionTier(unittest.TestCase):
    def test_free_when_logged_in_no_subscription(self):
        self.assertEqual(_auth(200, _result()).subscription_tier(), "free")

    def test_bronze(self):
        self.assertEqual(_auth(200, _result(tier=3)).subscription_tier(), "bronze")

    def test_silver(self):
        self.assertEqual(_auth(200, _result(tier=5)).subscription_tier(), "silver")

    def test_gold(self):
        self.assertEqual(_auth(200, _result(tier=6)).subscription_tier(), "gold")

    def test_premium_for_unmarketed_tier(self):
        # tier 4 exists in tierRanking but has no marketed name -> generic premium.
        self.assertEqual(_auth(200, _result(tier=4)).subscription_tier(), "premium")

    def test_free_when_subscription_not_active(self):
        # A cancelled/expired (non-ACTIVE) subscription -> effectively free.
        auth = _auth(200, _result(tier=6, action="CANCELLED"))
        self.assertEqual(auth.subscription_tier(), "free")

    def test_none_when_not_logged_in(self):
        self.assertIsNone(_auth(401).subscription_tier())


class TestSetLoginCookies(unittest.TestCase):
    def test_returns_true_and_validates_when_logged_in(self):
        auth = _auth(200, _result())
        self.assertTrue(auth.set_login_cookies("T-val", "Y-val"))
        # cookies stored on the shared client...
        auth._data.set_login_cookies.assert_called_once_with("T-val", "Y-val")
        # ...and validated via a live login check.
        self.assertTrue(auth._data.get.called)

    def test_returns_false_when_not_logged_in(self):
        auth = _auth(401)
        self.assertFalse(auth.set_login_cookies("bad", "bad"))
        auth._data.set_login_cookies.assert_called_once_with("bad", "bad")

    def test_warns_when_not_logged_in(self):
        auth = _auth(401)
        with mock.patch("yfinance.data.utils.get_yf_logger") as get_logger:
            auth.set_login_cookies("bad", "bad")
            get_logger.return_value.warning.assert_called_once()


class TestLoggedInFlag(unittest.TestCase):
    """Auth reconciles YfData._logged_in with the verified login state."""

    def test_entitlement_marks_logged_in_when_valid(self):
        auth = _auth(200, _result())
        auth.check_login()
        auth._data._set_logged_in.assert_called_with(True)

    def test_entitlement_marks_logged_out_when_anonymous(self):
        auth = _auth(401)
        auth.check_login()
        auth._data._set_logged_in.assert_called_with(False)

    def test_logged_in_unchanged_on_transient_error(self):
        # A transient exception can't confirm login, so the cookie-preservation
        # flag must NOT be touched (neither up nor down) — whether the error is
        # hidden or re-raised depends on config, but either way _set_logged_in
        # must not be called.
        auth = _auth()
        auth._data.get.side_effect = ConnectionError("boom")
        try:
            auth.check_login()
        except ConnectionError:
            pass
        auth._data._set_logged_in.assert_not_called()


if __name__ == "__main__":
    unittest.main()

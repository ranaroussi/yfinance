import unittest
from unittest import mock

from yfinance._http import requests
from yfinance.data import SingletonMeta, YfData


def _mock_response(status_code=200, text="", url="https://query1.finance.yahoo.com/v8/finance/chart/AAPL"):
    resp = mock.MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.content = text.encode() or b"<html></html>"
    resp.url = url
    resp.json.return_value = {}
    return resp


class _DegradeTestBase(unittest.TestCase):
    """YfData is a singleton: use throwaway instances so mocked state
    cannot leak into other tests."""

    def setUp(self):
        SingletonMeta._instances.pop(YfData, None)
        self.data = YfData()
        self.data._cookie = None
        self.data._crumb = None
        self._no_persistent_cookie = mock.patch.object(
            YfData, '_load_cookie_curlCffi', return_value=False)
        self._no_persistent_cookie.start()

    def tearDown(self):
        self._no_persistent_cookie.stop()
        SingletonMeta._instances.pop(YfData, None)


class TestCookieFetchDegradation(_DegradeTestBase):
    def test_fc_yahoo_timeout_is_not_fatal(self):
        # fc.yahoo.com timing out behind a SOCKS5 proxy must degrade,
        # not abort the eventual data request.
        self.data._session.get = mock.MagicMock(
            side_effect=requests.exceptions.Timeout("curl: (28) Connection timed out"))
        self.assertFalse(self.data._get_cookie_basic())

    def test_getcrumb_429_degrades_to_no_crumb_request(self):
        # getcrumb returning 429 must not abort the request: chart API
        # works without a crumb.
        def fake_get(url=None, **kwargs):
            if 'fc.yahoo.com' in url:
                return _mock_response(200, url=url)
            if 'getcrumb' in url:
                return _mock_response(429, "Too Many Requests", url=url)
            return _mock_response(200, "{}", url=url)

        self.data._session.get = mock.MagicMock(side_effect=fake_get)
        with mock.patch.object(YfData, '_load_cookie_curlCffi', return_value=False):
            resp = self.data.get('https://query1.finance.yahoo.com/v8/finance/chart/AAPL')
        self.assertEqual(resp.status_code, 200)
        sent_kwargs = self.data._session.get.call_args
        self.assertNotIn('crumb', str(sent_kwargs))
    def test_transient_crumb_failure_degrades(self):
        # A timeout on getcrumb must not abort the chart request either.
        def fake_get(url=None, **kwargs):
            if 'fc.yahoo.com' in url:
                return _mock_response(200, url=url)
            if 'getcrumb' in url:
                raise requests.exceptions.Timeout("timed out")
            return _mock_response(200, '{"chart": {}}', url=url)

        self.data._session.get = mock.MagicMock(side_effect=fake_get)
        with mock.patch.object(YfData, '_load_cookie_curlCffi', return_value=False):
            resp = self.data.get('https://query1.finance.yahoo.com/v8/finance/chart/AAPL')
        self.assertEqual(resp.status_code, 200)

    def test_target_429_still_raises_rate_limit(self):
        # Degrading cookie/crumb must not hide genuine Yahoo rate limiting:
        # when the target endpoint itself 429s, YFRateLimitError is raised.
        def fake_get(url=None, **kwargs):
            if 'fc.yahoo.com' in url:
                raise requests.exceptions.ConnectionError("proxy down")
            if 'getcrumb' in url:
                raise requests.exceptions.Timeout("timed out")
            return _mock_response(429, "Too Many Requests", url=url)

        from yfinance.exceptions import YFRateLimitError
        self.data._session.get = mock.MagicMock(side_effect=fake_get)
        try:
            self.data.get('https://query1.finance.yahoo.com/v8/finance/chart/AAPL')
            self.fail("expected YFRateLimitError")
        except YFRateLimitError:
            pass

    def test_custom_proxies_not_overwritten(self):
        # A user-supplied SOCKS5 proxy mapping must survive being passed
        # to yfinance.
        proxy_map = {"http": "socks5h://127.0.0.1:1080",
                     "https": "socks5h://127.0.0.1:1080"}
        session = requests.Session()
        session.proxies = dict(proxy_map)
        try:
            data = YfData(session=session)
            self.assertEqual(data._session.proxies, proxy_map)
        finally:
            SingletonMeta._instances.pop(YfData, None)

    def test_make_request_does_not_wipe_session_proxies(self):
        # Regression: _make_request() used to reset session proxies to the
        # global config value (None by default), silently disabling a
        # SOCKS5 proxy configured directly on the session.
        proxy_map = {"http": "socks5h://127.0.0.1:1080",
                     "https": "socks5h://127.0.0.1:1080"}
        self.data._session.proxies = dict(proxy_map)
        self.data._session.get = mock.MagicMock(
            return_value=_mock_response(200, '{"chart": {}}'))
        with mock.patch.object(YfData, '_load_cookie_curlCffi', return_value=False), \
             mock.patch.object(YfData, '_get_cookie_and_crumb',
                               return_value=(None, 'basic')):
            self.data.get('https://query1.finance.yahoo.com/v8/finance/chart/AAPL')
        self.assertEqual(self.data._session.proxies, proxy_map)


if __name__ == '__main__':
    unittest.main()

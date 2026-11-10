"""Offline regression tests for request-cache, cookie/crumb, session and
download() error-handling fixes. No network access needed."""
import unittest
import warnings
from unittest import mock

import pandas as pd

from tests.context import yfinance as yf
from yfinance import multi
from yfinance._http import new_session
from yfinance.config import YfConfig
from yfinance.data import SingletonMeta, YfData
from yfinance.scrapers.history import PriceHistory

CHART_URL = "https://query2.finance.yahoo.com/v8/finance/chart/AAPL"
MARKET_URL = "https://query1.finance.yahoo.com/v6/finance/quote/marketSummary"


def _mock_response(status_code=200, text="{}", url=CHART_URL):
    resp = mock.MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.content = text.encode() or b"<html></html>"
    resp.url = url
    resp.json.return_value = {}
    return resp


class _FreshYfData(unittest.TestCase):
    """YfData is a singleton: give each test its own instance and a fake
    network, so no state leaks between tests."""

    def setUp(self):
        SingletonMeta._instances.pop(YfData, None)
        self._ttl = YfConfig.network.cache_ttl
        self._no_persistent_cookie = mock.patch.object(
            YfData, '_load_cookie_curlCffi', return_value=False)
        self._no_persistent_cookie.start()
        self._no_cookie_save = mock.patch.object(
            YfData, '_save_cookie_curlCffi', return_value=True)
        self._no_cookie_save.start()
        self.data = YfData()
        self.requests = []
        self.status_for = {}  # url substring -> status code

        def fake_get(url=None, **kwargs):
            self.requests.append(url)
            if 'getcrumb' in url:
                return _mock_response(200, 'CRUMB', url=url)
            for part, code in self.status_for.items():
                if part in url:
                    return _mock_response(code, url=url)
            return _mock_response(200, url=url)

        self.data._session.get = mock.MagicMock(side_effect=fake_get)
        self.data._session.post = mock.MagicMock(side_effect=fake_get)

    def tearDown(self):
        YfConfig.network.cache_ttl = self._ttl
        self._no_persistent_cookie.stop()
        self._no_cookie_save.stop()
        SingletonMeta._instances.pop(YfData, None)

    def target_requests(self, part):
        return [u for u in self.requests if part in u]


class TestCacheExpiry(_FreshYfData):
    """Bug 1: cache_get() used to cache responses forever."""

    def test_repeat_within_ttl_uses_cache(self):
        YfConfig.network.cache_ttl = 60
        self.data.cache_get(MARKET_URL, params={'market': 'US'})
        self.data.cache_get(MARKET_URL, params={'market': 'US'})
        self.assertEqual(len(self.target_requests('marketSummary')), 1)

    def test_expired_entry_is_refetched(self):
        YfConfig.network.cache_ttl = 60
        with mock.patch('yfinance.data._time.monotonic', return_value=1000.0):
            self.data.cache_get(MARKET_URL, params={'market': 'US'})
        with mock.patch('yfinance.data._time.monotonic', return_value=1061.0):
            self.data.cache_get(MARKET_URL, params={'market': 'US'})
        self.assertEqual(len(self.target_requests('marketSummary')), 2)

    def test_error_responses_are_not_cached(self):
        self.status_for['marketSummary'] = 500
        self.data.cache_get(MARKET_URL)
        self.data.cache_get(MARKET_URL)
        # Each call reaches the network (500 also triggers one auth retry).
        self.assertEqual(len(self.target_requests('marketSummary')), 4)

    def test_ttl_zero_disables_cache(self):
        YfConfig.network.cache_ttl = 0
        self.data.cache_get(MARKET_URL)
        self.data.cache_get(MARKET_URL)
        self.assertEqual(len(self.target_requests('marketSummary')), 2)

    def test_ttl_none_never_expires(self):
        YfConfig.network.cache_ttl = None
        with mock.patch('yfinance.data._time.monotonic', return_value=0.0):
            self.data.cache_get(MARKET_URL)
        with mock.patch('yfinance.data._time.monotonic', return_value=1e9):
            self.data.cache_get(MARKET_URL)
        self.assertEqual(len(self.target_requests('marketSummary')), 1)

    def test_cache_clear(self):
        self.data.cache_get(MARKET_URL)
        self.data.cache_clear()
        self.data.cache_get(MARKET_URL)
        self.assertEqual(len(self.target_requests('marketSummary')), 2)

    def test_cache_size_is_bounded(self):
        YfConfig.network.cache_ttl = None
        for i in range(100):
            self.data.cache_get(MARKET_URL, params={'i': i})
        self.assertLessEqual(len(self.data._response_cache), 64)

    def test_default_ttl_is_set(self):
        self.assertEqual(YfConfig.network.cache_ttl, 60)


class TestNotFoundKeepsAuth(_FreshYfData):
    """Bug 2: a 404 (bad/delisted ticker) used to flip cookie strategy and
    wipe the crumb shared by every thread."""

    def test_404_does_not_toggle_strategy_or_drop_crumb(self):
        self.data.get(CHART_URL)  # warm up cookie + crumb
        strategy, crumb = self.data._cookie_strategy, self.data._crumb
        self.requests.clear()

        self.status_for['BADTICKER'] = 404
        resp = self.data.get("https://query2.finance.yahoo.com/v8/finance/chart/BADTICKER")

        self.assertEqual(resp.status_code, 404)
        self.assertEqual(self.data._cookie_strategy, strategy)
        self.assertEqual(self.data._crumb, crumb)
        self.assertEqual(self.requests, ["https://query2.finance.yahoo.com/v8/finance/chart/BADTICKER"])

    def test_401_still_retries_with_fresh_auth(self):
        self.data.get(CHART_URL)
        self.requests.clear()

        self.status_for['quoteSummary'] = 401
        self.data.get("https://query2.finance.yahoo.com/v10/finance/quoteSummary/AAPL")

        # Auth was reset and re-fetched, then the request retried once.
        self.assertTrue(self.target_requests('getcrumb'))
        self.assertEqual(len(self.target_requests('quoteSummary')), 2)


class TestCookieReuse(_FreshYfData):
    """Bug 6 (found while testing): with the default 'basic' strategy the
    fetched cookie was never marked, so every request re-hit fc.yahoo.com
    whenever the persistent cookie cache couldn't be used."""

    def test_cookie_fetched_once_across_requests(self):
        for _ in range(3):
            self.data.get(CHART_URL)
        self.assertEqual(len(self.target_requests('fc.yahoo.com')), 1)
        self.assertEqual(len(self.target_requests('getcrumb')), 1)


def _fake_download_one(ctx, ticker, *args, **kwargs):
    with ctx.lock:
        ctx.dfs[ticker.upper()] = pd.DataFrame(
            {'Close': [1.0]}, index=pd.DatetimeIndex(['2024-01-02']))


class TestSessionSwap(_FreshYfData):
    """Bug 4: download() replaced the shared session on every call and the
    old crumb was kept with the new (empty) cookie jar."""

    def test_new_session_resets_cookie_and_crumb(self):
        self.data._cookie, self.data._crumb = True, 'OLD'
        new = new_session()
        YfData(session=new)
        self.assertIs(self.data._session, new)
        self.assertIsNone(self.data._crumb)
        self.assertIsNone(self.data._cookie)

    def test_same_session_keeps_crumb(self):
        self.data._crumb = 'KEEP'
        YfData(session=self.data._session)
        self.assertEqual(self.data._crumb, 'KEEP')

    def test_download_without_session_keeps_shared_session(self):
        session_before = self.data._session
        with mock.patch.object(multi, '_download_one', side_effect=_fake_download_one):
            yf.download('AAPL', threads=False, progress=False)
        self.assertIs(self.data._session, session_before)

    def test_download_with_session_uses_it(self):
        mine = new_session()
        with mock.patch.object(multi, '_download_one', side_effect=_fake_download_one):
            yf.download('AAPL', threads=False, progress=False, session=mine)
        self.assertIs(self.data._session, mine)


class _FakePriceHistory:
    def __init__(self, error=None, last_error=None):
        self.error = error
        self._last_error = last_error
        self.calls = []

    def _history(self, **kwargs):
        self.calls.append((kwargs, YfConfig.debug.hide_exceptions))
        if self.error:
            raise self.error
        return pd.DataFrame({'Close': [1.0]},
                            index=pd.DatetimeIndex(['2024-01-02'], tz='America/New_York'))


class TestDownloadErrors(unittest.TestCase):
    """Bug 3: download() toggled a config key nobody reads
    (network.hide_exceptions), so errors were never raised into its
    collector; fixing the key name alone would race between threads."""

    def setUp(self):
        self._hide = YfConfig.debug.hide_exceptions

    def tearDown(self):
        YfConfig.debug.hide_exceptions = self._hide

    def _run(self, fake):
        ctx = multi._DownloadCtx()
        with mock.patch('yfinance.base.TickerBase._lazy_load_price_history', return_value=fake):
            multi._download_one(ctx, 'AAPL', period='1mo')
        return ctx

    def test_requests_raise_without_touching_global_config(self):
        fake = _FakePriceHistory()
        self._run(fake)
        kwargs, hide_during_call = fake.calls[0]
        self.assertTrue(kwargs['raise_errors'])
        self.assertTrue(hide_during_call)  # global left alone during the call
        self.assertTrue(YfConfig.debug.hide_exceptions)
        self.assertNotIn('hide_exceptions', YfConfig.network.data)

    def test_exception_is_collected_with_traceback(self):
        ctx = self._run(_FakePriceHistory(error=ValueError('boom')))
        self.assertIn('boom', ctx.errors['AAPL'])
        self.assertIn('ValueError', ctx.tracebacks['AAPL'])
        self.assertTrue(ctx.dfs['AAPL'].empty)

    def test_clean_message_preferred_over_repr(self):
        fake = _FakePriceHistory(error=yf.exceptions.YFTzMissingError('$XYZ'),
                                 last_error='possibly delisted; no timezone found')
        ctx = self._run(fake)
        self.assertEqual(ctx.errors['AAPL'], 'possibly delisted; no timezone found')

    def test_download_logs_failures_once(self):
        fake = _FakePriceHistory(error=ValueError('boom'))
        with mock.patch('yfinance.base.TickerBase._lazy_load_price_history', return_value=fake), \
                self.assertLogs('yfinance', level='ERROR') as logs:
            df = yf.download('AAPL', threads=False, progress=False)
        self.assertTrue(df.empty or df.isna().all().all())
        self.assertTrue(any('boom' in m for m in logs.output))

    def test_public_history_still_warns_on_raise_errors(self):
        ph = PriceHistory.__new__(PriceHistory)
        with mock.patch.object(PriceHistory, '_history', return_value='ok') as impl, \
                warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            self.assertEqual(ph.history(raise_errors=True), 'ok')
        self.assertTrue(any(issubclass(w.category, DeprecationWarning) for w in caught))
        self.assertTrue(impl.call_args.kwargs['raise_errors'])


class TestTickersDownload(unittest.TestCase):
    """Bug 5: Tickers.download() stored results on an attribute nothing
    reads (and its {} fallback would crash)."""

    def _fake_download(self, symbols, **kwargs):
        idx = pd.DatetimeIndex(['2024-01-02'])
        cols = pd.MultiIndex.from_product([symbols, ['Close', 'Open']], names=['Ticker', 'Price'])
        return pd.DataFrame([[1.0, 2.0] * len(symbols)], index=idx, columns=cols)

    def test_group_by_column_and_ticker(self):
        with mock.patch.object(multi, 'download', side_effect=self._fake_download):
            t = yf.Tickers('AAPL MSFT')
            by_col = t.download(group_by='column', progress=False)
            by_tkr = t.download(group_by='ticker', progress=False)
        self.assertEqual(list(by_col.columns.get_level_values(0).unique()), ['Close', 'Open'])
        self.assertEqual(sorted(by_tkr.columns.get_level_values(0).unique()), ['AAPL', 'MSFT'])
        for tkr in t.tickers.values():
            self.assertFalse(hasattr(tkr, '_history'))


if __name__ == '__main__':
    unittest.main()

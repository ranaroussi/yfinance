"""
Tests for cache

To run all tests in suite from commandline:
   python -m unittest tests.cache

Specific test class:
   python -m unittest tests.cache.TestCache

"""
from tests.context import yfinance as yf

import unittest
import tempfile
import os


class TestCache(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original_cache_dir = yf.cache._TzDBManager.get_location()
        cls.tempCacheDir = tempfile.TemporaryDirectory()
        yf.set_tz_cache_location(cls.tempCacheDir.name)

    @classmethod
    def tearDownClass(cls):
        yf.cache._TzDBManager.close_db()
        yf.cache._TzCacheManager._tz_cache = None
        yf.cache._CookieCacheManager._Cookie_cache = None
        yf.cache._ISINCacheManager._isin_cache = None
        cls.tempCacheDir.cleanup()
        yf.set_tz_cache_location(cls.original_cache_dir)

    def test_storeTzNoRaise(self):
        # storing TZ to cache should never raise exception
        tkr = 'AMZN'
        tz1 = "America/New_York"
        tz2 = "London/Europe"
        cache = yf.cache.get_tz_cache()
        cache.store(tkr, tz1)
        cache.store(tkr, tz2)

    def test_setTzCacheLocation(self):
        self.assertEqual(yf.cache._TzDBManager.get_location(), self.tempCacheDir.name)

        tkr = 'AMZN'
        tz1 = "America/New_York"
        cache = yf.cache.get_tz_cache()
        cache.store(tkr, tz1)

        self.assertTrue(os.path.exists(os.path.join(self.tempCacheDir.name, "tkr-tz.db")))


class TestCookieCache(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original_cache_dir = yf.cache._CookieDBManager.get_location()
        cls.tempCacheDir = tempfile.TemporaryDirectory()
        yf.cache._CookieDBManager.close_db()
        yf.cache._CookieCacheManager._Cookie_cache = None
        yf.cache._CookieDBManager.set_location(cls.tempCacheDir.name)

    @classmethod
    def tearDownClass(cls):
        yf.cache._CookieDBManager.close_db()
        yf.cache._CookieCacheManager._Cookie_cache = None
        cls.tempCacheDir.cleanup()
        yf.cache._CookieDBManager.set_location(cls.original_cache_dir)

    @staticmethod
    def _make_cookies():
        from http.cookiejar import Cookie
        c = Cookie(version=0, name='A3', value='abc', port=None, port_specified=False,
                   domain='.yahoo.com', domain_specified=True, domain_initial_dot=True,
                   path='/', path_specified=True, secure=True, expires=4102444800,
                   discard=False, comment=None, comment_url=None,
                   rest={'HttpOnly': None, 'SameSite': 'None'}, rfc2109=False)
        return {'.yahoo.com': {'/': {'A3': c}}}

    def _write_raw(self, strategy, blob):
        cache = yf.cache.get_cookie_cache()
        cache.initialise()
        yf.cache._CookieSchema.delete().where(yf.cache._CookieSchema.strategy == strategy).execute()
        yf.cache._CookieSchema.insert(strategy=strategy, cookie_bytes=blob).execute()
        return cache

    def test_json_roundtrip(self):
        cache = yf.cache.get_cookie_cache()
        cache.store('test-json', self._make_cookies())
        c = cache.lookup('test-json')['cookie']['.yahoo.com']['/']['A3']
        self.assertEqual((c.value, c.expires, c.get_nonstandard_attr('SameSite')),
                         ('abc', 4102444800, 'None'))

    @staticmethod
    def _raw(strategy):
        row = yf.cache._CookieSchema.get_or_none(yf.cache._CookieSchema.strategy == strategy)
        return None if row is None else bytes(row.cookie_bytes)

    def test_reads_legacy_pickle_without_rewriting(self):
        import pickle
        for proto in range(pickle.HIGHEST_PROTOCOL + 1):
            strategy = f'test-legacy-{proto}'
            blob = pickle.dumps(self._make_cookies(), proto)
            cache = self._write_raw(strategy, blob)
            res = cache.lookup(strategy)
            self.assertIsNotNone(res, f"protocol {proto}")
            self.assertEqual(res['cookie']['.yahoo.com']['/']['A3'].value, 'abc')
            # yfinance <= 1.8.0 reads this row with pickle.loads: it must stay as it was
            self.assertEqual(self._raw(strategy), blob)

    def test_store_keeps_legacy_row_readable_by_old_versions(self):
        # What yfinance <= 1.8.0 does on the same cache: pickle.loads of the row
        import pickle
        strategy = 'test-shared'
        cache = self._write_raw(strategy, pickle.dumps(self._make_cookies()))
        new = self._make_cookies()
        new['.yahoo.com']['/']['A3'].value = 'new'
        cache.store(strategy, new)
        self.assertEqual(cache.lookup(strategy)['cookie']['.yahoo.com']['/']['A3'].value, 'new')
        old = pickle.loads(self._raw(strategy))
        self.assertEqual(old['.yahoo.com']['/']['A3'].value, 'abc')

    def test_moves_json_out_of_legacy_row(self):
        # Dev builds briefly stored JSON under the legacy key, which old versions cannot read
        strategy = 'test-dev-json'
        cache = self._write_raw(strategy, yf.cache._cookies_to_json(self._make_cookies()))
        res = cache.lookup(strategy)
        self.assertEqual(res['cookie']['.yahoo.com']['/']['A3'].value, 'abc')
        self.assertIsNone(self._raw(strategy))
        self.assertIsNotNone(self._raw(strategy + yf.cache._COOKIE_JSON_SUFFIX))
        self.assertEqual(cache.lookup(strategy)['cookie']['.yahoo.com']['/']['A3'].value, 'abc')

    def test_moved_json_keeps_its_age(self):
        import datetime
        strategy = 'test-dev-json-age'
        cache = self._write_raw(strategy, yf.cache._cookies_to_json(self._make_cookies()))
        old = datetime.datetime.now() - datetime.timedelta(days=3)
        yf.cache._CookieSchema.update(fetch_date=old).where(
            yf.cache._CookieSchema.strategy == strategy).execute()
        self.assertGreater(cache.lookup(strategy)['age'], datetime.timedelta(days=2))
        # second lookup reads the moved row
        self.assertGreater(cache.lookup(strategy)['age'], datetime.timedelta(days=2))

    def test_store_does_not_create_legacy_row(self):
        strategy = 'test-no-legacy'
        cache = yf.cache.get_cookie_cache()
        cache.store(strategy, self._make_cookies())
        self.assertIsNone(self._raw(strategy))
        self.assertIsNotNone(self._raw(strategy + yf.cache._COOKIE_JSON_SUFFIX))

    def test_unreadable_json_row_falls_back_to_legacy_row(self):
        import pickle
        strategy = 'test-bad-json'
        self._write_raw(strategy + yf.cache._COOKIE_JSON_SUFFIX, b'\x00not a cookie')
        cache = self._write_raw(strategy, pickle.dumps(self._make_cookies()))
        self.assertEqual(cache.lookup(strategy)['cookie']['.yahoo.com']['/']['A3'].value, 'abc')

    def test_data_layer_leaves_legacy_cookie_for_old_versions(self):
        # The path a request takes: YfData loads the cookie an old version cached,
        # saves its own, and the old version's row is still a pickle afterwards.
        import pickle
        import time
        from yfinance.data import SingletonMeta, YfData
        cookies = self._make_cookies()
        cookies['.yahoo.com']['/']['A3'].expires = int(time.time()) + 86400
        blob = pickle.dumps(cookies)
        self._write_raw('curlCffi', blob)
        yf.cache._CookieSchema.delete().where(
            yf.cache._CookieSchema.strategy == 'curlCffi' + yf.cache._COOKIE_JSON_SUFFIX).execute()
        SingletonMeta._instances.pop(YfData, None)
        try:
            data = YfData()
            data._cookie = None
            self.assertTrue(data._load_cookie_curlCffi())
            self.assertEqual(data._cookie.value, 'abc')
            self.assertTrue(data._save_cookie_curlCffi())
        finally:
            SingletonMeta._instances.pop(YfData, None)
        self.assertEqual(self._raw('curlCffi'), blob)
        self.assertEqual(pickle.loads(self._raw('curlCffi'))['.yahoo.com']['/']['A3'].value, 'abc')
        self.assertIsNotNone(self._raw('curlCffi' + yf.cache._COOKIE_JSON_SUFFIX))

    def test_rejects_pickle_with_other_globals(self):
        import pickle

        class NotACookie:
            def __reduce__(self):
                return (print, ('should never run',))

        cache = self._write_raw('test-forbidden', pickle.dumps({'x': {'/': {'A3': NotACookie()}}}))
        self.assertIsNone(cache.lookup('test-forbidden'))

    def test_garbage_is_cache_miss(self):
        cache = self._write_raw('test-garbage', b'\x00not a cookie')
        self.assertIsNone(cache.lookup('test-garbage'))


if __name__ == '__main__':
    unittest.main()

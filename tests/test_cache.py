"""
Tests for cache

To run all tests in suite from commandline:
   python -m unittest tests.cache

Specific test class:
   python -m unittest tests.cache.TestCache

"""
from tests.context import yfinance as yf

import gc
import threading
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

    def test_migrates_legacy_pickle(self):
        import pickle
        for proto in range(pickle.HIGHEST_PROTOCOL + 1):
            strategy = f'test-legacy-{proto}'
            cache = self._write_raw(strategy, pickle.dumps(self._make_cookies(), proto))
            res = cache.lookup(strategy)
            self.assertIsNotNone(res, f"protocol {proto}")
            self.assertEqual(res['cookie']['.yahoo.com']['/']['A3'].value, 'abc')
            # entry was rewritten as JSON
            row = yf.cache._CookieSchema.get(yf.cache._CookieSchema.strategy == strategy)
            self.assertIsInstance(yf.cache._json.loads(bytes(row.cookie_bytes)), dict)

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


@unittest.skipUnless(os.path.isdir("/proc/self/fd"), "counts open files through /proc")
class TestCacheThreadConnections(unittest.TestCase):
    """Each thread gets its own peewee connection: a cache call must close it, or
    the threads of a threaded download leave one open file per cache behind."""

    managers = [
        (yf.cache._TzDBManager, yf.cache._TzCacheManager, "_tz_cache"),
        (yf.cache._CookieDBManager, yf.cache._CookieCacheManager, "_Cookie_cache"),
        (yf.cache._ISINDBManager, yf.cache._ISINCacheManager, "_isin_cache"),
    ]

    @classmethod
    def setUpClass(cls):
        cls.tempCacheDir = tempfile.TemporaryDirectory()
        cls.original_dirs = [db.get_location() for db, _, _ in cls.managers]
        for db, manager, attr in cls.managers:
            db.close_db()
            setattr(manager, attr, None)
            db.set_location(cls.tempCacheDir.name)

    @classmethod
    def tearDownClass(cls):
        for (db, manager, attr), original in zip(cls.managers, cls.original_dirs):
            db.close_db()
            setattr(manager, attr, None)
            db.set_location(original)
        cls.tempCacheDir.cleanup()

    @staticmethod
    def _open_files():
        return len(os.listdir("/proc/self/fd"))

    @staticmethod
    def _use_caches():
        yf.cache.get_tz_cache().store("AMZN", "America/New_York")
        yf.cache.get_tz_cache().lookup("AMZN")
        yf.cache.get_cookie_cache().lookup("basic")
        yf.cache.get_isin_cache().lookup("US0231351067")

    def test_threads_leave_no_connection_open(self):
        self._use_caches()
        gc.disable()
        try:
            before = self._open_files()
            for _ in range(50):
                thread = threading.Thread(target=self._use_caches)
                thread.start()
                thread.join()
            leaked = self._open_files() - before
        finally:
            gc.enable()
        self.assertLess(leaked, 10)

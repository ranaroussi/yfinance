from typing import Dict, Union

import pandas as _pd
import os as _os
import appdirs as _ad
import sqlite3 as _sqlite3
import atexit as _atexit

from threading import Lock

class _KVStore:
    """Simpel Sqlite backed key/value store, key and value are strings. Should be thread safe."""

    def __init__(self, filename):
        self._cache_mutex = Lock()
        with self._cache_mutex:
            self.conn = _sqlite3.connect(filename, timeout=10, check_same_thread=False)
            self.conn.execute('pragma journal_mode=wal')
            self.conn.execute('create table if not exists "kv" (key TEXT primary key, value TEXT) without rowid')
            self.conn.commit()
        _atexit.register(self.close)

    def close(self):
        if self.conn is not None:
            with self._cache_mutex:
                self.conn.close()
                self.conn = None

    def get(self, key: str) -> Union[str, None]:
        """Get value for key if it exists else returns None"""
        item = self.conn.execute('select value from "kv" where key=?', (key,))
        if item:
            return next(item, (None,))[0]

    def set(self, key: str, value: str) -> None:
        with self._cache_mutex:
            self.conn.execute('replace into "kv" (key, value) values (?,?)', (key, value))
            self.conn.commit()

    def bulk_set(self, kvdata: Dict[str, str]):
        records = tuple(i for i in kvdata.items())
        with self._cache_mutex:
            self.conn.executemany('replace into "kv" (key, value) values (?,?)', records)
            self.conn.commit()

    def delete(self, key: str):
        with self._cache_mutex:
            self.conn.execute('delete from "kv" where key=?', (key,))
            self.conn.commit()


class _TzCacheException(Exception):
    pass


class _TzCache:
    """Simple sqlite file cache of ticker->timezone"""

    def __init__(self):
        self._tz_db = None
        self._setup_cache_folder()

    def _setup_cache_folder(self):
        if not _os.path.isdir(self._db_dir):
            try:
                _os.makedirs(self._db_dir)
            except OSError as err:
                raise _TzCacheException("Error creating TzCache folder: '{}' reason: {}"
                                        .format(self._db_dir, err))

        elif not (_os.access(self._db_dir, _os.R_OK) and _os.access(self._db_dir, _os.W_OK)):
            raise _TzCacheException("Cannot read and write in TzCache folder: '{}'"
                                    .format(self._db_dir, ))

    def lookup(self, tkr):
        return self.tz_db.get(tkr)

    def store(self, tkr, tz):
        if tz is None:
            self.tz_db.delete(tkr)
        elif self.tz_db.get(tkr) is not None:
            raise Exception("Tkr {} tz already in cache".format(tkr))
        else:
            self.tz_db.set(tkr, tz)

    @property
    def _db_dir(self):
        global _cache_dir
        return _os.path.join(_cache_dir, "py-yfinance")

    @property
    def tz_db(self):
        # lazy init
        if self._tz_db is None:
            self._tz_db = _KVStore(_os.path.join(self._db_dir, "tkr-tz.db"))
            self._migrate_cache_tkr_tz()

        return self._tz_db

    def _migrate_cache_tkr_tz(self):
        """Migrate contents from old ticker CSV-cache to SQLite db"""
        old_cache_file_path = _os.path.join(self._db_dir, "tkr-tz.csv")

        if not _os.path.isfile(old_cache_file_path):
            return None
        try:
            df = _pd.read_csv(old_cache_file_path, index_col="Ticker")
        except _pd.errors.EmptyDataError:
            _os.remove(old_cache_file_path)
        else:
            self.tz_db.bulk_set(df.to_dict()['Tz'])
            _os.remove(old_cache_file_path)


class _TzCacheDummy:
    """Dummy cache to use if tz cache is disabled"""

    def lookup(self, tkr):
        return None

    def store(self, tkr, tz):
        pass

    @property
    def tz_db(self):
        return None


def get_tz_cache():
    """
    Get the timezone cache, initializes it and creates cache folder if needed on first call.
    If folder cannot be created for some reason it will fall back to initialize a
    dummy cache with same interface as real cash.
    """
    # as this can be called from multiple threads, protect it.
    with _cache_init_lock:
        global _tz_cache
        if _tz_cache is None:
            try:
                _tz_cache = _TzCache()
            except _TzCacheException as err:
                print("Failed to create TzCache, reason: {}".format(err))
                print("TzCache will not be used.")
                print("Tip: You can direct cache to use a different location with 'set_tz_cache_location(mylocation)'")
                _tz_cache = _TzCacheDummy()

        return _tz_cache


_cache_dir = _ad.user_cache_dir()
_cache_init_lock = Lock()
_tz_cache = None


def set_tz_cache_location(cache_dir: str):
    """
    Sets the path to create the "py-yfinance" cache folder in.
    Useful if the default folder returned by "appdir.user_cache_dir()" is not writable.
    Must be called before cache is used (that is, before fetching tickers).
    :param cache_dir: Path to use for caches
    :return: None
    """
    global _cache_dir, _tz_cache
    assert _tz_cache is None, "Time Zone cache already initialized, setting path must be done before cache is created"
    _cache_dir = cache_dir

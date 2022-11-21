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
            self.conn.commit()
        _atexit.register(self.close)

    def close(self):
        if self.conn is not None:
            with self._cache_mutex:
                self.conn.close()
                self.conn = None

    def table_exists(self, name) -> bool:
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        results = cursor.fetchall()
        return len(results)>0 and name in results[0]

    def get(self, tkr, key: str) -> Union[str, None]:
        """Get value for key if it exists else returns None"""
        if not self.table_exists(key):
            return None
        item = self.conn.execute('select value from '+key+' where Ticker=?', (tkr,))
        if item:
            return next(item, (None,))[0]

    def set(self, tkr, key: str, value: str) -> None:
        with self._cache_mutex:
            if not self.table_exists(key):
                self.conn.execute('create table if not exists '+key+' (Ticker TEXT primary key, Value TEXT) without rowid')
            self.conn.execute('replace into '+key+' (Ticker, Value) values (?,?)', (tkr, value))
            self.conn.commit()

    def bulk_set(self, key, tkrData: Dict[str, str]):
        records = tuple(i for i in tkrData.items())
        with self._cache_mutex:
            if not self.table_exists(key):
                print("Creating table", key)
                self.conn.execute('create table if not exists '+key+' (Ticker TEXT primary key, Value TEXT) without rowid')
            self.conn.executemany('replace into '+key+' (Ticker, Value) values (?,?)', records)
            self.conn.commit()

    def delete(self, tkr, key: str):
        if not self.table_exists(key):
            return
        with self._cache_mutex:
            self.conn.execute('delete from '+key+' where Ticker=?', (tkr,))
            self.conn.commit()

    def migrate_old_schema(self):
        with self._cache_mutex:
            if not self.table_exists("kv"):
                return
            self.conn.execute("ALTER TABLE `kv` RENAME TO `Timezone`")
            self.conn.execute("ALTER TABLE `Timezone` RENAME COLUMN key TO Ticker;")
            self.conn.execute("ALTER TABLE `Timezone` RENAME COLUMN value TO Value;")
            self.conn.commit()


class _DbCacheException(Exception):
    pass


class _DbCache:
    """Simple sqlite file cache of key->value"""

    def __init__(self):
        self._db = None
        self.fp = _os.path.join(self._db_dir, "tkr-keys.db")

        self._setup_cache_folder()
        self._migrate_old_schema()

    def _setup_cache_folder(self):
        if not _os.path.isdir(self._db_dir):
            try:
                _os.makedirs(self._db_dir)
            except OSError as err:
                raise _DbCacheException("Error creating DbCache folder: '{}' reason: {}"
                                        .format(self._db_dir, err))

        elif not (_os.access(self._db_dir, _os.R_OK) and _os.access(self._db_dir, _os.W_OK)):
            raise _DbCacheException("Cannot read and write in DbCache folder: '{}'"
                                    .format(self._db_dir, ))

    def lookup(self, tkr, key):
        return self.db.get(tkr, key)

    def store(self, tkr, key, value):
        if value is None:
            self.db.delete(tkr, key)
        elif self.db.get(tkr, key) is not None:
            raise Exception("Tkr {} {} already in cache".format(tkr, key))
        else:
            self.db.set(tkr, key, value)

    @property
    def _db_dir(self):
        global _cache_dir
        return _os.path.join(_cache_dir, "py-yfinance")

    @property
    def db(self):
        # lazy init
        if self._db is None:
            self._db = _KVStore(self.fp)
            self._migrate_cache_tkr_tz()

        return self._db

    def _migrate_old_schema(self):
        old_fp = _os.path.join(self._db_dir, "tkr-tz.db")
        if _os.path.isfile(old_fp):
            _db = _KVStore(old_fp)
            _db.migrate_old_schema()
            _db.close() ; _db = None
            _os.rename(old_fp, self.fp)

    def _migrate_cache_tkr_tz(self):
        """Migrate contents from old ticker CSV-cache to SQLite db"""
        csv_cache_fp = _os.path.join(self._db_dir, "tkr-tz.csv")

        if not _os.path.isfile(csv_cache_fp):
            return None
        try:
            df = _pd.read_csv(csv_cache_fp, index_col="Ticker")
        except _pd.errors.EmptyDataError:
            _os.remove(csv_cache_fp)
        else:
            self.db.bulk_set("Timezone", df.to_dict()['Tz'])
            _os.remove(csv_cache_fp)



class _DbCacheDummy:
    """Dummy cache to use if db cache is disabled"""

    def lookup(self, tkr, key):
        return None

    def store(self, tkr, key, value):
        pass

    @property
    def db(self):
        return None


def get_db_cache():
    """
    Get the database cache, initializes it and creates cache folder if needed on first call.
    If folder cannot be created for some reason it will fall back to initialize a
    dummy cache with same interface as real cash.
    """
    # as this can be called from multiple threads, protect it.
    with _cache_init_lock:
        global _db_cache
        if _db_cache is None:
            try:
                _db_cache = _DbCache()
            except _DbCacheException as err:
                print("Failed to create DbCache, reason: {}".format(err))
                print("DbCache will not be used.")
                print("Tip: You can direct cache to use a different location with 'set_db_cache_location(mylocation)'")
                _db_cache = _DbCacheDummy()

        return _db_cache


_cache_dir = _ad.user_cache_dir()
_cache_init_lock = Lock()
_db_cache = None


def set_db_cache_location(cache_dir: str):
    """
    Sets the path to create the "py-yfinance" cache folder in.
    Useful if the default folder returned by "appdir.user_cache_dir()" is not writable.
    Must be called before cache is used (that is, before fetching tickers).
    :param cache_dir: Path to use for caches
    :return: None
    """
    global _cache_dir, _db_cache
    assert _db_cache is None, "DB cache already initialized, setting path must be done before cache is created"
    _cache_dir = cache_dir

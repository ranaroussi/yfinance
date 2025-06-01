import peewee as _peewee
from threading import Lock
import os as _os
import platformdirs as _ad
import atexit as _atexit
import datetime as _datetime
import pickle as _pkl

from .utils import get_yf_logger

_cache_init_lock = Lock()

# --------------
# TimeZone cache
# --------------

class _TzCacheException(Exception):
    pass


class _TzCacheDummy:
    """Dummy cache to use if tz cache is disabled"""

    def lookup(self, tkr):
        return None

    def store(self, tkr, tz):
        pass

    @property
    def tz_db(self):
        return None


class _TzCacheManager:
    _tz_cache = None

    @classmethod
    def get_tz_cache(cls):
        if cls._tz_cache is None:
            with _cache_init_lock:
                cls._initialise()
        return cls._tz_cache

    @classmethod
    def _initialise(cls, cache_dir=None):
        cls._tz_cache = _TzCache()


class _TzDBManager:
    _db = None
    _cache_dir = _os.path.join(_ad.user_cache_dir(), "py-yfinance")

    @classmethod
    def get_database(cls):
        if cls._db is None:
            cls._initialise()
        return cls._db

    @classmethod
    def close_db(cls):
        if cls._db is not None:
            try:
                cls._db.close()
            except Exception:
                # Must discard exceptions because Python trying to quit.
                pass


    @classmethod
    def _initialise(cls, cache_dir=None):
        if cache_dir is not None:
            cls._cache_dir = cache_dir

        if not _os.path.isdir(cls._cache_dir):
            try:
                _os.makedirs(cls._cache_dir)
            except OSError as err:
                raise _TzCacheException(f"Error creating TzCache folder: '{cls._cache_dir}' reason: {err}")
        elif not (_os.access(cls._cache_dir, _os.R_OK) and _os.access(cls._cache_dir, _os.W_OK)):
            raise _TzCacheException(f"Cannot read and write in TzCache folder: '{cls._cache_dir}'")

        cls._db = _peewee.SqliteDatabase(
            _os.path.join(cls._cache_dir, 'tkr-tz.db'),
            pragmas={'journal_mode': 'wal', 'cache_size': -64}
        )

        old_cache_file_path = _os.path.join(cls._cache_dir, "tkr-tz.csv")
        if _os.path.isfile(old_cache_file_path):
            _os.remove(old_cache_file_path)

    @classmethod
    def set_location(cls, new_cache_dir):
        if cls._db is not None:
            cls._db.close()
            cls._db = None
        cls._cache_dir = new_cache_dir

    @classmethod
    def get_location(cls):
        return cls._cache_dir

# close DB when Python exists
_atexit.register(_TzDBManager.close_db)


tz_db_proxy = _peewee.Proxy()
class _KV(_peewee.Model):
    key = _peewee.CharField(primary_key=True)
    value = _peewee.CharField(null=True)
    
    class Meta:
        database = tz_db_proxy
        without_rowid = True


class _TzCache:
    def __init__(self):
        self.initialised = -1
        self.db = None
        self.dummy = False

    def get_db(self):
        if self.db is not None:
            return self.db

        try:
            self.db = _TzDBManager.get_database()
        except _TzCacheException as err:
            get_yf_logger().info(f"Failed to create TzCache, reason: {err}. "
                                 "TzCache will not be used. "
                                 "Tip: You can direct cache to use a different location with 'set_tz_cache_location(mylocation)'")
            self.dummy = True
            return None
        return self.db

    def initialise(self):
        if self.initialised != -1:
            return

        db = self.get_db()
        if db is None:
            self.initialised = 0  # failure
            return

        db.connect()
        tz_db_proxy.initialize(db)
        try:
            db.create_tables([_KV])
        except _peewee.OperationalError as e:
            if 'WITHOUT' in str(e):
                _KV._meta.without_rowid = False
                db.create_tables([_KV])
            else:
                raise
        self.initialised = 1  # success

    def lookup(self, key):
        if self.dummy:
            return None

        if self.initialised == -1:
            self.initialise()

        if self.initialised == 0:  # failure
            return None

        try:
            return _KV.get(_KV.key == key).value
        except _KV.DoesNotExist:
            return None

    def store(self, key, value):
        if self.dummy:
            return

        if self.initialised == -1:
            self.initialise()

        if self.initialised == 0:  # failure
            return

        db = self.get_db()
        if db is None:
            return
        try:
            if value is None:
                q = _KV.delete().where(_KV.key == key)
                q.execute()
                return
            with db.atomic():
                _KV.insert(key=key, value=value).execute()
        except _peewee.IntegrityError:
            # Integrity error means the key already exists. Try updating the key.
            old_value = self.lookup(key)
            if old_value != value:
                get_yf_logger().debug(f"Value for key {key} changed from {old_value} to {value}.")
                with db.atomic():
                    q = _KV.update(value=value).where(_KV.key == key)
                    q.execute()


def get_tz_cache():
    return _TzCacheManager.get_tz_cache()



# --------------
# Cookie cache
# --------------

class _CookieCacheException(Exception):
    pass


class _CookieCacheDummy:
    """Dummy cache to use if Cookie cache is disabled"""

    def lookup(self, tkr):
        return None

    def store(self, tkr, Cookie):
        pass

    @property
    def Cookie_db(self):
        return None


class _CookieCacheManager:
    _Cookie_cache = None

    @classmethod
    def get_cookie_cache(cls):
        if cls._Cookie_cache is None:
            with _cache_init_lock:
                cls._initialise()
        return cls._Cookie_cache

    @classmethod
    def _initialise(cls, cache_dir=None):
        cls._Cookie_cache = _CookieCache()


class _CookieDBManager:
    _db = None
    _cache_dir = _os.path.join(_ad.user_cache_dir(), "py-yfinance")

    @classmethod
    def get_database(cls):
        if cls._db is None:
            cls._initialise()
        return cls._db

    @classmethod
    def close_db(cls):
        if cls._db is not None:
            try:
                cls._db.close()
            except Exception:
                # Must discard exceptions because Python trying to quit.
                pass


    @classmethod
    def _initialise(cls, cache_dir=None):
        if cache_dir is not None:
            cls._cache_dir = cache_dir

        if not _os.path.isdir(cls._cache_dir):
            try:
                _os.makedirs(cls._cache_dir)
            except OSError as err:
                raise _CookieCacheException(f"Error creating CookieCache folder: '{cls._cache_dir}' reason: {err}")
        elif not (_os.access(cls._cache_dir, _os.R_OK) and _os.access(cls._cache_dir, _os.W_OK)):
            raise _CookieCacheException(f"Cannot read and write in CookieCache folder: '{cls._cache_dir}'")

        cls._db = _peewee.SqliteDatabase(
            _os.path.join(cls._cache_dir, 'cookies.db'),
            pragmas={'journal_mode': 'wal', 'cache_size': -64}
        )

    @classmethod
    def set_location(cls, new_cache_dir):
        if cls._db is not None:
            cls._db.close()
            cls._db = None
        cls._cache_dir = new_cache_dir

    @classmethod
    def get_location(cls):
        return cls._cache_dir

# close DB when Python exists
_atexit.register(_CookieDBManager.close_db)


Cookie_db_proxy = _peewee.Proxy()
class ISODateTimeField(_peewee.DateTimeField):
    # Ensure Python datetime is read & written correctly for sqlite, 
    # because user discovered peewee allowed an invalid datetime
    # to get written.
    def db_value(self, value):
        if value and isinstance(value, _datetime.datetime):
            return value.isoformat()
        return super().db_value(value)
    def python_value(self, value):
        if value and isinstance(value, str) and 'T' in value:
            return _datetime.datetime.fromisoformat(value)
        return super().python_value(value)
class _CookieSchema(_peewee.Model):
    strategy = _peewee.CharField(primary_key=True)
    fetch_date = ISODateTimeField(default=_datetime.datetime.now)
    
    # Which cookie type depends on strategy
    cookie_bytes = _peewee.BlobField()

    class Meta:
        database = Cookie_db_proxy
        without_rowid = True


class _CookieCache:
    def __init__(self):
        self.initialised = -1
        self.db = None
        self.dummy = False

    def get_db(self):
        if self.db is not None:
            return self.db

        try:
            self.db = _CookieDBManager.get_database()
        except _CookieCacheException as err:
            get_yf_logger().info(f"Failed to create CookieCache, reason: {err}. "
                                 "CookieCache will not be used. "
                                 "Tip: You can direct cache to use a different location with 'set_tz_cache_location(mylocation)'")
            self.dummy = True
            return None
        return self.db

    def initialise(self):
        if self.initialised != -1:
            return

        db = self.get_db()
        if db is None:
            self.initialised = 0  # failure
            return

        db.connect()
        Cookie_db_proxy.initialize(db)
        try:
            db.create_tables([_CookieSchema])
        except _peewee.OperationalError as e:
            if 'WITHOUT' in str(e):
                _CookieSchema._meta.without_rowid = False
                db.create_tables([_CookieSchema])
            else:
                raise
        self.initialised = 1  # success

    def lookup(self, strategy):
        if self.dummy:
            return None

        if self.initialised == -1:
            self.initialise()

        if self.initialised == 0:  # failure
            return None

        try:
            row = _CookieSchema.get(_CookieSchema.strategy == strategy)
            cookie = _pkl.loads(row.cookie_bytes)
            return {'data':cookie, 'age':_datetime.datetime.now()-row.fetch_date}
        except _CookieSchema.DoesNotExist:
            return None

    def store(self, strategy, cookie):
        if self.dummy:
            return

        if self.initialised == -1:
            self.initialise()

        if self.initialised == 0:  # failure
            return

        db = self.get_db()
        if db is None:
            return
        try:
            q = _CookieSchema.delete().where(_CookieSchema.strategy == strategy)
            q.execute()
            if cookie is None:
                return
            with db.atomic():
                cookie_pkl = _pkl.dumps(cookie, _pkl.HIGHEST_PROTOCOL)
                _CookieSchema.insert(strategy=strategy, cookie_bytes=cookie_pkl).execute()
        except _peewee.IntegrityError:
            raise
            # # Integrity error means the strategy already exists. Try updating the strategy.
            # old_value = self.lookup(strategy)
            # if old_value != cookie:
            #     get_yf_logger().debug(f"cookie for strategy {strategy} changed from {old_value} to {cookie}.")
            #     with db.atomic():
            #         q = _CookieSchema.update(cookie=cookie).where(_CookieSchema.strategy == strategy)
            #         q.execute()


def get_cookie_cache():
    return _CookieCacheManager.get_cookie_cache()



def set_cache_location(cache_dir: str):
    """
    Sets the path to create the "py-yfinance" cache folder in.
    Useful if the default folder returned by "appdir.user_cache_dir()" is not writable.
    Must be called before cache is used (that is, before fetching tickers).
    :param cache_dir: Path to use for caches
    :return: None
    """
    _TzDBManager.set_location(cache_dir)
    _CookieDBManager.set_location(cache_dir)

def set_tz_cache_location(cache_dir: str):
    set_cache_location(cache_dir)


"""Persistent caches used by yfinance for timezone, cookie, and ISIN lookups."""

import atexit as _atexit
import datetime as _dt
import os as _os
import pickle as _pkl
import sqlite3 as _sqlite3
from threading import Lock
from typing import Optional

import peewee as _peewee
import platformdirs as _ad

from .utils import get_yf_logger

_cache_init_lock = Lock()
_DB_CLOSE_EXCEPTIONS = (_peewee.PeeweeException, _sqlite3.Error, OSError, ValueError)


# --------------
# TimeZone cache
# --------------


class _TzCacheException(Exception):
    """Raised when timezone cache initialization fails."""


class _TzCacheDummy:
    """Dummy cache to use if timezone cache is disabled."""

    def lookup(self, _tkr):
        """Return no value when timezone cache is disabled."""
        return None

    def store(self, _tkr, _tz):
        """Discard values when timezone cache is disabled."""

    @property
    def tz_db(self):
        """Return no backing database for the dummy cache."""
        return None


class _TzCacheManager:
    """Singleton manager for timezone cache access."""

    _tz_cache: Optional["_TzCache"] = None

    @classmethod
    def get_tz_cache(cls) -> "_TzCache":
        """Return the timezone cache singleton."""
        if cls._tz_cache is None:
            with _cache_init_lock:
                cls._initialise()
        if cls._tz_cache is None:
            raise _TzCacheException("Failed to initialize timezone cache")
        return cls._tz_cache

    @classmethod
    def reset_cache(cls) -> None:
        """Reset the timezone cache singleton."""
        cls._tz_cache = None

    @classmethod
    def _initialise(cls) -> None:
        """Instantiate the timezone cache singleton."""
        cls._tz_cache = _TzCache()


class _TzDBManager:
    """Manage the sqlite database used by the timezone cache."""

    _db: Optional[_peewee.SqliteDatabase] = None
    _cache_dir = _os.path.join(_ad.user_cache_dir(), "py-yfinance")

    @classmethod
    def get_database(cls):
        """Return the timezone sqlite database instance."""
        if cls._db is None:
            cls._initialise()
        return cls._db

    @classmethod
    def close_db(cls):
        """Close the timezone sqlite database if it is open."""
        if cls._db is not None:
            try:
                cls._db.close()
            except _DB_CLOSE_EXCEPTIONS:
                # Must discard exceptions because Python is trying to quit.
                pass

    @classmethod
    def _initialise(cls, cache_dir=None):
        """Initialize the timezone sqlite database."""
        if cache_dir is not None:
            cls._cache_dir = cache_dir

        if not _os.path.isdir(cls._cache_dir):
            try:
                _os.makedirs(cls._cache_dir)
            except OSError as err:
                raise _TzCacheException(
                    f"Error creating TzCache folder: '{cls._cache_dir}' reason: {err}"
                ) from err
        elif not (_os.access(cls._cache_dir, _os.R_OK) and _os.access(cls._cache_dir, _os.W_OK)):
            raise _TzCacheException(
                f"Cannot read and write in TzCache folder: '{cls._cache_dir}'"
            )

        cls._db = _peewee.SqliteDatabase(
            _os.path.join(cls._cache_dir, "tkr-tz.db"),
            pragmas={"journal_mode": "wal", "cache_size": -64},
        )

        old_cache_file_path = _os.path.join(cls._cache_dir, "tkr-tz.csv")
        if _os.path.isfile(old_cache_file_path):
            _os.remove(old_cache_file_path)

    @classmethod
    def set_location(cls, new_cache_dir):
        """Set the folder where timezone cache artifacts are stored."""
        if cls._db is not None:
            cls._db.close()
            cls._db = None
        cls._cache_dir = new_cache_dir

    @classmethod
    def get_location(cls):
        """Return the folder where timezone cache artifacts are stored."""
        return cls._cache_dir


# Close DB when Python exits.
_atexit.register(_TzDBManager.close_db)


TZ_DB_PROXY = _peewee.Proxy()


class _TzKv(_peewee.Model):
    """Timezone key-value record schema."""

    key = _peewee.CharField(primary_key=True)
    value = _peewee.CharField(null=True)

    @classmethod
    def key_field(cls):
        """Return the model field used as cache key."""
        return cls.key

    @classmethod
    def value_field(cls):
        """Return the model field used as cache value."""
        return cls.value

    class Meta:
        """Peewee metadata for timezone key-value table."""

        database = TZ_DB_PROXY
        without_rowid = True

        @classmethod
        def database_proxy(cls):
            """Return the proxy configured for this model."""
            return cls.database

        @classmethod
        def without_rowid_enabled(cls):
            """Return whether the table uses SQLite WITHOUT ROWID."""
            return cls.without_rowid


class _TzCache:
    """SQLite-backed timezone cache."""

    def __init__(self):
        """Build an uninitialized timezone cache."""
        self.initialised = -1
        self.db = None
        self.dummy = False

    def get_db(self):
        """Return the timezone sqlite database, if available."""
        if self.db is not None:
            return self.db

        try:
            self.db = _TzDBManager.get_database()
        except _TzCacheException as err:
            get_yf_logger().info(
                "Failed to create TzCache, reason: %s. "
                "TzCache will not be used. "
                "Tip: You can direct cache to use a different location with "
                "'set_tz_cache_location(mylocation)'",
                err,
            )
            self.dummy = True
            return None
        return self.db

    def initialise(self):
        """Initialize database tables for the timezone cache."""
        if self.initialised != -1:
            return

        db = self.get_db()
        if db is None:
            self.initialised = 0  # failure
            return

        db.connect()
        TZ_DB_PROXY.initialize(db)
        try:
            db.create_tables([_TzKv])
        except _peewee.OperationalError as err:
            if "WITHOUT" in str(err):
                meta = getattr(_TzKv, "_meta", None)
                if meta is not None:
                    meta.without_rowid = False
                db.create_tables([_TzKv])
            else:
                raise
        self.initialised = 1  # success

    def lookup(self, key):
        """Look up timezone by ticker key."""
        if self.dummy:
            return None

        if self.initialised == -1:
            self.initialise()

        if self.initialised == 0:  # failure
            return None

        try:
            return _TzKv.get(_TzKv.key == key).value
        except _peewee.DoesNotExist:
            return None

    def store(self, key, value):
        """Store or delete timezone value for a ticker key."""
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
                query = _TzKv.delete().where(_TzKv.key == key)
                query.execute(db)
                return
            with db.atomic():
                insert_query = _TzKv.insert(key=key, value=value)
                insert_query.execute(db)
        except _peewee.IntegrityError:
            # Integrity error means the key already exists. Try updating the key.
            old_value = self.lookup(key)
            if old_value != value:
                get_yf_logger().debug(
                    "Value for key %s changed from %s to %s.",
                    key,
                    old_value,
                    value,
                )
                with db.atomic():
                    query = _TzKv.update(value=value).where(_TzKv.key == key)
                    query.execute(db)


def get_tz_cache() -> "_TzCache":
    """Return the timezone cache singleton."""
    return _TzCacheManager.get_tz_cache()


# ------------
# Cookie cache
# ------------


class _CookieCacheException(Exception):
    """Raised when cookie cache initialization fails."""


class _CookieCacheDummy:
    """Dummy cache to use if cookie cache is disabled."""

    def lookup(self, _strategy):
        """Return no value when cookie cache is disabled."""
        return None

    def store(self, _strategy, _cookie):
        """Discard values when cookie cache is disabled."""

    @property
    def cookie_db(self):
        """Return no backing database for the dummy cache."""
        return None


class _CookieCacheManager:
    """Singleton manager for cookie cache access."""

    _cookie_cache: Optional["_CookieCache"] = None

    @classmethod
    def get_cookie_cache(cls) -> "_CookieCache":
        """Return the cookie cache singleton."""
        if cls._cookie_cache is None:
            with _cache_init_lock:
                cls._initialise()
        if cls._cookie_cache is None:
            raise _CookieCacheException("Failed to initialize cookie cache")
        return cls._cookie_cache

    @classmethod
    def reset_cache(cls) -> None:
        """Reset the cookie cache singleton."""
        cls._cookie_cache = None

    @classmethod
    def _initialise(cls) -> None:
        """Instantiate the cookie cache singleton."""
        cls._cookie_cache = _CookieCache()


class _CookieDBManager:
    """Manage the sqlite database used by the cookie cache."""

    _db: Optional[_peewee.SqliteDatabase] = None
    _cache_dir = _os.path.join(_ad.user_cache_dir(), "py-yfinance")

    @classmethod
    def get_database(cls):
        """Return the cookie sqlite database instance."""
        if cls._db is None:
            cls._initialise()
        return cls._db

    @classmethod
    def close_db(cls):
        """Close the cookie sqlite database if it is open."""
        if cls._db is not None:
            try:
                cls._db.close()
            except _DB_CLOSE_EXCEPTIONS:
                # Must discard exceptions because Python is trying to quit.
                pass

    @classmethod
    def _initialise(cls, cache_dir=None):
        """Initialize the cookie sqlite database."""
        if cache_dir is not None:
            cls._cache_dir = cache_dir

        if not _os.path.isdir(cls._cache_dir):
            try:
                _os.makedirs(cls._cache_dir)
            except OSError as err:
                raise _CookieCacheException(
                    f"Error creating CookieCache folder: '{cls._cache_dir}' reason: {err}"
                ) from err
        elif not (_os.access(cls._cache_dir, _os.R_OK) and _os.access(cls._cache_dir, _os.W_OK)):
            raise _CookieCacheException(
                f"Cannot read and write in CookieCache folder: '{cls._cache_dir}'"
            )

        cls._db = _peewee.SqliteDatabase(
            _os.path.join(cls._cache_dir, "cookies.db"),
            pragmas={"journal_mode": "wal", "cache_size": -64},
        )

    @classmethod
    def set_location(cls, new_cache_dir):
        """Set the folder where cookie cache artifacts are stored."""
        if cls._db is not None:
            cls._db.close()
            cls._db = None
        cls._cache_dir = new_cache_dir

    @classmethod
    def get_location(cls):
        """Return the folder where cookie cache artifacts are stored."""
        return cls._cache_dir


# Close DB when Python exits.
_atexit.register(_CookieDBManager.close_db)


COOKIE_DB_PROXY = _peewee.Proxy()


class ISODateTimeField(_peewee.DateTimeField):
    """DateTime field that round-trips ISO datetime strings reliably."""

    def db_value(self, value):
        """Convert Python datetimes to ISO strings before writing."""
        if value and isinstance(value, _dt.datetime):
            return value.isoformat()
        return super().db_value(value)

    def python_value(self, value):
        """Convert ISO datetime strings to Python datetimes after reading."""
        if value and isinstance(value, str) and "T" in value:
            return _dt.datetime.fromisoformat(value)
        return super().python_value(value)


class _CookieSchema(_peewee.Model):
    """Cookie cache table schema."""

    strategy = _peewee.CharField(primary_key=True)
    fetch_date = ISODateTimeField(default=_dt.datetime.now)

    # Which cookie type depends on strategy.
    cookie_bytes = _peewee.BlobField()

    @classmethod
    def strategy_field(cls):
        """Return the strategy field for cookie records."""
        return cls.strategy

    @classmethod
    def cookie_bytes_field(cls):
        """Return the serialized cookie field for cookie records."""
        return cls.cookie_bytes

    class Meta:
        """Peewee metadata for cookie cache table."""

        database = COOKIE_DB_PROXY
        without_rowid = True

        @classmethod
        def database_proxy(cls):
            """Return the proxy configured for this model."""
            return cls.database

        @classmethod
        def without_rowid_enabled(cls):
            """Return whether the table uses SQLite WITHOUT ROWID."""
            return cls.without_rowid


class _CookieCache:
    """SQLite-backed cookie cache."""

    def __init__(self):
        """Build an uninitialized cookie cache."""
        self.initialised = -1
        self.db = None
        self.dummy = False

    def get_db(self):
        """Return the cookie sqlite database, if available."""
        if self.db is not None:
            return self.db

        try:
            self.db = _CookieDBManager.get_database()
        except _CookieCacheException as err:
            get_yf_logger().info(
                "Failed to create CookieCache, reason: %s. "
                "CookieCache will not be used. "
                "Tip: You can direct cache to use a different location with "
                "'set_cache_location(mylocation)'",
                err,
            )
            self.dummy = True
            return None
        return self.db

    def initialise(self):
        """Initialize database tables for the cookie cache."""
        if self.initialised != -1:
            return

        db = self.get_db()
        if db is None:
            self.initialised = 0  # failure
            return

        db.connect()
        COOKIE_DB_PROXY.initialize(db)
        try:
            db.create_tables([_CookieSchema])
        except _peewee.OperationalError as err:
            if "WITHOUT" in str(err):
                meta = getattr(_CookieSchema, "_meta", None)
                if meta is not None:
                    meta.without_rowid = False
                db.create_tables([_CookieSchema])
            else:
                raise
        self.initialised = 1  # success

    def lookup(self, strategy):
        """Look up cookie data for a strategy."""
        if self.dummy:
            return None

        if self.initialised == -1:
            self.initialise()

        if self.initialised == 0:  # failure
            return None

        try:
            data = _CookieSchema.get(_CookieSchema.strategy == strategy)
            cookie = _pkl.loads(data.cookie_bytes)
            return {"cookie": cookie, "age": _dt.datetime.now() - data.fetch_date}
        except _peewee.DoesNotExist:
            return None

    def store(self, strategy, cookie):
        """Store or delete cookie value for a strategy."""
        if self.dummy:
            return

        if self.initialised == -1:
            self.initialise()

        if self.initialised == 0:  # failure
            return

        db = self.get_db()
        if db is None:
            return

        delete_query = _CookieSchema.delete().where(_CookieSchema.strategy == strategy)
        delete_query.execute(db)
        if cookie is None:
            return

        with db.atomic():
            cookie_pkl = _pkl.dumps(cookie, _pkl.HIGHEST_PROTOCOL)
            insert_query = _CookieSchema.insert(strategy=strategy, cookie_bytes=cookie_pkl)
            insert_query.execute(db)


def get_cookie_cache() -> "_CookieCache":
    """Return the cookie cache singleton."""
    return _CookieCacheManager.get_cookie_cache()


# ----------
# ISIN cache
# ----------


class _IsinCacheException(Exception):
    """Raised when ISIN cache initialization fails."""


class _IsinCacheDummy:
    """Dummy cache to use if ISIN cache is disabled."""

    def lookup(self, _isin):
        """Return no value when ISIN cache is disabled."""
        return None

    def store(self, _isin, _tkr):
        """Discard values when ISIN cache is disabled."""

    @property
    def tz_db(self):
        """Return no backing database for the dummy cache."""
        return None


class _IsinCacheManager:
    """Singleton manager for ISIN cache access."""

    _isin_cache: Optional["_IsinCache"] = None

    @classmethod
    def get_isin_cache(cls) -> "_IsinCache":
        """Return the ISIN cache singleton."""
        if cls._isin_cache is None:
            with _cache_init_lock:
                cls._initialise()
        if cls._isin_cache is None:
            raise _IsinCacheException("Failed to initialize ISIN cache")
        return cls._isin_cache

    @classmethod
    def reset_cache(cls) -> None:
        """Reset the ISIN cache singleton."""
        cls._isin_cache = None

    @classmethod
    def _initialise(cls) -> None:
        """Instantiate the ISIN cache singleton."""
        cls._isin_cache = _IsinCache()


class _IsinDBManager:
    """Manage the sqlite database used by the ISIN cache."""

    _db: Optional[_peewee.SqliteDatabase] = None
    _cache_dir = _os.path.join(_ad.user_cache_dir(), "py-yfinance")

    @classmethod
    def get_database(cls):
        """Return the ISIN sqlite database instance."""
        if cls._db is None:
            cls._initialise()
        return cls._db

    @classmethod
    def close_db(cls):
        """Close the ISIN sqlite database if it is open."""
        if cls._db is not None:
            try:
                cls._db.close()
            except _DB_CLOSE_EXCEPTIONS:
                # Must discard exceptions because Python is trying to quit.
                pass

    @classmethod
    def _initialise(cls, cache_dir=None):
        """Initialize the ISIN sqlite database."""
        if cache_dir is not None:
            cls._cache_dir = cache_dir

        if not _os.path.isdir(cls._cache_dir):
            try:
                _os.makedirs(cls._cache_dir)
            except OSError as err:
                raise _IsinCacheException(
                    f"Error creating ISINCache folder: '{cls._cache_dir}' reason: {err}"
                ) from err
        elif not (_os.access(cls._cache_dir, _os.R_OK) and _os.access(cls._cache_dir, _os.W_OK)):
            raise _IsinCacheException(
                f"Cannot read and write in ISINCache folder: '{cls._cache_dir}'"
            )

        cls._db = _peewee.SqliteDatabase(
            _os.path.join(cls._cache_dir, "isin-tkr.db"),
            pragmas={"journal_mode": "wal", "cache_size": -64},
        )

    @classmethod
    def set_location(cls, new_cache_dir):
        """Set the folder where ISIN cache artifacts are stored."""
        if cls._db is not None:
            cls._db.close()
            cls._db = None
        cls._cache_dir = new_cache_dir

    @classmethod
    def get_location(cls):
        """Return the folder where ISIN cache artifacts are stored."""
        return cls._cache_dir


# Close DB when Python exits.
_atexit.register(_IsinDBManager.close_db)


ISIN_DB_PROXY = _peewee.Proxy()


class _IsinKv(_peewee.Model):
    """ISIN key-value record schema with insertion timestamp."""

    key = _peewee.CharField(primary_key=True)
    value = _peewee.CharField(null=True)
    created_at = _peewee.DateTimeField(default=_dt.datetime.now)

    @classmethod
    def key_field(cls):
        """Return the model field used as cache key."""
        return cls.key

    @classmethod
    def value_field(cls):
        """Return the model field used as cache value."""
        return cls.value

    class Meta:
        """Peewee metadata for ISIN key-value table."""

        database = ISIN_DB_PROXY
        without_rowid = True

        @classmethod
        def database_proxy(cls):
            """Return the proxy configured for this model."""
            return cls.database

        @classmethod
        def without_rowid_enabled(cls):
            """Return whether the table uses SQLite WITHOUT ROWID."""
            return cls.without_rowid


class _IsinCache:
    """SQLite-backed ISIN cache."""

    def __init__(self):
        """Build an uninitialized ISIN cache."""
        self.initialised = -1
        self.db = None
        self.dummy = False

    def get_db(self):
        """Return the ISIN sqlite database, if available."""
        if self.db is not None:
            return self.db

        try:
            self.db = _IsinDBManager.get_database()
        except _IsinCacheException as err:
            get_yf_logger().info(
                "Failed to create ISINCache, reason: %s. "
                "ISINCache will not be used. "
                "Tip: You can direct cache to use a different location with "
                "'set_isin_cache_location(mylocation)'",
                err,
            )
            self.dummy = True
            return None
        return self.db

    def initialise(self):
        """Initialize database tables for the ISIN cache."""
        if self.initialised != -1:
            return

        db = self.get_db()
        if db is None:
            self.initialised = 0  # failure
            return

        db.connect()
        ISIN_DB_PROXY.initialize(db)
        try:
            db.create_tables([_IsinKv])
        except _peewee.OperationalError as err:
            if "WITHOUT" in str(err):
                meta = getattr(_IsinKv, "_meta", None)
                if meta is not None:
                    meta.without_rowid = False
                db.create_tables([_IsinKv])
            else:
                raise
        self.initialised = 1  # success

    def lookup(self, key):
        """Look up ticker by ISIN key."""
        if self.dummy:
            return None

        if self.initialised == -1:
            self.initialise()

        if self.initialised == 0:  # failure
            return None

        try:
            return _IsinKv.get(_IsinKv.key == key).value
        except _peewee.DoesNotExist:
            return None

    def store(self, key, value):
        """Store or delete ticker value for an ISIN key."""
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
                delete_query = _IsinKv.delete().where(_IsinKv.key == key)
                delete_query.execute(db)
                return

            # Remove existing rows with the same value that are older than one week.
            one_week_ago = _dt.datetime.now() - _dt.timedelta(weeks=1)
            old_rows_query = _IsinKv.delete().where(
                (_IsinKv.value == value) & (_IsinKv.created_at < one_week_ago)
            )
            old_rows_query.execute(db)

            with db.atomic():
                insert_query = _IsinKv.insert(key=key, value=value)
                insert_query.execute(db)

        except _peewee.IntegrityError:
            # Integrity error means the key already exists. Try updating the key.
            old_value = self.lookup(key)
            if old_value != value:
                get_yf_logger().debug(
                    "Value for key %s changed from %s to %s.",
                    key,
                    old_value,
                    value,
                )
                with db.atomic():
                    update_query = _IsinKv.update(
                        value=value,
                        created_at=_dt.datetime.now(),
                    ).where(_IsinKv.key == key)
                    update_query.execute(db)


def get_isin_cache() -> "_IsinCache":
    """Return the ISIN cache singleton."""
    return _IsinCacheManager.get_isin_cache()


# -----
# Utils
# -----


def set_cache_location(cache_dir: str):
    """Set the root folder where yfinance cache files are stored."""
    _TzDBManager.set_location(cache_dir)
    _CookieDBManager.set_location(cache_dir)
    _IsinDBManager.set_location(cache_dir)

    # Reset cache manager singletons so they pick up the new DB location.
    _TzCacheManager.reset_cache()
    _CookieCacheManager.reset_cache()
    _IsinCacheManager.reset_cache()


def set_tz_cache_location(cache_dir: str):
    """Backward-compatible helper that sets the shared cache location."""
    set_cache_location(cache_dir)

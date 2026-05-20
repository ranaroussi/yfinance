"""HTTP backend abstraction.

Prefers ``curl_cffi`` for browser TLS impersonation. Falls back to plain
``requests`` with a realistic ``User-Agent`` when ``curl_cffi`` cannot be
imported (e.g. binary not buildable on the host platform — see issue #2692)
or when ``YF_DISABLE_CURL_CFFI`` is set in the environment (downstream
packagers may ship without ``curl_cffi`` even if it is installed).

The fallback is best-effort: plain ``requests`` cannot replicate the
JA3/JA4 fingerprint and HTTP/2 settings that ``curl_cffi`` provides, so
Yahoo Finance may rate-limit or block this client. ``curl_cffi`` remains
the preferred backend and the default install dependency.
"""
import functools
import os

from . import utils

_DISABLE = os.environ.get("YF_DISABLE_CURL_CFFI", "").lower() in ("1", "true", "yes")

if not _DISABLE:
    try:
        from curl_cffi import requests as _curl_backend
        _backend = _curl_backend
        HAS_CURL_CFFI = True
    except ImportError:
        import requests as _requests_backend
        _backend = _requests_backend
        HAS_CURL_CFFI = False
else:
    import requests as _requests_backend
    _backend = _requests_backend
    HAS_CURL_CFFI = False

requests = _backend
HTTPError = _backend.exceptions.HTTPError

_FALLBACK_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

_fallback_warned = False


def _warn_once_on_fallback():
    global _fallback_warned
    if HAS_CURL_CFFI or _fallback_warned:
        return
    _fallback_warned = True
    utils.get_yf_logger().warning(
        "curl_cffi not available; falling back to requests without browser TLS "
        "impersonation. Yahoo Finance may rate-limit or block this client. "
        "Install curl_cffi (>=0.15) for the supported configuration."
    )


def new_session():
    """Create a default Session for the active backend."""
    if HAS_CURL_CFFI:
        return _backend.Session(impersonate="chrome")
    _warn_once_on_fallback()
    s = _backend.Session()
    s.headers.update({
        "User-Agent": _FALLBACK_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })
    return s


def cookie_jar(session):
    """Return the underlying ``http.cookiejar.CookieJar`` for either backend.

    ``curl_cffi`` exposes the jar as ``session.cookies.jar``; ``requests``
    uses ``session.cookies`` directly (it subclasses ``CookieJar``).
    """
    cookies = session.cookies
    return getattr(cookies, "jar", cookies)


@functools.lru_cache(maxsize=1)
def _supported_session_classes() -> tuple:
    classes = []
    try:
        from curl_cffi.requests.session import Session as _CurlSession
        classes.append(_CurlSession)
    except ImportError:
        pass
    try:
        from requests.sessions import Session as _ReqSession
        classes.append(_ReqSession)
    except ImportError:
        pass
    return tuple(classes)


def is_supported_session(obj) -> bool:
    """True if ``obj`` is a Session from either supported backend."""
    classes = _supported_session_classes()
    return bool(classes) and isinstance(obj, classes)

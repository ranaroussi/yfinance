"""
Record and replay Yahoo responses, so tests can run without network.

Set environment variable YF_TEST_MODE:

- unset or "live": default, tests talk to Yahoo as before.
- "record": talk to Yahoo, and save every response to tests/data/responses/.
- "replay": never touch network, serve saved responses. A request without a
  recording fails the test, with the command to record it.

    YF_TEST_MODE=record pytest tests/test_ticker.py::TestTickerHistory
    YF_TEST_MODE=replay pytest tests/test_ticker.py::TestTickerHistory

How it works: install() patches Session.request of the HTTP backends
(curl_cffi and requests), so every request yfinance makes is intercepted,
whatever session object it uses. Nothing in yfinance/ is changed.

A recording is keyed on method + URL + sorted query params + POST body,
excluding 'crumb'. 'period1' and 'period2' are excluded from the key and
matched separately: a timestamp matches if equal, or if it has the same
offset from "now" as it had at record time (within 2 days). So both
history(start='2024-01-02') and history(period='max') replay on any day.

Cookie and crumb fetches are answered with canned responses in replay mode
and are not saved, so no cookies or crumbs are committed.

Record mode uses a fresh cache folder, so requests normally skipped by the
timezone cache are recorded too.
"""

import hashlib
import json
import os
import re
import sys
import threading
import time
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

MODE = os.environ.get("YF_TEST_MODE", "live").lower()
RESPONSES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "responses")

_VOLATILE_PARAMS = {"crumb"}
_TIMESTAMP_PARAMS = ("period1", "period2")
_TIMESTAMP_TOLERANCE = 2 * 86400  # seconds
_HANDSHAKE_HOSTS = ("fc.yahoo.com", "guce.yahoo.com", "consent.yahoo.com")

_lock = threading.Lock()
_loaded = {}
misses = []  # messages for requests without a recording, checked by conftest.py


class MissingRecordingError(Exception):
    pass


def _query_items(url, params):
    parts = urlsplit(url)
    items = parse_qsl(parts.query, keep_blank_values=True)
    if isinstance(params, dict):
        items += list(params.items())
    elif params:
        items += list(params)
    base_url = urlunsplit((parts.scheme, parts.netloc, parts.path, '', ''))
    items = sorted((str(k), str(v)) for k, v in items if k not in _VOLATILE_PARAMS)
    return base_url, items


def _body(json_body=None, data=None):
    if json_body is not None:
        return json.dumps(json_body, sort_keys=True)
    if data is not None:
        return data if isinstance(data, str) else json.dumps(data, sort_keys=True)
    return None


def _describe(method, url, params, json_body=None, data=None):
    base_url, items = _query_items(url, params)
    request = {"method": method.upper(), "url": base_url, "params": dict(items)}
    body = _body(json_body, data)
    if body is not None:
        request["body"] = body
    return request


def _filepath(request):
    stable = dict(request)
    stable["params"] = {k: v for k, v in request["params"].items() if k not in _TIMESTAMP_PARAMS}
    stable["timestamps"] = sorted(k for k in _TIMESTAMP_PARAMS if k in request["params"])
    digest = hashlib.sha1(json.dumps(stable, sort_keys=True).encode()).hexdigest()[:10]
    path = urlsplit(request["url"]).path
    slug = re.sub(r'[^A-Za-z0-9.=-]+', '_', f'{request["method"]}{path}').strip('_')[:80]
    return os.path.join(RESPONSES_DIR, f"{slug}_{digest}.json")


def _timestamp_distance(request, recorded, recorded_at, now):
    """How far apart two requests' timestamps are, or None if they don't match."""
    total = 0
    for k in _TIMESTAMP_PARAMS:
        if k not in request["params"]:
            continue
        ts, ts_rec = int(float(request["params"][k])), int(float(recorded["params"][k]))
        if ts == ts_rec:
            continue
        diff = abs((now - ts) - (recorded_at - ts_rec))
        if diff > _TIMESTAMP_TOLERANCE:
            return None
        total += diff
    return total


def _is_handshake(url):
    parts = urlsplit(url)
    return parts.hostname in _HANDSHAKE_HOSTS or parts.path.endswith("/v1/test/getcrumb")


def _load(filepath):
    if filepath not in _loaded:
        with open(filepath, encoding="utf-8") as f:
            _loaded[filepath] = json.load(f)
    return _loaded[filepath]


def _find(request, filepath):
    if not os.path.isfile(filepath):
        return None
    now = int(time.time())
    best, best_dist = None, None
    for interaction in _load(filepath):
        dist = _timestamp_distance(request, interaction["request"], interaction["recorded_at"], now)
        if dist is not None and (best_dist is None or dist < best_dist):
            best, best_dist = interaction, dist
    return best


def _save(request, response):
    content_type = response.headers.get("content-type", "")
    stored = {"status_code": response.status_code, "content_type": content_type}
    url = response.url
    if url:
        base_url, items = _query_items(url, None)
        stored["url"] = base_url + ('?' + urlencode(items) if items else '')
    try:
        if "json" not in content_type:
            raise ValueError
        stored["json"] = json.loads(response.text)
    except ValueError:
        stored["text"] = response.text
    now = int(time.time())
    interaction = {"request": request, "recorded_at": now, "response": stored}

    filepath = _filepath(request)
    with _lock:
        interactions = []
        if os.path.isfile(filepath):
            with open(filepath, encoding="utf-8") as f:
                interactions = json.load(f)
        # Replace an older recording of the same request
        interactions = [i for i in interactions
                        if _timestamp_distance(request, i["request"], i["recorded_at"], now) is None]
        interactions.append(interaction)
        os.makedirs(RESPONSES_DIR, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(_dumps(interactions) + "\n")


def _dumps(obj, indent=""):
    """Indented JSON, but lists of numbers/strings on one line to keep price arrays small."""
    inner = indent + " "
    if isinstance(obj, dict) and obj:
        items = [f"{inner}{json.dumps(k, ensure_ascii=False)}: {_dumps(v, inner)}" for k, v in obj.items()]
        return "{\n" + ",\n".join(items) + "\n" + indent + "}"
    if isinstance(obj, list) and any(isinstance(v, (dict, list)) for v in obj):
        return "[\n" + ",\n".join(inner + _dumps(v, inner) for v in obj) + "\n" + indent + "]"
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _make_response(session, status_code, url, content_type, text):
    from yfinance._http import HAS_CURL_CFFI
    content = text.encode("utf-8")
    headers = {"content-type": content_type} if content_type else {}
    if HAS_CURL_CFFI and _is_curl_session(session):
        from curl_cffi.requests import Response, Headers
        r = Response()
        r.content = content
        r.headers = Headers(headers)
        r.ok = status_code < 400
    else:
        from requests.models import Response
        from requests.structures import CaseInsensitiveDict
        r = Response()
        r._content = content
        r.headers = CaseInsensitiveDict(headers)
        r.encoding = "utf-8"
    r.status_code = status_code
    r.reason = "OK" if status_code < 400 else "Error"
    r.url = url
    return r


def _is_curl_session(session):
    try:
        from curl_cffi.requests.session import Session as CurlSession
    except ImportError:
        return False
    return isinstance(session, CurlSession)


def _replay(session, method, url, params, json_body, data):
    if _is_handshake(url):
        text = "replay-crumb" if url.endswith("/getcrumb") else ""
        return _make_response(session, 200, url, "text/plain", text)

    request = _describe(method, url, params, json_body, data)
    filepath = _filepath(request)
    interaction = _find(request, filepath)
    if interaction is None:
        test = os.environ.get("PYTEST_CURRENT_TEST", "").rsplit(" ", 1)[0] or "tests/<file>.py::<Class>::<test>"
        query = urlencode(sorted(request["params"].items()))
        msg = (f"No recorded response for {request['method']} {request['url']}?{query}\n"
               f"  expected in: {os.path.relpath(filepath)}\n"
               f"  record it with: YF_TEST_MODE=record pytest {test}")
        misses.append(msg)
        print(msg, file=sys.stderr)  # visible even if yfinance swallows the exception
        raise MissingRecordingError(msg)

    stored = interaction["response"]
    if "json" in stored:
        text = json.dumps(stored["json"], ensure_ascii=False, separators=(',', ':'))
    else:
        text = stored["text"]
    return _make_response(session, stored["status_code"], stored.get("url", url), stored["content_type"], text)


def _patch(session_cls):
    original = session_cls.request

    def request(self, method, url, params=None, *args, **kwargs):
        data = kwargs.get("data", args[0] if args else None)
        if MODE == "replay":
            return _replay(self, method, url, params, kwargs.get("json"), data)
        response = original(self, method, url, params, *args, **kwargs)
        if not _is_handshake(url):
            _save(_describe(method, url, params, kwargs.get("json"), data), response)
        return response

    session_cls.request = request


def install():
    """Activate record or replay, if YF_TEST_MODE asks for it. Safe to call again."""
    if MODE == "live":
        return
    if MODE not in ("record", "replay"):
        raise ValueError(f"YF_TEST_MODE must be live, record or replay, not '{MODE}'")

    import tempfile
    import yfinance
    # Fresh caches, so record and replay make the same requests.
    # Re-applied on every call in case a caller has set another location since.
    if not getattr(install, "cache_dir", None):
        install.cache_dir = tempfile.mkdtemp(prefix="yf-test-")
    yfinance.set_tz_cache_location(install.cache_dir)

    if getattr(install, "patched", False):
        return
    install.patched = True
    try:
        from curl_cffi.requests.session import Session as CurlSession
        _patch(CurlSession)
    except ImportError:
        pass
    try:
        from requests.sessions import Session as RequestsSession
        _patch(RequestsSession)
    except ImportError:
        pass

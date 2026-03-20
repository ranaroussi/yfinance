"""HTTP session and request orchestration for Yahoo Finance data access."""

import datetime
import functools
import socket
import threading
import time as _time
from functools import lru_cache
from typing import Any, Dict, Optional
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup
from curl_cffi import requests
from frozendict import frozendict

from . import cache, utils
from .config import YF_CONFIG as YfConfig
from .exceptions import YFException, YFDataException, YFRateLimitError


def _is_transient_error(exception):
    """Check if error is transient (network/timeout) and should be retried."""
    if isinstance(exception, (TimeoutError, socket.error, OSError)):
        return True
    error_type_name = type(exception).__name__
    transient_error_types = {
        "Timeout",
        "TimeoutError",
        "ConnectionError",
        "ConnectTimeout",
        "ReadTimeout",
        "ChunkedEncodingError",
        "RemoteDisconnected",
    }
    return error_type_name in transient_error_types


CACHE_MAXSIZE = 64


def lru_cache_freezeargs(func):
    """
    Decorator transforms mutable dictionary and list arguments into immutable types
    Needed so lru_cache can cache method calls what has dict or list arguments.
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        args = tuple(frozendict(arg) if isinstance(arg, dict) else arg for arg in args)
        kwargs = {
            k: frozendict(v) if isinstance(v, dict) else v for k, v in kwargs.items()
        }
        args = tuple(tuple(arg) if isinstance(arg, list) else arg for arg in args)
        kwargs = {k: tuple(v) if isinstance(v, list) else v for k, v in kwargs.items()}
        return func(*args, **kwargs)

    # copy over the lru_cache extra methods to this wrapper to be able to access them
    # after this decorator has been applied
    setattr(wrapped, "cache_info", getattr(func, "cache_info"))
    setattr(wrapped, "cache_clear", getattr(func, "cache_clear"))
    return wrapped


class SingletonMeta(type):
    """
    Metaclass that creates a Singleton instance.
    """

    _instances = {}
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
            else:
                # Update the existing instance
                if "session" in kwargs or (args and len(args) > 0):
                    session = kwargs.get("session") if "session" in kwargs else args[0]
                    cls._instances[cls]._set_session(session)
            return cls._instances[cls]


class YfData(metaclass=SingletonMeta):
    """
    Have one place to retrieve data from Yahoo API in order to ease caching and speed up operations.
    Singleton means one session one cookie shared by all threads.
    """

    def __init__(self, session=None):
        self._crumb: Optional[str] = None
        self._cookie: Any = None

        # Default to using 'basic' strategy
        self._cookie_strategy = "basic"
        # If it fails, then fallback method is 'csrf'
        # self._cookie_strategy = 'csrf'

        self._cookie_lock = threading.Lock()
        self._session_is_caching = False
        self._expire_after = None

        self._session: requests.session.Session = requests.Session(impersonate="chrome")
        self._set_session(session or self._session)

    def _set_session(self, session):
        if session is None:
            return

        try:
            session.cache
        except AttributeError:
            # Not caching
            self._session_is_caching = False
        else:
            # Is caching. This is annoying.
            # Can't simply use a non-caching session to fetch cookie & crumb,
            # because then the caching-session won't have cookie.
            self._session_is_caching = True
            # But since switch to curl_cffi, can't use requests_cache with it.
            raise YFDataException(
                "request_cache sessions don't work with curl_cffi, which is necessary "
                "now for Yahoo API. Solution: stop setting session, let YF handle."
            )

        if not isinstance(session, requests.session.Session):
            raise YFDataException(
                f"Yahoo API requires curl_cffi session not {type(session)}. "
                "Solution: stop setting session, let YF handle."
            )

        with self._cookie_lock:
            self._session = session
            self._sync_session_proxy()

    def _sync_session_proxy(self) -> None:
        self._session.proxies = self._resolve_proxy_config()

    def _resolve_proxy_config(self) -> Any:
        proxy_config = YfConfig.network.proxy
        if callable(proxy_config):
            proxy_config = proxy_config()
        if isinstance(proxy_config, str):
            return {"http": proxy_config, "https": proxy_config}
        return proxy_config

    def _get_network_request_options(self) -> Dict[str, Any]:
        self._sync_session_proxy()
        request_options: Dict[str, Any] = {}
        if YfConfig.network.verify is not None:
            request_options["verify"] = YfConfig.network.verify
        return request_options

    def _session_cookie_store(self) -> Dict[str, Any]:
        cookie_store = getattr(self._session.cookies.jar, "_cookies", None)
        if isinstance(cookie_store, dict):
            return cookie_store
        return {}

    def _update_session_cookie_store(self, cookies: Dict[str, Any]) -> None:
        cookie_store = getattr(self._session.cookies.jar, "_cookies", None)
        if isinstance(cookie_store, dict):
            cookie_store.update(cookies)

    def _set_cookie_strategy(self, strategy, have_lock=False):
        if strategy == self._cookie_strategy:
            return
        if have_lock:
            self._set_cookie_strategy_locked()
            return
        with self._cookie_lock:
            self._set_cookie_strategy_locked()

    def _set_cookie_strategy_locked(self):
        if self._cookie_strategy == "csrf":
            utils.get_yf_logger().debug(
                "toggling cookie strategy %s -> basic", self._cookie_strategy
            )
            self._session.cookies.clear()
            self._cookie_strategy = "basic"
        else:
            utils.get_yf_logger().debug(
                "toggling cookie strategy %s -> csrf", self._cookie_strategy
            )
            self._cookie_strategy = "csrf"
        self._cookie = None
        self._crumb = None

    @utils.log_indent_decorator
    def _save_cookie_curl_cffi(self):
        cookies = self._session_cookie_store()
        if len(cookies) == 0:
            return False
        yh_domains = [k for k in cookies if "yahoo" in k]
        if len(yh_domains) > 1:
            # Possible when cookie fetched with CSRF method. Discard consent cookie.
            yh_domains = [k for k in yh_domains if "consent" not in k]
        if len(yh_domains) > 1:
            utils.get_yf_logger().debug(
                "Multiple Yahoo cookies, not sure which to cache: %s",
                yh_domains,
            )
            return False
        if len(yh_domains) == 0:
            return False
        yh_domain = yh_domains[0]
        yh_cookie = {yh_domain: cookies[yh_domain]}
        cache.get_cookie_cache().store("curlCffi", yh_cookie)
        return True

    @utils.log_indent_decorator
    def _load_cookie_curl_cffi(self):
        cookie_dict = cache.get_cookie_cache().lookup("curlCffi")
        cookies = cookie_dict.get("cookie") if isinstance(cookie_dict, dict) else None
        if not isinstance(cookies, dict) or not cookies:
            return False

        _, domain_cookies = next(iter(cookies.items()))
        path_cookies = (
            domain_cookies.get("/") if isinstance(domain_cookies, dict) else None
        )
        cookie = path_cookies.get("A3") if isinstance(path_cookies, dict) else None
        expiry_ts = getattr(cookie, "expires", None) if cookie is not None else None
        if not isinstance(expiry_ts, (int, float)):
            return False

        if expiry_ts > 2e9:
            # convert ms to s
            expiry_ts //= 1e3
        expiry_dt = datetime.datetime.fromtimestamp(expiry_ts, tz=datetime.timezone.utc)
        expired = expiry_dt < datetime.datetime.now(datetime.timezone.utc)
        if expired:
            utils.get_yf_logger().debug("cached cookie expired")
            return False
        self._update_session_cookie_store(cookies)
        self._cookie = cookie
        return True

    @utils.log_indent_decorator
    def _get_cookie_basic(self, timeout=30):
        if self._cookie is not None:
            utils.get_yf_logger().debug("reusing cookie")
            return True
        if self._load_cookie_curl_cffi():
            utils.get_yf_logger().debug("reusing persistent cookie")
            return True

        # To avoid infinite recursion, do NOT use self.get()
        # - 'allow_redirects' copied from @psychoz971 solution - does it help USA?
        try:
            self._session.get(
                url="https://fc.yahoo.com",
                timeout=timeout,
                allow_redirects=True,
                **self._get_network_request_options(),
            )
        except requests.exceptions.RequestException as e:
            # Possible because fc.yahoo.com is blocked, unreachable, or timing out.
            # Allow caller to fall back to the alternate CSRF cookie strategy.
            utils.get_yf_logger().debug(
                "Handling cookie fetch error in basic strategy: %s", e
            )
            return False
        self._save_cookie_curl_cffi()
        return True

    @utils.log_indent_decorator
    def _get_crumb_basic(self, timeout=30):
        if self._crumb is not None:
            utils.get_yf_logger().debug("reusing crumb")
            return self._crumb

        if not self._get_cookie_basic():
            return None
        # - 'allow_redirects' copied from @psychoz971 solution - does it help USA?
        get_args = {
            "url": "https://query1.finance.yahoo.com/v1/test/getcrumb",
            "timeout": timeout,
            "allow_redirects": True,
        }
        if self._session_is_caching and self._expire_after is not None:
            get_args["expire_after"] = self._expire_after
        get_args.update(self._get_network_request_options())
        crumb_response = self._session.get(**get_args)
        self._crumb = crumb_response.text
        if crumb_response.status_code >= 400:
            utils.get_yf_logger().debug(
                "Didn't receive crumb because response code=%s body=%s",
                crumb_response.status_code,
                self._crumb,
            )
            self._crumb = None
            if crumb_response.status_code == 429:
                raise YFRateLimitError()
            return None
        if crumb_response.status_code == 429 or "Too Many Requests" in self._crumb:
            utils.get_yf_logger().debug(f"Didn't receive crumb {self._crumb}")
            self._crumb = None
            raise YFRateLimitError()

        if self._crumb is None or "<html>" in self._crumb:
            utils.get_yf_logger().debug("Didn't receive crumb")
            self._crumb = None
            return None

        utils.get_yf_logger().debug(f"crumb = '{self._crumb}'")
        return self._crumb

    @utils.log_indent_decorator
    def _get_cookie_and_crumb_basic(self, timeout):
        if not self._get_cookie_basic(timeout):
            return None
        return self._get_crumb_basic(timeout)

    def _extract_input_value(self, soup, input_name: str) -> Optional[str]:
        input_tag = soup.find("input", attrs={"name": input_name})
        if input_tag is None:
            utils.get_yf_logger().debug('Failed to find "%s" in response', input_name)
            return None
        input_value = input_tag.get("value")
        if not isinstance(input_value, str):
            utils.get_yf_logger().debug(
                'Failed to parse "%s" value in response', input_name
            )
            return None
        return input_value

    def _fetch_csrf_consent_page(self, base_args):
        request_args = {**base_args, "url": "https://guce.yahoo.com/consent"}
        try:
            if self._session_is_caching and self._expire_after is not None:
                request_args["expire_after"] = self._expire_after
            request_args.update(self._get_network_request_options())
            return self._session.get(**request_args)
        except requests.exceptions.ChunkedEncodingError:
            # No idea why happens, but handle nicely so can switch to other cookie method.
            utils.get_yf_logger().debug(
                "_get_cookie_csrf() encountering requests.exceptions.ChunkedEncodingError, aborting"
            )
            return None

    @utils.log_indent_decorator
    def _get_cookie_csrf(self, timeout):
        if self._cookie is not None:
            utils.get_yf_logger().debug("reusing cookie")
            return True

        if self._load_cookie_curl_cffi():
            utils.get_yf_logger().debug("reusing persistent cookie")
            self._cookie = True
            return True

        base_args = {"timeout": timeout}
        response = self._fetch_csrf_consent_page(base_args)
        if response is None:
            return False

        soup = BeautifulSoup(response.content, "html.parser")
        csrf_token = self._extract_input_value(soup, "csrfToken")
        session_id = self._extract_input_value(soup, "sessionId")
        if csrf_token is None or session_id is None:
            return False
        utils.get_yf_logger().debug("csrfToken = %s", csrf_token)
        utils.get_yf_logger().debug("sessionId = %s", session_id)

        original_done_url = "https://finance.yahoo.com/"
        namespace = "yahoo"
        data = {
            "agree": ["agree", "agree"],
            "consentUUID": "default",
            "sessionId": session_id,
            "csrfToken": csrf_token,
            "originalDoneUrl": original_done_url,
            "namespace": namespace,
        }
        post_args = {
            **base_args,
            "url": f"https://consent.yahoo.com/v2/collectConsent?sessionId={session_id}",
            "data": data,
        }
        get_args = {
            **base_args,
            "url": f"https://guce.yahoo.com/copyConsent?sessionId={session_id}",
            "data": data,
        }
        try:
            if self._session_is_caching and self._expire_after is not None:
                post_args["expire_after"] = self._expire_after
                get_args["expire_after"] = self._expire_after
            post_args.update(self._get_network_request_options())
            self._session.post(**post_args)
            get_args.update(self._get_network_request_options())
            self._session.get(**get_args)
        except requests.exceptions.ChunkedEncodingError:
            # No idea why happens, but handle nicely so can switch to other cookie method.
            utils.get_yf_logger().debug(
                "_get_cookie_csrf() encountering requests.exceptions.ChunkedEncodingError, aborting"
            )
        self._cookie = True
        self._save_cookie_curl_cffi()
        return True

    @utils.log_indent_decorator
    def _get_crumb_csrf(self, timeout=30):
        # Credit goes to @bot-unit #1729

        if self._crumb is not None:
            utils.get_yf_logger().debug("reusing crumb")
            return self._crumb

        if not self._get_cookie_csrf(timeout):
            # This cookie stored in session
            return None

        get_args = {
            "url": "https://query2.finance.yahoo.com/v1/test/getcrumb",
            "timeout": timeout,
        }
        if self._session_is_caching and self._expire_after is not None:
            get_args["expire_after"] = self._expire_after
        get_args.update(self._get_network_request_options())
        r = self._session.get(**get_args)
        self._crumb = r.text

        if r.status_code >= 400:
            utils.get_yf_logger().debug(
                "Didn't receive crumb because response code=%s body=%s",
                r.status_code,
                self._crumb,
            )
            self._crumb = None
            if r.status_code == 429:
                raise YFRateLimitError()
            return None

        if r.status_code == 429 or "Too Many Requests" in self._crumb:
            utils.get_yf_logger().debug(f"Didn't receive crumb {self._crumb}")
            self._crumb = None
            raise YFRateLimitError()

        if self._crumb is None or "<html>" in self._crumb or self._crumb == "":
            utils.get_yf_logger().debug("Didn't receive crumb")
            self._crumb = None
            return None

        utils.get_yf_logger().debug(f"crumb = '{self._crumb}'")
        return self._crumb

    @utils.log_indent_decorator
    def _get_cookie_and_crumb(self, timeout=30):
        crumb, strategy = None, None

        utils.get_yf_logger().debug(f"cookie_mode = '{self._cookie_strategy}'")

        with self._cookie_lock:
            if self._cookie_strategy == "csrf":
                crumb = self._get_crumb_csrf()
                if crumb is None:
                    # Fail
                    self._set_cookie_strategy("basic", have_lock=True)
                    crumb = self._get_cookie_and_crumb_basic(timeout)
            else:
                # Fallback strategy
                crumb = self._get_cookie_and_crumb_basic(timeout)
                if crumb is None:
                    # Fail
                    self._set_cookie_strategy("csrf", have_lock=True)
                    crumb = self._get_crumb_csrf()
            strategy = self._cookie_strategy
        return crumb, strategy

    @utils.log_indent_decorator
    def get(self, url, params=None, timeout=30):
        """Perform an HTTP GET request with cookie and crumb handling."""
        request_config = {"url": url, "params": params, "timeout": timeout}
        response = self._make_request(self._session.get, request_config)
        if self._is_this_consent_url(response.url):
            return self._accept_consent_form(response, timeout)
        return response

    def _normalize_post_args(self, args, kwargs):
        kwargs_copy = dict(kwargs)
        has_body_kwarg = "body" in kwargs_copy
        has_params_kwarg = "params" in kwargs_copy
        has_timeout_kwarg = "timeout" in kwargs_copy
        has_data_kwarg = "data" in kwargs_copy

        body = kwargs_copy.pop("body", None)
        params = kwargs_copy.pop("params", None)
        timeout = kwargs_copy.pop("timeout", 30)
        data = kwargs_copy.pop("data", None)

        if len(args) > 4:
            raise TypeError("post() takes at most 5 positional arguments")
        if len(args) >= 1:
            if has_body_kwarg:
                raise TypeError("post() got multiple values for argument 'body'")
            body = args[0]
        if len(args) >= 2:
            if has_params_kwarg:
                raise TypeError("post() got multiple values for argument 'params'")
            params = args[1]
        if len(args) >= 3:
            if has_timeout_kwarg:
                raise TypeError("post() got multiple values for argument 'timeout'")
            timeout = args[2]
        if len(args) == 4:
            if has_data_kwarg:
                raise TypeError("post() got multiple values for argument 'data'")
            data = args[3]
        if kwargs_copy:
            unexpected = ", ".join(sorted(kwargs_copy))
            raise TypeError(f"post() got unexpected keyword arguments: {unexpected}")
        return body, params, timeout, data

    @utils.log_indent_decorator
    def post(self, url, *args, **kwargs):
        """Perform an HTTP POST request with cookie and crumb handling."""
        body, params, timeout, data = self._normalize_post_args(args, kwargs)
        request_config = {
            "url": url,
            "body": body,
            "params": params,
            "timeout": timeout,
            "data": data,
        }
        return self._make_request(self._session.post, request_config)

    def _log_request_details(self, url, params):
        if len(url) > 200:
            utils.get_yf_logger().debug("url=%s...", url[:200])
        else:
            utils.get_yf_logger().debug("url=%s", url)
        utils.get_yf_logger().debug("params=%s", params)

    def _build_request_args(self, request_config):
        url = request_config["url"]
        params = request_config.get("params")
        timeout = request_config.get("timeout", 30)
        body = request_config.get("body")
        data = request_config.get("data")

        if params is None:
            params = {}
        if "crumb" in params:
            raise YFException(
                "Don't manually add 'crumb' to params dict, let data.py handle it"
            )

        crumb, strategy = self._get_cookie_and_crumb(timeout)
        crumbs = {"crumb": crumb} if crumb is not None else {}
        request_args = {"url": url, "params": {**params, **crumbs}, "timeout": timeout}
        if body:
            request_args["json"] = body
        if data:
            request_args["data"] = data
            request_args["headers"] = {"Content-Type": "application/json"}
        return request_args, strategy

    def _request_with_retry(self, request_method, request_args):
        retryable_exceptions = (
            requests.exceptions.RequestException,
            TimeoutError,
            socket.error,
            OSError,
        )
        for attempt in range(YfConfig.network.retries + 1):
            try:
                return request_method(**request_args)
            except retryable_exceptions as exc:
                if _is_transient_error(exc) and attempt < YfConfig.network.retries:
                    _time.sleep(2**attempt)
                    continue
                raise
        raise RuntimeError("Unreachable retry loop termination")

    def _retry_with_alternate_cookie_strategy(
        self, request_method, request_args, strategy, timeout
    ):
        self._set_cookie_strategy("csrf" if strategy == "basic" else "basic")
        crumb, _ = self._get_cookie_and_crumb(timeout)
        if crumb is not None:
            request_args["params"]["crumb"] = crumb
        else:
            request_args["params"].pop("crumb", None)

        response = request_method(**request_args)
        utils.get_yf_logger().debug("response code=%s", response.status_code)
        if response.status_code == 429:
            raise YFRateLimitError()
        return response

    @utils.log_indent_decorator
    def _make_request(self, request_method, request_config):
        """Execute a request and retry with fallback cookie strategy when needed."""
        self._log_request_details(request_config["url"], request_config.get("params"))
        request_args, strategy = self._build_request_args(request_config)
        request_args.update(self._get_network_request_options())
        response = self._request_with_retry(request_method, request_args)
        utils.get_yf_logger().debug("response code=%s", response.status_code)
        if response.status_code >= 400:
            timeout = request_config.get("timeout", 30)
            response = self._retry_with_alternate_cookie_strategy(
                request_method, request_args, strategy, timeout
            )
        return response

    @lru_cache_freezeargs
    @lru_cache(maxsize=CACHE_MAXSIZE)
    def cache_get(self, url, params=None, timeout=30):
        """Return cached GET responses for immutable argument combinations."""
        return self.get(url, params, timeout)

    def get_raw_json(self, url, params=None, timeout=30):
        """Fetch JSON payload and raise for HTTP errors."""
        utils.get_yf_logger().debug("get_raw_json(): %s", url)
        response = self.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return response.json()

    def _is_this_consent_url(self, response_url: str) -> bool:
        """
        Check if given response_url is consent page

        Args:
            response_url (str) : response.url

        Returns:
            True : This is cookie-consent page
            False : This is not cookie-consent page
        """
        try:
            hostname = urlsplit(response_url).hostname
            return bool(hostname and hostname.endswith("consent.yahoo.com"))
        except (AttributeError, TypeError, ValueError):
            return False

    def _build_consent_form_data(self, form) -> Dict[str, str]:
        payload: Dict[str, str] = {}
        for input_tag in form.find_all("input"):
            name_attr = input_tag.get("name")
            if not isinstance(name_attr, str) or name_attr == "":
                continue

            input_type = input_tag.get("type")
            normalized_type = (
                input_type.lower() if isinstance(input_type, str) else "text"
            )
            value_attr = input_tag.get("value")
            value = value_attr if isinstance(value_attr, str) else ""
            if normalized_type in ("checkbox", "radio"):
                name_lower = name_attr.lower()
                has_agree_name = "agree" in name_lower or "accept" in name_lower
                if has_agree_name or input_tag.has_attr("checked"):
                    payload[name_attr] = value if value != "" else "1"
            else:
                payload[name_attr] = value

        lowered = {key.lower() for key in payload}
        if not any("agree" in key or "accept" in key for key in lowered):
            payload["agree"] = "1"
        return payload

    def _accept_consent_form(
        self, consent_resp: requests.Response, timeout: int
    ) -> requests.Response:
        """
        Click 'Accept all' to cookie-consent form and return response object.

        Args:
            consent_resp (requests.Response) : Response instance of cookie-consent page
            timeout (int) : Raise TimeoutError if post doesn't respond

        Returns:
            response (requests.Response) : Response received after posting consent.
        """
        soup = BeautifulSoup(consent_resp.text, "html.parser")

        # Heuristic: pick the first form; Yahoo's CMP tends to have a single form for consent
        form = soup.find("form")
        if not form:
            return consent_resp

        action_attr = form.get("action")
        if isinstance(action_attr, str) and action_attr != "":
            action = urljoin(consent_resp.url, action_attr)
        else:
            action = consent_resp.url

        data = self._build_consent_form_data(form)
        # Some servers check referer as lightweight CSRF protection.
        headers = {"Referer": consent_resp.url}
        return self._session.post(
            action,
            data=data,
            headers=headers,
            timeout=timeout,
            allow_redirects=True,
            **self._get_network_request_options(),
        )

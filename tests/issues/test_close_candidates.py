"""Issue-specific verification tests for upstream close candidates."""

import datetime as dt
import unittest
import warnings
from unittest.mock import Mock, patch

import pandas as pd
from curl_cffi import requests

from tests.ticker_support import SessionTickerTestCase, call_private, yf
from yfinance.config import YF_CONFIG
from yfinance.data import YfData
from yfinance.scrapers.history.price_repair import _prepare_adjusted_price_data
from yfinance.scrapers.quote import FastInfo


class TestIssue2688(unittest.TestCase):
    """Verify repair logic handles read-only numpy arrays."""

    def test_price_repair_copies_read_only_price_data(self):
        """Repair logic should copy price arrays before mutating them."""
        price_columns = ["Open", "High", "Low", "Close"]
        frame = pd.DataFrame(
            {
                "Open": [100.0, 120.0],
                "High": [110.0, 125.0],
                "Low": [95.0, 115.0],
                "Close": [105.0, 120.0],
                "Adj Close": [52.5, 60.0],
            }
        )
        workings = frame[price_columns].copy()

        original_to_numpy = pd.DataFrame.to_numpy

        def patched_to_numpy(dataframe, *args, **kwargs):
            """Return a read-only array for the selected price columns."""
            result = original_to_numpy(dataframe, *args, **kwargs)
            if list(getattr(dataframe, "columns", [])) == price_columns:
                result.setflags(write=False)
            return result

        with patch("pandas.core.frame.DataFrame.to_numpy", new=patched_to_numpy):
            adjusted, zeros = _prepare_adjusted_price_data(frame, workings, price_columns)

        self.assertFalse(zeros.any())
        self.assertAlmostEqual(adjusted[0, 0], 50.0)
        self.assertAlmostEqual(adjusted[1, 3], 60.0)


class TestIssue1924(unittest.TestCase):
    """Verify cookie fetch can survive fc.yahoo.com DNS blocking."""

    def setUp(self):
        """Reset the cookie state before each test."""
        self.data = YfData()
        setattr(self.data, "_cookie", None)
        setattr(self.data, "_crumb", None)
        setattr(self.data, "_cookie_strategy", "basic")

    def test_get_cookie_basic_handles_dns_error(self):
        """Basic cookie fetch should fail cleanly on DNS errors."""
        with (
            patch.object(self.data, "_load_cookie_curl_cffi", return_value=False),
            patch.object(
                getattr(self.data, "_session"),
                "get",
                side_effect=requests.exceptions.DNSError("fc.yahoo.com blocked"),
            ),
        ):
            self.assertFalse(call_private(self.data, "_get_cookie_basic", timeout=1))

    def test_get_cookie_basic_handles_timeout_error(self):
        """Basic cookie fetch should fail cleanly on timeout errors."""
        with (
            patch.object(self.data, "_load_cookie_curl_cffi", return_value=False),
            patch.object(
                getattr(self.data, "_session"),
                "get",
                side_effect=requests.exceptions.Timeout("fc.yahoo.com timeout"),
            ),
        ):
            self.assertFalse(call_private(self.data, "_get_cookie_basic", timeout=1))

    def test_get_cookie_basic_handles_connection_error(self):
        """Basic cookie fetch should fail cleanly on connection errors."""
        with (
            patch.object(self.data, "_load_cookie_curl_cffi", return_value=False),
            patch.object(
                getattr(self.data, "_session"),
                "get",
                side_effect=requests.exceptions.ConnectionError("fc.yahoo.com unreachable"),
            ),
        ):
            self.assertFalse(call_private(self.data, "_get_cookie_basic", timeout=1))

    def test_cookie_flow_falls_back_to_csrf_after_basic_failure(self):
        """Cookie flow should switch to csrf after a basic-mode failure."""
        with (
            patch.object(self.data, "_get_cookie_and_crumb_basic", return_value=None),
            patch.object(self.data, "_get_crumb_csrf", return_value="csrf-crumb"),
        ):
            crumb, strategy = call_private(self.data, "_get_cookie_and_crumb", timeout=1)

        self.assertEqual(crumb, "csrf-crumb")
        self.assertEqual(strategy, "csrf")

    def test_cookie_flow_falls_back_to_csrf_after_timeout_error(self):
        """Cookie flow should switch to csrf after a timeout in basic mode."""
        with (
            patch.object(self.data, "_load_cookie_curl_cffi", return_value=False),
            patch.object(
                getattr(self.data, "_session"),
                "get",
                side_effect=requests.exceptions.Timeout("fc.yahoo.com timeout"),
            ),
            patch.object(self.data, "_get_crumb_csrf", return_value="csrf-crumb"),
        ):
            crumb, strategy = call_private(self.data, "_get_cookie_and_crumb", timeout=1)

        self.assertEqual(crumb, "csrf-crumb")
        self.assertEqual(strategy, "csrf")

    def test_cookie_flow_falls_back_to_csrf_after_connection_error(self):
        """Cookie flow should switch to csrf after a connection error in basic mode."""
        with (
            patch.object(self.data, "_load_cookie_curl_cffi", return_value=False),
            patch.object(
                getattr(self.data, "_session"),
                "get",
                side_effect=requests.exceptions.ConnectionError("fc.yahoo.com unreachable"),
            ),
            patch.object(self.data, "_get_crumb_csrf", return_value="csrf-crumb"),
        ):
            crumb, strategy = call_private(self.data, "_get_cookie_and_crumb", timeout=1)

        self.assertEqual(crumb, "csrf-crumb")
        self.assertEqual(strategy, "csrf")


class TestIssue1801(SessionTickerTestCase):
    """Verify download no longer emits utcfromtimestamp deprecation warnings."""

    def test_download_does_not_emit_utcfromtimestamp_warning(self):
        """Download should not emit utcfromtimestamp deprecation warnings."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            data = yf.download(
                ["AAPL"],
                start="2021-01-01",
                end="2021-02-01",
                progress=False,
                threads=False,
                session=self.session,
            )

        if data is None:
            self.fail("yf.download() returned None")
        self.assertFalse(data.empty)
        matching = [
            warning
            for warning in caught
            if "utcfromtimestamp" in str(warning.message)
        ]
        self.assertEqual(matching, [])


class ProxyNetworkIssueTestCase(unittest.TestCase):
    """Shared setup for proxy-related regression tests."""

    def setUp(self):
        """Reset the shared singleton state before each test."""
        self.data = YfData()
        self.session = getattr(self.data, "_session")
        self.original_proxy = YF_CONFIG.network.proxy
        self.original_verify = YF_CONFIG.network.verify
        self.original_session_proxies = getattr(self.session, "proxies", None)
        setattr(self.data, "_cookie", None)
        setattr(self.data, "_crumb", None)
        setattr(self.data, "_cookie_strategy", "basic")

    def tearDown(self):
        """Restore global proxy and singleton session state."""
        YF_CONFIG.network.proxy = self.original_proxy
        YF_CONFIG.network.verify = self.original_verify
        self.session.proxies = self.original_session_proxies
        setattr(self.data, "_cookie", None)
        setattr(self.data, "_crumb", None)
        setattr(self.data, "_cookie_strategy", "basic")


class TestIssue2146(ProxyNetworkIssueTestCase):
    """Verify proxy settings propagate to direct cookie/bootstrap requests."""

    def test_cookie_bootstrap_syncs_proxy_after_singleton_creation(self):
        """Basic cookie bootstrap should pick up late proxy changes."""
        proxy = {"https": "http://proxy.local:8080"}
        YF_CONFIG.network.proxy = proxy
        self.session.proxies = None

        def fake_get(**kwargs):
            self.assertEqual(self.session.proxies, proxy)
            self.assertEqual(kwargs["url"], "https://fc.yahoo.com")
            return Mock(status_code=200)

        with (
            patch.object(self.data, "_load_cookie_curl_cffi", return_value=False),
            patch.object(self.session, "get", side_effect=fake_get),
            patch.object(self.data, "_save_cookie_curl_cffi", return_value=None),
        ):
            self.assertTrue(call_private(self.data, "_get_cookie_basic", timeout=1))

    def test_socks_proxy_bootstrap_uses_proxy_dict(self):
        """SOCKS proxy dicts should flow through bootstrap and chart requests."""
        proxy = {
            "http": "socks5h://127.0.0.1:2080",
            "https": "socks5h://127.0.0.1:2080",
        }
        YF_CONFIG.network.proxy = proxy
        self.session.proxies = None

        chart_response = Mock(status_code=200)
        chart_response.json.return_value = {
            "chart": {
                "result": [{"meta": {"exchangeTimezoneName": "America/New_York"}}],
                "error": None,
            }
        }

        def fake_get(**kwargs):
            self.assertEqual(self.session.proxies, proxy)
            if kwargs["url"] == "https://fc.yahoo.com":
                return Mock(status_code=200)
            if kwargs["url"] == "https://query1.finance.yahoo.com/v1/test/getcrumb":
                return Mock(status_code=200, text="crumb-1811")
            self.assertEqual(
                kwargs["url"],
                "https://query2.finance.yahoo.com/v8/finance/chart/MSFT",
            )
            return chart_response

        timezone_cache = yf.cache.get_tz_cache()
        timezone_cache.store("MSFT", None)

        with (
            patch.object(self.data, "_load_cookie_curl_cffi", return_value=False),
            patch.object(self.session, "get", side_effect=fake_get),
            patch.object(self.data, "_save_cookie_curl_cffi", return_value=None),
        ):
            ticker = yf.Ticker("MSFT", session=self.session)
            timezone = call_private(ticker, "_get_ticker_tz", timeout=1)

        self.assertEqual(timezone, "America/New_York")

    def test_csrf_cookie_flow_syncs_proxy_after_singleton_creation(self):
        """CSRF consent flow should pick up late proxy changes for get/post calls."""
        proxy = {"https": "http://proxy.local:8080"}
        YF_CONFIG.network.proxy = proxy
        self.session.proxies = None

        consent_response = Mock()
        consent_response.content = (
            b'<form action="/submit">'
            b'<input name="csrfToken" value="csrf-token" />'
            b'<input name="sessionId" value="session-id" />'
            b'<input type="checkbox" name="agree" value="agree" checked />'
            b"</form>"
        )
        consent_response.url = "https://guce.yahoo.com/consent"

        def fake_get(**kwargs):
            self.assertEqual(self.session.proxies, proxy)
            if kwargs["url"] == "https://guce.yahoo.com/consent":
                return consent_response
            self.assertEqual(
                kwargs["url"],
                "https://guce.yahoo.com/copyConsent?sessionId=session-id",
            )
            return Mock(status_code=200)

        def fake_post(**kwargs):
            self.assertEqual(self.session.proxies, proxy)
            self.assertEqual(
                kwargs["url"],
                "https://consent.yahoo.com/v2/collectConsent?sessionId=session-id",
            )
            return Mock(status_code=200)

        with (
            patch.object(self.data, "_load_cookie_curl_cffi", return_value=False),
            patch.object(self.session, "get", side_effect=fake_get),
            patch.object(self.session, "post", side_effect=fake_post),
            patch.object(self.data, "_save_cookie_curl_cffi", return_value=None),
        ):
            self.assertTrue(call_private(self.data, "_get_cookie_csrf", timeout=1))

    def test_gspc_timezone_bootstrap_uses_proxy_after_singleton_creation(self):
        """Index timezone bootstrap should survive late proxy configuration."""
        proxy = {"https": "http://proxy.local:8080"}
        YF_CONFIG.network.proxy = proxy
        self.session.proxies = None

        chart_response = Mock(status_code=200)
        chart_response.json.return_value = {
            "chart": {
                "result": [{"meta": {"exchangeTimezoneName": "America/New_York"}}],
                "error": None,
            }
        }

        def fake_get(**kwargs):
            self.assertEqual(self.session.proxies, proxy)
            if kwargs["url"] == "https://fc.yahoo.com":
                return Mock(status_code=200)
            if kwargs["url"] == "https://query1.finance.yahoo.com/v1/test/getcrumb":
                return Mock(status_code=200, text="crumb-2146")
            self.assertEqual(
                kwargs["url"],
                "https://query2.finance.yahoo.com/v8/finance/chart/^GSPC",
            )
            return chart_response

        timezone_cache = yf.cache.get_tz_cache()
        timezone_cache.store("^GSPC", None)

        with (
            patch.object(self.data, "_load_cookie_curl_cffi", return_value=False),
            patch.object(self.session, "get", side_effect=fake_get),
            patch.object(self.data, "_save_cookie_curl_cffi", return_value=None),
        ):
            ticker = yf.Ticker("^GSPC", session=self.session)
            timezone = call_private(ticker, "_get_ticker_tz", timeout=1)

        self.assertEqual(timezone, "America/New_York")


class TestIssue1852(ProxyNetworkIssueTestCase):
    """Verify TLS verification settings propagate alongside proxy config."""

    def test_cookie_bootstrap_uses_verify_setting(self):
        """Basic cookie bootstrap should forward verify settings."""
        YF_CONFIG.network.verify = False

        def fake_get(**kwargs):
            self.assertFalse(kwargs["verify"])
            self.assertEqual(kwargs["url"], "https://fc.yahoo.com")
            return Mock(status_code=200)

        with (
            patch.object(self.data, "_load_cookie_curl_cffi", return_value=False),
            patch.object(self.session, "get", side_effect=fake_get),
            patch.object(self.data, "_save_cookie_curl_cffi", return_value=None),
        ):
            self.assertTrue(call_private(self.data, "_get_cookie_basic", timeout=1))

    def test_csrf_cookie_flow_uses_verify_setting(self):
        """CSRF consent requests should forward verify settings."""
        YF_CONFIG.network.verify = False

        consent_response = Mock()
        consent_response.content = (
            b'<form action="/submit">'
            b'<input name="csrfToken" value="csrf-token" />'
            b'<input name="sessionId" value="session-id" />'
            b'<input type="checkbox" name="agree" value="agree" checked />'
            b"</form>"
        )
        consent_response.url = "https://guce.yahoo.com/consent"

        def fake_get(**kwargs):
            self.assertFalse(kwargs["verify"])
            if kwargs["url"] == "https://guce.yahoo.com/consent":
                return consent_response
            self.assertEqual(
                kwargs["url"],
                "https://guce.yahoo.com/copyConsent?sessionId=session-id",
            )
            return Mock(status_code=200)

        def fake_post(**kwargs):
            self.assertFalse(kwargs["verify"])
            self.assertEqual(
                kwargs["url"],
                "https://consent.yahoo.com/v2/collectConsent?sessionId=session-id",
            )
            return Mock(status_code=200)

        with (
            patch.object(self.data, "_load_cookie_curl_cffi", return_value=False),
            patch.object(self.session, "get", side_effect=fake_get),
            patch.object(self.session, "post", side_effect=fake_post),
            patch.object(self.data, "_save_cookie_curl_cffi", return_value=None),
        ):
            self.assertTrue(call_private(self.data, "_get_cookie_csrf", timeout=1))


class TestIssue2500(ProxyNetworkIssueTestCase):
    """Verify callable proxy configuration works with shared network config."""

    def test_callable_proxy_string_is_normalized_for_cookie_bootstrap(self):
        """A callable proxy returning a URL should populate both schemes."""
        expected_proxy = "http://proxy-a.local:8080"
        YF_CONFIG.network.proxy = lambda: expected_proxy
        self.session.proxies = None

        def fake_get(**kwargs):
            self.assertEqual(
                self.session.proxies,
                {"http": expected_proxy, "https": expected_proxy},
            )
            self.assertEqual(kwargs["url"], "https://fc.yahoo.com")
            return Mock(status_code=200)

        with (
            patch.object(self.data, "_load_cookie_curl_cffi", return_value=False),
            patch.object(self.session, "get", side_effect=fake_get),
            patch.object(self.data, "_save_cookie_curl_cffi", return_value=None),
        ):
            self.assertTrue(call_private(self.data, "_get_cookie_basic", timeout=1))

    def test_callable_proxy_is_resolved_per_request(self):
        """Callable proxies should be re-evaluated for each network request."""
        proxy_urls = iter(
            [
                "http://proxy-a.local:8080",
                "http://proxy-b.local:8080",
                "http://proxy-c.local:8080",
            ]
        )
        seen_proxies = []

        def proxy_provider():
            return next(proxy_urls)

        YF_CONFIG.network.proxy = proxy_provider
        self.session.proxies = None

        def fake_get(**kwargs):
            seen_proxies.append(dict(self.session.proxies))
            self.assertEqual(kwargs["url"], "https://fc.yahoo.com")
            return Mock(status_code=200)

        with (
            patch.object(self.data, "_load_cookie_curl_cffi", return_value=False),
            patch.object(self.session, "get", side_effect=fake_get),
            patch.object(self.data, "_save_cookie_curl_cffi", return_value=None),
        ):
            for _ in range(3):
                self.assertTrue(call_private(self.data, "_get_cookie_basic", timeout=1))
                setattr(self.data, "_cookie", None)

        self.assertEqual(
            seen_proxies,
            [
                {"http": "http://proxy-a.local:8080", "https": "http://proxy-a.local:8080"},
                {"http": "http://proxy-b.local:8080", "https": "http://proxy-b.local:8080"},
                {"http": "http://proxy-c.local:8080", "https": "http://proxy-c.local:8080"},
            ],
        )


class TestIssue1957(SessionTickerTestCase):
    """Verify upgrade/downgrade fetches no longer fail on reported active tickers."""

    def test_reported_404_tickers_return_upgrade_downgrade_data(self):
        """Reported active tickers should not raise 404-style failures anymore."""
        symbols = ["COF", "CRL", "COO", "FANG", "LIN", "LULU"]
        tickers = yf.Tickers(" ".join(symbols), session=self.session)

        for symbol in symbols:
            with self.subTest(symbol=symbol):
                actions = tickers.tickers[symbol].upgrades_downgrades
                self.assertIsInstance(actions, pd.DataFrame)
                self.assertFalse(actions.empty)
                self.assertTrue(
                    {"Firm", "ToGrade", "FromGrade", "Action"}.issubset(actions.columns)
                )


class TestIssue2348(unittest.TestCase):
    """Verify fast_info tolerates missing currentTradingPeriod metadata."""

    def test_fast_info_regular_market_previous_close_handles_missing_current_trading_period(self):
        """fast_info.get() should not raise when chart metadata omits currentTradingPeriod."""
        today = pd.Timestamp.now("UTC").normalize()
        prices = pd.DataFrame(
            {"Close": [220.0, 225.5]},
            index=pd.DatetimeIndex([today - pd.Timedelta(days=2), today - pd.Timedelta(days=1)]),
        )

        class FakeTicker:
            """Minimal ticker stub with chart metadata missing currentTradingPeriod."""

            def history(self, **kwargs):
                """Return cached prices and validate the expected history call shape."""
                expected = (kwargs["period"], kwargs["auto_adjust"], kwargs["keepna"])
                if expected != ("1y", False, True):
                    raise AssertionError(f"Unexpected history kwargs: {kwargs}")
                return prices

            def get_history_metadata(self):
                """Return chart metadata without currentTradingPeriod."""
                return {"exchangeTimezoneName": "America/New_York"}

            def get_info(self):
                """Return an empty info payload for the FastInfo fallback path."""
                return {}

        fast_info = FastInfo(FakeTicker())

        cached_prices = call_private(fast_info, "_get_1y_prices")
        previous_close = fast_info.get("regularMarketPreviousClose")

        pd.testing.assert_frame_equal(cached_prices, prices)
        self.assertIsNone(getattr(fast_info, "_cache")["today_open"])
        self.assertIsNone(getattr(fast_info, "_cache")["today_close"])
        self.assertIsNone(getattr(fast_info, "_cache")["today_midnight"])
        self.assertEqual(previous_close, 220.0)


class TestIssue1855(SessionTickerTestCase):
    """Verify .L suffix tickers expose currentPrice through info."""

    def test_lse_etf_info_exposes_current_price(self):
        """MOAT.L should return a usable currentPrice instead of missing the key."""
        ticker = yf.Ticker("MOAT.L", session=self.session)
        info = ticker.info

        self.assertEqual(info.get("symbol"), "MOAT.L")
        self.assertIn("currentPrice", info)
        self.assertEqual(info["currentPrice"], info.get("regularMarketPrice"))
        self.assertIsInstance(info["currentPrice"], float)


class TestIssue1951(SessionTickerTestCase):
    """Verify unavailable quotes no longer crash fast_info access."""

    def test_fast_info_handles_unavailable_quote(self):
        """fast_info access should tolerate unavailable quotes."""
        ticker = yf.Ticker("DJI", session=self.session)
        fast_info = ticker.fast_info

        for key in fast_info:
            _ = fast_info.get(key)


class TestIssue930(SessionTickerTestCase):
    """Verify AAPL dividend events are still present in history output."""

    def test_aapl_dividend_dates_2022(self):
        """Dividend dates should match between history and dividends properties."""
        ticker = yf.Ticker("AAPL", session=self.session)
        start = dt.date(2022, 1, 1)
        end = dt.date(2023, 1, 1)

        history = ticker.history(start=start, end=end, interval="1d", actions=True)
        history_dividends = [
            pd.Timestamp(value).date()
            for value in history[history["Dividends"] != 0].index.tolist()
        ]
        property_dividends = [
            pd.Timestamp(value).date()
            for value in ticker.dividends.loc[str(start):str(end)].index.tolist()
        ]

        expected = [
            dt.date(2022, 2, 4),
            dt.date(2022, 5, 6),
            dt.date(2022, 8, 5),
            dt.date(2022, 11, 4),
        ]

        self.assertEqual(history_dividends, expected)
        self.assertEqual(property_dividends, expected)

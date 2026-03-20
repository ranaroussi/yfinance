"""Issue-specific verification tests for upstream close candidates."""

import datetime as dt
from typing import Any
import unittest
import warnings
from unittest.mock import Mock, patch

import pandas as pd
from curl_cffi import requests
import yfinance.client as yf
from yfinance.config import YF_CONFIG
from yfinance.data import YfData
from yfinance.scrapers.history.price_repair import _prepare_adjusted_price_data
from yfinance.scrapers.quote import FastInfo


def call_private(obj: Any, name: str, *args: Any, **kwargs: Any) -> Any:
    """Call a private API from tests without direct protected-member syntax."""
    member = getattr(obj, name)
    if callable(member):
        return member(*args, **kwargs)
    return member


class SessionTickerTestCase(unittest.TestCase):
    """Base class for ticker tests that use the shared session."""

    session = None


def require_dataframe(
    frame: pd.DataFrame | None,
    message: str = "Expected DataFrame",
) -> pd.DataFrame:
    """Narrow optional dataframe results for test assertions."""
    if frame is None:
        raise AssertionError(message)
    return frame


def require_datetime_index(
    index: pd.Index,
    message: str = "Expected DatetimeIndex",
) -> pd.DatetimeIndex:
    """Narrow generic pandas indexes when tests require datetimes."""
    if not isinstance(index, pd.DatetimeIndex):
        raise AssertionError(message)
    return index


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


class TestSessionTickerIssues(SessionTickerTestCase):
    """Session-backed regression tests collected from reported issues."""

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

    def test_download_with_one_empty_ticker_keeps_datetime_index(self):
        """A mixed download should not degrade the combined index to object dtype."""
        data = yf.download(
            ["AAPL", "ATVI"],
            period="5d",
            interval="1m",
            group_by="column",
            auto_adjust=False,
            prepost=True,
            progress=False,
            threads=True,
        )
        data = require_dataframe(data, "yf.download() returned None")
        data_index = require_datetime_index(data.index)

        self.assertFalse(data.empty)
        self.assertIsInstance(data_index, pd.DatetimeIndex)
        self.assertEqual(str(data_index.dtype), "datetime64[s, UTC]")
        self.assertEqual(data_index.tz, dt.timezone.utc)
        self.assertFalse(data[("Close", "AAPL")].isna().all())
        self.assertTrue(data[("Close", "ATVI")].isna().all())


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


class TestSessionTickerIssueScenarios(SessionTickerTestCase):
    """Additional session-backed regression tests collected from reported issues."""

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


    def test_info_current_price_matches_fast_info_last_price_for_reported_etfs(self):
        """ETF info payloads should expose currentPrice consistently with fast_info."""
        for symbol in ["VTI", "VXUS"]:
            with self.subTest(symbol=symbol):
                ticker = yf.Ticker(symbol, session=self.session)
                info = ticker.info
                fast_info = ticker.fast_info

                self.assertIn("currentPrice", info)
                self.assertIsInstance(info["currentPrice"], float)
                self.assertAlmostEqual(info["currentPrice"], fast_info["lastPrice"], places=3)


    def test_download_mixed_timezones_with_ignore_tz_false(self):
        """Mixed-exchange downloads should normalize to one tz-aware index without raising."""
        frame = yf.download(
            ["BMW.DE", "AAPL"],
            period="5d",
            interval="5m",
            group_by="ticker",
            prepost=True,
            ignore_tz=False,
            progress=False,
            threads=False,
            session=self.session,
        )
        frame = require_dataframe(frame, "yf.download() returned None")
        frame_index = require_datetime_index(frame.index)

        self.assertIsInstance(frame, pd.DataFrame)
        self.assertFalse(frame.empty)
        self.assertIsNotNone(frame_index.tz)


    def test_reported_asianpaint_single_day_history_returns_data(self):
        """The exact reported symbol/date should return a non-empty daily history frame."""
        frame = yf.Ticker("ASIANPAINT.NS", session=self.session).history(
            start="2022-05-04",
            end="2022-05-05",
            interval="1d",
        )

        self.assertIsInstance(frame, pd.DataFrame)
        self.assertFalse(frame.empty)
        self.assertEqual(len(frame), 1)
        self.assertEqual(str(frame.index[0].date()), "2022-05-04")


    def test_history_period_with_end_date_returns_data_for_reported_symbols(self):
        """Period-plus-end requests should return non-empty data on the reported symbols."""
        for symbol in ["AAPL", "^FTSE"]:
            with self.subTest(symbol=symbol):
                frame = yf.Ticker(symbol, session=self.session).history(
                    period="5y",
                    end="2022-11-11",
                )

                self.assertIsInstance(frame, pd.DataFrame)
                self.assertFalse(frame.empty)
                self.assertLessEqual(frame.index[-1].date().isoformat(), "2022-11-10")


    def test_weekly_histories_align_for_reported_symbol_pair(self):
        """GDX and QQQ should now return the same weekly index for the reported window."""
        df1 = yf.Ticker("GDX", session=self.session).history(
            start="2014-12-29",
            end="2020-11-29",
            interval="1wk",
            auto_adjust=False,
        )
        df2 = yf.Ticker("QQQ", session=self.session).history(
            start="2014-12-29",
            end="2020-11-29",
            interval="1wk",
            auto_adjust=False,
        )

        self.assertFalse(df1.empty)
        self.assertFalse(df2.empty)
        self.assertEqual(len(df1), len(df2))
        self.assertTrue(df1.index.equals(df2.index))


    def test_reported_60d_intraday_download_returns_data(self):
        """The original COP 60d/2m download path should return data instead of an internal error."""
        frame = yf.download(
            "COP",
            period="60d",
            interval="2m",
            progress=False,
            threads=False,
            session=self.session,
        )
        frame = require_dataframe(frame, "yf.download() returned None")

        self.assertIsInstance(frame, pd.DataFrame)
        self.assertFalse(frame.empty)
        if isinstance(frame.columns, pd.MultiIndex):
            frame = frame.xs("COP", axis=1, level=1)
        self.assertTrue({"Open", "High", "Low", "Close", "Volume"}.issubset(frame.columns))


    def test_reported_start_end_download_returns_data(self):
        """
        The original CL=F start/end request should return data instead of a
        delisted-style empty frame.
        """
        frame = yf.download(
            "CL=F",
            start="2023-12-01",
            end="2023-12-31",
            interval="1d",
            progress=False,
            threads=False,
            session=self.session,
        )
        frame = require_dataframe(frame, "yf.download() returned None")
        frame_index = require_datetime_index(frame.index)

        self.assertIsInstance(frame, pd.DataFrame)
        self.assertFalse(frame.empty)
        if isinstance(frame.columns, pd.MultiIndex):
            frame = frame.xs("CL=F", axis=1, level=1)
        self.assertTrue({"Open", "High", "Low", "Close", "Volume"}.issubset(frame.columns))
        self.assertEqual(str(frame_index[0].date()), "2023-12-01")
        self.assertEqual(str(frame_index[-1].date()), "2023-12-29")


    def test_reported_monthly_and_quarterly_history_stay_populated_after_2022(self):
        """
        The reported AAPL max-range 1mo/3mo paths should keep valid OHLC rows
        in recent periods.
        """
        cases = [
            ("download", "1mo"),
            ("download", "3mo"),
            ("history", "1mo"),
            ("history", "3mo"),
        ]

        for source, interval in cases:
            with self.subTest(source=source, interval=interval):
                if source == "download":
                    frame = yf.download(
                        "AAPL",
                        period="max",
                        interval=interval,
                        actions=True,
                        progress=False,
                        threads=False,
                        session=self.session,
                    )
                    frame = require_dataframe(frame, "yf.download() returned None")
                    if isinstance(frame.columns, pd.MultiIndex):
                        frame = frame.xs("AAPL", axis=1, level=1)
                else:
                    frame = yf.Ticker("AAPL", session=self.session).history(
                        period="max",
                        interval=interval,
                        actions=True,
                    )
                frame_index = require_datetime_index(frame.index)

                self.assertIsInstance(frame, pd.DataFrame)
                self.assertFalse(frame.empty)
                self.assertTrue({"Open", "High", "Low", "Close"}.issubset(frame.columns))
                self.assertFalse(frame["Open"].tail(5).isna().any())
                self.assertFalse(frame["Close"].tail(5).isna().any())
                self.assertGreaterEqual(frame_index[-1].date().isoformat(), "2025-12-01")


    def test_history_and_download_match_for_default_and_unadjusted_paths(self):
        """The reported AAPL history/download mismatch should no longer reproduce."""
        start = "2023-01-03"
        end = "2023-02-01"

        default_history = yf.Ticker("AAPL", session=self.session).history(
            start=start,
            end=end,
            actions=False,
        )
        default_download = yf.download(
            "AAPL",
            start=start,
            end=end,
            actions=False,
            progress=False,
            threads=False,
            session=self.session,
        )
        default_download = require_dataframe(default_download, "yf.download() returned None")
        if isinstance(default_download.columns, pd.MultiIndex):
            default_download = default_download.xs("AAPL", axis=1, level=1)

        self.assertNotIn("Adj Close", default_history.columns)
        self.assertNotIn("Adj Close", default_download.columns)
        default_history_close = default_history["Close"].copy()
        default_download_close = default_download["Close"].copy()
        default_history_close.index = pd.Index([item.date() for item in default_history.index])
        default_download_index = require_datetime_index(default_download.index)
        default_download_close.index = pd.Index([item.date() for item in default_download_index])
        pd.testing.assert_series_equal(default_history_close, default_download_close)

        raw_history = yf.Ticker("AAPL", session=self.session).history(
            start=start,
            end=end,
            auto_adjust=False,
            actions=False,
        )
        raw_download = yf.download(
            "AAPL",
            start=start,
            end=end,
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=False,
            session=self.session,
        )
        raw_download = require_dataframe(raw_download, "yf.download() returned None")
        if isinstance(raw_download.columns, pd.MultiIndex):
            raw_download = raw_download.xs("AAPL", axis=1, level=1)

        self.assertTrue({"Close", "Adj Close"}.issubset(raw_history.columns))
        self.assertTrue({"Close", "Adj Close"}.issubset(raw_download.columns))
        raw_download_index = require_datetime_index(raw_download.index)
        for column in ["Close", "Adj Close"]:
            history_series = raw_history[column].copy()
            download_series = raw_download[column].copy()
            history_series.index = pd.Index([item.date() for item in raw_history.index])
            download_series.index = pd.Index([item.date() for item in raw_download_index])
            pd.testing.assert_series_equal(history_series, download_series)


    def test_start_end_window_with_empty_non_trading_range_stays_empty(self):
        """
        The reported CRSR non-trading window should return an empty frame
        rather than the prior trading day.
        """
        start = "2022-12-31"
        end = "2023-01-01"

        history_frame = yf.Ticker("CRSR", session=self.session).history(
            start=start,
            end=end,
            interval="1d",
        )
        download_frame = yf.download(
            "CRSR",
            start=start,
            end=end,
            interval="1d",
            progress=False,
            threads=False,
            session=self.session,
        )
        download_frame = require_dataframe(download_frame, "yf.download() returned None")

        self.assertIsInstance(history_frame, pd.DataFrame)
        self.assertTrue(history_frame.empty)
        self.assertEqual(list(history_frame.index), [])

        self.assertIsInstance(download_frame, pd.DataFrame)
        self.assertTrue(download_frame.empty)
        self.assertEqual(list(download_frame.index), [])


class TestFastInfoIssues(unittest.TestCase):
    """Fast-info regression tests collected from reported issues."""

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


class TestSessionTickerIssueExtras(SessionTickerTestCase):
    """Remaining session-backed regression tests collected from reported issues."""

    def test_lse_etf_info_exposes_current_price(self):
        """MOAT.L should return a usable currentPrice instead of missing the key."""
        ticker = yf.Ticker("MOAT.L", session=self.session)
        info = ticker.info

        self.assertEqual(info.get("symbol"), "MOAT.L")
        self.assertIn("currentPrice", info)
        self.assertEqual(info["currentPrice"], info.get("regularMarketPrice"))
        self.assertIsInstance(info["currentPrice"], float)


    def test_fast_info_missing_metadata_returns_none_instead_of_keyerror(self):
        """Missing history metadata should degrade to None rather than raising."""

        class FakeTicker:
            """Minimal ticker stub for unavailable-quote fast_info paths."""

            def history(self, **kwargs):
                """Return an empty price history for unavailable quotes."""
                _ = kwargs
                return pd.DataFrame(
                    columns=["Open", "High", "Low", "Close", "Adj Close", "Volume"]
                )

            def get_history_metadata(self):
                """Return empty metadata for unavailable quotes."""
                return {}

            def get_info(self):
                """Return an empty info payload for unavailable quotes."""
                return {}

            def get_shares_full(self, start=None):
                """Return no share-count data for unavailable quotes."""
                _ = start

        fast_info = FastInfo(FakeTicker())

        for key in fast_info:
            with self.subTest(key=key):
                self.assertIsNone(fast_info.get(key))


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

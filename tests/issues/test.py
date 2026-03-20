"""Issue-specific verification tests for upstream close candidates."""

import datetime as dt
from typing import cast
import unittest
import warnings
from unittest.mock import Mock, patch

import pandas as pd
from curl_cffi import requests
import yfinance.client as yf
from yfinance.config import YF_CONFIG
from yfinance.data import YfData

from ..close_candidates_support import (
    SessionTickerTestCase,
    call_private,
    make_mm_suggest_payload,
    require_dataframe,
    require_datetime_index,
)


class TestSessionTickerIssues(SessionTickerTestCase):
    """Session-backed regression tests collected from reported issues."""

    def test_indian_price_to_book_matches_price_over_book_value(self):
        """Indian ticker info should not mix a local price with a mismatched book value."""
        symbols = ["INFY.NS", "TCS.NS", "HDFCBANK.NS"]

        for symbol in symbols:
            with self.subTest(symbol=symbol):
                info = yf.Ticker(symbol, session=self.session).info
                book_value = info.get("bookValue")
                current_price = info.get("currentPrice") or info.get("regularMarketPrice")
                price_to_book = info.get("priceToBook")

                if book_value is None or current_price is None or price_to_book is None:
                    self.fail("Ticker info missing required price-to-book fields")

                self.assertNotEqual(book_value, 0)

                calculated_ratio = current_price / book_value
                self.assertAlmostEqual(price_to_book, calculated_ratio, places=5)

    def test_option_expirations_remain_fetchable_across_repeated_passes(self):
        """Listed expirations should remain fetchable on repeated option-chain requests."""
        ticker = yf.Ticker("GILD", session=self.session)

        for _ in range(3):
            expirations = ticker.options
            self.assertGreater(len(expirations), 1)

            for expiration in expirations:
                with self.subTest(expiration=expiration):
                    chain = ticker.option_chain(expiration)
                    calls = require_dataframe(chain.calls, "option_chain().calls returned None")
                    puts = require_dataframe(chain.puts, "option_chain().puts returned None")

                    self.assertFalse(calls.empty)
                    self.assertFalse(puts.empty)

    def test_large_multi_download_keeps_valid_last_row_populated(self):
        """Large downloads with failed symbols should not blank the final valid row."""
        tickers = [
            "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "BRK-B",
            "LLY", "AVGO", "JPM", "V", "MA", "XOM", "COST", "WMT", "NFLX",
            "HD", "PG", "JNJ", "ABBV", "BAC", "KO", "CVX", "MRK", "ORCL",
            "AMD", "PEP", "TMO", "CSCO", "ADBE", "MCD", "CRM", "LIN", "ACN",
            "ABT", "QCOM", "INTU", "TXN", "NOW", "IBM", "AMAT", "GE", "CAT",
            "SPGI", "PLD", "GS", "RTX", "BKNG", "ISRG", "BLK", "PGR", "SYK",
            "AXP", "AMGN", "DE", "TJX", "MDT", "GILD", "MMC", "UNP", "LOW",
            "HON", "LRCX", "ADI", "ETN", "VRTX", "MU", "SCHW", "CB", "ANET",
            "COP", "PANW", "TMUS", "KLAC", "INTC", "CMCSA", "UPS", "SO", "DHR",
            "BA", "NKE", "ELV", "C", "ADP", "MO", "ICE", "MMM", "DUK", "MCO",
            "WM", "PH", "CVS", "PYPL", "AON", "FI", "WFC", "MDLZ", "USB", "CSX",
            "PNC", "BDX", "ZTS", "REGN", "FDX", "EW", "SLB", "APD", "SHW", "EOG",
            "CL", "ITW", "BSX", "GD", "EMR", "AEP", "MAR", "MCK", "TGT", "CI",
            "ROP", "HCA", "SPY", "QQQ", "IWM", "DIA", "XLF", "XLE", "XLK", "XLV",
            "XLI", "XLP", "XLU", "XLY", "XLB", "XLRE", "ARKK", "GLD", "TLT", "IEF",
            "EFA", "EEM", "FXI", "KWEB", "INVALIDZZZ", "ATVI", "DELISTEDXYZ",
        ]
        end_date = dt.date.today()
        start_date = end_date - dt.timedelta(days=384)

        bulk = yf.download(
            tickers,
            start=start_date,
            end=end_date,
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True,
            session=self.session,
        )
        bulk = require_dataframe(bulk, "bulk yf.download() returned None")

        single = yf.download(
            "AAPL",
            start=start_date,
            end=end_date,
            auto_adjust=False,
            progress=False,
            threads=False,
            session=self.session,
        )
        single = require_dataframe(single, "single yf.download() returned None")
        single_index = require_datetime_index(single.index)
        self.assertFalse(single.empty)

        aapl = cast(
            pd.DataFrame,
            bulk["AAPL"] if isinstance(bulk.columns, pd.MultiIndex) else bulk,
        )
        last_date = single_index[-1]

        self.assertIn(last_date, aapl.index)
        self.assertFalse(aapl.loc[last_date].isna().all())
        self.assertFalse(single.loc[last_date].isna().all())

        aapl_row = cast(pd.Series, aapl.loc[last_date])
        single_row = cast(
            pd.Series,
            single.xs("AAPL", axis=1, level=1).loc[last_date].reindex(aapl_row.index),
        )
        pd.testing.assert_series_equal(
            aapl_row,
            single_row,
            check_names=False,
            check_dtype=False,
        )

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

    def test_isin_resolution_prefers_company_name_match_over_receipts(self):
        """ISIN lookup should avoid depository and fidelity variants for Paris listings."""
        payload = make_mm_suggest_payload(
            (
                "LVMH Moet Hennessy Louis Vuitton S.A.",
                "Stocks",
                "LVMHF|FR0000121014|LVMHF||LVMH",
                "75",
                "",
                "lvmhf|LVMHF|1|719",
            ),
            (
                "LVMH Moet Hennessy Louis Vuitton SE Unsponsored Canadian "
                "Depository Receipt Hedged",
                "Stocks",
                "|CA50244Q1037|||",
                "75",
                "",
                "lvmh|1|2",
            ),
            (
                "Maisons du monde",
                "Stocks",
                "MDOUF|FR0013153541|MDOUF||",
                "75",
                "",
                "mdouf|MDOUF|1|10292551",
            ),
            (
                "Air Liquide S.A.",
                "Stocks",
                "AIQUF|FR0000120073|AIQUF||AIRP",
                "75",
                "",
                "aiquf|AIQUF|1|1249",
            ),
            (
                "Air Liquide prime fidelite",
                "Stocks",
                "|FR0000053951|||",
                "75",
                "",
                "air_liquide_prime_fidelite||1|39051",
            ),
        )

        samples = {
            "MC.PA": {
                "shortName": "LVMH",
                "longName": "LVMH Moët Hennessy - Louis Vuitton, Société Européenne",
                "expected": "FR0000121014",
            },
            "MDM.PA": {
                "shortName": "MAISONS DU MONDE",
                "longName": "Maisons du Monde S.A.",
                "expected": "FR0013153541",
            },
            "AI.PA": {
                "shortName": "AIR LIQUIDE",
                "longName": "L'Air Liquide S.A.",
                "expected": "FR0000120073",
            },
        }

        for ticker_symbol, sample in samples.items():
            with self.subTest(ticker=ticker_symbol):
                ticker = yf.Ticker(ticker_symbol)
                setattr(ticker, "_isin", None)
                setattr(
                    ticker,
                    "_quote",
                    Mock(
                        info={
                            "symbol": ticker_symbol,
                            "shortName": sample["shortName"],
                            "longName": sample["longName"],
                        }
                    ),
                )
                with patch.object(
                    getattr(ticker, "_data"),
                    "cache_get",
                    return_value=Mock(text=payload),
                ):
                    self.assertEqual(ticker.get_isin(), sample["expected"])

    def test_isin_resolution_prefers_exact_symbol_match_for_share_classes(self):
        """Distinct share-class symbols should keep distinct ISINs."""
        symbol_payloads = {
            "GOOG": make_mm_suggest_payload(
                (
                    "Alphabet Inc. Cl. C",
                    "Stocks",
                    "GOOG|US02079K1079|GOOG||",
                    "75",
                    "",
                    "goog|GOOG|1|1",
                ),
                (
                    "Alphabet Inc. Cl. A",
                    "Stocks",
                    "GOOGL|US02079K3059|GOOGL||",
                    "75",
                    "",
                    "googl|GOOGL|1|2",
                ),
            ),
            "GOOGL": make_mm_suggest_payload(
                (
                    "Alphabet Inc. Cl. A",
                    "Stocks",
                    "GOOGL|US02079K3059|GOOGL||",
                    "75",
                    "",
                    "googl|GOOGL|1|2",
                ),
                (
                    "Alphabet Inc. Cl. C",
                    "Stocks",
                    "GOOG|US02079K1079|GOOG||",
                    "75",
                    "",
                    "goog|GOOG|1|1",
                ),
            ),
        }
        company_payload = make_mm_suggest_payload(
            (
                "Alphabet Inc Unsponsored Canadian Depository Receipt Hedged",
                "Stocks",
                "|CA02080M1005|||",
                "75",
                "",
                "alphabet_3||1|660881670",
            ),
            (
                "Alphabet Inc (A) Cert Deposito Arg Repr 0.034482 Shs",
                "Stocks",
                "|ARDEUT116159|||",
                "75",
                "",
                "alphabe_a_1||1|1399235",
            ),
        )

        samples = {
            "GOOG": "US02079K1079",
            "GOOGL": "US02079K3059",
        }

        for ticker_symbol, expected_isin in samples.items():
            with self.subTest(ticker=ticker_symbol):
                ticker = yf.Ticker(ticker_symbol)
                setattr(ticker, "_isin", None)
                setattr(
                    ticker,
                    "_quote",
                    Mock(
                        info={
                            "symbol": ticker_symbol,
                            "shortName": "Alphabet Inc.",
                            "longName": "Alphabet Inc.",
                        }
                    ),
                )

                def fake_cache_get(*, url, ticker_key=ticker_symbol, **_kwargs):
                    if f"query={ticker_key}" in url:
                        return Mock(text=symbol_payloads[ticker_key])
                    return Mock(text=company_payload)

                with patch.object(
                    getattr(ticker, "_data"),
                    "cache_get",
                    side_effect=fake_cache_get,
                ):
                    self.assertEqual(ticker.get_isin(), expected_isin)


class ProxyNetworkIssueTestCase(unittest.TestCase):
    """Shared setup for proxy-related regression tests."""

    def setUp(self):
        """Reset the shared singleton state before each test."""
        self.data = YfData()
        self.session = getattr(self.data, "_session")
        self.original_proxy = YF_CONFIG.network.proxy
        self.original_retries = YF_CONFIG.network.retries
        self.original_verify = YF_CONFIG.network.verify
        self.original_session_proxies = getattr(self.session, "proxies", None)
        setattr(self.data, "_cookie", None)
        setattr(self.data, "_crumb", None)
        setattr(self.data, "_cookie_strategy", "basic")

    def tearDown(self):
        """Restore global proxy and singleton session state."""
        YF_CONFIG.network.proxy = self.original_proxy
        YF_CONFIG.network.retries = self.original_retries
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


class TestIssue445(ProxyNetworkIssueTestCase):
    """Verify transient chart timeouts are retried by the shared transport."""

    def test_chart_request_retries_after_timeout(self):
        """Chart requests should retry transient timeout failures before succeeding."""
        YF_CONFIG.network.retries = 2
        success = Mock(
            status_code=200,
            url="https://query2.finance.yahoo.com/v8/finance/chart/TKA.DE",
        )

        with (
            patch.object(self.data, "_get_cookie_and_crumb", return_value=(None, "basic")),
            patch.object(
                self.session,
                "get",
                side_effect=[
                    requests.exceptions.Timeout("query timeout"),
                    requests.exceptions.Timeout("query timeout"),
                    success,
                ],
            ) as mock_get,
            patch("yfinance.data._time.sleep", return_value=None) as mock_sleep,
        ):
            response = self.data.get(
                url="https://query2.finance.yahoo.com/v8/finance/chart/TKA.DE",
                params={"range": "1mo", "interval": "1d"},
                timeout=1,
            )

        self.assertIs(response, success)
        self.assertEqual(mock_get.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)


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
        self.assertEqual(frame_index[0].strftime("%Y-%m-%d"), "2023-12-01")
        self.assertEqual(frame_index[-1].strftime("%Y-%m-%d"), "2023-12-29")

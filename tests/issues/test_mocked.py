"""Mocked issue-specific verification tests for upstream close candidates."""

from datetime import datetime, timezone
import json
import unittest
from unittest.mock import Mock, patch

import pandas as pd
from curl_cffi import requests
import yfinance as yfinance_pkg
import yfinance.client as yf
import yfinance.http.worker as yf_download_worker
from yfinance.base import TickerBase
from yfinance.data import YfData
from yfinance.scrapers.quote import Quote
from yfinance.scrapers.history.price_repair import _prepare_adjusted_price_data

from ..close_candidates_support import call_private, require_dataframe


def _make_response(payload):
    response = Mock(status_code=200)
    response.text = json.dumps(payload)
    response.json.return_value = payload
    return response


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


class TestIssue2670(unittest.TestCase):
    """Verify history parsing handles ``{'chart': None}`` payloads safely."""

    def test_history_chart_none_returns_empty_dataframe(self):
        """A ``chart=None`` response should degrade to the normal empty frame path."""
        response = Mock()
        response.text = "{}"
        response.json.return_value = {"chart": None}

        ticker = yf.Ticker("AAPL")
        history = call_private(ticker, "_lazy_load_price_history")
        client = history.get_data_client()

        with (
            patch.object(client, "get", return_value=response),
            patch.object(client, "cache_get", return_value=response),
        ):
            data = history.history(period="5d", interval="1d")

        self.assertIsInstance(data, pd.DataFrame)
        self.assertTrue(data.empty)
        self.assertListEqual(
            list(data.columns),
            ["Open", "High", "Low", "Close", "Adj Close", "Volume"],
        )


class TestIssue2333(unittest.TestCase):
    """Verify cookie-strategy fallback does not poison the shared cookie lock."""

    def setUp(self):
        """Reset cookie state before each test."""
        self.data = YfData()
        setattr(self.data, "_cookie", None)
        setattr(self.data, "_crumb", None)
        setattr(self.data, "_cookie_strategy", "csrf")

    def test_cookie_strategy_fallback_leaves_lock_usable(self):
        """A failed csrf crumb fetch should not break subsequent valid requests."""
        basic_crumbs = iter(["basic-crumb-1", "basic-crumb-2"])
        lock = call_private(self.data, "_cookie_lock")

        with (
            patch.object(self.data, "_get_crumb_csrf", return_value=None),
            patch.object(
                self.data,
                "_get_cookie_and_crumb_basic",
                side_effect=lambda timeout=30: next(basic_crumbs),
            ),
        ):
            crumb_1, strategy_1 = call_private(self.data, "_get_cookie_and_crumb", timeout=1)

            self.assertEqual(crumb_1, "basic-crumb-1")
            self.assertEqual(strategy_1, "basic")
            self.assertTrue(lock.acquire(blocking=False))
            lock.release()

            crumb_2, strategy_2 = call_private(self.data, "_get_cookie_and_crumb", timeout=1)

        self.assertEqual(crumb_2, "basic-crumb-2")
        self.assertEqual(strategy_2, "basic")
        self.assertTrue(lock.acquire(blocking=False))
        lock.release()


class TestIssue2350(unittest.TestCase):
    """Verify ``history(auto_adjust=True)`` succeeds on a normal chart payload."""

    def test_ticker_history_auto_adjust_returns_frame(self):
        """The simplified GOOGL repro should not raise ``TypeError``."""
        chart_response = Mock(status_code=200)
        chart_response.text = "{}"
        chart_response.json.return_value = {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "currency": "USD",
                            "instrumentType": "EQUITY",
                            "exchangeTimezoneName": "America/New_York",
                            "validRanges": ["1d", "5d", "1mo"],
                        },
                        "timestamp": [1739217000, 1739303400, 1739389800],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [184.0, 186.0, 187.5],
                                    "high": [185.0, 187.0, 188.0],
                                    "low": [183.5, 185.5, 186.5],
                                    "close": [184.5, 186.5, 187.0],
                                    "volume": [1000, 1200, 1100],
                                }
                            ],
                            "adjclose": [{"adjclose": [184.5, 186.5, 187.0]}],
                        },
                    }
                ],
                "error": None,
            }
        }

        ticker = yf.Ticker("GOOGL")
        with patch.object(ticker, "_get_ticker_tz", return_value="America/New_York"):
            history = call_private(ticker, "_lazy_load_price_history")
        client = history.get_data_client()

        with (
            patch.object(client, "get", return_value=chart_response),
            patch.object(client, "cache_get", return_value=chart_response),
        ):
            data = ticker.history(auto_adjust=True).astype(float).round(3)

        self.assertIsInstance(data, pd.DataFrame)
        self.assertFalse(data.empty)
        self.assertListEqual(
            list(data.columns),
            ["Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"],
        )
        self.assertAlmostEqual(float(data.iloc[0]["Close"]), 184.5)


class TestIssue2360(unittest.TestCase):
    """Verify paired downloads survive a missing timezone bootstrap."""

    def test_dual_listed_download_recovers_after_timezone_bootstrap_miss(self):
        """A missing timezone bootstrap payload should not force a failed paired download."""

        def make_chart_payload(close_price):
            return {
                "chart": {
                    "result": [
                        {
                            "meta": {
                                "currency": "AUD",
                                "instrumentType": "EQUITY",
                                "exchangeTimezoneName": "Australia/Sydney",
                                "validRanges": ["1d", "5d", "1mo"],
                            },
                            "timestamp": [1741857038],
                            "indicators": {
                                "quote": [
                                    {
                                        "open": [close_price - 0.3],
                                        "high": [close_price + 0.2],
                                        "low": [close_price - 0.5],
                                        "close": [close_price],
                                        "volume": [1000],
                                    }
                                ],
                                "adjclose": [{"adjclose": [close_price]}],
                            },
                        }
                    ],
                    "error": None,
                }
            }

        timezone_cache = yf.cache.get_tz_cache()
        timezone_cache.store("CBA.AX", None)
        timezone_cache.store("FBU.AX", None)
        request_log = []

        def fake_get_tz(data_client, symbol, timeout):
            del data_client, timeout
            request_log.append(("get_tz", symbol))
            if symbol == "CBA.AX":
                return "Australia/Sydney"
            if symbol == "FBU.AX":
                return None
            raise AssertionError(f"Unexpected ticker bootstrap: {symbol}")

        def fake_get(_data, url, params=None, timeout=30):
            del _data, timeout
            request_log.append(("get", url, dict(params or {})))
            if url.endswith("/CBA.AX"):
                return _make_response(make_chart_payload(152.4))
            if url.endswith("/FBU.AX"):
                return _make_response(make_chart_payload(17.85))
            raise AssertionError(f"Unexpected get url: {url}")

        with (
            patch.object(yf_download_worker, "get_ticker_tz", side_effect=fake_get_tz),
            patch.object(YfData, "get", autospec=True, side_effect=fake_get),
        ):
            frame = yf.download(
                ["CBA.AX", "FBU.AX"],
                period="1d",
                auto_adjust=False,
                group_by="ticker",
                progress=False,
                threads=False,
            )

        frame = require_dataframe(frame, "yf.download() returned None")
        self.assertFalse(frame.empty)
        self.assertTrue(isinstance(frame.columns, pd.MultiIndex))
        self.assertIn("CBA.AX", frame.columns.get_level_values(0))
        self.assertIn("FBU.AX", frame.columns.get_level_values(0))
        cba_frame = pd.DataFrame(frame["CBA.AX"])
        fbu_frame = pd.DataFrame(frame["FBU.AX"])
        self.assertFalse(bool(cba_frame.tail(1).isna().to_numpy().all()))
        self.assertFalse(bool(fbu_frame.tail(1).isna().to_numpy().all()))
        self.assertAlmostEqual(float(fbu_frame["Close"].to_numpy(dtype=float)[-1]), 17.85)
        self.assertIn(("get_tz", "FBU.AX"), request_log)
        self.assertIn(
            (
                "get",
                "https://query2.finance.yahoo.com/v8/finance/chart/FBU.AX",
                {
                    "range": "1d",
                    "interval": "1d",
                    "includePrePost": False,
                    "events": "div,splits,capitalGains",
                },
            ),
            request_log,
        )


class TestIssue930(unittest.TestCase):
    """Verify unsorted dividend events remain visible across public APIs."""

    def test_unsorted_aapl_dividends_stay_present_in_history_download_and_property(
        self,
    ):
        """Unsorted Yahoo dividend events should still produce the full AAPL dividend history."""

        dividend_datetimes = [
            datetime(2021, 2, 5, 14, 30, tzinfo=timezone.utc),
            datetime(2021, 5, 7, 13, 30, tzinfo=timezone.utc),
            datetime(2021, 8, 6, 13, 30, tzinfo=timezone.utc),
            datetime(2021, 11, 5, 13, 30, tzinfo=timezone.utc),
        ]
        expected_dates = [timestamp.date() for timestamp in dividend_datetimes]
        timestamps = [int(timestamp.timestamp()) for timestamp in dividend_datetimes]
        chart_payload = {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "currency": "USD",
                            "instrumentType": "EQUITY",
                            "exchangeTimezoneName": "America/New_York",
                            "firstTradeDate": timestamps[0],
                            "regularMarketTime": timestamps[-1],
                            "gmtoffset": -18000,
                            "priceHint": 2,
                            "validRanges": ["1d", "5d", "1mo", "max"],
                        },
                        "timestamp": timestamps,
                        "events": {
                            "dividends": {
                                str(timestamps[-1]): {
                                    "amount": 0.22,
                                    "date": timestamps[-1],
                                },
                                str(timestamps[1]): {
                                    "amount": 0.21,
                                    "date": timestamps[1],
                                },
                                str(timestamps[0]): {
                                    "amount": 0.205,
                                    "date": timestamps[0],
                                },
                                str(timestamps[2]): {
                                    "amount": 0.22,
                                    "date": timestamps[2],
                                },
                            }
                        },
                        "indicators": {
                            "quote": [
                                {
                                    "open": [133.0, 125.0, 145.0, 151.0],
                                    "high": [137.0, 129.0, 148.0, 154.0],
                                    "low": [130.0, 123.0, 142.0, 149.0],
                                    "close": [136.0, 127.0, 146.0, 152.0],
                                    "volume": [90000000, 80000000, 70000000, 60000000],
                                }
                            ],
                            "adjclose": [{"adjclose": [135.795, 126.79, 145.78, 152.0]}],
                        },
                    }
                ],
                "error": None,
            }
        }

        def fake_get_tz(ticker_base, timeout):
            del timeout
            if ticker_base.ticker != "AAPL":
                raise AssertionError(f"Unexpected ticker bootstrap: {ticker_base.ticker}")
            return "America/New_York"

        def fake_get(_data, url, params=None, timeout=30):
            del _data, timeout
            if url != "https://query2.finance.yahoo.com/v8/finance/chart/AAPL":
                raise AssertionError(f"Unexpected get url: {url}")
            _ = params
            return _make_response(chart_payload)

        with (
            patch.object(TickerBase, "_get_ticker_tz", autospec=True, side_effect=fake_get_tz),
            patch.object(YfData, "get", autospec=True, side_effect=fake_get),
            patch.object(YfData, "cache_get", autospec=True, side_effect=fake_get),
        ):
            ticker = yf.Ticker("AAPL")
            history = ticker.history(period="max", auto_adjust=False, actions=True)
            download = yf.download(
                "AAPL",
                period="max",
                auto_adjust=False,
                actions=True,
                progress=False,
                threads=False,
                multi_level_index=False,
            )
            dividends = ticker.dividends

        history = require_dataframe(history, "ticker.history() returned None")
        download = require_dataframe(download, "yf.download() returned None")

        self.assertEqual(
            [
                pd.Timestamp(value).date()
                for value in history.loc[history["Dividends"] != 0, "Dividends"].index.tolist()
            ],
            expected_dates,
        )
        self.assertEqual(
            [
                pd.Timestamp(value).date()
                for value in download.loc[download["Dividends"] != 0, "Dividends"].index.tolist()
            ],
            expected_dates,
        )
        self.assertEqual(
            [pd.Timestamp(value).date() for value in dividends.index.tolist()],
            expected_dates,
        )
        self.assertEqual(
            float(history.loc[:, "Dividends"].to_numpy(dtype=float)[-1]),
            0.22,
        )
        self.assertEqual(
            float(download.loc[:, "Dividends"].to_numpy(dtype=float)[-1]),
            0.22,
        )
        self.assertEqual(float(dividends.to_numpy(dtype=float)[-1]), 0.22)


class TestIssue2605(unittest.TestCase):
    """Verify quarterly balance-sheet keys survive the public pretty-label path."""

    def test_quarterly_balance_sheet_exposes_new_brkb_keys(self):
        """New balance-sheet schema keys should appear on the public quarterly view."""
        as_of_date = "2025-06-30"
        as_of_timestamp = int(
            datetime.fromisoformat(as_of_date).replace(tzinfo=timezone.utc).timestamp()
        )
        expected_rows = {
            "Fixed Maturity Investments": 101.0,
            "Equity Investments": 202.0,
            "Net Loan": 303.0,
            "Deferred Assets": 404.0,
            "Other Assets": 505.0,
        }
        payload = {
            "timeseries": {
                "result": [
                    {
                        "meta": {"symbol": "BRK-B"},
                        "timestamp": [as_of_timestamp],
                        "quarterlyFixedMaturityInvestments": [
                            {"asOfDate": as_of_date, "reportedValue": {"raw": 101.0}}
                        ],
                        "quarterlyEquityInvestments": [
                            {"asOfDate": as_of_date, "reportedValue": {"raw": 202.0}}
                        ],
                        "quarterlyNetLoan": [
                            {"asOfDate": as_of_date, "reportedValue": {"raw": 303.0}}
                        ],
                        "quarterlyDeferredAssets": [
                            {"asOfDate": as_of_date, "reportedValue": {"raw": 404.0}}
                        ],
                        "quarterlyOtherAssets": [
                            {"asOfDate": as_of_date, "reportedValue": {"raw": 505.0}}
                        ],
                    }
                ]
            }
        }

        def fake_cache_get(_data, url):
            self.assertIn("BRK-B", url)
            self.assertIn("quarterlyFixedMaturityInvestments", url)
            self.assertIn("quarterlyEquityInvestments", url)
            self.assertIn("quarterlyNetLoan", url)
            self.assertIn("quarterlyDeferredAssets", url)
            self.assertIn("quarterlyOtherAssets", url)
            return _make_response(payload)

        with patch.object(YfData, "cache_get", autospec=True, side_effect=fake_cache_get):
            frame = yf.Ticker("BRK-B").quarterly_balance_sheet

        frame = require_dataframe(frame, "ticker.quarterly_balance_sheet returned None")
        self.assertFalse(frame.empty)
        column_date = pd.Timestamp(as_of_date)
        self.assertEqual(frame.columns.tolist(), [column_date])
        self.assertEqual(set(frame.index.tolist()), set(expected_rows.keys()))
        for row_name, expected_value in expected_rows.items():
            self.assertEqual(frame.at[row_name, column_date], expected_value)


class TestIssue2601(unittest.TestCase):
    """Verify sector and industry helpers are scoped through the public package."""

    def test_top_level_sector_accepts_region_and_forwards_it_to_domain_fetch(self):
        """The public yfinance package should expose Sector and forward the region scope."""
        payload = {
            "data": {
                "name": "Technology",
                "symbol": "^TECH",
                "overview": {},
                "topCompanies": [
                    {
                        "symbol": "VOD.L",
                        "name": "Vodafone",
                        "rating": "BUY",
                        "marketWeight": {"raw": 1.23},
                    }
                ],
                "researchReports": [],
                "topETFs": [],
                "topMutualFunds": [],
                "industries": [],
            }
        }
        requests_seen = []

        def fake_get_raw_json(_data, query_url, params=None):
            requests_seen.append((query_url, dict(params or {})))
            return payload

        self.assertTrue(hasattr(yfinance_pkg, "Sector"))
        self.assertTrue(hasattr(yfinance_pkg, "Industry"))
        self.assertTrue(hasattr(yfinance_pkg, "Market"))

        with patch.object(YfData, "get_raw_json", autospec=True, side_effect=fake_get_raw_json):
            top_companies = yfinance_pkg.Sector("technology", region="GB").top_companies

        top_companies = require_dataframe(top_companies, "Sector.top_companies returned None")
        self.assertEqual(len(requests_seen), 1)
        self.assertEqual(requests_seen[0][1]["region"], "GB")
        self.assertEqual(requests_seen[0][1]["lang"], "en-US")
        self.assertIn("VOD.L", top_companies.index)


class TestIssue2570(unittest.TestCase):
    """Verify pegRatio is restored on the public info surface."""

    def test_info_restores_peg_ratio_from_key_statistics_page(self):
        """Ticker.info should expose pegRatio even when quote-summary omits it."""
        quote_summary_payload = {
            "quoteSummary": {
                "result": [
                    {
                        "symbol": "AAPL",
                        "financialData": {},
                        "quoteType": {},
                        "defaultKeyStatistics": {},
                        "assetProfile": {},
                        "summaryDetail": {},
                    }
                ],
                "error": None,
            }
        }
        trailing_peg_payload = {
            "timeseries": {
                "result": [
                    {
                        "trailingPegRatio": [
                            {"reportedValue": {"raw": 2.2115}}
                        ]
                    }
                ],
                "error": None,
            }
        }
        key_statistics_html = """
        <html><body>
          <section data-testid=\"qsp-statistics\">
            <table>
              <tr><td>PEG Ratio (5 yr expected)</td><td>2.21</td></tr>
            </table>
          </section>
        </body></html>
        """

        def fake_cache_get(_data, url):
            if "fundamentals-timeseries" in url:
                return _make_response(trailing_peg_payload)
            if "/key-statistics/" in url:
                response = Mock(status_code=200)
                response.text = key_statistics_html
                return response
            raise AssertionError(f"Unexpected cache_get url: {url}")

        with (
            patch.object(Quote, "_fetch", return_value=quote_summary_payload),
            patch.object(Quote, "_fetch_additional_info", return_value={}),
            patch.object(YfData, "cache_get", autospec=True, side_effect=fake_cache_get),
        ):
            info = yf.Ticker("AAPL").info

        self.assertIn("pegRatio", info)
        self.assertIn("trailingPegRatio", info)
        self.assertEqual(info["pegRatio"], 2.21)
        self.assertEqual(info["trailingPegRatio"], 2.2115)

    def test_info_prefers_key_statistics_page_for_peg_ratio(self):
        """Ticker.info should treat key-statistics as the primary pegRatio source."""
        quote_summary_payload = {
            "quoteSummary": {
                "result": [
                    {
                        "symbol": "AAPL",
                        "financialData": {},
                        "quoteType": {},
                        "defaultKeyStatistics": {
                            "pegRatio": {"raw": 9.99, "fmt": "9.99"},
                        },
                        "assetProfile": {},
                        "summaryDetail": {},
                    }
                ],
                "error": None,
            }
        }
        trailing_peg_payload = {
            "timeseries": {
                "result": [
                    {
                        "trailingPegRatio": [
                            {"reportedValue": {"raw": 2.2115}}
                        ]
                    }
                ],
                "error": None,
            }
        }
        key_statistics_html = """
        <html><body>
          <section data-testid=\"qsp-statistics\">
            <table>
              <tr><td>PEG Ratio (5 yr expected)</td><td>2.21</td></tr>
            </table>
          </section>
        </body></html>
        """

        def fake_cache_get(_data, url):
            if "fundamentals-timeseries" in url:
                return _make_response(trailing_peg_payload)
            if "/key-statistics/" in url:
                response = Mock(status_code=200)
                response.text = key_statistics_html
                return response
            raise AssertionError(f"Unexpected cache_get url: {url}")

        with (
            patch.object(Quote, "_fetch", return_value=quote_summary_payload),
            patch.object(Quote, "_fetch_additional_info", return_value={}),
            patch.object(YfData, "cache_get", autospec=True, side_effect=fake_cache_get),
        ):
            info = yf.Ticker("AAPL").info

        self.assertEqual(info["pegRatio"], 2.21)

    def test_info_does_not_add_empty_peg_ratio_key(self):
        """Ticker.info should omit pegRatio when key-statistics has no value."""
        quote_summary_payload = {
            "quoteSummary": {
                "result": [
                    {
                        "symbol": "EXTO",
                        "quoteType": {"quoteType": "NONE"},
                        "underlyingSymbol": "EXTO",
                        "uuid": "8a4226cf-a1a9-3f83-9bea-d6984f3a3c4d",
                        "maxAge": 1,
                    }
                ],
                "error": None,
            },
            "quoteResponse": {
                "result": [
                    {
                        "symbol": "EXTO",
                        "underlyingSymbol": "EXTO",
                        "uuid": "8a4226cf-a1a9-3f83-9bea-d6984f3a3c4d",
                        "quoteType": "NONE",
                        "maxAge": 1,
                    }
                ],
                "error": None,
            },
        }
        trailing_peg_payload = {
            "timeseries": {
                "result": [{}],
                "error": None,
            }
        }
        key_statistics_html = """
        <html><body>
          <section data-testid=\"qsp-statistics\">
            <table>
              <tr><td>PEG Ratio (5 yr expected)</td><td>N/A</td></tr>
            </table>
          </section>
        </body></html>
        """

        def fake_cache_get(_data, url):
            if "fundamentals-timeseries" in url:
                return _make_response(trailing_peg_payload)
            if "/key-statistics/" in url:
                response = Mock(status_code=200)
                response.text = key_statistics_html
                return response
            raise AssertionError(f"Unexpected cache_get url: {url}")

        with (
            patch.object(Quote, "_fetch", return_value=quote_summary_payload),
            patch.object(Quote, "_fetch_additional_info", return_value={}),
            patch.object(YfData, "cache_get", autospec=True, side_effect=fake_cache_get),
        ):
            info = yf.Ticker("EXTO").info

        self.assertNotIn("pegRatio", info)
        self.assertCountEqual(
            ["quoteType", "symbol", "underlyingSymbol", "uuid", "maxAge", "trailingPegRatio"],
            info.keys(),
        )


class TestIssue2526(unittest.TestCase):
    """Verify cached timezone lookups do not bypass request authentication setup."""

    def test_history_with_cached_timezone_still_initializes_crumb(self):
        """A cache hit for timezone should not skip crumb initialization on history fetch."""
        timezone_cache = yf.cache.get_tz_cache()
        timezone_cache.store("AAPL", "America/New_York")
        request_log = []

        response = _make_response(
            {
                "chart": {
                    "result": [
                        {
                            "meta": {
                                "currency": "USD",
                                "instrumentType": "EQUITY",
                                "exchangeTimezoneName": "America/New_York",
                                "validRanges": ["1d", "5d", "1mo"],
                                "regularMarketPrice": 190.0,
                                "currentTradingPeriod": {
                                    "pre": {
                                        "timezone": "EST",
                                        "start": 0,
                                        "end": 0,
                                        "gmtoffset": -18000,
                                    },
                                    "regular": {
                                        "timezone": "EST",
                                        "start": 0,
                                        "end": 0,
                                        "gmtoffset": -18000,
                                    },
                                    "post": {
                                        "timezone": "EST",
                                        "start": 0,
                                        "end": 0,
                                        "gmtoffset": -18000,
                                    },
                                },
                            },
                            "timestamp": [1704067200],
                            "indicators": {
                                "quote": [
                                    {
                                        "open": [1.0],
                                        "high": [1.0],
                                        "low": [1.0],
                                        "close": [1.0],
                                        "volume": [1],
                                    }
                                ],
                                "adjclose": [{"adjclose": [1.0]}],
                            },
                        }
                    ],
                    "error": None,
                }
            }
        )

        def fake_get_cookie_and_crumb(self, timeout=30):
            del self
            request_log.append(("crumb", timeout))
            return "crumb-token", "basic"

        def fake_request_with_retry(self, request_method, request_args):
            del self, request_method
            request_log.append(("request", dict(request_args.get("params", {}))))
            return response

        with (
            patch(
                "yfinance.utils_tz.fetch_ticker_tz",
                side_effect=AssertionError("timezone fetch should be skipped on cache hit"),
            ),
            patch.object(YfData, "_get_cookie_and_crumb", new=fake_get_cookie_and_crumb),
            patch.object(YfData, "_request_with_retry", new=fake_request_with_retry),
        ):
            frame = yf.Ticker("AAPL").history(period="1d")

        frame = require_dataframe(frame, "Ticker.history() returned None")

        self.assertFalse(frame.empty)
        self.assertIn(("crumb", 10), request_log)
        request_entries = [entry for entry in request_log if entry[0] == "request"]
        self.assertEqual(len(request_entries), 1)
        self.assertEqual(request_entries[0][1].get("crumb"), "crumb-token")


class TestIssue2426(unittest.TestCase):
    """Verify stale valuation data is refreshed from key-statistics."""

    def test_info_prefers_key_statistics_page_for_forward_pe(self):
        """Ticker.info should treat key-statistics as the primary forwardPE source."""
        quote_summary_payload = {
            "quoteSummary": {
                "result": [
                    {
                        "symbol": "INTC",
                        "financialData": {},
                        "quoteType": {},
                        "defaultKeyStatistics": {
                            "forwardPE": {"raw": 20.72165, "fmt": "20.72"},
                        },
                        "assetProfile": {},
                        "summaryDetail": {},
                    }
                ],
                "error": None,
            }
        }
        trailing_peg_payload = {
            "timeseries": {
                "result": [{}],
                "error": None,
            }
        }
        key_statistics_html = """
        <html><body>
          <section data-testid="qsp-statistics">
            <table>
              <tr><td>Forward P/E</td><td>63.29</td></tr>
              <tr><td>PEG Ratio (5 yr expected)</td><td>2.21</td></tr>
            </table>
          </section>
        </body></html>
        """

        def fake_cache_get(_data, url):
            if "fundamentals-timeseries" in url:
                return _make_response(trailing_peg_payload)
            if "/key-statistics/" in url:
                response = Mock(status_code=200)
                response.text = key_statistics_html
                return response
            raise AssertionError(f"Unexpected cache_get url: {url}")

        with (
            patch.object(Quote, "_fetch", return_value=quote_summary_payload),
            patch.object(Quote, "_fetch_additional_info", return_value={}),
            patch.object(YfData, "cache_get", autospec=True, side_effect=fake_cache_get),
        ):
            info = yf.Ticker("INTC").info

        self.assertEqual(info["forwardPE"], 63.29)

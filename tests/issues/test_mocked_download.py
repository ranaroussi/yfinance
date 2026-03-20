"""Additional mocked issue-specific verification tests for upstream close candidates."""

import concurrent.futures
import json
import threading
import time
from typing import cast
import unittest
from unittest.mock import Mock, patch

import pandas as pd
import yfinance as yfinance_pkg
import yfinance.client as yf
import yfinance.http.worker as yf_download_worker
from yfinance.data import YfData
from yfinance.scrapers.history import PriceHistory

from ..close_candidates_support import call_private, require_dataframe


def _make_response(payload):
    response = Mock(status_code=200)
    response.text = json.dumps(payload)
    response.json.return_value = payload
    return response


class TestIssue2557(unittest.TestCase):
    """Verify concurrent download() calls keep isolated state."""

    def test_concurrent_download_calls_do_not_overwrite_each_other(self):
        """Concurrent single-ticker downloads should return their own date windows."""

        def make_df(start_value):
            return pd.DataFrame(
                {
                    "Open": [1.0],
                    "High": [1.0],
                    "Low": [1.0],
                    "Close": [1.0],
                    "Adj Close": [1.0],
                    "Volume": [1],
                },
                index=pd.DatetimeIndex([pd.Timestamp(start_value)]),
            )

        def fake_history(self, *args, **kwargs):
            del self, args
            start = kwargs.get("start")
            if start == "2023-01-01":
                return make_df("2023-01-01")
            if start == "2022-12-01":
                return make_df("2022-12-01")
            raise AssertionError(start)

        original_create = getattr(yf_download_worker, "_create_download_dataframe")

        def patched_create(dfs, ignore_tz):
            if threading.current_thread().name == "first":
                time.sleep(0.2)
            return original_create(dfs, ignore_tz)

        def run_download(start, thread_name):
            threading.current_thread().name = thread_name
            return yf.download(
                "AAPL",
                start=start,
                end="2023-01-10",
                progress=False,
                threads=False,
                auto_adjust=False,
                multi_level_index=False,
            )

        with (
            patch.object(PriceHistory, "history", new=fake_history),
            patch.object(yf_download_worker, "get_ticker_tz", return_value="America/New_York"),
            patch.object(yf_download_worker, "_create_download_dataframe", new=patched_create),
        ):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                first_future = executor.submit(run_download, "2023-01-01", "first")
                time.sleep(0.05)
                second_future = executor.submit(run_download, "2022-12-01", "second")
                first = first_future.result()
                second = second_future.result()

        first = require_dataframe(first, "first concurrent yf.download() returned None")
        second = require_dataframe(second, "second concurrent yf.download() returned None")

        self.assertEqual(str(first.index.min().date()), "2023-01-01")
        self.assertEqual(str(second.index.min().date()), "2022-12-01")
        self.assertFalse(first.equals(second))


class TestIssue2699(unittest.TestCase):
    """Verify earnings estimate exposes forecast currency metadata."""

    def test_earnings_estimate_includes_earnings_currency(self):
        """Ticker.earnings_estimate should retain earningsCurrency from earningsTrend."""
        payload = {
            "quoteSummary": {
                "result": [
                    {
                        "earningsTrend": {
                            "trend": [
                                {
                                    "period": "0q",
                                    "earningsEstimate": {
                                        "avg": {"raw": 2.686, "fmt": "2.69"},
                                        "low": {"raw": 2.686, "fmt": "2.69"},
                                        "high": {"raw": 2.686, "fmt": "2.69"},
                                        "numberOfAnalysts": {"raw": 1, "fmt": "1"},
                                        "yearAgoEps": {},
                                        "growth": {},
                                        "earningsCurrency": "USD",
                                    },
                                },
                                {
                                    "period": "+1q",
                                    "earningsEstimate": {
                                        "avg": {"raw": 4.765, "fmt": "4.77"},
                                        "low": {"raw": 4.765, "fmt": "4.77"},
                                        "high": {"raw": 4.765, "fmt": "4.77"},
                                        "numberOfAnalysts": {"raw": 1, "fmt": "1"},
                                        "yearAgoEps": {"raw": 4.37944, "fmt": "4.38"},
                                        "growth": {"raw": 0.0880, "fmt": "8.80%"},
                                        "earningsCurrency": "USD",
                                    },
                                },
                            ]
                        }
                    }
                ],
                "error": None,
            }
        }

        with patch("yfinance.scrapers.analysis.fetch_quote_summary", return_value=payload):
            estimate = yf.Ticker("TM").earnings_estimate

        estimate = require_dataframe(estimate, "Ticker.earnings_estimate returned None")

        self.assertIn("earningsCurrency", estimate.columns)
        self.assertEqual(list(estimate["earningsCurrency"]), ["USD", "USD"])
        avg = cast(float, estimate.loc["0q", "avg"])
        number_of_analysts = cast(int, estimate.loc["+1q", "numberOfAnalysts"])
        self.assertEqual(avg, 2.686)
        self.assertEqual(number_of_analysts, 1)


class TestIssue2495(unittest.TestCase):
    """Verify invalid-cookie crumb responses are rejected cleanly."""

    def setUp(self):
        """Reset cookie state before each test."""
        self.data = YfData()
        setattr(self.data, "_cookie", None)
        setattr(self.data, "_crumb", None)
        setattr(self.data, "_cookie_strategy", "basic")

    def test_make_request_does_not_reuse_unauthorized_json_as_crumb(self):
        """A 401 invalid-cookie crumb body should not become the next request crumb."""
        invalid_cookie_body = (
            '{"finance":{"result":null,"error":{"code":"Unauthorized",'
            '"description":"Invalid Cookie"}}}'
        )
        request_log = []
        responses = iter(
            [
                Mock(status_code=401, text=invalid_cookie_body),
                Mock(status_code=200, text="fresh-basic-crumb"),
                Mock(status_code=200, text="{}"),
            ]
        )

        def fake_session_get(**kwargs):
            request_log.append((kwargs["url"], dict(kwargs.get("params", {}))))
            response = next(responses)
            if response.text == "{}":
                response.json.return_value = {"quoteSummary": {"result": [{}], "error": None}}
            return response

        with (
            patch.object(self.data, "_get_cookie_basic", return_value=True),
            patch.object(self.data, "_get_cookie_csrf", return_value=True),
            patch.object(getattr(self.data, "_session"), "get", side_effect=fake_session_get),
        ):
            response = call_private(
                self.data,
                "_make_request",
                getattr(self.data, "_session").get,
                {
                    "url": "https://query2.finance.yahoo.com/v10/finance/quoteSummary/AAPL",
                    "params": {
                        "modules": (
                            "financialData,quoteType,defaultKeyStatistics,"
                            "assetProfile,summaryDetail"
                        ),
                        "formatted": "false",
                    },
                    "timeout": 1,
                },
            )

        self.assertEqual(response.status_code, 200)
        chart_request = request_log[-1]
        self.assertEqual(
            chart_request[1].get("crumb"),
            "fresh-basic-crumb",
        )
        self.assertNotIn("Invalid Cookie", chart_request[1]["crumb"])


class TestIssue2486(unittest.TestCase):
    """Verify incompatible request_cache sessions are rejected explicitly."""

    def test_request_cache_session_is_rejected_with_clear_error(self):
        """Passing a requests_cache-style session should raise a clear YFDataException."""

        caching_session = Mock()
        caching_session.cache = object()

        with self.assertRaisesRegex(
            yfinance_pkg.YFDataException,
            "request_cache sessions don't work with curl_cffi",
        ):
            YfData(session=caching_session)


class TestIssue2353(unittest.TestCase):
    """Verify that a partial in-progress daily bar with NaN OHLC is not returned."""

    def _make_chart_payload(self, symbol, *, include_partial_bar):
        """Return a minimal chart payload.

        When *include_partial_bar* is True the final timestamp has null OHLC
        values but a non-zero volume, mimicking what Yahoo's chart API returns
        for the current in-progress trading day when called with an explicit
        period2 epoch (i.e. period='max' mid-session).
        """
        timestamps = [1739217000, 1739303400]
        opens: list[float | None] = [184.0, 186.0]
        highs: list[float | None] = [185.0, 187.0]
        lows: list[float | None] = [183.5, 185.5]
        closes: list[float | None] = [184.5, 186.5]
        adjs: list[float | None] = [184.5, 186.5]
        vols = [1_000_000, 1_200_000]

        if include_partial_bar:
            timestamps.append(1739389800)
            opens.append(None)
            highs.append(None)
            lows.append(None)
            closes.append(None)
            adjs.append(None)
            vols.append(850_000)

        return {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": symbol,
                            "currency": "USD",
                            "instrumentType": "EQUITY",
                            "exchangeTimezoneName": "America/New_York",
                            "validRanges": ["1d", "5d", "1mo", "max"],
                        },
                        "timestamp": timestamps,
                        "indicators": {
                            "quote": [
                                {
                                    "open": opens,
                                    "high": highs,
                                    "low": lows,
                                    "close": closes,
                                    "volume": vols,
                                }
                            ],
                            "adjclose": [{"adjclose": adjs}],
                        },
                    }
                ],
                "error": None,
            }
        }

    def test_single_ticker_history_drops_nan_ohlc_partial_bar(self):
        """Ticker.history() must not return today's row when OHLC are all NaN."""
        payload = self._make_chart_payload("SPY", include_partial_bar=True)

        ticker = yf.Ticker("SPY")
        with patch.object(ticker, "_get_ticker_tz", return_value="America/New_York"):
            history = call_private(ticker, "_lazy_load_price_history")
        client = history.get_data_client()

        response = _make_response(payload)
        with (
            patch.object(client, "get", return_value=response),
            patch.object(client, "cache_get", return_value=response),
        ):
            data = history.history(period="max", interval="1d", auto_adjust=True)

        data = require_dataframe(data, "Ticker.history() returned None")
        self.assertEqual(len(data), 2, "partial bar must be dropped when keepna=False")
        self.assertFalse(
            data[["Open", "High", "Low", "Close"]].isna().any(axis=None),
            "no NaN OHLC values should remain in the output",
        )

    def test_multi_ticker_download_drops_nan_ohlc_partial_bar_for_one_ticker(self):
        """download() must not expose a NaN-OHLC row when only one ticker gets the partial bar."""
        spy_payload = self._make_chart_payload("SPY", include_partial_bar=False)
        qqq_payload = self._make_chart_payload("QQQ", include_partial_bar=True)

        def fake_get_tz(data_client, symbol, timeout):
            del data_client, timeout
            self.assertIn(symbol, {"SPY", "QQQ"})
            return "America/New_York"

        def fake_get(_data, url, params=None, timeout=30):
            del _data, timeout
            if url.endswith("/SPY"):
                return _make_response(spy_payload)
            if url.endswith("/QQQ"):
                return _make_response(qqq_payload)
            raise AssertionError(f"Unexpected get url: {url}, params={params}")

        with (
            patch.object(yf_download_worker, "get_ticker_tz", side_effect=fake_get_tz),
            patch.object(YfData, "get", autospec=True, side_effect=fake_get),
        ):
            frame = yf.download(
                ["SPY", "QQQ"],
                period="max",
                group_by="ticker",
                auto_adjust=True,
                threads=False,
                progress=False,
            )

        frame = require_dataframe(frame, "yf.download() returned None")
        self.assertFalse(frame.empty)

        spy_close = frame["SPY"]["Close"]
        qqq_close = frame["QQQ"]["Close"]

        self.assertEqual(len(spy_close.dropna()), 2)
        self.assertEqual(len(qqq_close.dropna()), 2)

        qqq_open = frame["QQQ"]["Open"]
        self.assertFalse(
            qqq_open.isna().any(),
            "QQQ must not expose the partial bar with NaN OHLC",
        )

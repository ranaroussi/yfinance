"""Tests for the dataframe backend dispatch.

Covers ``YfConfig.dataframe.backend`` validation and parity of every
wrapped boundary in the codebase — using mocked-data unit tests so the
suite stays network-independent.
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock

import pandas as pd

from yfinance._backend import df_to_backend, series_to_backend
from yfinance.config import YfConfig
from yfinance.lookup import Lookup


_LOOKUP_RESPONSE = {
    "finance": {
        "result": [
            {
                "documents": [
                    {"symbol": "AAPL", "longName": "Apple Inc.", "quoteType": "EQUITY"},
                    {"symbol": "MSFT", "longName": "Microsoft", "quoteType": "EQUITY"},
                ]
            }
        ]
    }
}


def _polars_df_type():
    import polars as pl
    return pl.DataFrame


# ---------------------------------------------------------------------------
# YfConfig.dataframe.backend validation
# ---------------------------------------------------------------------------
class TestDataframeBackendConfig(unittest.TestCase):

    def tearDown(self):
        YfConfig.dataframe.backend = "pandas"

    def test_default_backend_is_pandas(self):
        self.assertEqual(YfConfig.dataframe.backend, "pandas")

    def test_polars_backend_accepted(self):
        YfConfig.dataframe.backend = "polars"
        self.assertEqual(YfConfig.dataframe.backend, "polars")

    def test_pandas_backend_accepted(self):
        YfConfig.dataframe.backend = "polars"
        YfConfig.dataframe.backend = "pandas"
        self.assertEqual(YfConfig.dataframe.backend, "pandas")

    def test_unknown_backend_raises(self):
        with self.assertRaises(ValueError):
            YfConfig.dataframe.backend = "modin"

    def test_unknown_backend_does_not_corrupt_state(self):
        try:
            YfConfig.dataframe.backend = "modin"
        except ValueError:
            pass
        self.assertEqual(YfConfig.dataframe.backend, "pandas")


# ---------------------------------------------------------------------------
# Helpers in _backend.py
# ---------------------------------------------------------------------------
class TestDfToBackend(unittest.TestCase):

    def tearDown(self):
        YfConfig.dataframe.backend = "pandas"

    def test_pandas_is_passthrough(self):
        df = pd.DataFrame({"x": [1, 2]})
        out = df_to_backend(df)
        self.assertIs(out, df)

    def test_polars_returns_polars_frame(self):
        YfConfig.dataframe.backend = "polars"
        df = pd.DataFrame({"x": [1, 2]})
        out = df_to_backend(df)
        self.assertIsInstance(out, _polars_df_type())

    def test_polars_promotes_named_index_to_column(self):
        YfConfig.dataframe.backend = "polars"
        df = pd.DataFrame({"v": [1, 2]}, index=pd.Index(["a", "b"], name="key"))
        out = df_to_backend(df)
        self.assertIn("key", out.columns)
        self.assertEqual(out["key"].to_list(), ["a", "b"])

    def test_polars_override_index_name(self):
        YfConfig.dataframe.backend = "polars"
        df = pd.DataFrame({"v": [1, 2]}, index=pd.Index(["a", "b"]))
        out = df_to_backend(df, index_as_column="Date")
        self.assertIn("Date", out.columns)

    def test_polars_rangeindex_unnamed_is_dropped(self):
        YfConfig.dataframe.backend = "polars"
        df = pd.DataFrame({"v": [1, 2]})  # default RangeIndex, no name
        out = df_to_backend(df)
        self.assertNotIn("index", out.columns)
        self.assertEqual(out["v"].to_list(), [1, 2])

    def test_polars_empty_frame(self):
        YfConfig.dataframe.backend = "polars"
        out = df_to_backend(pd.DataFrame())
        self.assertIsInstance(out, _polars_df_type())
        self.assertEqual(out.shape, (0, 0))


class TestSeriesToBackend(unittest.TestCase):

    def tearDown(self):
        YfConfig.dataframe.backend = "pandas"

    def test_pandas_is_passthrough(self):
        s = pd.Series([1, 2], name="x")
        out = series_to_backend(s)
        self.assertIs(out, s)

    def test_polars_no_index_two_columns(self):
        YfConfig.dataframe.backend = "polars"
        s = pd.Series([1.0, 2.0], index=pd.Index(["a", "b"], name="Date"), name="Dividends")
        out = series_to_backend(s, index_as_column="Date", value_name="Dividends")
        self.assertIsInstance(out, _polars_df_type())
        self.assertEqual(out.columns, ["Date", "Dividends"])
        self.assertEqual(out["Dividends"].to_list(), [1.0, 2.0])


# ---------------------------------------------------------------------------
# lookup.py
# ---------------------------------------------------------------------------
class TestLookupBackendParity(unittest.TestCase):

    def tearDown(self):
        YfConfig.dataframe.backend = "pandas"

    def test_pandas_output_uses_symbol_index(self):
        YfConfig.dataframe.backend = "pandas"
        df = Lookup._parse_response(_LOOKUP_RESPONSE)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.index.name, "symbol")
        self.assertEqual(list(df.index), ["AAPL", "MSFT"])

    def test_polars_output_keeps_symbol_column(self):
        YfConfig.dataframe.backend = "polars"
        df = Lookup._parse_response(_LOOKUP_RESPONSE)
        self.assertIsInstance(df, _polars_df_type())
        self.assertIn("symbol", df.columns)
        self.assertEqual(df["symbol"].to_list(), ["AAPL", "MSFT"])

    def test_empty_result_returns_empty_frame(self):
        for backend in ("pandas", "polars"):
            YfConfig.dataframe.backend = backend
            df = Lookup._parse_response({"finance": {"result": []}})
            self.assertEqual(len(df), 0, f"{backend}: expected empty frame")


# ---------------------------------------------------------------------------
# base.py wrappers — exercise the get_* methods directly with fake scrapers.
# ---------------------------------------------------------------------------
def _fake_ticker(scraper_attrs: dict) -> MagicMock:
    """Build a TickerBase-like mock pre-populated with scraper attributes
    so we can call get_* methods without network."""
    from yfinance.base import TickerBase
    t = TickerBase.__new__(TickerBase)
    for k, v in scraper_attrs.items():
        setattr(t, k, v)
    return t


class TestBaseGetterWrappers(unittest.TestCase):

    def tearDown(self):
        YfConfig.dataframe.backend = "pandas"

    def _ticker_with_quote(self, **kwargs):
        quote = MagicMock()
        for k, v in kwargs.items():
            setattr(quote, k, v)
        return _fake_ticker({"_quote": quote})

    def _ticker_with_holders(self, **kwargs):
        holders = MagicMock()
        for k, v in kwargs.items():
            setattr(holders, k, v)
        return _fake_ticker({"_holders": holders})

    def _ticker_with_analysis(self, **kwargs):
        analysis = MagicMock()
        for k, v in kwargs.items():
            setattr(analysis, k, v)
        return _fake_ticker({"_analysis": analysis})

    def _df(self):
        return pd.DataFrame({"a": [1, 2], "b": [3, 4]})

    def test_get_recommendations(self):
        t = self._ticker_with_quote(recommendations=self._df())
        YfConfig.dataframe.backend = "pandas"
        self.assertIsInstance(t.get_recommendations(), pd.DataFrame)
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_recommendations(), _polars_df_type())

    def test_get_upgrades_downgrades(self):
        t = self._ticker_with_quote(upgrades_downgrades=self._df())
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_upgrades_downgrades(), _polars_df_type())

    def test_get_sustainability(self):
        t = self._ticker_with_quote(sustainability=self._df())
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_sustainability(), _polars_df_type())

    def test_get_valuation_measures(self):
        t = self._ticker_with_quote(valuation_measures=self._df())
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_valuation_measures(), _polars_df_type())

    def test_get_major_holders(self):
        t = self._ticker_with_holders(major=self._df())
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_major_holders(), _polars_df_type())

    def test_get_institutional_holders(self):
        t = self._ticker_with_holders(institutional=self._df())
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_institutional_holders(), _polars_df_type())

    def test_get_mutualfund_holders(self):
        t = self._ticker_with_holders(mutualfund=self._df())
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_mutualfund_holders(), _polars_df_type())

    def test_get_insider_purchases(self):
        t = self._ticker_with_holders(insider_purchases=self._df())
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_insider_purchases(), _polars_df_type())

    def test_get_insider_transactions(self):
        t = self._ticker_with_holders(insider_transactions=self._df())
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_insider_transactions(), _polars_df_type())

    def test_get_insider_roster_holders(self):
        t = self._ticker_with_holders(insider_roster=self._df())
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_insider_roster_holders(), _polars_df_type())

    def test_get_earnings_estimate(self):
        t = self._ticker_with_analysis(earnings_estimate=self._df())
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_earnings_estimate(), _polars_df_type())

    def test_get_revenue_estimate(self):
        t = self._ticker_with_analysis(revenue_estimate=self._df())
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_revenue_estimate(), _polars_df_type())

    def test_get_earnings_history(self):
        t = self._ticker_with_analysis(earnings_history=self._df())
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_earnings_history(), _polars_df_type())

    def test_get_eps_trend(self):
        t = self._ticker_with_analysis(eps_trend=self._df())
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_eps_trend(), _polars_df_type())

    def test_get_eps_revisions(self):
        t = self._ticker_with_analysis(eps_revisions=self._df())
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_eps_revisions(), _polars_df_type())

    def test_get_growth_estimates(self):
        t = self._ticker_with_analysis(growth_estimates=self._df())
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_growth_estimates(), _polars_df_type())

    def test_as_dict_short_circuit_returns_dict(self):
        """`as_dict=True` must always return a plain dict regardless of backend."""
        t = self._ticker_with_quote(recommendations=self._df())
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(t.get_recommendations(as_dict=True), dict)


# ---------------------------------------------------------------------------
# Cache invariant: changing backend mid-session must propagate to next access.
# ---------------------------------------------------------------------------
class TestBackendSwitchInvariant(unittest.TestCase):

    def tearDown(self):
        YfConfig.dataframe.backend = "pandas"

    def test_lookup_repeated_parse_honors_current_backend(self):
        YfConfig.dataframe.backend = "pandas"
        pd_df = Lookup._parse_response(_LOOKUP_RESPONSE)
        self.assertIsInstance(pd_df, pd.DataFrame)
        YfConfig.dataframe.backend = "polars"
        pl_df = Lookup._parse_response(_LOOKUP_RESPONSE)
        self.assertIsInstance(pl_df, _polars_df_type())
        YfConfig.dataframe.backend = "pandas"
        pd_df_again = Lookup._parse_response(_LOOKUP_RESPONSE)
        self.assertIsInstance(pd_df_again, pd.DataFrame)


# ---------------------------------------------------------------------------
# calendars.py — _cleanup_df + _to_backend
# ---------------------------------------------------------------------------
class TestCalendarsBackendParity(unittest.TestCase):

    def tearDown(self):
        YfConfig.dataframe.backend = "pandas"

    def _make_calendars(self, calendar_type="sp_earnings"):
        from yfinance.calendars import Calendars
        # Synthetic raw frame matching what _create_df builds.
        if calendar_type == "sp_earnings":
            df = pd.DataFrame({
                "Symbol": ["AAPL", "MSFT"],
                "Company Name": ["Apple", "Microsoft"],
                "Market Cap (Intraday)": [1.0, 2.0],
                "Event Name": ["Q1", "Q2"],
                "Event Start Date": ["2025-01-01", "2025-02-01"],
                "Timing": ["BMO", "AMC"],
                "EPS Estimate": [1.0, 2.0],
                "Reported EPS": [1.1, 2.1],
                "Surprise (%)": [0.1, 0.05],
            })
        c = Calendars()
        c.calendars[calendar_type] = df
        return c

    def test_to_backend_pandas(self):
        c = self._make_calendars()
        df = c._to_backend("sp_earnings")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.index.name, "Symbol")

    def test_to_backend_polars_keeps_index_as_column(self):
        YfConfig.dataframe.backend = "polars"
        c = self._make_calendars()
        df = c._to_backend("sp_earnings")
        self.assertIsInstance(df, _polars_df_type())
        self.assertIn("Symbol", df.columns)


# ---------------------------------------------------------------------------
# domain/sector.py
# ---------------------------------------------------------------------------
class TestSectorBackendParity(unittest.TestCase):

    def tearDown(self):
        YfConfig.dataframe.backend = "pandas"

    def test_industries_property_switches_with_backend(self):
        from yfinance.domain.sector import Sector
        s = Sector.__new__(Sector)
        s._industries = pd.DataFrame(
            {"name": ["A", "B"], "symbol": ["X", "Y"], "market weight": [0.5, 0.5]},
            index=pd.Index(["a", "b"], name="key"),
        )
        s._ensure_fetched = lambda *_a, **_kw: None  # type: ignore[assignment]

        YfConfig.dataframe.backend = "pandas"
        self.assertIsInstance(s.industries, pd.DataFrame)
        YfConfig.dataframe.backend = "polars"
        self.assertIsInstance(s.industries, _polars_df_type())
        self.assertIn("key", s.industries.columns)


if __name__ == "__main__":
    unittest.main()

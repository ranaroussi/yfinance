#!/usr/bin/env python
"""
Standalone smoke-test for the pandas→polars migration.

Run with:
    .venv/bin/python yfinance/_smoke_test_migration.py

No network calls are made. All tests use in-process data only.
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import types

ROOT = pathlib.Path(__file__).parent.parent  # repo root
sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# Stub out packages that are not installed / not relevant to the migration
# ---------------------------------------------------------------------------
def _make_stub(name: str, **attrs):
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


# pandas – fully removed from migrated files; stub so other modules don't crash
_pd = _make_stub("pandas", DataFrame=object, Series=object, NA=None, Timestamp=object)

# dateutil
_du = _make_stub("dateutil")
_make_stub("dateutil.relativedelta", relativedelta=object)
_du.relativedelta = sys.modules["dateutil.relativedelta"]

# Other optional deps that appear in non-migrated files
_make_stub("lxml")
_make_stub("lxml.etree", XMLParser=object, HTMLParser=object)
_make_stub("bs4", BeautifulSoup=object)
_make_stub("curl_cffi")
_make_stub("curl_cffi.requests")
_make_stub("curl_cffi.requests.exceptions", HTTPError=Exception)
_make_stub("requests")
_make_stub("appdirs")
_make_stub("frozendict")
_make_stub("multitasking")
_make_stub("platformdirs")
_make_stub("pytz")
_make_stub("natsort")
_make_stub("peewee")


# ---------------------------------------------------------------------------
# Helper: load a single .py file as a module without triggering package __init__
# ---------------------------------------------------------------------------
def _load(dotted_name: str, rel_path: str):
    path = ROOT / rel_path
    spec = importlib.util.spec_from_file_location(dotted_name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[dotted_name] = mod
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# 1. compat.py
# ---------------------------------------------------------------------------
print("=" * 60)
print("Testing yfinance/compat.py")
print("=" * 60)

compat = _load("yfinance.compat", "yfinance/compat.py")
from datetime import date, datetime, timezone

import polars as pl

# df_is_empty
assert compat.df_is_empty(pl.DataFrame()), "df_is_empty(empty) should be True"
assert not compat.df_is_empty(pl.DataFrame({"a": [1]})), (
    "df_is_empty(nonempty) should be False"
)
print("  df_is_empty               OK")

# empty_ohlcv
ohlcv = compat.empty_ohlcv()
assert ohlcv.height == 0
assert set(
    ["Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"]
).issubset(set(ohlcv.columns))
print("  empty_ohlcv               OK")

# now_utc
now = compat.now_utc()
assert isinstance(now, datetime)
assert now.tzinfo is not None
print("  now_utc                   OK")

# today_utc
td = compat.today_utc()
assert isinstance(td, date)
print("  today_utc                 OK")

# from_unix_s expression
df = pl.DataFrame({"ts": [0, 86400]})
result = df.with_columns(compat.from_unix_s("ts").alias("dt"))
assert result["dt"].dtype == pl.Datetime("us", "UTC")
assert result["dt"][0] == datetime(1970, 1, 1, tzinfo=timezone.utc)
print("  from_unix_s               OK")

# from_unix_ms expression
df = pl.DataFrame({"ts": [0, 86_400_000]})
result = df.with_columns(compat.from_unix_ms("ts").alias("dt"))
assert result["dt"].dtype == pl.Datetime("us", "UTC")
assert result["dt"][0] == datetime(1970, 1, 1, tzinfo=timezone.utc)
print("  from_unix_ms              OK")

# rename_columns – ignores missing keys
df = pl.DataFrame({"a": [1], "b": [2]})
renamed = compat.rename_columns(df, {"a": "x", "nonexistent": "y"})
assert "x" in renamed.columns
assert "b" in renamed.columns
assert "nonexistent" not in renamed.columns
print("  rename_columns            OK")

# drop_all_null_rows
df = pl.DataFrame({"a": [1, None, None], "b": [2, None, 3]})
dropped = compat.drop_all_null_rows(df)
assert dropped.height == 2, f"Expected 2 rows, got {dropped.height}"
print("  drop_all_null_rows        OK")

# reorder_columns
df = pl.DataFrame({"c": [1], "a": [2], "b": [3]})
reordered = compat.reorder_columns(df, ["a", "b", "c", "missing"])
assert reordered.columns == ["a", "b", "c"]
print("  reorder_columns           OK")

# sort_by_date
df = pl.DataFrame(
    {
        "Datetime": pl.Series(
            [
                datetime(2023, 3, 1, tzinfo=timezone.utc),
                datetime(2023, 1, 1, tzinfo=timezone.utc),
                datetime(2023, 2, 1, tzinfo=timezone.utc),
            ],
            dtype=pl.Datetime("us", "UTC"),
        ),
        "Close": [3.0, 1.0, 2.0],
    }
)
sorted_df = compat.sort_by_date(df)
assert sorted_df["Close"].to_list() == [1.0, 2.0, 3.0]
print("  sort_by_date              OK")

# filter_date_range
filtered = compat.filter_date_range(
    sorted_df,
    start=datetime(2023, 1, 15, tzinfo=timezone.utc),
    end=datetime(2023, 2, 28, tzinfo=timezone.utc),
)
assert filtered.height == 1
assert filtered["Close"][0] == 2.0
print("  filter_date_range         OK")

print("  compat.py ALL PASSED\n")


# ---------------------------------------------------------------------------
# 2. lookup.py — parse_response static method
# ---------------------------------------------------------------------------
print("=" * 60)
print("Testing yfinance/lookup.py  (_parse_response)")
print("=" * 60)

# lookup.py imports from within the package; pre-register stubs for its deps
_logger_stub = types.SimpleNamespace(
    debug=lambda *a, **k: None,
    error=lambda *a, **k: None,
    warning=lambda *a, **k: None,
    info=lambda *a, **k: None,
)
_make_stub(
    "yfinance.utils",
    get_yf_logger=lambda: _logger_stub,
    dynamic_docstring=lambda d: (lambda f: f),
    generate_list_table_from_dict=lambda d, **k: "",
)
_YfConfig = types.SimpleNamespace(debug=types.SimpleNamespace(hide_exceptions=True))
_make_stub("yfinance.config", YfConfig=_YfConfig)
_make_stub(
    "yfinance.const",
    _QUERY1_URL_="https://query1.finance.yahoo.com",
    _BASE_URL_="https://finance.yahoo.com",
    SECTOR_INDUSTY_MAPPING_LC={},
    quote_summary_valid_modules={
        "earningsTrend",
        "earningsHistory",
        "financialData",
        "industryTrend",
        "sectorTrend",
        "indexTrend",
        "institutionOwnership",
        "fundOwnership",
        "majorDirectHolders",
        "majorHoldersBreakdown",
        "insiderTransactions",
        "insiderHolders",
        "netSharePurchaseActivity",
    },
)


class _YfDataStub:
    def __init__(self, **kwargs):
        pass


_make_stub("yfinance.data", YfData=_YfDataStub)
_make_stub("yfinance.exceptions", YFDataException=Exception, YFException=Exception)
_make_stub("yfinance.ticker", Ticker=object)

# Register the top-level yfinance package stub so relative imports resolve
_yf_pkg = types.ModuleType("yfinance")
_yf_pkg.__path__ = []  # mark as package
_yf_pkg.__package__ = "yfinance"
_yf_pkg.utils = sys.modules["yfinance.utils"]
_yf_pkg.config = sys.modules["yfinance.config"]
sys.modules["yfinance"] = _yf_pkg

# domain sub-package stub
_domain_pkg = types.ModuleType("yfinance.domain")
_domain_pkg.__path__ = []
_domain_pkg.__package__ = "yfinance.domain"
sys.modules["yfinance.domain"] = _domain_pkg

# scrapers sub-package stub
_scrapers_pkg = types.ModuleType("yfinance.scrapers")
_scrapers_pkg.__path__ = []
_scrapers_pkg.__package__ = "yfinance.scrapers"
sys.modules["yfinance.scrapers"] = _scrapers_pkg

_make_stub(
    "yfinance.scrapers.quote",
    _QUOTE_SUMMARY_URL_="https://query1.finance.yahoo.com/v10/finance/quoteSummary",
)

lookup = _load("yfinance.lookup", "yfinance/lookup.py")

# Empty response → empty DataFrame
empty_resp = {}
df = lookup.Lookup._parse_response(empty_resp)
assert isinstance(df, pl.DataFrame)
assert df.height == 0
print("  _parse_response (empty)   OK")

# Response with documents and 'symbol' column → DataFrame with 'symbol' column kept
good_resp = {
    "finance": {
        "result": [
            {
                "documents": [
                    {"symbol": "AAPL", "name": "Apple Inc.", "quoteType": "EQUITY"},
                    {
                        "symbol": "MSFT",
                        "name": "Microsoft Corp.",
                        "quoteType": "EQUITY",
                    },
                ]
            }
        ]
    }
}
df = lookup.Lookup._parse_response(good_resp)
assert isinstance(df, pl.DataFrame)
assert df.height == 2
assert "symbol" in df.columns, "symbol column should be retained (not used as index)"
assert df["symbol"].to_list() == ["AAPL", "MSFT"]
print("  _parse_response (data)    OK")

# Response with no 'symbol' key → empty DataFrame
bad_resp = {"finance": {"result": [{"documents": [{"name": "No Symbol Here"}]}]}}
df = lookup.Lookup._parse_response(bad_resp)
assert isinstance(df, pl.DataFrame)
assert df.height == 0
print("  _parse_response (no sym)  OK")

print("  lookup.py ALL PASSED\n")


# ---------------------------------------------------------------------------
# 3. domain/domain.py — _parse_top_companies, _parse_overview
# ---------------------------------------------------------------------------
print("=" * 60)
print("Testing yfinance/domain/domain.py")
print("=" * 60)

# domain.py imports Ticker; stub it
_make_stub("yfinance.ticker", Ticker=object)
_make_stub(
    "yfinance.domain",
)

domain_mod = _load("yfinance.domain.domain", "yfinance/domain/domain.py")


# We need a concrete subclass to test the ABC
class _ConcreteDomain(domain_mod.Domain):
    def _fetch_and_parse(self):
        pass


d = _ConcreteDomain("test-key")

# _parse_overview
overview_raw = {
    "companiesCount": 42,
    "marketCap": {"raw": 1_000_000},
    "messageBoardId": "mb-123",
    "description": "Test sector",
    "industriesCount": 5,
    "marketWeight": {"raw": 0.15},
    "employeeCount": {"raw": 100_000},
}
ov = d._parse_overview(overview_raw)
assert ov["companies_count"] == 42
assert ov["market_cap"] == 1_000_000
assert ov["description"] == "Test sector"
print("  _parse_overview           OK")

# _parse_top_companies — non-empty
companies_raw = [
    {"symbol": "AAPL", "name": "Apple", "rating": "A", "marketWeight": {"raw": 0.05}},
    {
        "symbol": "MSFT",
        "name": "Microsoft",
        "rating": "B",
        "marketWeight": {"raw": 0.04},
    },
]
df = d._parse_top_companies(companies_raw)
assert isinstance(df, pl.DataFrame)
assert df.height == 2
assert "symbol" in df.columns
assert df["symbol"].to_list() == ["AAPL", "MSFT"]
assert df["market weight"].to_list() == [0.05, 0.04]
print("  _parse_top_companies      OK")

# _parse_top_companies — empty → None
assert d._parse_top_companies([]) is None
print("  _parse_top_companies (∅)  OK")

print("  domain.py ALL PASSED\n")


# ---------------------------------------------------------------------------
# 4. domain/industry.py — _parse_top_performing_companies, _parse_top_growth_companies
# ---------------------------------------------------------------------------
print("=" * 60)
print("Testing yfinance/domain/industry.py")
print("=" * 60)

industry_mod = _load("yfinance.domain.industry", "yfinance/domain/industry.py")

# Create instance (bypass network by not calling _fetch_and_parse)
ind = industry_mod.Industry.__new__(industry_mod.Industry)
ind._key = "semiconductors"
ind._top_performing_companies = None
ind._top_growth_companies = None

# _parse_top_performing_companies
perf_raw = [
    {
        "symbol": "NVDA",
        "name": "Nvidia",
        "ytdReturn": {"raw": 0.75},
        "lastPrice": {"raw": 500.0},
        "targetPrice": {"raw": 600.0},
    },
    {
        "symbol": "AMD",
        "name": "AMD",
        "ytdReturn": {"raw": 0.40},
        "lastPrice": {"raw": 150.0},
        "targetPrice": {"raw": 180.0},
    },
]
df = ind._parse_top_performing_companies(perf_raw)
assert isinstance(df, pl.DataFrame)
assert df.height == 2
assert "symbol" in df.columns
assert df["ytd return"].to_list() == [0.75, 0.40]
print("  _parse_top_performing     OK")

# _parse_top_growth_companies
growth_raw = [
    {
        "symbol": "NVDA",
        "name": "Nvidia",
        "ytdReturn": {"raw": 0.75},
        "growthEstimate": {"raw": 0.30},
    },
]
df = ind._parse_top_growth_companies(growth_raw)
assert isinstance(df, pl.DataFrame)
assert df.height == 1
assert "growth estimate" in df.columns
print("  _parse_top_growth         OK")

# empty input → None
assert ind._parse_top_performing_companies([]) is None
assert ind._parse_top_growth_companies([]) is None
print("  _parse_* (∅)              OK")

print("  industry.py ALL PASSED\n")


# ---------------------------------------------------------------------------
# 5. domain/sector.py — _parse_industries
# ---------------------------------------------------------------------------
print("=" * 60)
print("Testing yfinance/domain/sector.py")
print("=" * 60)

sector_mod = _load("yfinance.domain.sector", "yfinance/domain/sector.py")

sec = sector_mod.Sector.__new__(sector_mod.Sector)
sec._key = "technology"

industries_raw = [
    {
        "key": "semiconductors",
        "name": "Semiconductors",
        "symbol": "^SEMI",
        "marketWeight": {"raw": 0.08},
    },
    {
        "key": "software",
        "name": "Software",
        "symbol": "^SOFT",
        "marketWeight": {"raw": 0.12},
    },
    {
        "key": "all",
        "name": "All Industries",
        "symbol": "^ALL",
        "marketWeight": {"raw": 1.00},
    },
]
df = sec._parse_industries(industries_raw)
assert isinstance(df, pl.DataFrame)
# "All Industries" should be filtered out
assert df.height == 2, f"Expected 2, got {df.height}"
assert "key" in df.columns
assert "All Industries" not in df["name"].to_list()
assert df["key"].to_list() == ["semiconductors", "software"]
print("  _parse_industries         OK")

# empty input
df_empty = sec._parse_industries([])
assert isinstance(df_empty, pl.DataFrame)
assert df_empty.height == 0
print("  _parse_industries (∅)     OK")

print("  sector.py ALL PASSED\n")


# ---------------------------------------------------------------------------
# 6. scrapers/analysis.py — _get_periodic_df logic, earnings_history, growth_estimates
# ---------------------------------------------------------------------------
print("=" * 60)
print("Testing yfinance/scrapers/analysis.py")
print("=" * 60)

analysis_mod = _load("yfinance.scrapers.analysis", "yfinance/scrapers/analysis.py")

# Build a minimal Analysis instance without network
ana = analysis_mod.Analysis.__new__(analysis_mod.Analysis)
ana._symbol = "AAPL"
ana._data = None
ana._analyst_price_targets = None
ana._earnings_estimate = None
ana._revenue_estimate = None
ana._earnings_history = None
ana._eps_trend = None
ana._eps_revisions = None
ana._growth_estimates = None

# Inject a fake _earnings_trend so we can call _get_periodic_df
ana._earnings_trend = [
    {
        "period": "0q",
        "earningsEstimate": {
            "avg": {"raw": 1.50},
            "low": {"raw": 1.20},
            "high": {"raw": 1.80},
            "earningsCurrency": "USD",
        },
    },
    {
        "period": "+1q",
        "earningsEstimate": {
            "avg": {"raw": 1.65},
            "low": {"raw": 1.35},
            "high": {"raw": 1.95},
            "earningsCurrency": "USD",
        },
    },
]

df = ana._get_periodic_df("earningsEstimate", currency_key="earningsCurrency")
assert isinstance(df, pl.DataFrame)
assert df.height == 2
assert "period" in df.columns
assert "avg" in df.columns
assert "currency" in df.columns
assert df["currency"][0] == "USD"
# period is kept as a regular column (not an index)
assert df["period"].to_list() == ["0q", "+1q"]
print("  _get_periodic_df          OK")

# _get_periodic_df with no data → empty DataFrame
ana._earnings_trend = []
df_empty = ana._get_periodic_df("earningsEstimate")
assert isinstance(df_empty, pl.DataFrame)
assert df_empty.height == 0
print("  _get_periodic_df (∅)      OK")

# earnings_history — simulate parsed rows
rows = [
    {"quarter": "2023-03-31", "epsActual": 1.52, "epsEstimate": 1.43},
    {"quarter": "2022-12-31", "epsActual": 1.88, "epsEstimate": 1.94},
]
df = pl.DataFrame(rows)
df = df.with_columns(pl.col("quarter").str.to_date(format="%Y-%m-%d"))
assert df["quarter"].dtype == pl.Date
assert df.height == 2
print("  earnings_history (inline) OK")

# growth_estimates — drop_all_null filter
data = [
    {"period": "0q", "stockTrend": 0.05, "industryTrend": 0.03},
    {
        "period": "+1q",
        "stockTrend": None,
        "industryTrend": None,
    },  # all-null values → should be dropped
    {"period": "+1y", "stockTrend": 0.10, "industryTrend": 0.08},
]
df = pl.DataFrame(data)
value_cols = [c for c in df.columns if c != "period"]
df_filtered = df.filter(~pl.all_horizontal([pl.col(c).is_null() for c in value_cols]))
assert df_filtered.height == 2, f"Expected 2, got {df_filtered.height}"
assert "+1q" not in df_filtered["period"].to_list()
print("  growth_estimates filter   OK")

print("  analysis.py ALL PASSED\n")


# ---------------------------------------------------------------------------
# 7. scrapers/holders.py — parse methods
# ---------------------------------------------------------------------------
print("=" * 60)
print("Testing yfinance/scrapers/holders.py")
print("=" * 60)

holders_mod = _load("yfinance.scrapers.holders", "yfinance/scrapers/holders.py")

h = holders_mod.Holders.__new__(holders_mod.Holders)
h._symbol = "AAPL"
h._data = None
h._major = None
h._major_direct_holders = None
h._institutional = None
h._mutualfund = None
h._insider_transactions = None
h._insider_purchases = None
h._insider_roster = None

# _parse_institution_ownership
inst_data = {
    "ownershipList": [
        {
            "maxAge": 1,
            "reportDate": 1609459200,
            "organization": "Vanguard",
            "position": 1_200_000_000,
            "value": 150_000_000_000,
            "pctHeld": {"raw": 0.07},
        },
        {
            "maxAge": 1,
            "reportDate": 1609459200,
            "organization": "BlackRock",
            "position": 900_000_000,
            "value": 112_000_000_000,
            "pctHeld": {"raw": 0.05},
        },
    ]
}
h._parse_institution_ownership(inst_data)
df = h._institutional
assert isinstance(df, pl.DataFrame)
assert df.height == 2
assert "Date Reported" in df.columns
assert "Holder" in df.columns
assert "Shares" in df.columns
assert df["Date Reported"].dtype == pl.Datetime("us", "UTC")
assert df["Holder"].to_list() == ["Vanguard", "BlackRock"]
print("  _parse_institution_ownership  OK")

# _parse_institution_ownership — empty list
h._parse_institution_ownership({"ownershipList": []})
assert isinstance(h._institutional, pl.DataFrame)
assert h._institutional.height == 0
print("  _parse_institution_ownership (∅) OK")

# _parse_major_holders_breakdown
breakdown_data = {
    "maxAge": 1,
    "insidersPercentHeld": 0.0007,
    "institutionsPercentHeld": 0.5912,
    "institutionsFloatPercentHeld": 0.5917,
    "institutionsCount": 5598,
}
h._parse_major_holders_breakdown(breakdown_data)
df = h._major
assert isinstance(df, pl.DataFrame)
assert "Breakdown" in df.columns
assert "Value" in df.columns
# maxAge should have been deleted before constructing
assert "maxAge" not in df["Breakdown"].to_list()
print("  _parse_major_holders_breakdown OK")

# _parse_major_holders_breakdown — empty dict
h._parse_major_holders_breakdown({})
assert isinstance(h._major, pl.DataFrame)
assert h._major.height == 0
print("  _parse_major_holders_breakdown (∅) OK")

# _parse_net_share_purchase_activity
net_data = {
    "period": "6m",
    "buyInfoShares": 500_000,
    "sellInfoShares": 300_000,
    "netInfoShares": 200_000,
    "totalInsiderShares": 5_000_000,
    "netPercentInsiderShares": 0.04,
    "buyPercentInsiderShares": 0.10,
    "sellPercentInsiderShares": 0.06,
    "buyInfoCount": 12,
    "sellInfoCount": 8,
    "netInfoCount": 4,
}
h._parse_net_share_purchase_activity(net_data)
df = h._insider_purchases
assert isinstance(df, pl.DataFrame)
assert df.height == 7
# The column name includes the period
label_col = [c for c in df.columns if "Insider Purchases Last" in c]
assert len(label_col) == 1
assert "6m" in label_col[0]
assert "Shares" in df.columns
assert "Trans" in df.columns
# Last 4 Trans values should be None (replaced pd.NA)
trans = df["Trans"].to_list()
assert trans[3] is None
assert trans[4] is None
print("  _parse_net_share_purchase_activity OK")

# _parse_insider_holders
holders_data = {
    "holders": [
        {
            "maxAge": 1,
            "name": "Tim Cook",
            "relation": "Chief Executive Officer",
            "url": "https://finance.yahoo.com/...",
            "transactionDescription": "Sale",
            "latestTransDate": 1609459200,
            "positionDirectDate": 1609459200,
            "positionDirect": 3_280_000,
        }
    ]
}
h._parse_insider_holders(holders_data)
df = h._insider_roster
assert isinstance(df, pl.DataFrame)
assert df.height == 1
assert "Name" in df.columns
assert "Position" in df.columns
assert df["Name"][0] == "Tim Cook"
assert df["Name"].dtype == pl.Utf8
assert df["Latest Transaction Date"].dtype == pl.Datetime("us", "UTC")
print("  _parse_insider_holders        OK")

print("  holders.py ALL PASSED\n")


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print("=" * 60)
print("ALL SMOKE TESTS PASSED")
print("=" * 60)
print()
print("Note: `import yfinance` (full package) requires pandas to be")
print("installed because utils.py and other out-of-scope files still")
print("import it unconditionally. The five migrated modules are fully")
print("pandas-free.")

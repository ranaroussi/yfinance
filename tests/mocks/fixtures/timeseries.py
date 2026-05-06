"""
Factory for financial-statement timeseries endpoints:
  - GET https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{ticker}
  - GET https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{ticker}
"""

import datetime
import json

from ..response import MockResponse

# Four years of annual dates and five quarters
_ANNUAL_DATES = ["2023-12-31", "2022-12-31", "2021-12-31", "2020-12-31"]
_QUARTERLY_DATES = ["2024-09-30", "2024-06-30", "2024-03-31", "2023-12-31", "2023-09-30"]
_TRAILING_DATES = ["2024-09-30"]

# Tickers that have no trailing PEG ratio data — indices, crypto, and certain
# foreign equities where Yahoo Finance does not publish this metric.
_NO_PEG_RATIO_TICKERS = frozenset({"ESLT.TA", "^GSPC", "BTC-USD", "^DJI", "DJI"})


def _date_to_ts(date_str):
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp())


def _make_items(timescale, dates, key, values):
    """Build a list of timeseries data items."""
    return [
        {
            "asOfDate": d,
            "periodType": "12M" if timescale == "annual" else "3M",
            "reportedValue": {"raw": v},
        }
        for d, v in zip(dates, values)
    ]


def _pick_dates(timescale):
    if timescale == "annual":
        return _ANNUAL_DATES
    if timescale == "trailing":
        return _TRAILING_DATES
    return _QUARTERLY_DATES


# Income-statement metrics and their approximate annual values
_INCOME_VALUES = {
    "TotalRevenue":              [383_285_000_000, 394_328_000_000, 365_817_000_000, 274_515_000_000],
    "GrossProfit":               [169_148_000_000, 170_782_000_000, 152_836_000_000, 104_956_000_000],
    "OperatingIncome":           [114_301_000_000, 119_437_000_000, 108_949_000_000,  66_288_000_000],
    "NetIncome":                 [ 97_000_000_000,  99_803_000_000,  94_680_000_000,  57_411_000_000],
    "EBIT":                      [114_301_000_000, 119_437_000_000, 108_949_000_000,  66_288_000_000],
    "EBITDA":                    [123_000_000_000, 130_000_000_000, 120_000_000_000,  77_000_000_000],
    "BasicEPS":                  [6.13, 6.11, 5.67, 3.31],
    "DilutedEPS":                [6.13, 6.11, 5.67, 3.28],
    "BasicAverageShares":        [15_744_231_000, 16_215_963_000, 16_701_272_000, 17_352_119_000],
    "DilutedAverageShares":      [15_812_547_000, 16_325_819_000, 16_864_919_000, 17_528_214_000],
    "TotalOperatingExpenses":    [268_984_000_000, 274_891_000_000, 256_868_000_000, 208_227_000_000],
    "ResearchAndDevelopment":    [ 29_915_000_000,  26_251_000_000,  21_914_000_000,  18_752_000_000],
    "SellingGeneralAndAdministration": [24_932_000_000, 25_094_000_000, 21_973_000_000, 19_916_000_000],
    "InterestExpense":           [ -3_933_000_000,  -2_931_000_000,  -2_645_000_000,  -2_873_000_000],
    "TaxProvision":              [ 29_749_000_000,  19_300_000_000,  14_527_000_000,   9_680_000_000],
    "PretaxIncome":              [116_000_000_000, 119_103_000_000, 109_207_000_000,  67_091_000_000],
    "IncomeTaxRate":             [0.256, 0.162, 0.133, 0.144],
    "NetIncomeCommonStockholders": [97_000_000_000, 99_803_000_000, 94_680_000_000, 57_411_000_000],
    "OtherIncomeExpense":        [  -565_000_000,    334_000_000,    258_000_000,   -803_000_000],
    "CostOfRevenue":             [214_137_000_000, 223_546_000_000, 212_981_000_000, 169_559_000_000],
    "NormalizedIncome":          [ 97_000_000_000,  99_803_000_000,  94_680_000_000,  57_411_000_000],
    "InterestIncomeNonOperating": [3_750_000_000,  2_825_000_000,   1_571_000_000,     973_000_000],
    "TotalUnusualItems":         [0, 0, 0, 0],
    "TotalUnusualItemsExcludingGoodwill": [0, 0, 0, 0],
    "NetIncomeIncludingNoncontrollingInterests": [97_000_000_000, 99_803_000_000, 94_680_000_000, 57_411_000_000],
    "NetIncomeDiscontinuousOperations": [0, 0, 0, 0],
    "MinorityInterests":         [0, 0, 0, 0],
    "OtherNonOperatingIncomeExpenses": [-565_000_000, 334_000_000, 258_000_000, -803_000_000],
    "ReconciledDepreciation":    [11_519_000_000, 11_104_000_000, 10_051_000_000,  9_138_000_000],
    "ReconciledCostOfRevenue":   [214_137_000_000, 223_546_000_000, 212_981_000_000, 169_559_000_000],
    "NormalizedEBITDA":          [123_000_000_000, 130_000_000_000, 120_000_000_000, 77_000_000_000],
    "TaxRateForCalcs":           [0.256, 0.162, 0.133, 0.144],
    "TaxEffectOfUnusualItems":   [0, 0, 0, 0],
}

_BALANCE_VALUES = {
    "TotalAssets":               [352_583_000_000, 352_755_000_000, 351_002_000_000, 323_888_000_000],
    "TotalLiabilitiesNetMinorityInterest": [290_437_000_000, 302_083_000_000, 287_912_000_000, 258_549_000_000],
    "StockholdersEquity":        [ 62_146_000_000,  50_672_000_000,  63_090_000_000,  65_339_000_000],
    "TotalDebt":                 [110_000_000_000, 120_000_000_000, 126_000_000_000, 112_000_000_000],
    "NetDebt":                   [ 48_445_000_000,  68_000_000_000,  78_000_000_000,  61_000_000_000],
    "CashAndCashEquivalents":    [ 30_000_000_000,  23_646_000_000,  17_635_000_000,  37_119_000_000],
    "CashCashEquivalentsAndShortTermInvestments": [61_555_000_000, 48_304_000_000, 62_639_000_000, 90_518_000_000],
    "CurrentAssets":             [ 97_000_000_000, 135_405_000_000, 134_836_000_000, 143_713_000_000],
    "CurrentLiabilities":        [145_308_000_000, 153_982_000_000, 125_481_000_000, 105_392_000_000],
    "WorkingCapital":            [-48_308_000_000, -18_577_000_000,   9_355_000_000,  38_321_000_000],
    "OrdinarySharesNumber":      [15_550_000_000, 15_943_425_000, 16_426_786_000, 17_102_786_000],
    "ShareIssuedCapital":        [73_812_000_000, 64_849_000_000, 57_365_000_000, 50_779_000_000],
    "RetainedEarnings":          [-214_000_000, -3_068_000_000, 5_562_000_000, 14_966_000_000],
    "GainsLossesNotAffectingRetainedEarnings": [-11_452_000_000, -11_109_000_000, 163_000_000, -406_000_000],
    "AccountsPayable":           [ 62_611_000_000,  64_115_000_000,  54_763_000_000,  42_296_000_000],
    "Inventory":                 [  6_331_000_000,   4_946_000_000,   6_580_000_000,   4_061_000_000],
    "TotalNonCurrentAssets":     [255_583_000_000, 217_350_000_000, 216_166_000_000, 180_175_000_000],
    "TotalNonCurrentLiabilitiesNetMinorityInterest": [145_129_000_000, 148_101_000_000, 162_431_000_000, 153_157_000_000],
    "LongTermDebt":              [ 95_281_000_000,  98_959_000_000, 109_106_000_000,  98_667_000_000],
    "NetPPE":                    [ 43_715_000_000,  42_117_000_000,  39_440_000_000,  36_766_000_000],
}

_CASHFLOW_VALUES = {
    "FreeCashFlow":              [ 84_726_000_000,  90_215_000_000,  92_953_000_000,  73_365_000_000],
    "OperatingCashFlow":         [113_260_000_000, 122_151_000_000, 104_038_000_000,  80_674_000_000],
    "CapitalExpenditure":        [-10_959_000_000,  -10_708_000_000, -11_085_000_000,  -7_309_000_000],
    "InvestingActivities":       [ -3_936_000_000,  -22_354_000_000, -14_545_000_000, -33_774_000_000],
    "FinancingActivities":       [-108_488_000_000, -110_749_000_000, -93_353_000_000, -86_820_000_000],
    "DividendsPaid":             [-15_025_000_000,  -14_841_000_000, -14_467_000_000, -14_081_000_000],
    "RepurchaseOfCapitalStock":  [-77_550_000_000,  -89_402_000_000, -85_971_000_000, -72_358_000_000],
    "IssuanceOfDebt":            [  5_228_000_000,   9_963_000_000,  20_393_000_000,  16_091_000_000],
    "RepaymentOfDebt":           [-11_151_000_000,  -9_543_000_000, -19_517_000_000, -12_629_000_000],
    "ChangesInCash":             [    736_000_000,  -10_952_000_000,  -3_860_000_000, -39_920_000_000],
    "NetIncome":                 [ 97_000_000_000,  99_803_000_000,  94_680_000_000,  57_411_000_000],
    "DepreciationAmortizationDepletion": [11_519_000_000, 11_104_000_000, 10_051_000_000, 9_138_000_000],
    "ChangeInWorkingCapital":    [-1_742_000_000,  9_240_000_000, -4_911_000_000,  14_061_000_000],
    "StockBasedCompensation":    [10_833_000_000,  9_038_000_000,  7_906_000_000,  6_829_000_000],
    "DeferredTax":               [  -227_000_000,  1_484_000_000,    576_000_000,  -3_003_000_000],
    "OtherNonCashItems":         [-4_123_000_000,  -7_978_000_000, -4_300_000_000,  -3_601_000_000],
    "NetPPEPurchaseAndSale":     [-10_959_000_000, -10_708_000_000, -11_085_000_000,  -7_309_000_000],
}

_ALL_VALUES = {
    "financials": _INCOME_VALUES,
    "balance-sheet": _BALANCE_VALUES,
    "cash-flow": _CASHFLOW_VALUES,
}


def make_response(ticker, params):
    """
    Returns a MockResponse whose .text is a JSON string.
    The timeseries URL encodes many metric types in a single request; we parse
    the 'type' query-string portion from the URL stored in params.
    """
    params = params or {}

    # The router passes the full URL as the first positional arg; types are
    # embedded in the URL as '&type=annualTotalRevenue,...'.  We detect the
    # timescale from whichever prefix is used.
    url_types = params.get("_url_types", "")  # injected by router

    # Determine timescale prefix from the requested types
    if url_types.startswith("annual"):
        timescale_prefix = "annual"
        dates = _ANNUAL_DATES
    elif url_types.startswith("quarterly"):
        timescale_prefix = "quarterly"
        dates = _QUARTERLY_DATES
    elif url_types.startswith("trailing"):
        timescale_prefix = "trailing"
        dates = _TRAILING_DATES
    else:
        timescale_prefix = "annual"
        dates = _ANNUAL_DATES

    # Build results for all known keys across all statement types
    results = []
    for statement_values in _ALL_VALUES.values():
        for key, vals in statement_values.items():
            full_key = timescale_prefix + key
            if url_types and full_key not in url_types:
                continue
            result_entry = {
                "meta": {},
                "timestamp": [_date_to_ts(d) for d in dates],
                full_key: _make_items(timescale_prefix, dates, key, vals[:len(dates)]),
            }
            results.append(result_entry)

    if not results:
        # Fallback: return a minimal set of common metrics
        for key, vals in _INCOME_VALUES.items():
            full_key = timescale_prefix + key
            results.append({
                "meta": {},
                "timestamp": [_date_to_ts(d) for d in dates],
                full_key: _make_items(timescale_prefix, dates, key, vals[:len(dates)]),
            })

    data = {"timeseries": {"result": results, "error": None}}
    return MockResponse(data, text=json.dumps(data))


def make_shares_response(ticker):
    """For get_shares_full() — timeseries call with no 'type' param."""
    timestamps = [_date_to_ts(d) for d in _ANNUAL_DATES]
    shares_out = [15_550_000_000, 15_943_425_000, 16_426_786_000, 17_102_786_000]
    results = [{"timestamp": timestamps, "shares_out": shares_out}]
    data = {"timeseries": {"result": results, "error": None}}
    return MockResponse(data, text=json.dumps(data))


def make_complementary_response(ticker, params):
    """For the trailingPegRatio and similar single-value timeseries lookups."""
    if ticker in _NO_PEG_RATIO_TICKERS:
        # Return a result with no trailingPegRatio key -> code sets value to None
        data = {"timeseries": {"result": [{"timestamp": []}], "error": None}}
        return MockResponse(data, text=json.dumps(data))

    dates = _TRAILING_DATES
    results = [
        {
            "meta": {},
            "timestamp": [_date_to_ts(d) for d in dates],
            "trailingPegRatio": _make_items("trailing", dates, "PegRatio", [2.93]),
        }
    ]
    data = {"timeseries": {"result": results, "error": None}}
    return MockResponse(data, text=json.dumps(data))

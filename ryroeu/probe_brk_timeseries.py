"""
Probe Yahoo timeseries endpoint for BRK-B with candidate insurance-specific
balance-sheet keys to find the exact names Yahoo uses.

Run with:  python -m ryroeu.probe_brk_timeseries
"""

import datetime
import json

import pandas as pd
import yfinance as yf

SYMBOL = "BRK-B"
TIMESCALE = "quarterly"  # prefix Yahoo expects

# Candidate keys guessed from the reported pretty labels plus plausible
# variants.  Any key that Yahoo has no data for will be absent from the
# response, so false-positive guesses are harmless.
CANDIDATE_KEYS = [
    # Primary guesses
    "FixedMaturityInvestments",
    "EquityInvestments",
    "NetLoan",
    "DeferredAssets",
    "OtherAssets",
    # Alternative spellings
    "FixedMaturitiesInvestments",
    "FixedMaturity",
    "EquitySecuritiesInvestments",
    "LoansReceivableNet",
    "NetLoansReceivable",
    "TotalDeferredAssets",
    "DeferredPolicyAcquisitionCost",
    "PolicyLoansFromInsuranceCompanies",
    "TotalInvestments",
    "InvestmentIncome",
    "SeparateAccountAssets",
    "ReinsuranceRecoverables",
    "OtherAssetsCurrent",
    "OtherAssetsNonCurrent",
    # Insurance-specific lines sometimes reported separately
    "ShortTermInvestments",
    "PremiumsReceivable",
    "FuturePolicyBenefits",
    "UnearnedPremiums",
]


def build_url(timescale: str, keys: list[str]) -> str:
    base = (
        f"https://query2.finance.yahoo.com/ws/fundamentals-timeseries/"
        f"v1/finance/timeseries/{SYMBOL}?symbol={SYMBOL}"
    )
    type_param = ",".join(timescale + k for k in keys)
    start = datetime.datetime(2016, 12, 31)
    end = pd.Timestamp.now("UTC").ceil("D")
    return (
        f"{base}&type={type_param}"
        f"&period1={int(start.timestamp())}"
        f"&period2={int(end.timestamp())}"
    )


def main():
    # Warm up a Ticker so the singleton YfData is initialised with cookies/crumbs.
    ticker = yf.Ticker(SYMBOL)
    data = ticker._data  # YfData singleton

    url = build_url(TIMESCALE, CANDIDATE_KEYS)
    print(f"Querying Yahoo timeseries for {SYMBOL} ({TIMESCALE})…")
    print(f"Candidate keys: {CANDIDATE_KEYS}\n")

    resp = data.cache_get(url=url)
    raw = json.loads(resp.text)

    results = raw.get("timeseries", {}).get("result", [])
    if not results:
        print("No results returned. Response:")
        print(json.dumps(raw, indent=2)[:2000])
        return

    # Strip the timescale prefix so we see the bare key name.
    prefix = TIMESCALE
    found: dict[str, list[str]] = {}
    for item in results:
        for key in item:
            if key in ("timestamp", "meta"):
                continue
            bare = key.removeprefix(prefix)
            dates = [pt["asOfDate"] for pt in item[key] if pt]
            found[bare] = dates

    if not found:
        print("Yahoo returned results but no keyed data was present.")
        return

    print(f"Keys returned by Yahoo ({len(found)} matched):")
    for bare, dates in sorted(found.items()):
        print(f"  {bare:45s}  {len(dates)} periods  latest={max(dates) if dates else 'n/a'}")

    not_found = sorted(set(CANDIDATE_KEYS) - set(found))
    print(f"\nKeys with NO data returned ({len(not_found)}):")
    for k in not_found:
        print(f"  {k}")


if __name__ == "__main__":
    main()

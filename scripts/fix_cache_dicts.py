"""Convert cache classes to dict access in scrapers."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# analysis.py
analysis = (ROOT / "yfinance/scrapers/analysis.py").read_text(encoding="utf-8")
analysis = analysis.split("class _AnalysisCache:")[0] + analysis.split("class Analysis:")[1]
analysis = "class Analysis:" + analysis
init_old = """        self._data = data
        self._symbol = symbol
        self._cache = _AnalysisCache()"""
init_new = """        self._data = data
        self._symbol = symbol
        self._cache = {k: None for k in (
            'earnings_trend', 'analyst_price_targets', 'earnings_estimate',
            'revenue_estimate', 'earnings_history', 'eps_trend', 'eps_revisions',
            'growth_estimates',
        )}"""
analysis = analysis.replace(init_old, init_new)
for key in [
    'earnings_trend', 'analyst_price_targets', 'earnings_estimate',
    'revenue_estimate', 'earnings_history', 'eps_trend', 'eps_revisions',
    'growth_estimates',
]:
    analysis = analysis.replace(f"self._cache.{key}", f"self._cache['{key}']")
(ROOT / "yfinance/scrapers/analysis.py").write_text(analysis, encoding="utf-8")

# funds.py
funds = (ROOT / "yfinance/scrapers/funds.py").read_text(encoding="utf-8")
funds = funds.split("class _FundsDataCache:")[0] + funds.split("class FundsData:")[1]
funds = "class FundsData:" + funds
funds = funds.replace(
    "        self._cache = _FundsDataCache()",
    "        self._cache = {k: None for k in (\n"
    "            'quote_type', 'description', 'fund_overview', 'fund_operations',\n"
    "            'asset_classes', 'top_holdings', 'equity_holdings', 'bond_holdings',\n"
    "            'bond_ratings', 'sector_weightings',\n"
    "        )}",
)
for key in [
    'quote_type', 'description', 'fund_overview', 'fund_operations',
    'asset_classes', 'top_holdings', 'equity_holdings', 'bond_holdings',
    'bond_ratings', 'sector_weightings',
]:
    funds = funds.replace(f"self._cache.{key}", f"self._cache['{key}']")
(ROOT / "yfinance/scrapers/funds.py").write_text(funds, encoding="utf-8")

print("analysis and funds updated")

"""Funds data example."""

import yfinance.client as yf


def main():
    """Fetch fund-specific metadata for an ETF."""
    spy = yf.Ticker('SPY')
    data = spy.funds_data
    if data is None:
        raise RuntimeError("No funds data available for this ticker")
    return {
        "description": data.description,
        "fund_overview": data.fund_overview,
        "fund_operations": data.fund_operations,
        "asset_classes": data.asset_classes,
        "top_holdings": data.top_holdings,
        "equity_holdings": data.equity_holdings,
        "bond_holdings": data.bond_holdings,
        "bond_ratings": data.bond_ratings,
        "sector_weightings": data.sector_weightings,
    }

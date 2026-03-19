"""Lookup example."""

import yfinance as yf


def main():
    """Fetch each lookup category for a query."""
    lookup = yf.Lookup("AAPL")
    return {
        "all_results": lookup.get_all(count=100),
        "stocks": lookup.get_stock(count=100),
        "mutual_funds": lookup.get_mutualfund(count=100),
        "etfs": lookup.get_etf(count=100),
        "indices": lookup.get_index(count=100),
        "futures": lookup.get_future(count=100),
        "currencies": lookup.get_currency(count=100),
        "cryptocurrencies": lookup.get_cryptocurrency(count=100),
    }

"""Proxy configuration example."""

import yfinance as yf


def main():
    """Configure a proxy and fetch several ticker resources."""
    yf.config.network.proxy = "PROXY_SERVER"
    msft = yf.Ticker("MSFT")
    return {
        "history": msft.history(period="1mo"),
        "actions": msft.get_actions(),
        "dividends": msft.get_dividends(),
        "splits": msft.get_splits(),
        "capital_gains": msft.get_capital_gains(),
        "balance_sheet": msft.get_balance_sheet(),
        "cashflow": msft.get_cashflow(),
        "option_chain": msft.option_chain(msft.options[0]),
    }

"""Multiple ticker example."""

import yfinance.client as yf


def main():
    """Fetch data for several tickers at once."""
    tickers = yf.Tickers('msft aapl goog')
    tickers.live()
    return {
        "msft_info": tickers.tickers['MSFT'].info,
        "aapl_history": tickers.tickers['AAPL'].history(period="1mo"),
        "goog_actions": tickers.tickers['GOOG'].actions,
        "tickers": tickers,
    }

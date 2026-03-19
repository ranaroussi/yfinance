"""Download example."""

import yfinance.client as yf

data = yf.download("SPY AAPL", period="1mo")

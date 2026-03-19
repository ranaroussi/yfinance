"""Market endpoint example."""

import yfinance.client as yf

EUROPE = yf.Market("EUROPE")

status = EUROPE.status
summary = EUROPE.summary

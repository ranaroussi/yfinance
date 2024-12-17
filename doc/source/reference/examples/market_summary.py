import yfinance as yf

EUROPE = yf.Summary("EUROPE")
PARIS = EUROPE["^N100"]


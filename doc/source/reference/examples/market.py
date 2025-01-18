import yfinance as yf

EUROPE = yf.Market("EUROPE")

status = EUROPE.status
summary = EUROPE.summary

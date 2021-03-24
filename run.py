import yfinance as yf

msft = yf.Ticker("TSM")
msft.info
hist = msft.history(period="6mo")
msft.dividends
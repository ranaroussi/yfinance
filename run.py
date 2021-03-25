import yfinance as yf

msft = yf.Ticker("AAPL")
msft.info
hist = msft.history(period="6mo")
msft.dividends
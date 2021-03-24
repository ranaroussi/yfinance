import yfinance as yf

msft = yf.Ticker("UH7.F")
msft.info
hist = msft.history(period="6mo")
msft.dividends
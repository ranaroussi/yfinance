import yfinance as yf

tickers = yf.Tickers('msft aapl goog')

# access each ticker using (example)
tickers.tickers['MSFT'].info
tickers.tickers['AAPL'].history(period="1mo")
tickers.tickers['GOOG'].actions

# fetch an attribute for all tickers at once, in parallel
tickers.info      # dict: symbol -> info dict
tickers.calendar  # DataFrame: one row per ticker

# fewer threads are gentler on Yahoo's rate limit
tickers.get_info(threads=2, progress=True)

# websocket
tickers.live()

import yfinance as yf

tickers = yf.Tickers('msft aapl goog')

# access each ticker using (example)
tickers.tickers['MSFT'].info
tickers.tickers['AAPL'].history(period="1mo")
tickers.tickers['GOOG'].actions

# fetch info for all symbols at once
all_info = tickers.info
print(all_info['MSFT'].get('symbol'))

# websocket
tickers.live()

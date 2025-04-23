import yfinance as yf

# Get All
all = yf.Lookup("AAPL").all
all = yf.Lookup("AAPL").get_all(count=100)

# Get Stocks
stock = yf.Lookup("AAPL").stock
stock = yf.Lookup("AAPL").get_stock(count=100)

# Get Mutual Funds
mutualfund = yf.Lookup("AAPL").mutualfund
mutualfund = yf.Lookup("AAPL").get_mutualfund(count=100)

# Get ETFs
etf = yf.Lookup("AAPL").etf
etf = yf.Lookup("AAPL").get_etf(count=100)

# Get Indices
index = yf.Lookup("AAPL").index
index = yf.Lookup("AAPL").get_index(count=100)

# Get Futures
future = yf.Lookup("AAPL").future
future = yf.Lookup("AAPL").get_future(count=100)

# Get Currencies
currency = yf.Lookup("AAPL").currency
currency = yf.Lookup("AAPL").get_currency(count=100)

# Get Cryptocurrencies
cryptocurrency = yf.Lookup("AAPL").cryptocurrency
cryptocurrency = yf.Lookup("AAPL").get_cryptocurrency(count=100)

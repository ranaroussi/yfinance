import yfinance as yf

# Get the market status of america
US = yf.Status("US")

# Get the status

US.status

# Get when the market opens

US.open

# Get when the market closes

US.close

# Get the whether the market is open

US.is_open
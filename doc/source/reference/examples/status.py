import yfinance as yf

# Get the market status of america
US = yf.Status("US")

# Get the open time
US.open

# Get the close time
US.close

# Get the timezone
US.timezone

# Get the request time
US.request_time

# Get the market status
US.is_open

# Get the rest of the data
US.data
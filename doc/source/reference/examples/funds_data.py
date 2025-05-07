import yfinance as yf

spy = yf.Ticker("SPY")
data = spy.funds_data

# show fund description
data.description

# show operational information
data.fund_overview
data.fund_operations

# show holdings related information
data.asset_classes
data.top_holdings
data.equity_holdings
data.bond_holdings
data.bond_ratings
data.sector_weightings

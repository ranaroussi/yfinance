import yfinance as yf

msft = yf.Ticker("MSFT")

# Get all stock info and print it
print("Stock Info:")
print(msft.info)

# Get historical market data and print it
print("\nHistorical Market Data:")
hist = msft.history(period="1mo")
print(hist)

# Show actions (dividends, splits, capital gains)
print("\nActions:")
print(msft.actions)
print("\nDividends:")
print(msft.dividends)
print("\nSplits:")
print(msft.splits)
print("\nCapital Gains:")
print(msft.capital_gains)  # only for mutual funds & etfs

# Show share count
print("\nShare Count:")
print(msft.get_shares_full(start="2022-01-01", end=None))

# Show financials
print("\nIncome Statement:")
print(msft.income_stmt)
print("\nQuarterly Income Statement:")
print(msft.quarterly_income_stmt)
# - Balance Sheet
print("\nBalance Sheet:")
print(msft.balance_sheet)
print("\nQuarterly Balance Sheet:")
print(msft.quarterly_balance_sheet)
# - Cash Flow Statement
print("\nCash Flow Statement:")
print(msft.cashflow)
print("\nQuarterly Cash Flow Statement:")
print(msft.quarterly_cashflow)

# Show holders
print("\nMajor Holders:")
print(msft.major_holders)
print("\nInstitutional Holders:")
print(msft.institutional_holders)
print("\nMutual Fund Holders:")
print(msft.mutualfund_holders)
print("\nInsider Transactions:")
print(msft.insider_transactions)
print("\nInsider Purchases:")
print(msft.insider_purchases)
print("\nInsider Roster Holders:")
print(msft.insider_roster_holders)

# Show recommendations
print("\nRecommendations:")
print(msft.recommendations)
print("\nRecommendations Summary:")
print(msft.recommendations_summary)
print("\nUpgrades Downgrades:")
print(msft.upgrades_downgrades)

# Show future and historic earnings dates
print("\nEarnings Dates:")
print(msft.earnings_dates)

# Show ISIN code
print("\nISIN Code:")
print(msft.isin)

# Show options expirations
print("\nOptions Expirations:")
print(msft.options)

# Show news
print("\nNews:")
print(msft.news)

# Get option chain for specific expiration and print it
print("\nOption Chain for 2024-03-01:")
opt = msft.option_chain('2024-03-01')
print("Calls:")
print(opt.calls)
print("\nPuts:")
print(opt.puts)

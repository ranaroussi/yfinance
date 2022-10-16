import yfinance as yf
import os
print(os.path.abspath(yf.__file__))

msft = yf.Ticker("MSFT")

print(msft.isin)
print(msft.major_holders)
print(msft.institutional_holders)
print(msft.mutualfund_holders)
print(msft.dividends)
print(msft.splits)
print(msft.actions)
print(msft.shares)
print(msft.info)
print(msft.calendar)
print(msft.recommendations)
print(msft.earnings)
print(msft.quarterly_earnings)
print(msft.income_stmt)
print(msft.quarterly_income_stmt)
print(msft.balance_sheet)
print(msft.quarterly_balance_sheet)
print(msft.cashflow)
print(msft.quarterly_cashflow)
print(msft.current_recommendations)
print(msft.analyst_price_target)
print(msft.revenue_forecasts)
print(msft.sustainability)
print(msft.options)
print(msft.news)
print(msft.analysis)
print(msft.earnings_history)
print(msft.earnings_dates)
print(msft.earnings_forecasts)


# # get stock info
# print(msft.info)

# # get historical market data
# hist = msft.history(period="max")

# # show actions (dividends, splits)
# print(msft.actions)

# # show dividends
# print(msft.dividends)

# # show splits
# print(msft.splits)

# # show financials
# print(msft.financials)
# print(msft.quarterly_financials)

# # show major holders
# print(msft.major_holders)

# # show institutional holders
# print(msft.institutional_holders)

# # show balance sheet
# print(msft.balance_sheet)
# print(msft.quarterly_balance_sheet)

# # show cashflow
# print(msft.cashflow)
# print(msft.quarterly_cashflow)

# # show earnings
# print(msft.earnings)
# print(msft.quarterly_earnings)

# # show sustainability
# print(msft.sustainability)

# # show analysts recommendations
# print(msft.recommendations)

# # show next event (earnings, etc)
# print(msft.calendar)

# # show all earnings dates
# print(msft.earnings_dates)

# # show ISIN code - *experimental*
# # ISIN = International Securities Identification Number
# print(msft.isin)

# # show options expirations
# print(msft.options)

# # show news
# print(msft.news)

# # get option chain for specific expiration
# # opt = msft.option_chain('YYYY-MM-DD')
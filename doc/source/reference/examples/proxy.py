import yfinance as yf

yf.config.network.proxy = "PROXY_SERVER"
msft = yf.Ticker("MSFT")

msft.history(...)
msft.get_actions()
msft.get_dividends()
msft.get_splits()
msft.get_capital_gains()
msft.get_balance_sheet()
msft.get_cashflow()
msft.option_chain(...)
...

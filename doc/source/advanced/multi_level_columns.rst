************************
Multi-Level Column Index
************************

The following answer on Stack Overflow is for `How to deal with
multi-level column names downloaded with yfinance? <https://stackoverflow.com/questions/63107801>`_

- `yfinance` returns a `pandas.DataFrame` with multi-level column names, with a level for the ticker and a level for the stock price data

The answer discusses:

- How to correctly read the the multi-level columns after saving the dataframe to a csv with `pandas.DataFrame.to_csv`
- How to download single or multiple tickers into a singledataframe with single level column names and a ticker column
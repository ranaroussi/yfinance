Test-Case-Report:

Test Stage: Unit

Tester Name: Shiyu Xiu

Date(s) of Test:2021/03/24

Description: test the function of parse_actions inutils.py

Prerequisites for this test: None

Operating System: MacOs Big Sur 11.2.3

Required Configuration: python 3.7 or up, numpy, pandas

Results: pass

Input1:
Ticker ‘TSM’ Data that contains two different dividends and no splits.

input is data1={'meta': {'currency': 'USD', 'symbol': 'TSM', 'exchangeName': 'NYQ', 'instrumentType': 'EQUITY', 'firstTradeDate': 876403800, 'regularMarketTime': 1616529601, 'gmtoffset': -14400, 'timezone': 'EDT', 'exchangeTimezoneName': 'America/New_York', 'regularMarketPrice': 114.89, 'chartPreviousClose': 77.92, 'priceHint': 2, 'currentTradingPeriod': {'pre': {'timezone': 'EDT', 'start': 1616572800, 'end': 1616592600, 'gmtoffset': -14400}, 'regular': {'timezone': 'EDT', 'start': 1616592600, 'end': 1616616000, 'gmtoffset': -14400}, 'post': {'timezone': 'EDT', 'start': 1616616000, 'end': 1616630400, 'gmtoffset': -14400}}, 'dataGranularity': '1d', 'range': '6mo', 'validRanges': ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']}, 'events': {'dividends': {'1615987800': {'amount': 0.448, 'date': 1615987800}, '1608215400': {'amount': 0.442, 'date': 1608215400}}}}

Expected output2:
Returned Dataframe object with correct dates as index and corresponding dividends amount as columns and empty splits Dataframe.
screen print the expected dataframe:
( Dividends
date  
2020-12-17 14:30:00 0.442
2021-03-17 13:30:00 0.448, Empty DataFrame
Columns: [Stock Splits]
Index: [])

Actual output2:
the test passed and the datafram object matches with the expected output dataframe.
screen print the output dataframe:
( Dividends
date  
2020-12-17 14:30:00 0.442
2021-03-17 13:30:00 0.448, Empty DataFrame
Columns: [Stock Splits]
Index: [])

Input2:
Ticker ‘TSM’ Data that contains no different dividends and no splits events

data2={'meta': {'currency': 'USD', 'symbol': 'TSM', 'exchangeName': 'NYQ', 'instrumentType': 'EQUITY', 'firstTradeDate': 876403800, 'regularMarketTime': 1616529601, 'gmtoffset': -14400, 'timezone': 'EDT', 'exchangeTimezoneName': 'America/New_York', 'regularMarketPrice': 114.89, 'chartPreviousClose': 77.92, 'priceHint': 2, 'currentTradingPeriod': {'pre': {'timezone': 'EDT', 'start': 1616572800, 'end': 1616592600, 'gmtoffset': -14400}, 'regular': {'timezone': 'EDT', 'start': 1616592600, 'end': 1616616000, 'gmtoffset': -14400}, 'post': {'timezone': 'EDT', 'start': 1616616000, 'end': 1616630400, 'gmtoffset': -14400}}, 'dataGranularity': '1d', 'range': '6mo', 'validRanges': ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']}}

Expected output2:
Return two empty Dataframe

Actual output2:
The test passed and the two dataframe returned are empty as expected.
screen print the output dataframe:
(Empty DataFrame
Columns: [Dividends]
Index: [], Empty DataFrame
Columns: [Stock Splits]
Index: [])

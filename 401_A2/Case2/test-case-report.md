Test-Case-Report:

Test Stage: Unit

Tester Name: Ruochen Lin

Date(s) of Test:2021/03/24

test case number:2

Description: test the function of parsing the date and time correctly in the parse_actions in utils.py

Set Up procedure: go to the util_parse_action_test.py file and execute that file

Stop Procedure: program terminated automatically.

Prerequisites for this test: None

Operating System: MacOs Big Sur 11.0.1

Software: python 3.7 or up, numpy, pandas, dividends

Hardware: None

Procedural Requirements: None

Results: pass

Test Items and Features: test the parsing function for date under dividends argument in the parse_actions function.

Procedure steps:
Create mock data objects as a parameter to pass in the parse_actions
then Get the expected correct dataframe objects for later comparison
then Use python unittest library and pass the mock data in the utils.parse_actions
final thing is to compare the result.

Input1:
Ticker ‘APPL’ Data that contains date and time info.

input is data4={'meta': {'currency': 'USD', 'symbol': 'AAPL', 'exchangeName': 'NMS', 'instrumentType': 'EQUITY', 'firstTradeDate': 345479400, 'regularMarketTime': 1616616002, 'gmtoffset': -14400, 'timezone': 'EDT', 'exchangeTimezoneName': 'America/New_York', 'regularMarketPrice': 120.09, 'chartPreviousClose': 108.22, 'priceHint': 2, 'currentTradingPeriod': {'pre': {'timezone': 'EDT', 'start': 1616659200, 'end': 1616679000, 'gmtoffset': -14400}, 'regular': {'timezone': 'EDT', 'start': 1616679000, 'end': 1616702400, 'gmtoffset': -14400}, 'post': {'timezone': 'EDT', 'start': 1616702400, 'end': 1616716800, 'gmtoffset': -14400}}, 'dataGranularity': '1d', 'range': '6mo', 'validRanges': ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']}, 'events': {'dividends': {'1604673000': {'amount': 0.205, 'date': 1604673000}, '1612535400': {'amount': 0.205, 'date': 1612535400}}}}

Expected output:
expected a Dataframe object with correct dates as index and corresponding dividends amount as columns and splits empty Dataframe.
screen print the expected dataframe:
                     Dividends
date                          
2020-11-06 14:30:00      0.205
2021-02-05 14:30:00      0.205 Empty DataFrame
Columns: [Stock Splits]
Index: []

Actual output2:
the test passed and the datafram object matches with the expected output dataframe.
screen print the output dataframe:
                     Dividends
date                          
2020-11-06 14:30:00      0.205
2021-02-05 14:30:00      0.205 Empty DataFrame
Columns: [Stock Splits]
Index: []


Input2:
Empty input with ''

data5=''

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

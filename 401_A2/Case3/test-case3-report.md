Test-Case-Report:

Test Stage: Unit

Tester Name: Luke Kim

Date(s) of Test:2021/03/24

test case number:3

Description: test the function of parsing the splits correctly in the parse_actions in utils.py

Set Up procedure: go to the util_parse_action_test.py file and execute that file

Stop Procedure: program terminated automatically.

Prerequisites for this test: None

Operating System: MacOs Big Sur 11.2.3

Software: python 3.7 or up, numpy, pandas

Hardware: None

Procedural Requirements: None

Results: pass

Test Items and Features: test the parsing function for splits in the parse_actions function.

Procedure steps:
Create mock data objects as a parameter to pass in the parse_actions
then Get the expected correct dataframe objects for later comparison
then Use python unittest library and pass the mock data in the utils.parse_actions
final thing is to compare the result.

Input1:
Ticker ‘UH7.F’ Data that contains one stock split and no dividends option.

input is data3={'meta': {'currency': 'EUR', 'symbol': 'UH7.F', 'exchangeName': 'FRA', 'instrumentType': 'EQUITY', 'firstTradeDate': 1140678000, 'regularMarketTime': 1584084000, 'gmtoffset': 3600, 'timezone': 'CET', 'exchangeTimezoneName': 'Europe/Berlin', 'regularMarketPrice': 0.0005, 'chartPreviousClose': 0.0005, 'priceHint': 4, 'currentTradingPeriod': {'pre': {'timezone': 'CET', 'end': 1616569200, 'start': 1616569200, 'gmtoffset': 3600}, 'regular': {'timezone': 'CET', 'end': 1616619600, 'start': 1616569200, 'gmtoffset': 3600}, 'post': {'timezone': 'CET', 'end': 1616619600, 'start': 1616619600, 'gmtoffset': 3600}}, 'dataGranularity': '1d', 'range': '6mo', 'validRanges': ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']}, 'events': {'splits': {'1616482800': {'date': 1616482800, 'numerator': 1, 'denominator': 50, 'splitRatio': '1:50'}}}}

Expected output:
expected a Dataseries object with correct dates as index and corresponding split ratio
screen print the expected dataseries:
(
.date
2021-03-23 07:00:00    0.02
Name: Stock Splits, dtype: float64
)

Actual output2:
the test passed and the dataseries object matches with the expected output dataseries.
screen print the output dataseries:
(
.date
2021-03-23 07:00:00    0.02
Name: Stock Splits, dtype: float64
)



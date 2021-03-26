# Test Case: test_if_dataframe

### Purpose:

Test the case of the returned value is in the DataFrame format.

### Tester name:

Anjesh Shrestha

### Date of Test:

March 26, 2021

### Operating System:

Windows 10

### Required Configuration:

No special setup

### Test Script/Results

| Test | Input                                                          | Expected Result                                                                     | Actual Result               | Pass/Fail |
| ---- | -------------------------------------------------------------- | ----------------------------------------------------------------------------------- | --------------------------- | --------- |
| 1    | data = 'https://finance.yahoo.com/quote/MSFT', symbol = 'MSFT' | type(TickerBase.analyst_recommendations(self, data)) == pandas.core.frame.DataFrame | pandas.core.frame.DataFrame | Pass      |

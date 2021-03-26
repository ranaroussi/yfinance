# Test Case: test_happyPath_shouldReturnCorrectData

### Purpose:

Test the case where correctly formatted data is passed into `analyst_recommendations()`. When the var `output` is not None, the recommendation's index name should be updated to "Date".

### Tester name:

Michelle Wang

### Date of Test:

March 25, 2021

### Operating System:

Windows 10, MacOS

### Required Configuration:

No special setup

### Test Script/Results

| Test | Input                                                              | Expected Result                                              | Actual Result | Pass/Fail |
| ---- | ------------------------------------------------------------------ | ------------------------------------------------------------ | ------------- | --------- |
| 1    | data = 'https://finance.yahoo.com/quote/MSFT', symbol = 'MSFT'     | tickerbase.analyst_recommendations(data).index.name = 'Date' | 'Date'        | Pass      |
| 2    | data = 'https://finance.yahoo.com/quote/MSFT', symbol = not 'MSFT' | None                                                         | None          | Pass      |

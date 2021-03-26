# Test Case: test_incorrectInputData_shouldReturnNone

### Purpose:

Test the case where incorrect data is passed into `analyst_recommendations()`, `None` Should be returned.

### Tester name:

Michelle Wang

### Date of Test:

March 25, 2021

### Operating System:

Windows 10, MacOS

### Required Configuration:

No special setup

### Test Script/Results

| Test | Input                      | Expected Result | Actual Result | Pass/Fail |
| ---- | -------------------------- | --------------- | ------------- | --------- |
| 1    | data = True                | None            | None          | Pass      |
| 2    | data = False               | None            | None          | Pass      |
| 3    | data = None                | None            | None          | Pass      |
| 4    | data = 'Wrong data format' | None            | None          | Pass      |
| 5    | data = [1, 2, 3]           | None            | None          | Pass      |
| 6    | data = 1                   | None            | None          | Pass      |

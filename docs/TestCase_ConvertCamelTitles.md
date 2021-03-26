# Test Case: test_camel2title_should_correctly_camel_titles

### Purpose:

Test if the titles of the output of analyst_recommendations(data) is correctly converted from camel case to title case

### Tester name:

Grace Fu

### Date of Test:

March 26, 2021

### Operating System:

Windows 10, MacOS

### Required Configuration:

No special setup

### Test Script/Results

| Test | Input                                            | Expected Result                                                   | Actual Result                                                     | Pass/Fail |
| ---- | ------------------------------------------------ | ----------------------------------------------------------------- | ----------------------------------------------------------------- | --------- |
| 1    | analyst_recommendations(data).columns.to_numpy() | array(['Firm', 'To Grade', 'From Grade', 'Action'], dtype=object) | array(['Firm', 'To Grade', 'From Grade', 'Action'], dtype=object) | Pass      |

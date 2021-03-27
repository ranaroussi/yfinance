# Test Case Report

## General Infromation
Test Stage: ✅ Unit

Test Date: Wed. Mar. 24, 2021

Tester: Chelsea Hong, Cameron Ridderikhoff

Test Case Description:  Testing incomplete, null, and broken data for the .earnings and .quarterly_earnings properties of ticker.py for the yfinance module.


Test Case Number: 3

Results: ✅ Passed

## Introduction

Requirement(s) to be tested: .earnings, .quarterly_earnings

Roles and Responsibilities: Quality assurance team’s responsibility was to ensure quarterly and annual earnings were correctly calculated regardless of the state of the data. 



Set Up Procedures:
Retrieve codebase
*Assume user has github account and git installed onto their machine*
1. `git clone https://github.com/sazzy4o/yfinance.git`
Setup Virtual Environment (recommended)
3. Install venv for Python
4. Create virtual environment
`virtualenv <desired path>`
4. Activate virtual environment (macOS)
`source <desired path/bin/activate>`
Install Necessary Modules
5. `pip3 install unittest`
6. `pip3 install . -editable`



Stop Procedures:
Running tests can be terminated by pressing:
`CTRL+C`



## Environmental Needs
Hardware: N/A (any modern computer)

Software: Python 3.4+, unittest module, yfinance module 



Procedural Requirements:

Ensure the “data” folder contains the correct json files. Must run `pip install . -editable` inside the yfinance folder in order to install yfinance’s required additional modules.



## Test
Test Items and Features:
Features: 
System's ability to retrieve annual and quarterly earnings, system’s ability to handle missing annual and quarterly earnings data

Items: Ticker("GOOG"), goog.earnings,  goog.earnings["Earnings"].size, goog.quarterly_earnings

Input Specifications: None

Procedural Steps:

1. Go to test directory `cd test`
2. run command `python3 test_earnings.py`
The test case is run with the unittest module. This test case is set up under the “TestDataValues” class, and the tests themselves are all run using the python code: `unittest.main()`, which is run when the file “test_earnings.py” is run.


Expected Results of Case:
The output will say that a test failed if a test failed. It will show the following if the test is completed successfully:

`.....`
`---------------------------------------------------`
`Ran 5 tests in 9.537s`
`OK`


## Actual Results
Output Specifications:

The actual results that we got from running our test is the following:

Passed Test will include how many tests were run and how long it took to run. Finally displaying a status of “OK” if all tests were passed. 

Passed Test Example Output:

`.....`
`---------------------------------------------------`
`Ran 5 tests in 9.537s`
`OK`


A failed test will include the traceback of where the test failed and why it failed. Along with how many tests were run and how long it took to run. Finally it will display a “FAILED” status with how many tests failed.

Failed Test Example Output:

`...F.`
`======================================================================`
`FAIL: test_quarterly_earnings_field_missing (__main__.TestDataValues)`
`----------------------------------------------------------------------`
`Traceback (most recent call last):`
`File "/Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/3.8/lib/python3.8/unittest/mock.py", line 1348, in patched`
`    return func(*newargs, **newkeywargs)`
`  File "test/test_earnings.py", line 96, in test_quarterly_earnings_field_missing`
`    self.assertTrue(isnan(earnings.iloc[1]))`
`AssertionError: False is not true`
`----------------------------------------------------------------------`
`Ran 5 tests in 9.812s`

`FAILED (failures=1)`
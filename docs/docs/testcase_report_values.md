# Test Case Report

## General Infromation
Test Stage: Unit

Test Date: Wed. Mar. 27, 2021

Tester: Spencer von der Ohe

Test Case Description: 
Test that the `earnings` and `quarterly_earnings` properties are able to generate and handle positive, negative and zero values

Test Case Number: 1

Results: ✅ Passed

## Introduction

Requirement(s) to be tested:
The `earnings` and `quarterly_earnings` properties should support positive, negative and zero values

Roles and Responsibilities: Quality assurance team’s responsibility was to ensure quarterly and annual earnings were correctly calculated regardless of the state of the data. 

Set Up Procedures:
Retrieve codebase
*Assume user has github account and git installed onto their machine*
1. `git clone https://github.com/sazzy4o/yfinance.git`
2. `cd yfinance`
Setup Virtual Environment (recommended)
3. Install venv for Python
4. Create virtual environment
`virtualenv <desired path>`
5. Activate virtual environment (macOS)
`source <desired path/bin/activate>`
Install Necessary Modules
6. `pip3 install unittest`
7. `pip3 install . -editable`

Stop Procedures:
Running tests can be terminated by pressing:
`CTRL+C`


## Environmental Needs
Hardware: N/A (any modern computer)

Software: Python 3.4+, unittest module, yfinance module 

Procedural Requirements: None

## Test
Test Items and Features:
Features: 
System's ability to retrieve annual and quarterly earnings of positive, negative and zero values

Items: Ticker(), Ticker.earnings,  Ticker.quarterly_earnings

Input Specifications: None

Procedural Steps:

1. Go to test directory `cd test`
2. run command `python3 test_earnings_values.py`
The test case is run with the unittest module. This test case is set up under the `TestDataValues` class, and the tests themselves are all run using the python code: `unittest.main()`, which is run when the file `test_earnings_values.py` is run.


Expected Results of Case:
The output will say that a test failed if a test failed. It will show the following if the test is completed successfully:

```
......
----------------------------------------------------------------------
Ran 6 tests in 7.069s

OK
```

## Actual Results
Passed Test Example Output:

```
......
----------------------------------------------------------------------
Ran 6 tests in 7.069s

OK
```


A failed test will include the traceback of where the test failed and why it failed. Along with how many tests were run and how long it took to run. Finally it will display a `FAILED` status with how many tests failed.

Failed Test Example Output:
```
..F...
======================================================================
FAIL: test_positive_earnings (__main__.TestEarnings)
Test positive earnings
----------------------------------------------------------------------
Traceback (most recent call last):
  File "C:\Users\svond\AppData\Local\Programs\Python\Python39\lib\unittest\mock.py", line 1337, in patched
    return func(*newargs, **newkeywargs)
  File "C:\Users\svond\OneDrive\Desktop\Github\yfinance\test\test_earnings_values.py", line 49, in test_positive_earnings
    self.assertEqual(earning_2020,40_269_000_00)
AssertionError: 40269000000 != 4026900000

----------------------------------------------------------------------
Ran 6 tests in 5.525s

FAILED (failures=1)
```
# Unit Test Report
## General Information
**Test Stage**: Unit  
**Test Date**: 2020-03-27 (Y-M-D)  
**Tester**: hgou 
**Test Case Number**: 4   

**Test case description**: Test the quarterly_financials property of a ticker input invaild value, check if it throws an exception.      

**Results**: Pass  
**Incident Number**: [#142](https://github.com/ranaroussi/yfinance/issues/142)  

## Introduction

**Requirements to be tested**: Unknown  
**Roles and responsibilities**:
| Person | role   |
|--------|--------|
| hgou | Tester |

**Set Up procedures**: if desired install pytest  
Stop Procedures: kill it like any other program, if you can do that fast enough to actually stop the test  
**Hardware**: Any
**Software**: Pytest (opt.), see [requirements.txt](/requirements.txt)  
**Procedural Requirements**: none known

## Test
**Test Items and features**: [negative_quarterly_financials_test.py](/tests/negative_quarterly_financial_test.py)#test_quarterly_financials_is_in_correct_format  
**Input Specifications**: No input  
**Procedural Steps**: if using pytest, call `pytest` from the root folder or inside the tests folder. Otherwise call `python3 tests/quarterly_financial_test.py`  
**Expected Results of Case**: pytest should exit with a code of 0 and display that all tests passed.

## Actual Results
**Output Specifications**: As the test passed, it return empty

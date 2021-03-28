# Unit Test Report
## General Information
**Test Stage**: Unit  
**Test Date**: 2020-03-27 (Y-M-D)  
**Tester**: Mehrshad  
**Test Case Number**: 2 
**Test case description**: Test the get_financials function in the case of bad data, ensuring graceful return. 
**Results**: Pass  
**Incident Number**: [#639](https://github.com/ranaroussi/yfinance/issues/639)  

## Introduction

**Requirements to be tested**: Graceful invalid data handling.   
**Roles and responsibilities**:
| Person    | role   |
|-----------|--------|
| Mehershad | Tester |

**Set Up procedures**: -  
**Stop Procedures**: Will stop once done tests, or run: Ctrl-D
**Hardware**: Any
**Software**: [requirements.txt](/requirements.txt)  
**Procedural Requirements**: -

## Test
**Test Items and features**: [test_finacnials.py](/test_financials.py)#test_quarterly_financials_is_in_correct_format  
**Input Specifications**: No input  
**Procedural Steps**: Run `python3 test_financials.py`  
**Expected Results of Case**: All tests passed and OK should be returned

## Actual Results
**Output Specifications**: "OK" is outputted, all tests passed
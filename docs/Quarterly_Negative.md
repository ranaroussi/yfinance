# Test Case
## General Information
Test Stage: Unit  

Test Date: 2020-03-27 (Y-M-D)  

Tester: hgou 

Test Case Number: 3  

Purpose: Test the quarterly_financials property of a ticker by using invaild value, check if it throws an exception.

Incident Number: [#142](https://github.com/ranaroussi/yfinance/issues/142)  

Prerequisites: None  

Software version: yfinance 0.1.59  

Required setup: None

## Test Steps/Input
 - Create invalid mock input
 - Run the quarterly_financials function on it
    - Expect function return empty

Alternative flows:
 1. function throw exception

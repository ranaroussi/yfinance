# Test Case
## General Information
Test Stage: Unit  
Test Date: 2020-03-27 (Y-M-D)  
Tester: Pierre  
Test Case Number: 3  
Purpose: this will test the quarterly_financials function along a happy path, making sure the data is returned properly. 
Incident Number: [#639](https://github.com/ranaroussi/yfinance/issues/639)  
Prerequisites: None  
Software version: yfinance 0.1.59  
Required setup: None
## Test Steps/Input
 - Create valid fake input
 - Run the quarterly_financials function on it
    - Expect data to be present and is split in a quarterly fashion.

Alternative flows:
 1. Data does not exist
 2. Data is not in a quarterly format
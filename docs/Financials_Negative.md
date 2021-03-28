# Test Case
## General Information
Test Stage: Unit  
Test Date: 2020-03-27 (Y-M-D)  
Tester: Mehrshad  
Test Case Number: 2
Purpose: Test the get_financials function in the case of bad data, ensuring graceful return. 
Incident Number: [#639](https://github.com/ranaroussi/yfinance/issues/639)  
Prerequisites: None  
Software version: yfinance 0.1.59  
Required setup: None
## Test Steps/Input
 - Mock invalid data return
 - Run the get_financials function
    - Expect empty to be returned and no errors thrown

Alternative flows:
 1. Data is valid
 2. Data is not in a yearly format